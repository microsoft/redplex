package redplex

import (
	"crypto/tls"
	"net"
	"time"

	"bufio"
	"errors"
	"fmt"

	"github.com/cenkalti/backoff"
	"github.com/garyburd/redigo/redis"
	"github.com/sirupsen/logrus"
)

// The Dialer is a type that can create a TCP connection to the Redis server.
type Dialer interface {
	// Dial attempts to create a connection to the server.
	Dial() (net.Conn, error)
}

// defaultTimeout is used in the DirectDialer
// if a non-zero timeout is not provided.
const defaultTimeout = time.Second * 5

// DirectDialer creates a direct connection to a single Redis server or IP.
type DirectDialer struct {
	network  string
	address  string
	password string
	useTLS   bool
	timeout  time.Duration
}

// NewDirectDialer creates a DirectDialer that dials to the given address.
// If the timeout is 0, a default value of 5 seconds will be used.
func NewDirectDialer(network string, address string, password string, useTLS bool, timeout time.Duration) DirectDialer {
	if timeout == 0 {
		timeout = defaultTimeout
	}

	return DirectDialer{network, address, password, useTLS, timeout}
}

// Dial implements Dialer.Dial
func (d DirectDialer) Dial() (cnx net.Conn, err error) {
	if d.useTLS {
		dialer := new(net.Dialer)
		dialer.Timeout = d.timeout
		cnx, err = tls.DialWithDialer(dialer, d.network, d.address, nil)
	} else {
		cnx, err = net.DialTimeout(d.network, d.address, d.timeout)
	}

	if err != nil {
		return
	}

	if d.password != "" {
		line := "*2\r\n$4\r\nAUTH\n\n$" + string(len(d.password)) + "\r\n" + d.password + "\r\n"
		_, err = cnx.Write([]byte(line))
		if err != nil {
			return
		}

		reader := bufio.NewReader(cnx)

		line, err = reader.ReadString('\n')
		if err != nil {
			return
		}

		if line[0] != '+' {
			err = errors.New(line[1:])
		}
	}
	return
}

// The SentinelDialer dials into the Redis cluster master as defined by
// the assortment of sentinel servers.
type SentinelDialer struct {
	network          string
	sentinels        []string
	masterName       string
	password         string
	sentinelPassword string
	useTLS           bool
	timeout          time.Duration
	closer           chan<- struct{}
}

// NewSentinelDialer creates a SentinelDialer.
func NewSentinelDialer(network string, sentinels []string, masterName string, password string, sentinelPassword string, useTLS bool, timeout time.Duration) *SentinelDialer {
	if timeout == 0 {
		timeout = defaultTimeout
	}

	return &SentinelDialer{
		network:          network,
		sentinels:        sentinels,
		masterName:       masterName,
		password:         password,
		sentinelPassword: sentinelPassword,
		useTLS:           useTLS,
		timeout:          timeout,
		closer:           make(chan struct{}),
	}
}

var _ Dialer = &SentinelDialer{}

// Dial implements Dialer.Dial. It looks up and connects to the Redis master
// dictated by the sentinels. The connection will be closed if we detect
// that the master changes.
func (s *SentinelDialer) Dial() (net.Conn, error) {
	close(s.closer)
	closer := make(chan struct{})
	s.closer = closer

	master, err := s.getMaster()
	if err != nil {
		return nil, err
	}

	cnx, err := DirectDialer{s.network, master, s.password, s.useTLS, s.timeout}.Dial()
	if err != nil {
		return nil, err
	}

	logrus.Debugf("redplex/dialer: created connection to %s as the sentinel master", master)

	go s.monitorMaster(cnx, master, closer)

	return cnx, nil
}

// monitorMaster subscribes to the Sentinel cluster and watches for master
// changes. When a change happens, it closes the old connection. Often a
// failover-triggering event will result in the connection being terminated
// anyway, but this is not always the case--the Sentinels can decide to trigger
// a failover if load is too high on one server, for instance, even though the
// server is generally healthy and the connection will remain alive.
func (s *SentinelDialer) monitorMaster(cnx net.Conn, currentMaster string, closer <-chan struct{}) {
	backoff := backoff.NewExponentialBackOff()
	backoff.MaxInterval = time.Second * 10

	defer func() {
		cnx.Close()
		logrus.Infof("redplex/dialer: failed over from %s as it is no longer the cluster master", currentMaster)
	}()

	for {
		err := s.watchForReelection(backoff, currentMaster, closer)
		if err == nil {
			return
		}

		logrus.WithError(err).Info("redplex/dialer: error connecting to Redis sentinels")
		select {
		case <-time.After(backoff.NextBackOff()):
			continue
		case <-closer:
			return
		}
	}
}

// watchForReelection returns a channel which is closed to when the sentinel
// master changes. Blocks until the subscription is set up. Returns a connection
// which should be closed when we're no longer interested in it. Resets the
// backoff once a connection is established successfully.
func (s *SentinelDialer) watchForReelection(backoff backoff.BackOff, currentMaster string, closer <-chan struct{}) (err error) {
	for {
		// First off, subscribe to master changes.
		psc, err := s.subscribeToElections()
		if err != nil {
			return err
		}

		// Validate the master is current after we set up the pubsub conn
		// to avoid races.
		if master, err := s.getMaster(); err != nil || master != currentMaster {
			psc.Close()
			return err
		}

		recvd := make(chan struct{})
		go func() {
			select {
			case <-closer:
				psc.Close()
			case <-recvd:
			}
		}()

		// Wait until we get a message on the pubsub channel. Once we do and
		// if it's a subscription message, return.
		backoff.Reset()
		_, didReelect := psc.Receive().(redis.Message)
		close(recvd)
		psc.Close()

		if didReelect {
			return nil
		}

		// Otherwise, loop through if we didn't close intentionally.. Occasional
		// crashes and timeouts are expected. If we can't connect to anyone
		// else, the next loop iteration will return an error.
		select {
		case <-closer:
			return nil
		default:
		}
	}
}

// getMaster returns the current cluster master.
func (s *SentinelDialer) getMaster() (string, error) {
	result, err := s.doUntilSuccess(func(conn redis.Conn) (interface{}, error) {
		res, err := redis.Strings(conn.Do("SENTINEL", "get-master-addr-by-name", s.masterName))
		if err != nil {
			return "", err
		}

		conn.Close()
		return net.JoinHostPort(res[0], res[1]), nil
	})

	if err != nil {
		return "", err
	}

	return result.(string), nil
}

// subscribeToElections returns a pubsub channel subscribed on a Redis sentinel
// to cluster master changes.
func (s *SentinelDialer) subscribeToElections() (redis.PubSubConn, error) {
	result, err := s.doUntilSuccess(func(conn redis.Conn) (interface{}, error) {
		psc := redis.PubSubConn{Conn: conn}
		if err := psc.Subscribe("+switch-master"); err != nil {
			return psc, err
		}

		msg := psc.Receive()
		if _, ok := msg.(redis.Subscription); !ok {
			return psc, fmt.Errorf("redplex/dialer: expected subscription ack from sentinel, got: %+v", msg)
		}

		return psc, nil
	})

	if err != nil {
		return redis.PubSubConn{}, err
	}

	return result.(redis.PubSubConn), nil
}

// doUntilSuccess runs the querying function against sentinels until one
// returns with a success. If no sentinels respond successfully, the last
// returned error is bubbled. The connection used in a successful reply will
// remain open.
func (s *SentinelDialer) doUntilSuccess(fn func(conn redis.Conn) (interface{}, error)) (result interface{}, err error) {
	var cnx redis.Conn

	for _, addr := range s.sentinels {
		options := []redis.DialOption{
			redis.DialConnectTimeout(s.timeout),
			redis.DialUseTLS(s.useTLS),
		}

		if s.sentinelPassword != "" {
			options = append(options, redis.DialPassword(s.sentinelPassword))
		}
		cnx, err = redis.Dial(s.network, addr, options...)
		if err != nil {
			continue
		}

		result, err = fn(cnx)
		if err != nil {
			cnx.Close()
			continue
		}

		return result, nil
	}

	return nil, err
}
