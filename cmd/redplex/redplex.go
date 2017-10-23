package main

import (
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"

	"github.com/FZambia/go-sentinel"
	"github.com/garyburd/redigo/redis"
	"github.com/mixer/redplex"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"
	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

var (
	address        = kingpin.Flag("listen", "Address to listen on").Short('l').Default("127.0.0.1:3000").String()
	network        = kingpin.Flag("network", "Network to listen on").Short('n').Default("tcp").String()
	remote         = kingpin.Flag("remote", "Remote address of the Redis server").Default("127.0.0.1:6379").String()
	sentinels      = kingpin.Flag("sentinels", "A list of Redis sentinel addresses").Strings()
	sentinelMaster = kingpin.Flag("sentinel-name", "The name of the sentinel master").String()
	logLevel       = kingpin.Flag("log-level", "Log level (one of debug, info, warn, error").Default("info").String()
	dialTimeout    = kingpin.Flag("dial-timeout", "Timeout connecting to Redis").Default("10s").Duration()
	writeTimeout   = kingpin.Flag("write-timeout", "Timeout during write operations").Default("2s").Duration()
	pprofServer    = kingpin.Flag("pprof-server", "Address to bind a pprof server on. Not bound if empty.").String()
)

func main() {
	kingpin.Parse()

	level, err := logrus.ParseLevel(*logLevel)
	if err != nil {
		logrus.WithError(err).Fatal("redplex/main: error parsing log level")
		os.Exit(1)
	}

	listener, err := net.Listen(*network, *address)
	if err != nil {
		logrus.WithError(err).Fatal("redplex/main: could not listen on the requested address")
		os.Exit(1)
	}

	logrus.SetLevel(level)

	var dialer redplex.Dialer
	if *sentinelMaster != "" {
		dialer = dialSentinel
	} else {
		dialer = dialSingle
	}

	closed := make(chan struct{})
	go func() {
		awaitInterrupt()
		close(closed)
		listener.Close()
	}()

	go startPprof()

	logrus.Debugf("redplex/main: listening on %s://%s", *network, *address)
	if err := redplex.NewServer(listener, redplex.NewPubsub(dialer, *writeTimeout)).Listen(); err != nil {
		select {
		case <-closed:
			os.Exit(0)
		default:
			logrus.WithError(err).Fatal("redplex/main: error accepting incoming connections")
		}
	}
}

func startPprof() {
	if *pprofServer == "" {
		return
	}

	http.Handle("/stats", promhttp.Handler())

	if err := http.ListenAndServe(*pprofServer, nil); err != nil {
		logrus.WithError(err).Error("redplex/main: could not start pprof server")
	}
}

func awaitInterrupt() {
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, syscall.SIGINT)
	<-interrupt
}

func dialSingle() (net.Conn, error) { return dialToAddress(*remote) }

func dialToAddress(addr string) (net.Conn, error) {
	return net.DialTimeout("tcp", addr, *dialTimeout)
}

func dialSentinel() (net.Conn, error) {
	snt := &sentinel.Sentinel{
		Addrs:      *sentinels,
		MasterName: *sentinelMaster,
		Dial: func(addr string) (redis.Conn, error) {
			return redis.Dial("tcp", addr, redis.DialConnectTimeout(*dialTimeout))
		},
	}

	master, err := snt.MasterAddr()
	if err != nil {
		return nil, err
	}

	return dialToAddress(master)
}
