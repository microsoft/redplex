package redplex

import (
	"net"
	"testing"
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

const redisAddress = "127.0.0.1:6379"

type EndToEndServerSuite struct {
	suite.Suite
	server      *Server
	redplexConn redis.Conn
	directConn  redis.Conn
}

func TestEndToEndServerSuite(t *testing.T) {
	suite.Run(t, new(EndToEndServerSuite))
}

func (e *EndToEndServerSuite) SetupSuite() {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.Nil(e.T(), err)

	e.server = NewServer(listener, NewPubsub(
		func() (net.Conn, error) { return net.Dial("tcp", redisAddress) },
		time.Second*5,
	))
	go e.server.Listen()

	directConn, err := redis.Dial("tcp", redisAddress)
	require.Nil(e.T(), err)
	e.directConn = directConn

	redplexConn, err := redis.Dial("tcp", listener.Addr().String())
	require.Nil(e.T(), err)
	e.redplexConn = redplexConn
}

func (e *EndToEndServerSuite) TearDownSuite() {
	e.server.Close()
	e.redplexConn.Close()
	e.directConn.Close()
}

func (e *EndToEndServerSuite) TestSubscribesAndGetsMessages() {
	psc := redis.PubSubConn{Conn: e.redplexConn}
	require.Nil(e.T(), psc.Subscribe("foo"))
	require.Equal(e.T(), redis.Subscription{Kind: "subscribe", Channel: "foo", Count: 1}, psc.Receive())
	require.Nil(e.T(), psc.PSubscribe("ba*"))
	require.Equal(e.T(), redis.Subscription{Kind: "psubscribe", Channel: "ba*", Count: 1}, psc.Receive())

	e.retryUntilReturns(
		func() {
			_, err := e.directConn.Do("PUBLISH", "foo", "bar")
			require.Nil(e.T(), err)
		},
		func() {
			require.Equal(e.T(), redis.Message{Channel: "foo", Data: []byte("bar")}, psc.Receive())
		},
	)

	e.retryUntilReturns(
		func() {
			_, err := e.directConn.Do("PUBLISH", "bar", "heyo!")
			require.Nil(e.T(), err)
		},
		func() {
			require.Equal(e.T(), redis.PMessage{Pattern: "ba*", Channel: "bar", Data: []byte("heyo!")}, psc.Receive())
		},
	)
}

func (e *EndToEndServerSuite) retryUntilReturns(retried func(), awaitedFn func()) {
	ok := make(chan struct{})
	go func() {
		for {
			retried()
			select {
			case <-time.After(time.Millisecond * 500):
			case <-ok:
				return
			}
		}
	}()

	awaitedFn()
	ok <- struct{}{}
}
