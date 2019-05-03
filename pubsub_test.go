package redplex

import (
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type mockWritable struct{ mock.Mock }

func (m *mockWritable) Write(b []byte) { m.Called(b) }

func makeWritableMocks() []*mockWritable {
	var mocks []*mockWritable
	for i := 0; i < 10; i++ {
		mocks = append(mocks, &mockWritable{})
	}

	return mocks
}

func assertMocks(t *testing.T, mocks []*mockWritable) {
	for _, mock := range mocks {
		mock.AssertExpectations(t)
	}
}

func TestListenersMap(t *testing.T) {
	mocks := makeWritableMocks()
	l := listenerMap{}

	t.Run("AddsSubscribers", func(t *testing.T) {
		require.True(t, l.add("foo", mocks[0]))
		require.False(t, l.add("foo", mocks[1]))
		require.True(t, l.add("bar", mocks[2]))
	})

	t.Run("Broadcasts", func(t *testing.T) {
		mocks[0].On("Write", []byte{1, 2, 3}).Return()
		mocks[1].On("Write", []byte{1, 2, 3}).Return()
		l.broadcast([]byte("foo"), []byte{1, 2, 3})
		assertMocks(t, mocks)
	})

	t.Run("Unsubscribes", func(t *testing.T) {
		require.Equal(t, 2, len(l))
		require.True(t, l.remove("bar", mocks[2]))
		require.Equal(t, 1, len(l))
		require.False(t, l.remove("foo", mocks[0]))
		require.True(t, l.remove("foo", mocks[1]))
		require.Equal(t, 0, len(l))
	})

	t.Run("RemoveAll", func(t *testing.T) {
		require.True(t, l.add("foo", mocks[0]))
		require.False(t, l.add("foo", mocks[1]))
		require.True(t, l.add("bar", mocks[0]))

		require.Equal(t,
			[][]byte{[]byte("bar")},
			l.removeAll(mocks[0]),
		)

		require.Equal(t, 1, len(l))
	})
}

type PubsubSuite struct {
	suite.Suite
	server      net.Listener
	connections <-chan net.Conn
	pubsubWg    sync.WaitGroup
	pubsub      *Pubsub
}

func TestPubsubSuite(t *testing.T) {
	suite.Run(t, new(PubsubSuite))
}

func (p *PubsubSuite) SetupSuite() {
	server, err := net.Listen("tcp", "127.0.0.1:0")
	require.Nil(p.T(), err)
	connections := make(chan net.Conn)

	p.connections = connections
	p.server = server

	go func() {
		for {
			cnx, err := server.Accept()
			if err != nil {
				return
			}

			connections <- cnx
		}
	}()
}

func (p *PubsubSuite) SetupTest() {
	p.pubsub = NewPubsub(
		NewDirectDialer("tcp", p.server.Addr().String(), "", false, 0),
		time.Second,
	)

	p.pubsubWg.Add(1)
	go func() { p.pubsub.Start(); p.pubsubWg.Done() }()
}

func (p *PubsubSuite) TearDownTest() {
	p.pubsub.Close()
	p.pubsubWg.Wait()
}

func (p *PubsubSuite) setupSubscription() (cnx net.Conn, mw *mockWritable) {
	cnx = <-p.connections
	mw = &mockWritable{}
	p.pubsub.Subscribe(Listener{IsPattern: false, Channel: "foo", Conn: mw})
	assertReads(p.T(), cnx, "*2\r\n$9\r\nsubscribe\r\n$3\r\nfoo\r\n")
	p.pubsub.Subscribe(Listener{IsPattern: true, Channel: "ba*", Conn: mw})
	assertReads(p.T(), cnx, "*2\r\n$10\r\npsubscribe\r\n$3\r\nba*\r\n")
	return cnx, mw
}

func (p *PubsubSuite) TestMakeSimpleSubscription() {
	cnx, mw := p.setupSubscription()
	p.assertReceivesMessage(cnx, mw, "*3\r\n$7\r\nmessage\r\n$3\r\nfoo\r\n$5\r\nheyo!\r\n")
	p.assertReceivesMessage(cnx, mw, "*4\r\n$8\r\npmessage\r\n$3\r\nba*\r\n$3\r\nbar\r\n$5\r\nheyo!\r\n")
	mw.AssertExpectations(p.T())
}

func (p *PubsubSuite) TestUnsubscribesAll() {
	cnx, mw := p.setupSubscription()
	p.pubsub.UnsubscribeAll(mw)
	assertReads(p.T(), cnx, "*2\r\n$12\r\npunsubscribe\r\n$3\r\nba*\r\n")
	assertReads(p.T(), cnx, "*2\r\n$11\r\nunsubscribe\r\n$3\r\nfoo\r\n")
}

func (p *PubsubSuite) TestUnsubscribesChannel() {
	cnx, mw := p.setupSubscription()
	p.pubsub.Unsubscribe(Listener{IsPattern: false, Channel: "foo", Conn: mw})
	assertReads(p.T(), cnx, "*2\r\n$11\r\nunsubscribe\r\n$3\r\nfoo\r\n")
	p.pubsub.Unsubscribe(Listener{IsPattern: true, Channel: "ba*", Conn: mw})
	assertReads(p.T(), cnx, "*2\r\n$12\r\npunsubscribe\r\n$3\r\nba*\r\n")
}

func (p *PubsubSuite) TestResubscribesOnFailure() {
	cnx, mw := p.setupSubscription()
	cnx.Close()

	newCnx := <-p.connections
	assertReads(p.T(), newCnx, "*2\r\n$9\r\nsubscribe\r\n$3\r\nfoo\r\n")
	assertReads(p.T(), newCnx, "*2\r\n$10\r\npsubscribe\r\n$3\r\nba*\r\n")

	p.assertReceivesMessage(newCnx, mw, "*3\r\n$7\r\nmessage\r\n$3\r\nfoo\r\n$5\r\nheyo!\r\n")
}

func (p *PubsubSuite) TestIgnoresExtraneousMessages() {
	cnx, mw := p.setupSubscription()
	cnx.Write([]byte("+OK\r\n"))
	p.assertReceivesMessage(cnx, mw, "*3\r\n$7\r\nmessage\r\n$3\r\nfoo\r\n$5\r\nheyo!\r\n")
}

func (p *PubsubSuite) assertReceivesMessage(cnx net.Conn, mw *mockWritable, message string) {
	ok := make(chan struct{})
	mw.On("Write", []byte(message)).Run(func(_ mock.Arguments) { ok <- struct{}{} }).Return()
	cnx.Write([]byte(message))
	select {
	case <-ok:
	case <-time.After(time.Second):
		p.Fail("Expected to read sub message, but didn't")
	}
	mw.AssertExpectations(p.T())
}

func assertReads(t *testing.T, cnx net.Conn, message string) {
	cnx.SetReadDeadline(time.Now().Add(time.Second * 5))
	actual := make([]byte, len(message))
	_, err := io.ReadFull(cnx, actual)
	require.Nil(t, err, "error reading expected message %q", message)
	require.Equal(t, message, string(actual))
}

func (p *PubsubSuite) TearDownSuite() {
	p.server.Close()
}
