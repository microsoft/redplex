package redplex

import (
	"bufio"
	"fmt"
	"net"
	"sync"

	"github.com/sirupsen/logrus"
)

// toSendQueueLimit is the number of pubsub messages we can
// buffer on the outgoing connection.
const toSendQueueLimit = 128

// Dialer is passed into redplex. Calling it should create a new connection to
// the pubsub master.
type Dialer func() (net.Conn, error)

type Server struct {
	l      net.Listener
	pubsub *Pubsub
}

// NewServer creates a new Redplex server. It listens for connections from
// the listener and uses the Dialer to proxy to the remote server.
func NewServer(l net.Listener, pubsub *Pubsub) *Server {
	return &Server{
		l:      l,
		pubsub: pubsub,
	}
}

// Listen accepts and serves incoming connections.
func (s *Server) Listen() error {
	go s.pubsub.Start()

	for {
		cnx, err := s.l.Accept()
		if err != nil {
			return err
		}
		go (&connection{
			cnx:        cnx,
			pubsub:     s.pubsub,
			toSendCond: *sync.NewCond(&sync.Mutex{}),
			toSend:     make([][]byte, 0, toSendQueueLimit),
		}).Start()
	}
}

type connection struct {
	cnx       net.Conn
	pubsub    *Pubsub
	listeners []*Listener

	toSendCond sync.Cond
	isClosed   bool
	toSend     [][]byte
}

// Start reads data from the connection until we're no longer able to.
func (s *connection) Start() {
	reader := bufio.NewReader(s.cnx)
	defer func() {
		s.toSendCond.L.Lock()
		s.isClosed = true
		s.toSendCond.Broadcast()
		s.toSendCond.L.Unlock()
		s.pubsub.UnsubscribeAll(s)
	}()

	logrus.Debug("redplex/server: accepted connection")
	go s.loopWrite()

	for {
		method, args, err := ParseRequest(reader)
		if err != nil {
			logrus.WithError(err).Debug("redplex/server: error reading command, terminating client connection")
			return
		}

		switch method {
		case commandSubscribe:
			for _, channel := range args {
				s.pubsub.Subscribe(Listener{false, string(channel), s})
			}
		case commandPSubscribe:
			for _, channel := range args {
				s.pubsub.Subscribe(Listener{true, string(channel), s})
			}
		case commandUnsubscribe:
			for _, channel := range args {
				s.pubsub.Unsubscribe(Listener{false, string(channel), s})
			}
		case commandPUnsubscribe:
			for _, channel := range args {
				s.pubsub.Unsubscribe(Listener{true, string(channel), s})
			}
		case commandQuit:
			logrus.Debug("redplex/server: terminating connection at client's request")
			return
		default:
			s.cnx.Write([]byte(fmt.Sprintf("-ERR unknown command '%s'", method)))
			continue
		}

		for _, channel := range args {
			s.cnx.Write(SubscribeResponse(method, channel))
		}
	}
}

func (s *connection) loopWrite() {
	buffers := net.Buffers{}
	for {
		s.toSendCond.L.Lock()
		for len(s.toSend) == 0 && !s.isClosed {
			s.toSendCond.Wait()
		}
		if s.isClosed {
			s.toSendCond.L.Unlock()
			return
		}

		buffers = append(buffers, s.toSend...)
		s.toSend = s.toSend[:0]
		s.toSendCond.L.Unlock()
		buffers.WriteTo(s.cnx)
		buffers = buffers[:0]
	}
}

func (s *connection) write(b []byte) {
	s.toSendCond.L.Lock()
	if len(s.toSend) < cap(s.toSend) && !s.isClosed {
		s.toSend = append(s.toSend, b)
		s.toSendCond.Broadcast()
	}
	s.toSendCond.L.Unlock()
}
