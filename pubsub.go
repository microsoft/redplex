package redplex

import (
	"bufio"
	"bytes"
	"net"
	"sync"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/sirupsen/logrus"
)

// Writable is an interface passed into Pubsub. It's called when we want to
// publish data.
type Writable interface {
	Write(b []byte)
}

// The Listener wraps a function that's called when a pubsub message it sent.
type Listener struct {
	IsPattern bool
	Channel   string
	Conn      Writable
}

// listenerMap is a map of patterns or channels to Listeners.
type listenerMap map[string][]Writable

// broadcast pushes the byte slice asynchronously to the list of listeners.
// Blocks until all listeners have been called.
func (l listenerMap) broadcast(pattern []byte, b []byte) {
	listeners := l[string(pattern)]

	count := float64(len(listeners))
	throughputMessages.Add(count)
	throughputBytes.Add(count * float64(len(b)))

	var wg sync.WaitGroup
	wg.Add(len(listeners))
	for _, l := range listeners {
		go func(l Writable) { l.Write(b); wg.Done() }(l)
	}
	wg.Wait()
}

// add inserts the listener into the pattern's set of listeners.
func (l listenerMap) add(channel string, listener Writable) (shouldSubscribe bool) {
	list := l[channel]
	shouldSubscribe = len(list) == 0
	l[channel] = append(list, listener)
	return shouldSubscribe
}

// remove pulls the listener out of the map.
func (l listenerMap) remove(channel string, listener Writable) (shouldUnsubscribe bool) {
	list := l[channel]
	changed := false
	for i, other := range list {
		if other == listener {
			changed = true
			list[i] = list[len(list)-1]
			list[len(list)-1] = nil
			list = list[:len(list)-1]
			break
		}
	}

	if !changed {
		return false
	}

	if len(list) == 0 {
		delete(l, channel)
		return true
	}

	l[channel] = list
	return false
}

// removeAll removes all channels the listener is connected to.
func (l listenerMap) removeAll(conn Writable) (toUnsub [][]byte) {
	for channel, list := range l {
		for i := 0; i < len(list); i++ {
			if list[i] == conn {
				list[i] = list[len(list)-1]
				list[len(list)-1] = nil
				list = list[:len(list)-1]
				i--
				continue
			}
		}

		if len(list) == 0 {
			delete(l, channel)
			toUnsub = append(toUnsub, []byte(channel))
		} else {
			l[channel] = list
		}
	}

	return toUnsub
}

// Pubsub manages the connection of redplex to the remote pubsub server.
type Pubsub struct {
	dialer       Dialer
	closer       chan struct{}
	writeTimeout time.Duration

	mu         sync.Mutex
	connection net.Conn
	patterns   listenerMap
	channels   listenerMap
}

// NewPubsub creates a new Pubsub instance.
func NewPubsub(dialer Dialer, writeTimeout time.Duration) *Pubsub {
	return &Pubsub{
		dialer:       dialer,
		writeTimeout: writeTimeout,
		patterns:     listenerMap{},
		channels:     listenerMap{},
		closer:       make(chan struct{}),
	}
}

// Start creates a pubsub listener to proxy connection data.
func (p *Pubsub) Start() {
	backoff := backoff.NewExponentialBackOff()
	backoff.MaxInterval = time.Second * 10

	for {
		cnx, err := p.dialer.Dial()
		if err != nil {
			logrus.WithError(err).Info("redplex/pubsub: error dialing to pubsub master")
			select {
			case <-time.After(backoff.NextBackOff()):
				serverReconnects.Inc()
				continue
			case <-p.closer:
				return
			}
		}

		backoff.Reset()
		err = p.read(cnx)

		select {
		case <-p.closer:
			return
		default:
			logrus.WithError(err).Info("redplex/pubsub: lost connection to pubsub server")
		}
	}
}

// Close frees resources associated with the pubsub server.
func (p *Pubsub) Close() {
	close(p.closer)
	p.mu.Lock()
	if p.connection != nil {
		p.connection.Close()
	}
	p.mu.Unlock()
}

// Subscribe adds the listener to the channel.
func (p *Pubsub) Subscribe(listener Listener) {
	p.mu.Lock()
	if listener.IsPattern {
		if p.patterns.add(listener.Channel, listener.Conn) {
			p.command(NewRequest(commandPSubscribe, 1).Bulk([]byte(listener.Channel)))
		}
	} else {
		if p.channels.add(listener.Channel, listener.Conn) {
			p.command(NewRequest(commandSubscribe, 1).Bulk([]byte(listener.Channel)))
		}
	}
	p.mu.Unlock()
}

// Unsubscribe removes the listener from the channel.
func (p *Pubsub) Unsubscribe(listener Listener) {
	p.mu.Lock()
	if listener.IsPattern {
		if p.patterns.remove(listener.Channel, listener.Conn) {
			p.command(NewRequest(commandPUnsubscribe, 1).Bulk([]byte(listener.Channel)))
		}
	} else {
		if p.channels.remove(listener.Channel, listener.Conn) {
			p.command(NewRequest(commandUnsubscribe, 1).Bulk([]byte(listener.Channel)))
		}
	}
	p.mu.Unlock()
}

// UnsubscribeAll removes all channels the writer is subscribed to.
func (p *Pubsub) UnsubscribeAll(c Writable) {
	p.mu.Lock()

	var (
		toUnsub = p.patterns.removeAll(c)
		command []byte
	)

	if len(toUnsub) > 0 {
		r := NewRequest(commandPUnsubscribe, len(toUnsub))
		for _, p := range toUnsub {
			r.Bulk(p)
		}

		command = append(command, r.Bytes()...)
	}

	toUnsub = p.channels.removeAll(c)
	if len(toUnsub) > 0 {
		r := NewRequest(commandUnsubscribe, len(toUnsub))
		for _, p := range toUnsub {
			r.Bulk(p)
		}

		command = append(command, r.Bytes()...)
	}

	if p.connection != nil && len(command) > 0 {
		p.connection.SetWriteDeadline(time.Now().Add(p.writeTimeout))
		go p.connection.Write(command)
	}

	p.mu.Unlock()
}

// command sends the request to the pubsub server asynchronously.
func (p *Pubsub) command(r *Request) {
	if p.connection != nil {
		p.connection.SetWriteDeadline(time.Now().Add(p.writeTimeout))
		go p.connection.Write(r.Bytes())
	}
}

// command sends the request to the pubsub server and blocks until it sends.
func (p *Pubsub) commandSync(r *Request) {
	if p.connection != nil {
		p.connection.SetWriteDeadline(time.Now().Add(p.writeTimeout))
		p.connection.Write(r.Bytes())
	}
}

func (p *Pubsub) resubscribe(cnx net.Conn) {
	p.mu.Lock()
	p.connection = cnx

	if len(p.channels) > 0 {
		cmd := NewRequest(commandSubscribe, len(p.channels))
		for channel := range p.channels {
			cmd.Bulk([]byte(channel))
		}
		p.commandSync(cmd)
	}

	if len(p.patterns) > 0 {
		cmd := NewRequest(commandPSubscribe, len(p.patterns))
		for pattern := range p.patterns {
			cmd.Bulk([]byte(pattern))
		}
		p.commandSync(cmd)
	}

	p.mu.Unlock()
}

// read grabs commands from the connection, reading them until the
// connection terminates.
func (p *Pubsub) read(cnx net.Conn) error {
	var (
		reader = bufio.NewReader(cnx)
		buffer = bytes.NewBuffer(nil)
	)

	p.resubscribe(cnx)

	// The only thing that
	for {
		buffer.Reset()
		if err := ReadNextFull(buffer, reader); err != nil {
			p.mu.Lock()
			p.connection = nil
			p.mu.Unlock()
			return err
		}

		bytes := copyBytes(buffer.Bytes())
		parsed, err := ParsePublishCommand(bytes)
		if err != nil {
			continue // expected, we can get replies from subscriptions, which we'll ignore
		}

		p.mu.Lock()
		if parsed.IsPattern {
			p.patterns.broadcast(parsed.ChannelOrPattern, bytes)
		} else {
			p.channels.broadcast(parsed.ChannelOrPattern, bytes)
		}
		p.mu.Unlock()
	}
}
