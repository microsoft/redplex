package redplex

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"strconv"
	"strings"
)

const (
	MessageError  = '-'
	MessageStatus = '+'
	MessageInt    = ':'
	MessageBulk   = '$'
	MessageMutli  = '*'
)

var (
	// messageDelimiter is the CRLF separator between Redis messages.
	messageDelimiter = []byte("\r\n")
	// messagePrefix is the prefix for pubsub messages on the Redis protocol.
	messagePrefix = []byte("*3\r\n$7\r\nmessage\r\n")
	// pmessagePrefix is the prefix for pattern pubsub messages on the protocol.
	pmessagePrefix = []byte("*4\r\n$8\r\npmessage\r\n")
	// WrongMessageErr is returned in Parse commands if the command
	// is not a pubsub command.
	WrongMessageErr = errors.New("redplex/protocol: unexpected message type")

	commandSubscribe    = `subscribe`
	commandPSubscribe   = `psubscribe`
	commandUnsubscribe  = `unsubscribe`
	commandPUnsubscribe = `punsubscribe`
	commandQuit         = `quit`
)

// ReadNextFull copies the next full command from the reader to the buffer.
func ReadNextFull(writeTo *bytes.Buffer, r *bufio.Reader) error {
	line, err := r.ReadSlice('\n')
	if err != nil {
		return err
	}
	writeTo.Write(line)
	line = line[:len(line)-2]
	switch line[0] {

	case MessageError:
		return nil
	case MessageStatus:
		return nil
	case MessageInt:
		return nil

	case MessageBulk:
		l, err := strconv.ParseInt(string(line[1:]), 10, 64)
		if err != nil {
			return err
		}

		if l < 0 {
			return nil
		}

		_, err = writeTo.ReadFrom(io.LimitReader(r, l+2))
		return err

	case MessageMutli:
		l, err := strconv.Atoi(string(line[1:]))
		if err != nil {
			return err
		}

		if l < 0 {
			return nil
		}
		for i := 0; i < l; i++ {
			if err := ReadNextFull(writeTo, r); err != nil {
				return err
			}
		}
		return nil
	}

	return errors.New("redplex/protocol: received illegal data from redis")
}

// PublishCommand is returned from ParsePublishCommand.
type PublishCommand struct {
	IsPattern        bool
	ChannelOrPattern []byte
}

// ParseBulkMessage expects that the byte slice starts with the length
// delimiter, and returns the contained message. Does not include the
// trailing delimiter.
func ParseBulkMessage(line []byte) ([]byte, error) {
	if line[0] != MessageBulk {
		return nil, WrongMessageErr
	}

	delimiter := bytes.IndexByte(line, '\n')

	n, err := strconv.ParseInt(string(line[1:delimiter-1]), 10, 64)
	if err != nil {
		return nil, err
	}
	if len(line) <= delimiter+1+int(n) {
		return nil, WrongMessageErr
	}

	return line[delimiter+1 : delimiter+1+int(n)], nil
}

// Parses the given pubsub command efficiently. Returns a NotPubsubError if the
// command isn't a pubsub command.
func ParsePublishCommand(b []byte) (cmd PublishCommand, err error) {
	switch {
	case bytes.HasPrefix(b, messagePrefix):
		name, err := ParseBulkMessage(b[len(messagePrefix):])
		if err != nil {
			return cmd, err
		}

		return PublishCommand{IsPattern: false, ChannelOrPattern: name}, nil
	case bytes.HasPrefix(b, pmessagePrefix):
		name, err := ParseBulkMessage(b[len(pmessagePrefix):])
		if err != nil {
			return cmd, err
		}
		return PublishCommand{IsPattern: true, ChannelOrPattern: name}, nil
	default:
		return cmd, WrongMessageErr
	}
}

// SubscribeCommand is a command to subscribe to one or more Redis channels.
type Request []byte

// NewRequest creates a new request to send to the Redis server.
func NewRequest(name string, argCount int) *Request {
	b := []byte{MessageMutli}
	b = append(b, []byte(strconv.Itoa(argCount+1))...)
	b = append(b, messageDelimiter...)
	r := Request(b)
	return (&r).Append([]byte(name))
}

// Append adds a new argument value to the request.
func (r *Request) Append(arg []byte) *Request {
	data := *r
	data = append(data, MessageBulk)
	data = append(data, []byte(strconv.Itoa(len(arg)))...)
	data = append(data, messageDelimiter...)
	data = append(data, arg...)
	data = append(data, messageDelimiter...)

	*r = data
	return r
}

// Bytes returns the request bytes.
func (r *Request) Bytes() []byte { return *r }

// ParseRequest parses a method and arguments from the reader.
func ParseRequest(r *bufio.Reader) (method string, args [][]byte, err error) {
	line, err := r.ReadSlice('\n')
	if err != nil {
		return "", nil, err
	}

	n, err := strconv.Atoi(string(line[1 : len(line)-2]))
	if err != nil {
		return "", nil, err
	}

	if n < 0 {
		return "", nil, nil
	}

	buffer := bytes.NewBuffer(nil)
	for i := 0; i < n; i++ {
		if err := ReadNextFull(buffer, r); err != nil {
			return "", nil, err
		}

		msg, err := ParseBulkMessage(buffer.Bytes())
		if err != nil {
			return "", nil, err
		}

		if method == "" {
			method = strings.ToLower(string(msg))
		} else {
			args = append(args, copyBytes(msg))
		}

		buffer.Reset()
	}

	return method, args, nil
}

func copyBytes(b []byte) (dup []byte) {
	dup = make([]byte, len(b))
	copy(dup, b)
	return dup
}

// SubscribeResponse returns an appropriate response to the given subscribe
// or unsubscribe command.
func SubscribeResponse(command string, channel []byte) []byte {
	return NewRequest(command, 2).Append(channel).Append([]byte(`:1`)).Bytes()
}
