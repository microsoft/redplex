package redplex

import (
	"bufio"
	"bytes"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReadNextFull(t *testing.T) {
	tt := [][]string{
		{"$7", "he\r\nllo"},
		{"$0", ""},
		{"$-1"},
		{":1"},
		{"+OK"},
		{"-hello world!"},
		{"*0"},
		{"*-1"},
		{"*2", "$5", "hello", ":2"},
	}

	for _, tcase := range tt {
		output := bytes.NewBuffer(nil)
		expected := []byte(strings.Join(tcase, "\r\n") + "\r\n")
		reader := bytes.NewReader(expected)
		bufferedReader := bufio.NewReader(reader)

		require.Nil(t, ReadNextFull(output, bufferedReader), "unexpected error in %+v", tcase)
		require.Equal(t, output.Bytes(), expected, "expected parsed %+v to be equal", tcase)
		require.Zero(t, reader.Len()+bufferedReader.Buffered(), "should have consumed all of %+v", tcase)
	}
}

func TestParseBulkMessage(t *testing.T) {
	tt := []struct {
		Message  string
		Expected string
		Error    error
	}{
		{"$-1\r\n", "", nil},
		{"$0\r\n", "", nil},
		{"$5\r\nhello\r\n", "hello", nil},
		{"$5\r\nhe", "", ErrWrongMessage},
		{":1\r\nasdf\r\n", "", ErrWrongMessage},
	}

	for _, tcase := range tt {
		actual, err := ParseBulkMessage([]byte(tcase.Message))

		if tcase.Error != nil {
			require.Equal(t, tcase.Error, err, "unexpected error parsing %s", tcase.Message)
		} else if len(tcase.Expected) == 0 {
			require.Nil(t, actual, "expected empty byte slice parsing %s", tcase.Message)
		} else {
			require.Equal(t, []byte(tcase.Expected), actual, "unexpected result parsing %s", tcase.Message)
		}
	}
}

func TestParsePublishCommand(t *testing.T) {
	tt := []struct {
		Message  string
		Expected PublishCommand
		Error    error
	}{
		{"$-1\r\n", PublishCommand{}, ErrWrongMessage},
		{"*3\r\n$7\r\nmessage\r\n$6\r\nsecond\r\n$5\r\nHello\r\n", PublishCommand{false, []byte("second")}, nil},
		{"*4\r\n$8\r\npmessage\r\n$6\r\nsecond\r\n$2\r\ns*\r\n$5\r\nHello\r\n", PublishCommand{true, []byte("second")}, nil},
	}

	for _, tcase := range tt {
		actual, err := ParsePublishCommand([]byte(tcase.Message))

		if err != nil {
			require.Equal(t, tcase.Error, err, "unexpected error parsing %s", tcase.Message)
		} else {
			require.Equal(t, tcase.Expected, actual, "unexpected result parsing %s", tcase.Message)
		}
	}
}

func TestNewRequest(t *testing.T) {
	require.Equal(t,
		NewRequest("PING", 0).Bytes(),
		[]byte("*1\r\n$4\r\nPING\r\n"),
	)
	require.Equal(t,
		NewRequest("SUBSCRIBE", 1).Bulk([]byte("channel-name")).Bytes(),
		[]byte("*2\r\n$9\r\nSUBSCRIBE\r\n$12\r\nchannel-name\r\n"),
	)
}

func TestParseRequest(t *testing.T) {
	tt := [][]string{
		{"ping"},
		{"subscribe", "foo"},
		{"subscribe", "foo", "bar"},
	}

	for _, tcase := range tt {
		var args [][]byte
		cmd := NewRequest(tcase[0], len(tcase)-1)
		for _, arg := range tcase[1:] {
			args = append(args, []byte(arg))
			cmd.Bulk([]byte(arg))
		}

		method, actualArgs, err := ParseRequest(bufio.NewReader(bytes.NewReader(cmd.Bytes())))
		require.Nil(t, err, "unexpected error parsing %+v", tcase)
		require.Equal(t, tcase[0], method)
		require.Equal(t, args, actualArgs)
	}
}

func TestSubscribeResponse(t *testing.T) {
	require.Equal(t,
		SubscribeResponse("subscribe", []byte("first")),
		[]byte("*3\r\n$9\r\nsubscribe\r\n$5\r\nfirst\r\n:1\r\n"),
	)
}
