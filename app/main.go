package main

import (
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"time"
)

const (
	readTimeout  = 5 * time.Second
	writeTimeout = 5 * time.Second

	acceptMinDelay = 5 * time.Millisecond
	acceptMaxDelay = time.Second
)

func main() {
	ln, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Println("Failed to bind to port 9092:", err)
		os.Exit(1)
	}
	defer ln.Close()

	b := newBroker()

	var tempDelay time.Duration

	for {
		conn, err := ln.Accept()
		if errors.Is(err, net.ErrClosed) {
			return
		}
		if err != nil {
			if tempDelay == 0 {
				tempDelay = acceptMinDelay
			} else {
				tempDelay *= 2
			}
			tempDelay = min(tempDelay, acceptMaxDelay)
			fmt.Printf("Error accepting connection: %v; retrying in %v\n", err, tempDelay)
			time.Sleep(tempDelay)
			continue
		}

		tempDelay = 0

		go func() {
			if err := b.handleConn(conn); err != nil {
				fmt.Printf("Error handling connection %s: %v\n", conn.RemoteAddr(), err)
			}
		}()
	}
}

type api struct {
	minVersion int16
	maxVersion int16
	handle     func(Request) ResponseBody
}

type broker struct {
	apis map[int16]api // by API key
}

func newBroker() *broker {
	b := &broker{}
	// Handlers need pointer receivers: a method value with a value receiver
	// would copy b here, before apis is set.
	b.apis = map[int16]api{
		apiKeyAPIVersions: {
			minVersion: apiVersionsMinVersion,
			maxVersion: apiVersionsMaxVersion,
			handle:     b.handleAPIVersions,
		},
	}
	return b
}

func (b *broker) handleConn(conn net.Conn) error {
	defer conn.Close()
	for {
		if err := conn.SetReadDeadline(time.Now().Add(readTimeout)); err != nil {
			return fmt.Errorf("set read deadline: %w", err)
		}

		msg, err := readFrame(conn)
		switch {
		case errors.Is(err, io.EOF):
			return nil
		case err != nil:
			return fmt.Errorf("read frame: %w", err)
		}

		req, err := decodeRequest(msg)
		if err != nil {
			return fmt.Errorf("decode request: %w", err)
		}

		a, ok := b.apis[req.apiKey]
		if !ok {
			return fmt.Errorf("unknown api key %d", req.apiKey)
		}

		header := ResponseHeader{correlationID: req.correlationID}
		body := a.handle(req)

		buf := encodeResponse(header, body)

		if err := conn.SetWriteDeadline(time.Now().Add(writeTimeout)); err != nil {
			return fmt.Errorf("set write deadline: %w", err)
		}

		if err := writeFrame(conn, buf); err != nil {
			return fmt.Errorf("write response: %w", err)
		}
	}
}
