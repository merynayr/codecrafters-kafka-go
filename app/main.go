package main

import (
	"fmt"
	"net"
	"os"
)

func main() {
	ln, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Println("Failed to bind to port 9092:", err)
		os.Exit(1)
	}
	defer ln.Close()

	b := newBroker()

	conn, err := ln.Accept()
	if err != nil {
		fmt.Println("Error accepting connection:", err)
		os.Exit(1)
	}

	if err := b.handleConn(conn); err != nil {
		fmt.Println("Error handling connection:", err)
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

	msg, err := readFrame(conn)
	if err != nil {
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

	if err := writeFrame(conn, buf); err != nil {
		return fmt.Errorf("write response: %w", err)
	}
	return nil
}
