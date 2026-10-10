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

	conn, err := ln.Accept()
	if err != nil {
		fmt.Println("Error accepting connection:", err)
		os.Exit(1)
	}

	if err := handleConn(conn); err != nil {
		fmt.Println("Error handling connection:", err)
	}
}

var handlers = map[int16]func(Request) ResponseBody{
	apiKeyAPIVersions: handleAPIVersions,
}

func handleConn(conn net.Conn) error {
	defer conn.Close()

	msg, err := readFrame(conn)
	if err != nil {
		return fmt.Errorf("read frame: %w", err)
	}

	req, err := decodeRequest(msg)
	if err != nil {
		return fmt.Errorf("decode request: %w", err)
	}

	handle, ok := handlers[req.apiKey]
	if !ok {
		return fmt.Errorf("unknown api key %d", req.apiKey)
	}

	header := ResponseHeader{correlationID: req.correlationID}
	body := handle(req)

	buf := encodeResponse(header, body)

	if err := writeFrame(conn, buf); err != nil {
		return fmt.Errorf("write response: %w", err)
	}
	return nil
}
