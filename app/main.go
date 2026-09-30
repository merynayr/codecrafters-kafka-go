package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
)

// maxMessageSize limits a single request size, like socket.request.max.bytes in Kafka.
const maxMessageSize = 100 << 20 // 100 MiB

// requestHeaderSize is the size of the fixed-length part of Request Header v2:
// request_api_key (2) + request_api_version (2) + correlation_id (4) + client_id length (2).
const requestHeaderSize = 2 + 2 + 4 + 2

// Request is a Kafka request with a parsed Request Header v2.
type Request struct {
	apiKey        int16
	apiVersion    int16
	correlationID int32
	clientID      *string
	body          []byte
}

// ResponseHeader is Kafka Response Header v0.
type ResponseHeader struct {
	correlationID int32
}

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

func handleConn(conn net.Conn) error {
	defer conn.Close()

	req, err := readRequest(conn)
	if err != nil {
		return fmt.Errorf("read request: %w", err)
	}

	header := ResponseHeader{correlationID: req.correlationID}
	if err := writeResponse(conn, header); err != nil {
		return fmt.Errorf("write response: %w", err)
	}
	return nil
}

func readRequest(r io.Reader) (Request, error) {
	var size int32
	if err := binary.Read(r, binary.BigEndian, &size); err != nil {
		return Request{}, fmt.Errorf("read message size: %w", err)
	}
	if size < requestHeaderSize || size > maxMessageSize {
		return Request{}, fmt.Errorf("invalid message size %d, want %d..%d", size, requestHeaderSize, maxMessageSize)
	}

	var req Request
	if err := binary.Read(r, binary.BigEndian, &req.apiKey); err != nil {
		return Request{}, fmt.Errorf("read request_api_key: %w", err)
	}
	if err := binary.Read(r, binary.BigEndian, &req.apiVersion); err != nil {
		return Request{}, fmt.Errorf("read request_api_version: %w", err)
	}
	if err := binary.Read(r, binary.BigEndian, &req.correlationID); err != nil {
		return Request{}, fmt.Errorf("read correlation_id: %w", err)
	}

	var clientIDLen int16
	if err := binary.Read(r, binary.BigEndian, &clientIDLen); err != nil {
		return Request{}, fmt.Errorf("read client_id length: %w", err)
	}
	remaining := size - requestHeaderSize
	if clientIDLen < -1 || int32(clientIDLen) > remaining {
		return Request{}, fmt.Errorf("invalid client_id length %d, want -1..%d", clientIDLen, remaining)
	}
	if clientIDLen >= 0 {
		clientID := make([]byte, clientIDLen)
		if _, err := io.ReadFull(r, clientID); err != nil {
			return Request{}, fmt.Errorf("read client_id: %w", err)
		}
		req.clientID = new(string(clientID))
		remaining -= int32(clientIDLen)
	}

	req.body = make([]byte, remaining)
	if _, err := io.ReadFull(r, req.body); err != nil {
		return Request{}, fmt.Errorf("read message body: %w", err)
	}
	return req, nil
}

func writeResponse(w io.Writer, header ResponseHeader) error {
	buf := binary.BigEndian.AppendUint32(nil, uint32(binary.Size(header)))
	buf, err := binary.Append(buf, binary.BigEndian, header)
	if err != nil {
		return fmt.Errorf("encode header: %w", err)
	}

	_, err = w.Write(buf)
	return err
}
