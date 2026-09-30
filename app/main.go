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

type Request struct {
	body []byte
}

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

	if _, err := readRequest(conn); err != nil {
		return fmt.Errorf("read request: %w", err)
	}

	header := ResponseHeader{correlationID: 7}
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
	if size < 0 || size > maxMessageSize {
		return Request{}, fmt.Errorf("invalid message size %d, limit %d", size, maxMessageSize)
	}

	body := make([]byte, size)
	if _, err := io.ReadFull(r, body); err != nil {
		return Request{}, fmt.Errorf("read message body: %w", err)
	}
	return Request{body: body}, nil
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
