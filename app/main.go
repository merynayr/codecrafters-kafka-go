package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"net"
	"os"
)

// maxMessageSize limits a single request size, like socket.request.max.bytes in Kafka.
const maxMessageSize = 100 << 20 // 100 MiB

// minRequestSize is the smallest possible Request Header v2: request_api_key (2) +
// request_api_version (2) + correlation_id (4) + client_id length (2) + empty TAG_BUFFER (1).
const minRequestSize = 2 + 2 + 4 + 2 + 1

// Request is a Kafka request with a parsed Request Header v2.
type Request struct {
	apiKey        int16
	apiVersion    int16
	correlationID int32
	clientID      *string
	taggedFields  map[uint32][]byte
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

	msg, err := readMessage(conn)
	if err != nil {
		return fmt.Errorf("read message: %w", err)
	}

	req, err := parseRequest(msg)
	if err != nil {
		return fmt.Errorf("parse request: %w", err)
	}

	header := ResponseHeader{correlationID: req.correlationID}
	if err := writeResponse(conn, header); err != nil {
		return fmt.Errorf("write response: %w", err)
	}
	return nil
}

func readMessage(r io.Reader) ([]byte, error) {
	var size int32
	if err := binary.Read(r, binary.BigEndian, &size); err != nil {
		return nil, fmt.Errorf("read size: %w", err)
	}
	if size < minRequestSize || size > maxMessageSize {
		return nil, fmt.Errorf("invalid size %d, want %d..%d", size, minRequestSize, maxMessageSize)
	}

	msg := make([]byte, size)
	if _, err := io.ReadFull(r, msg); err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}
	return msg, nil
}

func parseRequest(msg []byte) (Request, error) {
	r := bytes.NewReader(msg)

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

	clientID, err := readNullableString(r)
	if err != nil {
		return Request{}, fmt.Errorf("read client_id: %w", err)
	}
	req.clientID = clientID

	taggedFields, err := readTaggedFields(r)
	if err != nil {
		return Request{}, fmt.Errorf("read tagged fields: %w", err)
	}
	req.taggedFields = taggedFields

	req.body = msg[len(msg)-r.Len():]
	return req, nil
}

// readNullableString returns nil for a null string.
func readNullableString(r *bytes.Reader) (*string, error) {
	var n int16
	if err := binary.Read(r, binary.BigEndian, &n); err != nil {
		return nil, fmt.Errorf("read length: %w", err)
	}
	if n < -1 || int(n) > r.Len() {
		return nil, fmt.Errorf("invalid length %d, want -1..%d", n, r.Len())
	}
	if n == -1 {
		return nil, nil
	}

	b := make([]byte, n)
	if _, err := io.ReadFull(r, b); err != nil {
		return nil, fmt.Errorf("read data: %w", err)
	}
	return new(string(b)), nil
}

// readTaggedFields reads a TAG_BUFFER (KIP-482) and returns nil if it is empty.
func readTaggedFields(r *bytes.Reader) (map[uint32][]byte, error) {
	n, err := binary.ReadUvarint(r)
	if err != nil {
		return nil, fmt.Errorf("read count: %w", err)
	}
	// Each field takes at least 2 bytes: a tag and a size.
	if maxFields := uint64(r.Len()) / 2; n > maxFields {
		return nil, fmt.Errorf("invalid count %d, want 0..%d", n, maxFields)
	}
	if n == 0 {
		return nil, nil
	}

	fields := make(map[uint32][]byte, n)
	for range n {
		tag, err := binary.ReadUvarint(r)
		if err != nil {
			return nil, fmt.Errorf("read tag: %w", err)
		}
		if tag > math.MaxUint32 {
			return nil, fmt.Errorf("invalid tag %d, want 0..%d", tag, uint32(math.MaxUint32))
		}
		if _, ok := fields[uint32(tag)]; ok {
			return nil, fmt.Errorf("duplicate tag %d", tag)
		}

		size, err := binary.ReadUvarint(r)
		if err != nil {
			return nil, fmt.Errorf("read tag %d size: %w", tag, err)
		}
		if size > uint64(r.Len()) {
			return nil, fmt.Errorf("invalid tag %d size %d, want 0..%d", tag, size, r.Len())
		}

		data := make([]byte, size)
		if _, err := io.ReadFull(r, data); err != nil {
			return nil, fmt.Errorf("read tag %d data: %w", tag, err)
		}
		fields[uint32(tag)] = data
	}
	return fields, nil
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
