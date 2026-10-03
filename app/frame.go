package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
)

// maxMessageSize limits a single request size, like socket.request.max.bytes in Kafka.
const maxMessageSize = 100 << 20 // 100 MiB

// readFrame reads a message_size-prefixed frame and returns its payload.
func readFrame(r io.Reader) ([]byte, error) {
	var size int32
	if err := binary.Read(r, binary.BigEndian, &size); err != nil {
		return nil, fmt.Errorf("read size: %w", err)
	}
	if size < 0 || size > maxMessageSize {
		return nil, fmt.Errorf("invalid size %d, want 0..%d", size, maxMessageSize)
	}

	payload := make([]byte, size)
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, fmt.Errorf("read payload: %w", err)
	}
	return payload, nil
}

// writeFrame writes payload prefixed with its message_size in a single Write.
func writeFrame(w io.Writer, payload []byte) error {
	if len(payload) > math.MaxInt32 {
		return fmt.Errorf("payload too large: %d bytes", len(payload))
	}

	buf := make([]byte, 0, 4+len(payload))
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(payload)))
	buf = append(buf, payload...)

	_, err := w.Write(buf)
	return err
}
