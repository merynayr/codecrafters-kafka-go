package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
)

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

// appendInt16 appends v as Kafka INT16.
func appendInt16(b []byte, v int16) []byte {
	return binary.BigEndian.AppendUint16(b, uint16(v))
}

// appendInt32 appends v as Kafka INT32.
func appendInt32(b []byte, v int32) []byte {
	return binary.BigEndian.AppendUint32(b, uint32(v))
}
