package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"maps"
	"math"
	"slices"
)

// maxPrealloc caps the capacity reserved for a client-sent item count: an item
// may take far more memory than its wire bytes, so append grows the rest.
const maxPrealloc = 64

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

	fields := make(map[uint32][]byte, min(n, maxPrealloc))
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

// readInt reads a Kafka INT8, INT16, INT32 or INT64, depending on T.
func readInt[T int8 | int16 | int32 | int64](r *bytes.Reader) (T, error) {
	var n T
	if err := binary.Read(r, binary.BigEndian, &n); err != nil {
		return 0, fmt.Errorf("read %T: %w", n, err)
	}
	return n, nil
}

// readUUID reads a Kafka UUID: 16 raw bytes with no length prefix.
func readUUID(r *bytes.Reader) ([16]byte, error) {
	var uuid [16]byte
	if _, err := io.ReadFull(r, uuid[:]); err != nil {
		return [16]byte{}, fmt.Errorf("read UUID: %w", err)
	}

	return uuid, nil
}

// readCompactString reads a Kafka COMPACT_STRING, which, unlike
// COMPACT_NULLABLE_STRING, rejects the null encoding (length 0).
func readCompactString(r *bytes.Reader) (string, error) {
	n, err := binary.ReadUvarint(r)
	if err != nil {
		return "", fmt.Errorf("read length: %w", err)
	}
	if n == 0 || n-1 > uint64(r.Len()) {
		return "", fmt.Errorf("invalid length %d, want 1..%d", n, r.Len()+1)
	}

	str := make([]byte, n-1)
	if _, err := io.ReadFull(r, str); err != nil {
		return "", fmt.Errorf("read data: %w", err)
	}

	return string(str), nil
}

// readCompactArray reads a Kafka COMPACT_ARRAY using readItem for each item.
// It rejects the null encoding (length 0), so it suits non-nullable arrays only.
func readCompactArray[T any](r *bytes.Reader, readItem func(*bytes.Reader) (T, error)) ([]T, error) {
	n, err := binary.ReadUvarint(r)
	if err != nil {
		return nil, fmt.Errorf("read length: %w", err)
	}
	if n == 0 || n-1 > uint64(r.Len()) {
		return nil, fmt.Errorf("invalid length %d, want 1..%d", n, r.Len()+1)
	}

	items := make([]T, 0, min(n-1, maxPrealloc))
	for i := range n - 1 {
		item, err := readItem(r)
		if err != nil {
			return nil, fmt.Errorf("read item %d: %w", i, err)
		}
		items = append(items, item)
	}
	return items, nil
}

// appendInt16 appends v as Kafka INT16.
func appendInt16(b []byte, v int16) []byte {
	return binary.BigEndian.AppendUint16(b, uint16(v))
}

// appendInt32 appends v as Kafka INT32.
func appendInt32(b []byte, v int32) []byte {
	return binary.BigEndian.AppendUint32(b, uint32(v))
}

// appendInt64 appends v as Kafka INT64.
func appendInt64(b []byte, v int64) []byte {
	return binary.BigEndian.AppendUint64(b, uint64(v))
}

// appendUUID appends uuid as a Kafka UUID: 16 raw bytes with no length prefix.
func appendUUID(b []byte, uuid [16]byte) []byte {
	return append(b, uuid[:]...)
}

func appendCompactArray[T interface{ AppendTo([]byte) []byte }](b []byte, items []T) []byte {
	if items == nil {
		b = binary.AppendUvarint(b, 0)
	} else {
		b = binary.AppendUvarint(b, uint64(len(items)+1))
		for _, v := range items {
			b = v.AppendTo(b)
		}
	}

	return b
}

func appendTaggedFields(b []byte, fields map[uint32][]byte) []byte {
	b = binary.AppendUvarint(b, uint64(len(fields)))

	for _, tag := range slices.Sorted(maps.Keys(fields)) {
		b = binary.AppendUvarint(b, uint64(tag))
		b = binary.AppendUvarint(b, uint64(len(fields[tag])))
		b = append(b, fields[tag]...)
	}
	return b
}

// appendCompactNullableBytes appends data as Kafka COMPACT_NULLABLE_BYTES;
// nil is written as null. COMPACT_NULLABLE_RECORDS uses the same encoding.
func appendCompactNullableBytes(b []byte, data []byte) []byte {
	if data == nil {
		b = binary.AppendUvarint(b, 0)
	} else {
		b = binary.AppendUvarint(b, uint64(len(data)+1))
		b = append(b, data...)
	}
	return b
}
