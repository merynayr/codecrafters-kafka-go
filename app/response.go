package main

import (
	"encoding/binary"
	"fmt"
	"io"
)

// ResponseHeader is Kafka Response Header v0.
type ResponseHeader struct {
	correlationID int32
	errorCode     int16
}

func writeResponse(w io.Writer, header ResponseHeader) error {
	buf, err := binary.Append(nil, binary.BigEndian, header)
	if err != nil {
		return fmt.Errorf("encode header: %w", err)
	}
	return writeFrame(w, buf)
}
