package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

// Request is a Kafka request with a parsed Request Header v2.
type Request struct {
	apiKey        int16
	apiVersion    int16
	correlationID int32
	clientID      *string
	taggedFields  map[uint32][]byte
	body          []byte
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
