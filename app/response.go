package main

// ResponseHeader is Kafka Response Header v0 or v1.
type ResponseHeader struct {
	version       int16
	correlationID int32
	taggedFields  map[uint32][]byte // v1+
}

// ResponseBody is the API-specific part of a response that follows the header.
type ResponseBody interface {
	AppendTo(b []byte) []byte
}

// encodeResponse returns the response without message_size, which writeFrame adds.
func encodeResponse(header ResponseHeader, body ResponseBody) []byte {
	buf := appendInt32(nil, header.correlationID)
	if header.version >= 1 {
		buf = appendTaggedFields(buf, header.taggedFields)
	}

	buf = body.AppendTo(buf)
	return buf
}
