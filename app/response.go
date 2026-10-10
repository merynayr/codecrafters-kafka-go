package main

// ResponseHeader is Kafka Response Header v0.
type ResponseHeader struct {
	correlationID int32
}

// ResponseBody is the API-specific part of a response that follows the header.
type ResponseBody interface {
	AppendTo(b []byte) []byte
}

// encodeResponse returns the response without message_size, which writeFrame adds.
func encodeResponse(header ResponseHeader, body ResponseBody) []byte {
	buf := appendInt32(nil, header.correlationID)
	buf = body.AppendTo(buf)
	return buf
}
