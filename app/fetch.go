package main

type FetchResponse struct {
	throttleTimeMs int32
	errorCode      int16
	sessionID      int32
	responses      []Partition
	nodeEndpoints  map[uint32][]byte
}

type Partition struct{}

func (p Partition) AppendTo(b []byte) []byte {
	return b
}

type NodeEndpoint struct{}

func (n NodeEndpoint) AppendTo(b []byte) []byte {
	return b
}

func (r FetchResponse) AppendTo(b []byte) []byte {
	b = appendInt32(b, r.throttleTimeMs)
	b = appendInt16(b, r.errorCode)
	b = appendInt32(b, r.sessionID)
	b = appendCompactArray(b, r.responses)
	b = appendTaggedFields(b, r.nodeEndpoints)
	return b
}

func (b *broker) handleFetch(req Request) ResponseBody {
	return FetchResponse{errorCode: errorCodeNone, throttleTimeMs: 0, sessionID: 0, responses: []Partition{}, nodeEndpoints: make(map[uint32][]byte)}
}
