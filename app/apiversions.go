package main

type APIVersionsResponse struct {
	errorCode      int16
	apiKeys        []APIVersion
	throttleTimeMs int32
	taggedFields   map[uint32][]byte
}

func (r APIVersionsResponse) AppendTo(b []byte) []byte {
	b = appendInt16(b, r.errorCode)
	b = appendCompactArray(b, r.apiKeys)
	b = appendInt32(b, r.throttleTimeMs)
	b = appendTaggedFields(b, r.taggedFields)
	return b
}

// APIVersion is named after the api_keys element type in Kafka's message schema.
type APIVersion struct {
	apiKey       int16
	minVersion   int16
	maxVersion   int16
	taggedFields map[uint32][]byte
}

func (v APIVersion) AppendTo(b []byte) []byte {
	b = appendInt16(b, v.apiKey)
	b = appendInt16(b, v.minVersion)
	b = appendInt16(b, v.maxVersion)
	b = appendTaggedFields(b, v.taggedFields)
	return b
}

func (b *broker) handleAPIVersions(req Request) ResponseBody {
	body := APIVersionsResponse{errorCode: errorCodeNone}
	if req.apiVersion < apiVersionsMinVersion || req.apiVersion > apiVersionsMaxVersion {
		body.errorCode = errorCodeUnsupportedVersion
	}

	body.apiKeys = make([]APIVersion, 0, len(b.apis))
	for key, a := range b.apis {
		body.apiKeys = append(body.apiKeys, APIVersion{
			apiKey:     key,
			minVersion: a.minVersion,
			maxVersion: a.maxVersion,
		})
	}

	return body
}
