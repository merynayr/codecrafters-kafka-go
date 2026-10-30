package main

// API keys, see https://kafka.apache.org/protocol#protocol_api_keys.
const (
	apiKeyFetch       int16 = 1
	apiKeyAPIVersions int16 = 18
)

// ApiVersions versions supported by the broker.
const (
	apiVersionsMinVersion int16 = 0
	apiVersionsMaxVersion int16 = 4
)

// Fetch versions supported by the broker.
const (
	FetchMinVersion      int16 = 0
	FetchMaxVersion      int16 = 16
	FetchFlexibleVersion int16 = 12
)

// neverFlexible marks an API that always uses Response Header v0, like ApiVersions:
// a client must read its error_code before it knows which version the broker supports.
const neverFlexible int16 = -1

// Error codes, see https://kafka.apache.org/protocol#protocol_error_codes.
const (
	errorCodeNone               int16 = 0
	errorCodeUnsupportedVersion int16 = 35
)
