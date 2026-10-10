package main

// API keys, see https://kafka.apache.org/protocol#protocol_api_keys.
const (
	apiKeyAPIVersions int16 = 18
)

// ApiVersions versions supported by the broker.
const (
	apiVersionsMinVersion int16 = 0
	apiVersionsMaxVersion int16 = 4
)

// Error codes, see https://kafka.apache.org/protocol#protocol_error_codes.
const (
	errorCodeNone               int16 = 0
	errorCodeUnsupportedVersion int16 = 35
)
