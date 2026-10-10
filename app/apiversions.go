package main

// APIVersionsResponse is the ApiVersions response body.
type APIVersionsResponse struct {
	errorCode int16
}

func (r APIVersionsResponse) AppendTo(b []byte) []byte {
	return appendInt16(b, r.errorCode)
}

// handleAPIVersions reports an unsupported version in error_code, not as a Go error,
// so the client still gets a response.
func handleAPIVersions(req Request) ResponseBody {
	body := APIVersionsResponse{errorCode: errorCodeNone}
	if req.apiVersion < apiVersionsMinVersion || req.apiVersion > apiVersionsMaxVersion {
		body.errorCode = errorCodeUnsupportedVersion
	}
	return body
}
