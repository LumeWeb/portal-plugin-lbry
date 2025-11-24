package dto

import (
	z "github.com/Oudwins/zog"
	"go.lumeweb.com/httputil"
)

var _ httputil.DTOValidator = (*StreamPinRequest)(nil)
var _ httputil.DTORequest[*StreamPinRequest] = (*StreamPinRequest)(nil)

// StreamPinRequest represents the request for pinning a stream
type StreamPinRequest struct {
	// SDHash is the stream descriptor hash of the stream to pin
	SDHash string `json:"sd_hash"`
}

// Schema returns a Zog schema for validating StreamPinRequest
func (p *StreamPinRequest) Schema() *z.StructSchema {
	return z.Struct(z.Shape{
		"SDHash": z.String().
			Required().
			Min(1),
	})
}

// ToModel converts the StreamPinRequest to a model (self-reference for simple requests)
func (p *StreamPinRequest) ToModel() (*StreamPinRequest, error) {
	return p, nil
}
