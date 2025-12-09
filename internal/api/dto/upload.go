// Package dto provides Data Transfer Objects for the LBRY API
package dto

import (
	z "github.com/Oudwins/zog"
	"go.lumeweb.com/httputil"
)

// Ensure PostStreamUploadResponse implements DTOResponse
var _ httputil.DTOResponse[*PostStreamUploadResponse] = (*PostStreamUploadResponse)(nil)
var _ httputil.DTOValidator = (*StreamMetadataRequest)(nil)

// PostStreamUploadResponse represents the response for a stream upload request
type PostStreamUploadResponse struct {
	// UploadHash is the hash of the uploaded stream (it is not the stream hash)
	UploadHash string `json:"upload_hash"`
}

// FromModel creates a PostStreamUploadResponse from a model
func (p *PostStreamUploadResponse) FromModel(_ *PostStreamUploadResponse) error {
	// For this simple response, we don't need to extract data from a model
	// The response is already populated with the stream hash
	return nil
}

// StreamMetadataRequest represents the metadata for LBRY stream uploads
type StreamMetadataRequest struct {
	// StreamName is the name of the stream in the LBRY network
	StreamName string `json:"stream_name,omitempty"`

	// SuggestedFileName is the recommended filename when downloading the stream
	SuggestedFileName string `json:"suggested_file_name,omitempty"`
}

// Schema returns a Zog schema for validating StreamMetadataRequest
func (s *StreamMetadataRequest) Schema() *z.StructSchema {
	return z.Struct(z.Shape{
		"StreamName": z.String().
			Optional().
			Max(255),
		"SuggestedFileName": z.String().
			Optional().
			Max(255),
	})
}
