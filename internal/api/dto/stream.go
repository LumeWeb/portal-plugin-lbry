package dto

import (
	"fmt"
	"time"

	z "github.com/Oudwins/zog"
	"go.lumeweb.com/httputil"
	"go.lumeweb.com/liblbry/blob"
	"go.lumeweb.com/portal-plugin-lbry/internal/db"
)

// Ensure StreamResponse implements DTOResponse
var _ httputil.DTOResponse[*db.Stream] = (*StreamResponse)(nil)
var _ httputil.DTOValidator = (*StreamDeleteRequest)(nil)

// StreamResponse represents a stream in the response
type StreamResponse struct {
	// ID is the database ID of the stream
	ID uint64 `json:"id"`

	// StreamHash is the hash of the stream content
	StreamHash string `json:"stream_hash"`

	// SDHash is the stream descriptor hash
	SDHash string `json:"sd_hash" filter:"true" sort:"true"`

	// StreamName is the name of the stream in the LBRY network
	StreamName string `json:"stream_name" filter:"true" sort:"true"`

	// StreamType is the type of the stream
	StreamType string `json:"stream_type" filter:"true" sort:"true"`

	// SuggestedFileName is the recommended filename when downloading the stream
	SuggestedFileName string `json:"suggested_file_name"`

	// CreatedAt is when the stream was created
	CreatedAt time.Time `json:"created_at" sort:"true"`

	// UpdatedAt is when the stream was last updated
	UpdatedAt time.Time `json:"updated_at"`
}

// FromModel creates a StreamResponse from a db.Stream model
func (s *StreamResponse) FromModel(model *db.Stream) error {
	if model == nil {
		return fmt.Errorf("model cannot be nil")
	}

	// Map fields from db.Stream to StreamResponse
	s.ID = uint64(model.ID)
	s.StreamHash = model.StreamHash
	s.SDHash = model.SDHash
	s.StreamName = model.StreamName
	s.StreamType = model.StreamType
	s.SuggestedFileName = model.SuggestedFileName
	s.CreatedAt = model.CreatedAt
	s.UpdatedAt = model.UpdatedAt

	return nil
}

// StreamDeleteRequest represents the request for deleting a stream
type StreamDeleteRequest struct {
	// SDHash is the stream descriptor hash of the stream to delete
	SDHash string `param:"sd_hash"`
}

// Schema returns a Zog schema for validating StreamDeleteRequest
func (s *StreamDeleteRequest) Schema() *z.StructSchema {
	return z.Struct(z.Shape{
		"SDHash": z.String().Required().Len(blob.BlobHashHexLength),
	})
}

// ToModel converts the StreamDeleteRequest to a model (self-reference for simple requests)
func (s *StreamDeleteRequest) ToModel() (*StreamDeleteRequest, error) {
	return s, nil
}
