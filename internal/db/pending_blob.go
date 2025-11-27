package db

import (
	"gorm.io/gorm"
)

// PendingBlob represents a regular blob that has been uploaded but not yet assembled into a stream
type PendingBlob struct {
	gorm.Model
	// BlobHash is the hash of the uploaded blob
	BlobHash string

	// UserID is the owner of this pending blob
	UserID uint

	// DeviceID is the device that uploaded this blob
	DeviceID uint

	// StreamID is the reference to the pending stream this blob belongs to
	StreamID uint

	// BlobSize is the size of the blob in bytes
	BlobSize int

	// BlobNumber is the sequence number of this blob in the stream
	BlobNumber int

	// Received indicates whether the blob data has been received (true) or is still waiting (false)
	Received bool

	// IV is the initialization vector for the blob
	IVData []byte
}

func (PendingBlob) TableName() string {
	return "lbry_pending_blobs"
}
