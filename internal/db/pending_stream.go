package db

import "gorm.io/gorm"

// PendingStream represents a stream that has been uploaded but not yet assembled into active state
type PendingStream struct {
	gorm.Model
	// StreamHash is the hash of the stream
	StreamHash string

	// SDHash is the hash of the SD blob
	SDHash string

	// StreamName is the name of the stream
	StreamName string

	// StreamType is the type of the stream
	StreamType string

	// SuggestedFileName is the suggested filename for the stream
	SuggestedFileName string

	// KeyData contains the encryption key data
	KeyData []byte

	// TotalBlobs is the total number of blobs in this stream
	TotalBlobs int

	// UserID is the owner of this pending stream
	UserID uint

	// DeviceID is the device that uploaded this stream
	DeviceID uint

	// TerminatingBlobNumber is the blob number of the terminating blob in this stream, if any
	// NULL indicates no terminating blob
	TerminatingBlobNumber *int
}

// TableName returns the table name for PendingStream
func (PendingStream) TableName() string {
	return "lbry_pending_streams"
}
