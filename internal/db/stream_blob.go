package db

import (
	"gorm.io/gorm"
)

type StreamBlob struct {
	gorm.Model
	StreamID   uint64
	BlobID     uint64
	BlobNumber int
}

func (StreamBlob) TableName() string {
	return "lbry_stream_blobs"
}
