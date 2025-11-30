package db

import (
	"gorm.io/gorm"
)

type Blob struct {
	gorm.Model
	BlobHash    string
	BlobSize    int
	IVData      []byte
	Terminating bool
}

func (Blob) TableName() string {
	return "lbry_blobs"
}
