package db

import (
	"gorm.io/gorm"
)

type Stream struct {
	gorm.Model
	StreamHash        string
	SDHash            string
	StreamName        string
	StreamType        string
	SuggestedFileName string
	KeyData           []byte
	StreamPin         []StreamPin `gorm:"foreignKey:StreamID"`

	// TerminatingBlobNumber is the blob number of the terminating blob in this stream, if any
	// NULL indicates no terminating blob
	TerminatingBlobNumber *int
}

func (Stream) TableName() string {
	return "lbry_streams"
}
