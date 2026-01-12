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

	// TerminatingBlobIV is the IV (initialization vector) for the terminating blob, if any
	// NULL indicates no IV for the terminating blob
	TerminatingBlobIV []byte
}

func (Stream) TableName() string {
	return "lbry_streams"
}
