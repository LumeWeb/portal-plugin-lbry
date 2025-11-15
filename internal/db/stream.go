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
}

func (Stream) TableName() string {
	return "lbry_streams"
}
