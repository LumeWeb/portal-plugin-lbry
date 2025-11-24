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
}

func (Stream) TableName() string {
	return "lbry_streams"
}
