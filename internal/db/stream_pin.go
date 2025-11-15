package db

import (
	"gorm.io/gorm"
)

type StreamPin struct {
	gorm.Model
	UserID   uint64
	StreamID uint64
}

func (StreamPin) TableName() string {
	return "lbry_stream_pins"
}
