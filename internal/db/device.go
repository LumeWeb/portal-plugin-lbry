package db

import (
	"gorm.io/gorm"
)

// Device represents a whitelisted device for access control
type Device struct {
	gorm.Model
	// UserID is the owner of this device
	UserID uint `gorm:"not null;index"`

	// Name is a human-readable name for the device
	Name string

	// IPAddress is the IP address (IPv4 or IPv6) of the device, must be unique
	IPAddress string
}

// TableName returns the table name for the Device model
func (Device) TableName() string {
	return "lbry_devices"
}
