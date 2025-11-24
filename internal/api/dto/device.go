package dto

import (
	"time"

	z "github.com/Oudwins/zog"
	"go.lumeweb.com/httputil"
)

// Ensure DTOs implement required interfaces
var _ httputil.DTOResponse[*DeviceResponse] = (*DeviceResponse)(nil)
var _ httputil.DTOValidator = (*CreateDeviceRequest)(nil)
var _ httputil.DTOValidator = (*UpdateDeviceRequest)(nil)

// DeviceResponse represents a device in the whitelist
type DeviceResponse struct {
	// ID is the unique identifier for the device
	ID uint `json:"id"`

	// UserID is the owner of the device
	UserID uint `json:"user_id"`

	// Name is the human-readable name for the device
	Name string `json:"name"`

	// IPAddress is the IP address (IPv4 or IPv6) of the device
	IPAddress string `json:"ip_address"`

	// CreatedAt is when the device was created
	CreatedAt time.Time `json:"created_at"`

	// UpdatedAt is when the device was last updated
	UpdatedAt time.Time `json:"updated_at"`
}

// FromModel creates a DeviceResponse from a db.Device model
func (d *DeviceResponse) FromModel(model *DeviceResponse) error {
	if model != nil {
		d.ID = model.ID
		d.UserID = model.UserID
		d.Name = model.Name
		d.IPAddress = model.IPAddress
		d.CreatedAt = model.CreatedAt
		d.UpdatedAt = model.UpdatedAt
	}
	return nil
}

// CreateDeviceRequest represents a request to create a new device
type CreateDeviceRequest struct {
	// Name is the human-readable name for the device
	Name string `json:"name"`

	// IPAddress is the IP address (IPv4 or IPv6) of the device
	IPAddress string `json:"ip_address"`
}

// Schema returns a Zog schema for validating CreateDeviceRequest
func (c *CreateDeviceRequest) Schema() *z.StructSchema {
	return z.Struct(z.Shape{
		"Name": z.String().
			Required().
			Min(1).
			Max(255),
		"IPAddress": z.String().
			Required().
			IP(),
	})
}

// ToModel converts the CreateDeviceRequest to a simple model representation
func (c *CreateDeviceRequest) ToModel() (*CreateDeviceRequest, error) {
	return c, nil
}

// UpdateDeviceRequest represents a request to update an existing device
type UpdateDeviceRequest struct {
	// Name is the human-readable name for the device
	Name string `json:"name"`
}

// Schema returns a Zog schema for validating UpdateDeviceRequest
func (u *UpdateDeviceRequest) Schema() *z.StructSchema {
	return z.Struct(z.Shape{
		"Name": z.String().
			Required().
			Min(1).
			Max(255),
	})
}

// ToModel converts the UpdateDeviceRequest to a simple model representation
func (u *UpdateDeviceRequest) ToModel() (*UpdateDeviceRequest, error) {
	return u, nil
}
