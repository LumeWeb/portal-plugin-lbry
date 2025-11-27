package core

import (
	"context"

	"go.lumeweb.com/portal-plugin-lbry/internal/db"
	portalCore "go.lumeweb.com/portal/core"
	"go.lumeweb.com/queryutil"
)

// DeviceService handles device whitelist management for LBRY protocol
type DeviceService interface {
	portalCore.Service

	// CreateDevice creates a new device in the whitelist
	CreateDevice(ctx context.Context, userID uint, name, ipAddress string) (*db.Device, error)

	// UpdateDevice updates an existing device by ID (idempotent)
	UpdateDevice(ctx context.Context, userID, id uint, name, ipAddress string) (*db.Device, error)

	// UpdateDeviceName updates only the name of an existing device by ID
	UpdateDeviceName(ctx context.Context, userID, id uint, name string) (*db.Device, error)

	// GetDevice retrieves a device by ID
	GetDevice(ctx context.Context, userID, id uint) (*db.Device, error)

	// ListDevices returns a paginated list of devices
	ListDevices(ctx context.Context, userID uint, filters []queryutil.CrudFilter, sorts []queryutil.Sort, pagination queryutil.Pagination) ([]*db.Device, int64, error)

	// DeleteDevice removes a device by ID (idempotent)
	DeleteDevice(ctx context.Context, userID, id uint) error

	// GetDeviceByIPAddress retrieves a device by IP address
	GetDeviceByIPAddress(ctx context.Context, ipAddress string) (*db.Device, error)
}

// Device service name
const DEVICE_SERVICE = "lbry.device"
