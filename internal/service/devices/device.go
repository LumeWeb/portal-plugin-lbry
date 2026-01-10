package devices

import (
	"context"
	"errors"
	"fmt"
	"net"

	pluginCore "go.lumeweb.com/portal-plugin-lbry/core"
	pluginDb "go.lumeweb.com/portal-plugin-lbry/internal/db"
	"go.lumeweb.com/portal/core"
	"go.lumeweb.com/portal/db"
	"go.lumeweb.com/queryutil"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

// DeviceServiceDefault implements the DeviceService interface for LBRY protocol
type DeviceServiceDefault struct {
	*core.BaseComponent
}

// NewDeviceService creates a new LBRY device service instance
func NewDeviceService() (core.Service, []core.ContextBuilderOption, error) {
	service := &DeviceServiceDefault{}

	return service, nil, nil
}

// Name returns the service name
func (s *DeviceServiceDefault) Name() string {
	return pluginCore.DEVICE_SERVICE
}

// ID returns the service ID
func (s *DeviceServiceDefault) ID() string {
	return pluginCore.DEVICE_SERVICE
}

// validateIPAddress validates that the IP address is valid IPv4 or IPv6
func (s *DeviceServiceDefault) validateIPAddress(ipAddress string) error {
	if net.ParseIP(ipAddress) == nil {
		return fmt.Errorf("invalid IP address: %s", ipAddress)
	}
	return nil
}

// CreateDevice creates a new device in the whitelist
func (s *DeviceServiceDefault) CreateDevice(ctx context.Context, userID uint, name, ipAddress string) (*pluginDb.Device, error) {
	// Validate IP address
	if err := s.validateIPAddress(ipAddress); err != nil {
		return nil, err
	}

	var device *pluginDb.Device
	if err := db.RetryableComponentTransaction(s, ctx, func(tx *gorm.DB) *gorm.DB {
		// Check if device with this IP already exists
		var existingDevice pluginDb.Device
		if err := tx.Where("ip_address = ?", ipAddress).First(&existingDevice).Error; err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			_ = tx.AddError(fmt.Errorf("failed to check existing device: %w", err))
			return tx
		}
		if existingDevice.ID != 0 {
			_ = tx.AddError(fmt.Errorf("device with IP address %s already exists", ipAddress))
			return tx
		}

		// Create new device
		device = &pluginDb.Device{
			UserID:    userID,
			Name:      name,
			IPAddress: ipAddress,
		}

		if err := tx.Create(device).Error; err != nil {
			_ = tx.AddError(err)
			return tx
		}
		return tx
	}); err != nil {
		return nil, err
	}

	s.Logger().Info("Device created successfully",
		zap.Uint("device_id", device.ID),
		zap.String("name", name),
		zap.String("ip_address", ipAddress))

	return device, nil
}

// UpdateDevice updates an existing device by ID (supports both name and IP for internal use)
func (s *DeviceServiceDefault) UpdateDevice(ctx context.Context, userID, id uint, name, ipAddress string) (*pluginDb.Device, error) {
	// Validate IP address
	if err := s.validateIPAddress(ipAddress); err != nil {
		return nil, err
	}

	var device pluginDb.Device
	if err := db.RetryableComponentTransaction(s, ctx, func(tx *gorm.DB) *gorm.DB {
		// Check if device exists and belongs to user
		if err := tx.Where("id = ? AND user_id = ?", id, userID).First(&device).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				_ = tx.AddError(err)
				return tx
			}
			_ = tx.AddError(fmt.Errorf("failed to find device: %w", err))
			return tx
		}

		// Check if another device with this IP already exists
		var existingDevice pluginDb.Device
		if err := tx.Where("ip_address = ? AND id != ?", ipAddress, id).First(&existingDevice).Error; err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			_ = tx.AddError(fmt.Errorf("failed to check existing device: %w", err))
			return tx
		}
		if existingDevice.ID != 0 && existingDevice.ID != id {
			_ = tx.AddError(fmt.Errorf("device with IP address %s already exists", ipAddress))
			return tx
		}

		// Update device
		updates := map[string]interface{}{
			"name":       name,
			"ip_address": ipAddress,
		}

		if err := tx.Model(&device).Updates(updates).Error; err != nil {
			_ = tx.AddError(fmt.Errorf("failed to update device: %w", err))
			return tx
		}

		// Refresh device data
		return tx.First(&device, id)
	}); err != nil {
		return nil, err
	}

	s.Logger().Info("Device updated successfully",
		zap.Uint("device_id", id),
		zap.String("name", name),
		zap.String("ip_address", ipAddress))

	return &device, nil
}

// UpdateDeviceName updates only the name of an existing device by ID
func (s *DeviceServiceDefault) UpdateDeviceName(ctx context.Context, userID, id uint, name string) (*pluginDb.Device, error) {
	var device pluginDb.Device
	if err := db.RetryableComponentTransaction(s, ctx, func(tx *gorm.DB) *gorm.DB {
		// Check if device exists and belongs to user
		if err := tx.Where("id = ? AND user_id = ?", id, userID).First(&device).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				_ = tx.AddError(err)
				return tx
			}
			_ = tx.AddError(fmt.Errorf("failed to find device: %w", err))
			return tx
		}

		// Update device name only
		updates := map[string]interface{}{
			"name": name,
		}

		if err := tx.Model(&device).Updates(updates).Error; err != nil {
			_ = tx.AddError(fmt.Errorf("failed to update device: %w", err))
			return tx
		}

		// Refresh device data
		return tx.First(&device, id)
	}); err != nil {
		return nil, err
	}

	s.Logger().Info("Device name updated successfully",
		zap.Uint("device_id", id),
		zap.String("name", name))

	return &device, nil
}

// GetDevice retrieves a device by ID
func (s *DeviceServiceDefault) GetDevice(ctx context.Context, userID, id uint) (*pluginDb.Device, error) {
	var device pluginDb.Device
	err := s.DB().WithContext(ctx).Where("id = ? AND user_id = ?", id, userID).First(&device).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, err
		}
		return nil, fmt.Errorf("failed to get device: %w", err)
	}

	return &device, nil
}

// ListDevices returns a paginated list of devices
func (s *DeviceServiceDefault) ListDevices(ctx context.Context, userID uint, filters []queryutil.CrudFilter, sorts []queryutil.Sort, pagination queryutil.Pagination) ([]*pluginDb.Device, int64, error) {
	var devices []*pluginDb.Device
	var total int64

	// Build the base query with user filter
	baseQuery := s.DB().WithContext(ctx).Model(&pluginDb.Device{}).Where("user_id = ?", userID)

	// Apply filters to base query
	filteredQuery := queryutil.ApplyFilters(baseQuery, filters, nil)

	// Count total records using filtered query
	if err := filteredQuery.Count(&total).Error; err != nil {
		return nil, 0, fmt.Errorf("failed to count devices: %w", err)
	}

	// Apply sorting and pagination to filtered query for the final result
	finalQuery := queryutil.ApplySort(filteredQuery, sorts)
	finalQuery = queryutil.ApplyPagination(finalQuery, pagination)

	// Execute final query
	if err := finalQuery.Find(&devices).Error; err != nil {
		return nil, 0, fmt.Errorf("failed to list devices: %w", err)
	}

	return devices, total, nil
}

// DeleteDevice removes a device by ID (idempotent)
func (s *DeviceServiceDefault) DeleteDevice(ctx context.Context, userID, id uint) error {
	var device pluginDb.Device
	if err := db.RetryableComponentTransaction(s, ctx, func(tx *gorm.DB) *gorm.DB {
		// First check if device exists at all
		if err := tx.Where("id = ?", id).First(&device).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				// Idempotent: device doesn't exist, return success (no error)
				return tx
			}
			_ = tx.AddError(fmt.Errorf("failed to find device: %w", err))
			return tx
		}

		// Check if device belongs to the user
		if device.UserID != userID {
			_ = tx.AddError(gorm.ErrRecordNotFound)
			return tx
		}

		// Delete device (soft delete)
		return tx.Delete(&device)
	}); err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil
		}
		return err
	}

	s.Logger().Info("Device deleted successfully",
		zap.Uint("device_id", id),
		zap.String("name", device.Name),
		zap.String("ip_address", device.IPAddress))

	return nil
}

// GetDeviceByIPAddress retrieves a device by IP address
func (s *DeviceServiceDefault) GetDeviceByIPAddress(ctx context.Context, ipAddress string) (*pluginDb.Device, error) {
	var device pluginDb.Device
	err := s.DB().WithContext(ctx).Where("ip_address = ?", ipAddress).First(&device).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, err
		}
		return nil, fmt.Errorf("failed to get device by IP address: %w", err)
	}

	return &device, nil
}
