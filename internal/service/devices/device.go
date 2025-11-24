package devices

import (
	"context"
	"errors"
	"fmt"
	"net"

	pluginCore "go.lumeweb.com/portal-plugin-lbry/core"
	"go.lumeweb.com/portal-plugin-lbry/internal/db"
	"go.lumeweb.com/portal/core"
	"go.lumeweb.com/queryutil"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

// DeviceServiceDefault implements the DeviceService interface for LBRY protocol
type DeviceServiceDefault struct {
	ctx    core.Context
	db     *gorm.DB
	logger *core.Logger
}

// NewDeviceService creates a new LBRY device service instance
func NewDeviceService() (core.Service, []core.ContextBuilderOption, error) {
	service := &DeviceServiceDefault{}

	return service, core.ContextOptions(
		core.ContextWithStartupFunc(func(ctx core.Context) error {
			service.ctx = ctx
			service.db = ctx.DB()
			service.logger = ctx.Logger()

			if service.db == nil {
				return fmt.Errorf("database not initialized")
			}

			return nil
		}),
	), nil
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
func (s *DeviceServiceDefault) CreateDevice(ctx context.Context, userID uint, name, ipAddress string) (*db.Device, error) {
	// Validate IP address
	if err := s.validateIPAddress(ipAddress); err != nil {
		return nil, err
	}

	// Check if device with this IP already exists (globally across all users)
	var existingDevice db.Device
	err := s.db.WithContext(ctx).Where("ip_address = ?", ipAddress).First(&existingDevice).Error
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, fmt.Errorf("failed to check existing device: %w", err)
	}
	if err == nil {
		return nil, fmt.Errorf("device with IP address %s already exists", ipAddress)
	}

	// Create new device
	device := &db.Device{
		UserID:    userID,
		Name:      name,
		IPAddress: ipAddress,
	}

	if err := s.db.WithContext(ctx).Create(device).Error; err != nil {
		return nil, fmt.Errorf("failed to create device: %w", err)
	}

	s.logger.Info("Device created successfully",
		zap.Uint("device_id", device.ID),
		zap.String("name", name),
		zap.String("ip_address", ipAddress))

	return device, nil
}

// UpdateDevice updates an existing device by ID (supports both name and IP for internal use)
func (s *DeviceServiceDefault) UpdateDevice(ctx context.Context, userID, id uint, name, ipAddress string) (*db.Device, error) {
	// Validate IP address
	if err := s.validateIPAddress(ipAddress); err != nil {
		return nil, err
	}

	// Check if device exists and belongs to user
	var device db.Device
	err := s.db.WithContext(ctx).Where("id = ? AND user_id = ?", id, userID).First(&device).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, fmt.Errorf("device not found")
		}
		return nil, fmt.Errorf("failed to find device: %w", err)
	}

	// Check if another device with this IP already exists (globally across all users)
	var existingDevice db.Device
	err = s.db.WithContext(ctx).Where("ip_address = ? AND id != ?", ipAddress, id).First(&existingDevice).Error
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, fmt.Errorf("failed to check existing device: %w", err)
	}
	if err == nil {
		return nil, fmt.Errorf("device with IP address %s already exists", ipAddress)
	}

	// Update device
	updates := map[string]interface{}{
		"name":       name,
		"ip_address": ipAddress,
	}

	if err := s.db.WithContext(ctx).Model(&device).Updates(updates).Error; err != nil {
		return nil, fmt.Errorf("failed to update device: %w", err)
	}

	// Refresh device data
	if err := s.db.WithContext(ctx).First(&device, id).Error; err != nil {
		return nil, fmt.Errorf("failed to refresh device data: %w", err)
	}

	s.logger.Info("Device updated successfully",
		zap.Uint("device_id", id),
		zap.String("name", name),
		zap.String("ip_address", ipAddress))

	return &device, nil
}

// UpdateDeviceName updates only the name of an existing device by ID
func (s *DeviceServiceDefault) UpdateDeviceName(ctx context.Context, userID, id uint, name string) (*db.Device, error) {
	// Check if device exists and belongs to user
	var device db.Device
	err := s.db.WithContext(ctx).Where("id = ? AND user_id = ?", id, userID).First(&device).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, fmt.Errorf("device not found")
		}
		return nil, fmt.Errorf("failed to find device: %w", err)
	}

	// Update device name only
	updates := map[string]interface{}{
		"name": name,
	}

	if err := s.db.WithContext(ctx).Model(&device).Updates(updates).Error; err != nil {
		return nil, fmt.Errorf("failed to update device: %w", err)
	}

	// Refresh device data
	if err := s.db.WithContext(ctx).First(&device, id).Error; err != nil {
		return nil, fmt.Errorf("failed to refresh device data: %w", err)
	}

	s.logger.Info("Device name updated successfully",
		zap.Uint("device_id", id),
		zap.String("name", name))

	return &device, nil
}

// GetDevice retrieves a device by ID
func (s *DeviceServiceDefault) GetDevice(ctx context.Context, userID, id uint) (*db.Device, error) {
	var device db.Device
	err := s.db.WithContext(ctx).Where("id = ? AND user_id = ?", id, userID).First(&device).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, err
		}
		return nil, fmt.Errorf("failed to get device: %w", err)
	}

	return &device, nil
}

// ListDevices returns a paginated list of devices
func (s *DeviceServiceDefault) ListDevices(ctx context.Context, userID uint, filters []queryutil.CrudFilter, sorts []queryutil.Sort, pagination queryutil.Pagination) ([]*db.Device, int64, error) {
	var devices []*db.Device
	var total int64

	// Build the base query with user filter
	baseQuery := s.db.WithContext(ctx).Model(&db.Device{}).Where("user_id = ?", userID)

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
	// First check if device exists at all
	var device db.Device
	err := s.db.WithContext(ctx).Where("id = ?", id).First(&device).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			// Idempotent: device doesn't exist, return success
			return nil
		}
		return fmt.Errorf("failed to find device: %w", err)
	}

	// Check if device belongs to the user
	if device.UserID != userID {
		return fmt.Errorf("device not found")
	}

	// Delete device (soft delete)
	if err := s.db.WithContext(ctx).Delete(&device).Error; err != nil {
		return fmt.Errorf("failed to delete device: %w", err)
	}

	s.logger.Info("Device deleted successfully",
		zap.Uint("device_id", id),
		zap.String("name", device.Name),
		zap.String("ip_address", device.IPAddress))

	return nil
}

// GetDeviceByIPAddress retrieves a device by IP address
func (s *DeviceServiceDefault) GetDeviceByIPAddress(ctx context.Context, userID uint, ipAddress string) (*db.Device, error) {
	var device db.Device
	err := s.db.WithContext(ctx).Where("ip_address = ? AND user_id = ?", ipAddress, userID).First(&device).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, fmt.Errorf("device not found")
		}
		return nil, fmt.Errorf("failed to get device by IP address: %w", err)
	}

	return &device, nil
}
