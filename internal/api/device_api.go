package api

import (
	"errors"
	"net/http"
	"strconv"

	"github.com/labstack/echo/v4"
	"go.lumeweb.com/httputil"
	mcontext "go.lumeweb.com/portal-middleware/context"
	"go.lumeweb.com/portal-plugin-lbry/internal/api/dto"
	"go.lumeweb.com/portal-plugin-lbry/internal/db"
	router "go.lumeweb.com/portal-router"
	"go.lumeweb.com/portal/core"
	"go.lumeweb.com/queryutil"
	queryutilhttp "go.lumeweb.com/queryutil/http"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

// setupDeviceRoutes defines all device-related routes
func (a *API) setupDeviceRoutes() []router.Route {
	// Create reusable schema for DeviceResponse
	deviceSchema := queryutil.NewSchemaProvider().ForType(&dto.DeviceResponse{})

	return []router.Route{
		router.NewRoute(
			http.MethodPost,
			"/devices",
			a.handleDeviceCreate,
			router.WithAccess(core.ACCESS_USER_ROLE),
			router.WithSwagger(
				router.WithSummary("Create a device"),
				router.WithDescription("Create a new device in the whitelist. This endpoint requires authentication."),
				router.WithRequestBody(&dto.CreateDeviceRequest{}, "Device creation request", true),
				router.WithSuccessResponse(http.StatusCreated, "Device created successfully", router.WithJSONContent(&dto.DeviceResponse{})),
			),
		),
		router.NewRoute(
			http.MethodPut,
			"/devices/:id",
			a.handleDeviceUpdate,
			router.WithAccess(core.ACCESS_USER_ROLE),
			router.WithSwagger(
				router.WithSummary("Update a device"),
				router.WithPathParam("id", "The ID of the device to update", "integer"),
				router.WithDescription("Update an existing device in the whitelist. This endpoint requires authentication and is idempotent."),
				router.WithRequestBody(&dto.UpdateDeviceRequest{}, "Device update request", true),
				router.WithSuccessResponse(http.StatusOK, "Device updated successfully", router.WithJSONContent(&dto.DeviceResponse{})),
			),
		),
		router.NewRoute(
			http.MethodGet,
			"/devices",
			a.handleDeviceList,
			router.WithAccess(core.ACCESS_USER_ROLE),
			router.WithSwagger(
				router.WithSummary("List devices"),
				router.WithDescription("List all devices in the whitelist with pagination, filtering, and sorting support."),
				router.WithSchema(deviceSchema),
				router.WithFilterParamsFromSchema(deviceSchema),
				router.WithSuccessResponse(http.StatusOK, "Devices retrieved successfully", router.WithJSONContent(&queryutil.Response[*dto.DeviceResponse]{})),
			),
		),
		router.NewRoute(
			http.MethodGet,
			"/devices/:id",
			a.handleDeviceGet,
			router.WithAccess(core.ACCESS_USER_ROLE),
			router.WithSwagger(
				router.WithSummary("Get a device"),
				router.WithPathParam("id", "The ID of the device to get", "integer"),
				router.WithDescription("Get a specific device from the whitelist by ID."),
				router.WithSuccessResponse(http.StatusOK, "Device retrieved successfully", router.WithJSONContent(&dto.DeviceResponse{})),
			),
		),
		router.NewRoute(
			http.MethodDelete,
			"/devices/:id",
			a.handleDeviceDelete,
			router.WithAccess(core.ACCESS_USER_ROLE),
			router.WithSwagger(
				router.WithSummary("Delete a device"),
				router.WithPathParam("id", "The ID of the device to delete", "integer"),
				router.WithDescription("Delete a device from the whitelist. This endpoint requires authentication and is idempotent."),
				router.WithSuccessResponse(http.StatusNoContent, "Device deleted successfully"),
			),
		),
	}
}

// handleDeviceCreate handles device creation requests
func (a *API) handleDeviceCreate(c echo.Context) error {
	ctx := httputil.Context(c)

	// Validate user authentication
	userID, err := mcontext.GetUserID(ctx.Context)
	if err != nil {
		apiErr := core.NewAccountError(core.ErrKeyLoginFailed, nil)
		_ = ctx.Error(apiErr, apiErr.HttpStatus())
		return nil
	}

	// Parse and validate request body using httputil
	var req dto.CreateDeviceRequest
	_, ok := httputil.DecodeAndValidateRequest(ctx, &req)
	if !ok {
		return nil
	}

	// Create device
	device, err := a.deviceService.CreateDevice(ctx.Request().Context(), uint(userID), req.Name, req.IPAddress)
	if err != nil {
		a.logger.Error("Failed to create device",
			zap.Error(err),
			zap.String("name", req.Name),
			zap.String("ip_address", req.IPAddress))
		_ = ctx.Error(NewError(ErrKeyDeviceCreateFailed, err), http.StatusBadRequest)
		return nil
	}

	// Prepare response
	response := &dto.DeviceResponse{
		ID:        device.ID,
		UserID:    device.UserID,
		Name:      device.Name,
		IPAddress: device.IPAddress,
		CreatedAt: device.CreatedAt,
		UpdatedAt: device.UpdatedAt,
	}

	a.logger.Info("Device created successfully",
		zap.Uint("device_id", device.ID),
		zap.String("name", device.Name),
		zap.String("ip_address", device.IPAddress))

	return ctx.JSON(http.StatusCreated, response)
}

// handleDeviceUpdate handles device update requests
func (a *API) handleDeviceUpdate(c echo.Context) error {
	ctx := httputil.Context(c)

	// Validate user authentication
	userID, err := mcontext.GetUserID(ctx.Context)
	if err != nil {
		apiErr := core.NewAccountError(core.ErrKeyLoginFailed, nil)
		_ = ctx.Error(apiErr, apiErr.HttpStatus())
		return nil
	}

	// Parse device ID from URL parameter
	deviceIDStr := c.Param("id")
	deviceID, err := strconv.ParseUint(deviceIDStr, 10, 32)
	if err != nil {
		_ = ctx.Error(NewError(ErrKeyDeviceDeleteFailed, err), http.StatusBadRequest)
		return nil
	}

	// Parse and validate request body using httputil
	var req dto.UpdateDeviceRequest
	_, ok := httputil.DecodeAndValidateRequest(ctx, &req)
	if !ok {
		return nil
	}

	device, err := a.deviceService.UpdateDeviceName(ctx.Request().Context(), uint(userID), uint(deviceID), req.Name)
	if err != nil {
		a.logger.Error("Failed to update device",
			zap.Error(err),

			zap.Uint("device_id", uint(deviceID)),
			zap.String("name", req.Name))
		_ = ctx.Error(NewError(ErrKeyDeviceUpdateFailed, err), http.StatusBadRequest)
		return nil
	}

	// Prepare response
	response := &dto.DeviceResponse{
		ID:        device.ID,
		UserID:    device.UserID,
		Name:      device.Name,
		IPAddress: device.IPAddress,
		CreatedAt: device.CreatedAt,
		UpdatedAt: device.UpdatedAt,
	}

	a.logger.Info("Device updated successfully",

		zap.Uint("device_id", device.ID),
		zap.String("name", device.Name))

	return ctx.JSON(http.StatusOK, response)
}

// handleDeviceList handles device list requests
func (a *API) handleDeviceList(c echo.Context) error {
	// Extract request context and authenticate user
	ctx := httputil.Context(c)

	// Extract user ID from the request context for authentication
	userID, err := mcontext.GetUserID(ctx.Context)
	if err != nil {
		// If user authentication fails, return an account error
		apiErr := core.NewAccountError(core.ErrKeyLoginFailed, nil)
		_ = ctx.Error(apiErr, apiErr.HttpStatus())
		return nil
	}

	// Use queryutilhttp.ProcessListRequest for standardized HTTP handling
	return queryutilhttp.ProcessListRequest(
		c.Response(),
		c.Request(),
		"devices",
		// Create service function that includes user filtering
		func(filters []queryutil.CrudFilter, sorts []queryutil.Sort, pagination queryutil.Pagination) ([]*db.Device, int64, error) {
			return a.deviceService.ListDevices(ctx.Request().Context(), uint(userID), filters, sorts, pagination)
		},
		// Convert domain entities to DTOs
		func(device *db.Device) *dto.DeviceResponse {
			return &dto.DeviceResponse{
				ID:        device.ID,
				UserID:    device.UserID,
				Name:      device.Name,
				IPAddress: device.IPAddress,
				CreatedAt: device.CreatedAt,
				UpdatedAt: device.UpdatedAt,
			}
		},
	)
}

// handleDeviceGet handles single device retrieval requests
func (a *API) handleDeviceGet(c echo.Context) error {
	ctx := httputil.Context(c)

	// Validate user authentication
	userID, err := mcontext.GetUserID(ctx.Context)
	if err != nil {
		apiErr := core.NewAccountError(core.ErrKeyLoginFailed, nil)
		_ = ctx.Error(apiErr, apiErr.HttpStatus())
		return nil
	}

	// Parse device ID from URL parameter
	deviceIDStr := c.Param("id")
	deviceID, err := strconv.ParseUint(deviceIDStr, 10, 32)
	if err != nil {
		_ = ctx.Error(NewError(ErrKeyDeviceGetFailed, err), http.StatusBadRequest)
		return nil
	}

	// Get device
	device, err := a.deviceService.GetDevice(ctx.Request().Context(), uint(userID), uint(deviceID))
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			a.logger.Error("Device not found",
				zap.Uint("device_id", uint(deviceID)))
			_ = ctx.Error(NewError(ErrKeyDeviceGetFailed, err), http.StatusNotFound)
		} else {
			a.logger.Error("Failed to get device",
				zap.Error(err),
				zap.Uint("device_id", uint(deviceID)))
			_ = ctx.Error(NewError(ErrKeyDeviceGetFailed, err), http.StatusInternalServerError)
		}
		return nil
	}

	// Prepare response
	response := &dto.DeviceResponse{
		ID:        device.ID,
		UserID:    device.UserID,
		Name:      device.Name,
		IPAddress: device.IPAddress,
		CreatedAt: device.CreatedAt,
		UpdatedAt: device.UpdatedAt,
	}

	return ctx.JSON(http.StatusOK, response)
}

// handleDeviceDelete handles device deletion requests
func (a *API) handleDeviceDelete(c echo.Context) error {
	ctx := httputil.Context(c)

	// Validate user authentication
	userID, err := mcontext.GetUserID(ctx.Context)
	if err != nil {
		apiErr := core.NewAccountError(core.ErrKeyLoginFailed, nil)
		_ = ctx.Error(apiErr, apiErr.HttpStatus())
		return nil
	}

	// Parse device ID from URL parameter
	deviceIDStr := c.Param("id")
	deviceID, err := strconv.ParseUint(deviceIDStr, 10, 32)
	if err != nil {
		_ = ctx.Error(NewError(ErrKeyDeviceUpdateFailed, err), http.StatusBadRequest)
		return nil
	}

	// Delete device (idempotent)
	err = a.deviceService.DeleteDevice(ctx.Request().Context(), uint(userID), uint(deviceID))
	if err != nil {
		a.logger.Error("Failed to delete device",
			zap.Error(err),

			zap.Uint("device_id", uint(deviceID)))
		_ = ctx.Error(NewError(ErrKeyDeviceDeleteFailed, err), http.StatusBadRequest)
		return nil
	}

	a.logger.Info("Device deleted successfully",

		zap.Uint("device_id", uint(deviceID)))

	return ctx.NoContent(http.StatusNoContent)
}
