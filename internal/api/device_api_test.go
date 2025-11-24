package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	pluginCore "go.lumeweb.com/portal-plugin-lbry/core"
	"go.lumeweb.com/portal-plugin-lbry/internal"
	"go.lumeweb.com/portal-plugin-lbry/internal/api/dto"
	pluginConfig "go.lumeweb.com/portal-plugin-lbry/internal/config"
	"go.lumeweb.com/portal-plugin-lbry/internal/db/migrations"
	"go.lumeweb.com/portal-plugin-lbry/internal/protocol"
	"go.lumeweb.com/portal-plugin-lbry/internal/service/devices"
	"go.lumeweb.com/portal-plugin-lbry/internal/service/upload"
	pluginTesting "go.lumeweb.com/portal-plugin-lbry/internal/testing"
	"go.lumeweb.com/portal/core"
	coreTesting "go.lumeweb.com/portal/core/testing"
	"go.lumeweb.com/portal/service"
	"go.lumeweb.com/queryutil"
)

func getDeviceTestOptions() coreTesting.TestContextBuilderOption {
	freePeerPort, err := pluginTesting.GetFreePort()
	if err != nil {
		panic(err)
	}
	freeDhtPort, err := pluginTesting.GetFreePort()
	if err != nil {
		panic(err)
	}
	freeReflectorPort, err := pluginTesting.GetFreePort()
	if err != nil {
		panic(err)
	}

	return coreTesting.CombineOptions(
		coreTesting.WithServiceFactory(core.CRON_SERVICE, service.NewCronService),
		coreTesting.WithServiceFactory(core.UPLOAD_SERVICE, service.NewMetadataService),
		coreTesting.WithServiceFactory(core.PIN_SERVICE, service.NewPinService),
		coreTesting.WithServiceFactory(core.REQUEST_SERVICE, service.NewRequestService),
		coreTesting.WithServiceFactory(core.WORKFLOW_SERVICE, service.NewWorkflowCoordinator),
		coreTesting.WithServiceFactory(core.USER_SERVICE, service.NewUserService),
		coreTesting.WithServiceFactory(core.AUTH_SERVICE, service.NewAuthService),
		coreTesting.WithServiceFactory(core.STORAGE_SERVICE, service.NewStorageService),
		coreTesting.WithServiceFactory(pluginCore.UPLOAD_SERVICE, upload.NewUploadService),
		coreTesting.WithServiceFactory(pluginCore.DEVICE_SERVICE, devices.NewDeviceService),
		coreTesting.WithAPI(internal.ProtocolName, NewAPI),
		coreTesting.WithAPIID(internal.ProtocolName),
		coreTesting.WithProtocol(internal.ProtocolName, protocol.NewProtocol),
		coreTesting.WithConfig("plugin.lbry.protocol.peer_port", uint(freePeerPort)),
		coreTesting.WithConfig("plugin.lbry.protocol.dht_port", uint(freeDhtPort)),
		coreTesting.WithConfig("plugin.lbry.protocol.reflector_port", uint(freeReflectorPort)),
		coreTesting.WithConfig("plugin.lbry.protocol.dht_seed_peers", []string{"0.0.0.1"}),
		coreTesting.WithProtocolConfig(internal.ProtocolName, pluginConfig.ProtocolConfig{
			PeerPort:      uint(freePeerPort),
			DHTPort:       uint(freeDhtPort),
			ReflectorPort: uint(freeReflectorPort),
		}),
		coreTesting.WithSQLitePluginMigrations(
			internal.ProtocolName, migrations.GetSQLite(),
		),
	)
}

func createTestUserAndLoginForDevice(ctx coreTesting.TestContext) (string, uint) {
	userSvc := core.GetService[core.UserService](ctx, core.USER_SERVICE)
	require.NotNil(ctx.T(), userSvc)

	authSvc := core.GetService[core.AuthService](ctx, core.AUTH_SERVICE)
	require.NotNil(ctx.T(), authSvc)

	// Create test user
	user, err := userSvc.CreateAccount(TestEmail, TestPassword, false)
	require.NoError(ctx.T(), err)

	// Login to get token
	token, _, err := authSvc.LoginPassword(TestEmail, TestPassword, TestIP, false)
	require.NoError(ctx.T(), err)

	return token, user.ID
}

func TestDeviceAPI_CreateDevice(t *testing.T) {
	tests := []struct {
		name           string
		requestBody    dto.CreateDeviceRequest
		expectedStatus int
		expectError    bool
		errorContains  string
	}{
		{
			name: "successful device creation with IPv4",
			requestBody: dto.CreateDeviceRequest{
				Name:      "Test Device IPv4",
				IPAddress: "192.168.1.100",
			},
			expectedStatus: http.StatusCreated,
			expectError:    false,
		},
		{
			name: "successful device creation with IPv6",
			requestBody: dto.CreateDeviceRequest{
				Name:      "Test Device IPv6",
				IPAddress: "2001:db8::1",
			},
			expectedStatus: http.StatusCreated,
			expectError:    false,
		},
		{
			name: "invalid IP address",
			requestBody: dto.CreateDeviceRequest{
				Name:      "Invalid Device",
				IPAddress: "invalid.ip.address",
			},
			expectedStatus: http.StatusBadRequest,
			expectError:    true,
		},
		{
			name: "missing name",
			requestBody: dto.CreateDeviceRequest{
				IPAddress: "192.168.1.100",
			},
			expectedStatus: http.StatusBadRequest,
			expectError:    true,
		},
		{
			name: "missing IP address",
			requestBody: dto.CreateDeviceRequest{
				Name: "Test Device",
			},
			expectedStatus: http.StatusBadRequest,
			expectError:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
				// Arrange
				token, _ := createTestUserAndLoginForDevice(ctx)

				// Create request body
				reqBody, err := json.Marshal(tt.requestBody)
				require.NoError(tb, err)

				// Create HTTP request using test context helper
				req := ctx.NewAPIRequest(http.MethodPost, "/api/devices", reqBody)
				req.Header.Set("Content-Type", "application/json")
				req.Header.Set("Authorization", "Bearer "+token)

				// Create response recorder
				rec := httptest.NewRecorder()

				// Serve the request
				ctx.Router().ServeHTTP(rec, req)

				// Assert
				assert.Equal(tb, tt.expectedStatus, rec.Code)

				if !tt.expectError {
					var response dto.DeviceResponse
					err := json.Unmarshal(rec.Body.Bytes(), &response)
					require.NoError(tb, err)
					assert.Equal(tb, tt.requestBody.Name, response.Name)
					assert.Equal(tb, tt.requestBody.IPAddress, response.IPAddress)
					assert.NotZero(tb, response.ID)
				}
			}, getDeviceTestOptions())
		})
	}
}

func TestDeviceAPI_UpdateDevice(t *testing.T) {
	tests := []struct {
		name           string
		setupDevice    func(ctx coreTesting.TestContext, token string) (uint, error)
		requestBody    dto.UpdateDeviceRequest
		expectedStatus int
		expectError    bool
	}{
		{
			name: "successful device update",
			setupDevice: func(ctx coreTesting.TestContext, token string) (uint, error) {
				createReq := dto.CreateDeviceRequest{
					Name:      "Original Device",
					IPAddress: "192.168.1.100",
				}
				reqBody, _ := json.Marshal(createReq)
				req := ctx.NewAPIRequest(http.MethodPost, "/api/devices", reqBody)
				req.Header.Set("Content-Type", "application/json")
				req.Header.Set("Authorization", "Bearer "+token)

				rec := httptest.NewRecorder()
				ctx.Router().ServeHTTP(rec, req)
				if rec.Code != http.StatusCreated {
					return 0, fmt.Errorf("failed to create device")
				}

				var createdDevice dto.DeviceResponse
				err := json.Unmarshal(rec.Body.Bytes(), &createdDevice)
				if err != nil {
					return 0, err
				}
				return createdDevice.ID, nil
			},
			requestBody: dto.UpdateDeviceRequest{
				Name: "Updated Device",
			},
			expectedStatus: http.StatusOK,
			expectError:    false,
		},
		{
			name: "device not found",
			setupDevice: func(ctx coreTesting.TestContext, token string) (uint, error) {
				return 999, nil // Non-existent device ID
			},
			requestBody: dto.UpdateDeviceRequest{
				Name: "Updated Device",
			},
			expectedStatus: http.StatusBadRequest,
			expectError:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
				// Arrange
				token, _ := createTestUserAndLoginForDevice(ctx)
				deviceID, err := tt.setupDevice(ctx, token)
				require.NoError(tb, err)

				// Create request body
				reqBody, err := json.Marshal(tt.requestBody)
				require.NoError(t, err)

				// Create HTTP request using test context helper
				url := "/api/devices/" + strconv.FormatUint(uint64(deviceID), 10)
				req := ctx.NewAPIRequest(http.MethodPut, url, reqBody)
				req.Header.Set("Content-Type", "application/json")
				req.Header.Set("Authorization", "Bearer "+token)

				// Create response recorder
				rec := httptest.NewRecorder()

				// Serve the request
				ctx.Router().ServeHTTP(rec, req)

				// Assert
				assert.Equal(t, tt.expectedStatus, rec.Code)

				if !tt.expectError {
					var response dto.DeviceResponse
					err := json.Unmarshal(rec.Body.Bytes(), &response)
					require.NoError(t, err)
					assert.Equal(t, tt.requestBody.Name, response.Name)
					assert.Equal(t, deviceID, response.ID)
					// IP address should remain unchanged from original device
					assert.Equal(t, "192.168.1.100", response.IPAddress)
				}
			}, getDeviceTestOptions())
		})
	}
}

func TestDeviceAPI_ListDevices(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		token, _ := createTestUserAndLoginForDevice(ctx)

		// Create test devices
		_devices := []dto.CreateDeviceRequest{
			{Name: "Device 1", IPAddress: "192.168.1.100"},
			{Name: "Device 2", IPAddress: "192.168.1.101"},
			{Name: "Device 3", IPAddress: "2001:db8::1"},
		}

		for _, device := range _devices {
			reqBody, _ := json.Marshal(device)
			req := ctx.NewAPIRequest(http.MethodPost, "/api/devices", reqBody)
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Authorization", "Bearer "+token)

			rec := httptest.NewRecorder()
			ctx.Router().ServeHTTP(rec, req)
			require.Equal(tb, http.StatusCreated, rec.Code)
		}

		tests := []struct {
			name           string
			filters        []queryutil.CrudFilter
			sorts          []queryutil.Sort
			pagination     *queryutil.Pagination
			expectedStatus int
			expectResults  bool
		}{
			{
				name:           "list all devices",
				filters:        []queryutil.CrudFilter{},
				sorts:          []queryutil.Sort{},
				pagination:     nil,
				expectedStatus: http.StatusOK,
				expectResults:  true,
			},
			{
				name: "filter by name",
				filters: []queryutil.CrudFilter{
					queryutil.Equal("name", "Device 1"),
				},
				sorts:          []queryutil.Sort{},
				pagination:     nil,
				expectedStatus: http.StatusOK,
				expectResults:  true,
			},
			{
				name: "filter by IP address",
				filters: []queryutil.CrudFilter{
					queryutil.Equal("ip_address", "192.168.1.100"),
				},
				sorts:          []queryutil.Sort{},
				pagination:     nil,
				expectedStatus: http.StatusOK,
				expectResults:  true,
			},
			{
				name:           "pagination",
				filters:        []queryutil.CrudFilter{},
				sorts:          []queryutil.Sort{},
				pagination:     &queryutil.Pagination{Start: 0, End: 2, PageSize: 2},
				expectedStatus: http.StatusOK,
				expectResults:  true,
			},
			{
				name: "filter by name with pagination",
				filters: []queryutil.CrudFilter{
					queryutil.Equal("name", "Device 1"),
				},
				sorts:          []queryutil.Sort{},
				pagination:     nil,
				expectedStatus: http.StatusOK,
				expectResults:  true,
			},
			{
				name: "sort by name ascending",
				filters: []queryutil.CrudFilter{
					queryutil.Equal("name", "Device 1"),
				},
				sorts: []queryutil.Sort{
					{Field: "name", Order: "asc"},
				},
				pagination:     nil,
				expectedStatus: http.StatusOK,
				expectResults:  true,
			},
			{
				name: "sort by name descending",
				filters: []queryutil.CrudFilter{
					queryutil.Equal("name", "Device 1"),
				},
				sorts: []queryutil.Sort{
					{Field: "name", Order: "desc"},
				},
				pagination:     nil,
				expectedStatus: http.StatusOK,
				expectResults:  true,
			},
			{
				name: "sort by IP address ascending",
				filters: []queryutil.CrudFilter{
					queryutil.Equal("name", "Device 1"),
				},
				sorts: []queryutil.Sort{
					{Field: "ip_address", Order: "asc"},
				},
				pagination:     nil,
				expectedStatus: http.StatusOK,
				expectResults:  true,
			},
			{
				name: "sort by IP address descending",
				filters: []queryutil.CrudFilter{
					queryutil.Equal("name", "Device 1"),
				},
				sorts: []queryutil.Sort{
					{Field: "ip_address", Order: "desc"},
				},
				pagination:     nil,
				expectedStatus: http.StatusOK,
				expectResults:  true,
			},
			{
				name: "sort by created_at ascending",
				filters: []queryutil.CrudFilter{
					queryutil.Equal("name", "Device 1"),
				},
				sorts: []queryutil.Sort{
					{Field: "created_at", Order: "asc"},
				},
				pagination:     nil,
				expectedStatus: http.StatusOK,
				expectResults:  true,
			},
			{
				name: "sort by created_at descending",
				filters: []queryutil.CrudFilter{
					queryutil.Equal("name", "Device 1"),
				},
				sorts: []queryutil.Sort{
					{Field: "created_at", Order: "desc"},
				},
				pagination:     nil,
				expectedStatus: http.StatusOK,
				expectResults:  true,
			},
			{
				name: "multiple filters",
				filters: []queryutil.CrudFilter{
					queryutil.Equal("name", "Device 1"),
					queryutil.Equal("ip_address", "192.168.1.100"),
				},
				sorts:          []queryutil.Sort{},
				pagination:     nil,
				expectedStatus: http.StatusOK,
				expectResults:  true,
			},
			{
				name: "filter with sort and pagination",
				filters: []queryutil.CrudFilter{
					queryutil.Equal("name", "Device 1"),
				},
				sorts: []queryutil.Sort{
					{Field: "created_at", Order: "asc"},
				},
				pagination:     nil,
				expectedStatus: http.StatusOK,
				expectResults:  true,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				// Build URL with filters, sorts, and pagination
				baseURL := "/api/devices"
				var requestURL string
				var err error

				if tt.filters != nil || tt.sorts != nil || tt.pagination != nil {
					requestURL, err = queryutil.BuildURL(baseURL, tt.sorts, tt.pagination, tt.filters...)
					if err != nil {
						tb.Fatalf("Failed to build URL: %v", err)
					}
				} else {
					requestURL = baseURL
				}

				// Create HTTP request using test context helper
				req := ctx.NewAPIRequest(http.MethodGet, requestURL, nil)
				req.Header.Set("Authorization", "Bearer "+token)

				// Create response recorder
				rec := httptest.NewRecorder()

				// Serve the request
				ctx.Router().ServeHTTP(rec, req)

				// Assert
				assert.Equal(t, tt.expectedStatus, rec.Code)

				if tt.expectResults {
					var response queryutil.Response[[]*dto.DeviceResponse]
					err := json.Unmarshal(rec.Body.Bytes(), &response)
					require.NoError(t, err)
					assert.NotNil(t, response.Data)
					assert.NotZero(t, response.Total)

					// Additional checks for data structure
					if len(response.Data) > 0 {
						item := response.Data[0]
						assert.NotZero(t, item.ID)
						assert.NotEmpty(t, item.Name)
						assert.NotEmpty(t, item.IPAddress)
						assert.False(t, item.CreatedAt.IsZero())
						assert.False(t, item.UpdatedAt.IsZero())
					}
				}

			})
		}
	}, getDeviceTestOptions())
}

func TestDeviceAPI_GetDevice(t *testing.T) {
	tests := []struct {
		name           string
		setupDevice    func(ctx coreTesting.TestContext, token string) (uint, error)
		expectedStatus int
		expectError    bool
	}{
		{
			name: "successful device retrieval",
			setupDevice: func(ctx coreTesting.TestContext, token string) (uint, error) {
				createReq := dto.CreateDeviceRequest{
					Name:      "Test Device",
					IPAddress: "192.168.1.100",
				}
				reqBody, _ := json.Marshal(createReq)
				req := ctx.NewAPIRequest(http.MethodPost, "/api/devices", reqBody)
				req.Header.Set("Content-Type", "application/json")
				req.Header.Set("Authorization", "Bearer "+token)

				rec := httptest.NewRecorder()
				ctx.Router().ServeHTTP(rec, req)
				if rec.Code != http.StatusCreated {
					return 0, fmt.Errorf("failed to create device")
				}

				var createdDevice dto.DeviceResponse
				err := json.Unmarshal(rec.Body.Bytes(), &createdDevice)
				if err != nil {
					return 0, err
				}
				return createdDevice.ID, nil
			},
			expectedStatus: http.StatusOK,
			expectError:    false,
		},
		{
			name: "device not found",
			setupDevice: func(ctx coreTesting.TestContext, token string) (uint, error) {
				return 999, nil // Non-existent device ID
			},
			expectedStatus: http.StatusNotFound,
			expectError:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
				// Arrange
				token, _ := createTestUserAndLoginForDevice(ctx)
				deviceID, err := tt.setupDevice(ctx, token)
				require.NoError(tb, err)

				// Create HTTP request using test context helper
				url := "/api/devices/" + strconv.FormatUint(uint64(deviceID), 10)
				req := ctx.NewAPIRequest(http.MethodGet, url, nil)
				req.Header.Set("Authorization", "Bearer "+token)

				// Create response recorder
				rec := httptest.NewRecorder()

				// Serve the request
				ctx.Router().ServeHTTP(rec, req)

				// Assert
				assert.Equal(t, tt.expectedStatus, rec.Code)

				if !tt.expectError {
					var response dto.DeviceResponse
					err := json.Unmarshal(rec.Body.Bytes(), &response)
					require.NoError(t, err)
					assert.Equal(t, "Test Device", response.Name)
					assert.Equal(t, "192.168.1.100", response.IPAddress)
					assert.Equal(t, deviceID, response.ID)
				}
			}, getDeviceTestOptions())
		})
	}
}

func TestDeviceAPI_DeleteDevice(t *testing.T) {
	tests := []struct {
		name           string
		setupDevice    func(ctx coreTesting.TestContext, token string) (uint, error)
		expectedStatus int
		verifyDeletion bool
	}{
		{
			name: "successful device deletion",
			setupDevice: func(ctx coreTesting.TestContext, token string) (uint, error) {
				createReq := dto.CreateDeviceRequest{
					Name:      "Test Device",
					IPAddress: "192.168.1.100",
				}
				reqBody, _ := json.Marshal(createReq)
				req := ctx.NewAPIRequest(http.MethodPost, "/api/devices", reqBody)
				req.Header.Set("Content-Type", "application/json")
				req.Header.Set("Authorization", "Bearer "+token)

				rec := httptest.NewRecorder()
				ctx.Router().ServeHTTP(rec, req)
				if rec.Code != http.StatusCreated {
					return 0, fmt.Errorf("failed to create device")
				}

				var createdDevice dto.DeviceResponse
				err := json.Unmarshal(rec.Body.Bytes(), &createdDevice)
				if err != nil {
					return 0, err
				}
				return createdDevice.ID, nil
			},
			expectedStatus: http.StatusNoContent,
			verifyDeletion: true,
		},
		{
			name: "idempotent deletion of non-existent device",
			setupDevice: func(ctx coreTesting.TestContext, token string) (uint, error) {
				return 999, nil // Non-existent device ID
			},
			expectedStatus: http.StatusNoContent,
			verifyDeletion: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
				// Arrange
				token, _ := createTestUserAndLoginForDevice(ctx)
				deviceID, err := tt.setupDevice(ctx, token)
				require.NoError(tb, err)

				// Create HTTP request using test context helper
				url := "/api/devices/" + strconv.FormatUint(uint64(deviceID), 10)
				req := ctx.NewAPIRequest(http.MethodDelete, url, nil)
				req.Header.Set("Authorization", "Bearer "+token)

				// Create response recorder
				rec := httptest.NewRecorder()

				// Serve the request
				ctx.Router().ServeHTTP(rec, req)

				// Assert
				assert.Equal(t, tt.expectedStatus, rec.Code)

				// Verify deletion if required
				if tt.verifyDeletion {
					url := "/api/devices/" + strconv.FormatUint(uint64(deviceID), 10)
					req := ctx.NewAPIRequest(http.MethodGet, url, nil)
					req.Header.Set("Authorization", "Bearer "+token)
					rec := httptest.NewRecorder()
					ctx.Router().ServeHTTP(rec, req)
					assert.Equal(t, http.StatusNotFound, rec.Code)
				}
			}, getDeviceTestOptions())
		})
	}
}
