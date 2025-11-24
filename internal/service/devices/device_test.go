package devices

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	pluginCore "go.lumeweb.com/portal-plugin-lbry/core"
	"go.lumeweb.com/portal-plugin-lbry/internal"
	pluginConfig "go.lumeweb.com/portal-plugin-lbry/internal/config"
	"go.lumeweb.com/portal-plugin-lbry/internal/db/migrations"
	"go.lumeweb.com/portal-plugin-lbry/internal/protocol"
	pluginTesting "go.lumeweb.com/portal-plugin-lbry/internal/testing"
	"go.lumeweb.com/portal/core"
	coreTesting "go.lumeweb.com/portal/core/testing"
	"go.lumeweb.com/portal/service"
	"go.lumeweb.com/queryutil"
)

const (
	testDeviceName1 = "Test Device 1"
	testDeviceName2 = "Test Device 2"
	testIPv4        = "192.168.1.100"
	testIPv6        = "2001:db8::1"
	testInvalidIP   = "invalid.ip.address"
)

func getTestOptions() coreTesting.TestContextBuilderOption {
	var freePeerPort, _ = pluginTesting.GetFreePort()
	var freeDhtPort, _ = pluginTesting.GetFreePort()
	var freeReflectorPort, _ = pluginTesting.GetFreePort()

	return coreTesting.CombineOptions(
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
		coreTesting.WithServiceFactory(core.WORKFLOW_SERVICE, service.NewWorkflowCoordinator),
		coreTesting.WithServiceFactory(pluginCore.DEVICE_SERVICE, NewDeviceService),
		coreTesting.WithSQLitePluginMigrations(
			internal.ProtocolName, migrations.GetSQLite(),
		),
	)
}

func TestNewDeviceService(t *testing.T) {
	// Act
	svc, options, err := NewDeviceService()

	// Assert
	assert.NoError(t, err)
	assert.NotNil(t, svc)
	assert.NotNil(t, options)
}

func TestDeviceServiceDefault_CreateDevice(t *testing.T) {
	tests := []struct {
		name               string
		userID             uint
		deviceName         string
		ipAddress          string
		expectError        bool
		errorMsg           string
		createConflictUser bool
	}{
		{
			name:        "successful IPv4 device creation",
			userID:      1,
			deviceName:  testDeviceName1,
			ipAddress:   testIPv4,
			expectError: false,
		},
		{
			name:        "successful IPv6 device creation",
			userID:      1,
			deviceName:  testDeviceName2,
			ipAddress:   testIPv6,
			expectError: false,
		},
		{
			name:        "invalid IP address",
			userID:      1,
			deviceName:  testDeviceName1,
			ipAddress:   testInvalidIP,
			expectError: true,
			errorMsg:    "invalid IP address",
		},
		{
			name:               "duplicate IP address",
			userID:             1,
			deviceName:         testDeviceName2,
			ipAddress:          testIPv4,
			expectError:        true,
			errorMsg:           "already exists",
			createConflictUser: true,
		},
		{
			name:               "different users cannot have same IP",
			userID:             2,
			deviceName:         testDeviceName1,
			ipAddress:          testIPv4,
			expectError:        true,
			errorMsg:           "already exists",
			createConflictUser: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
				// Arrange
				deviceSvc := core.GetService[pluginCore.DeviceService](ctx, pluginCore.DEVICE_SERVICE)
				require.NotNil(tb, deviceSvc)

				// Create first device for duplicate test
				if tt.createConflictUser {
					_, err := deviceSvc.CreateDevice(context.Background(), 1, testDeviceName1, testIPv4)
					require.NoError(tb, err)
				}

				// Act
				device, err := deviceSvc.CreateDevice(context.Background(), tt.userID, tt.deviceName, tt.ipAddress)

				// Assert
				if tt.expectError {
					assert.Error(tb, err)
					assert.Contains(tb, err.Error(), tt.errorMsg)
					assert.Nil(tb, device)
				} else {
					assert.NoError(tb, err)
					assert.NotNil(tb, device)
					assert.Equal(tb, tt.userID, device.UserID)
					assert.Equal(tb, tt.deviceName, device.Name)
					assert.Equal(tb, tt.ipAddress, device.IPAddress)
					assert.NotZero(tb, device.ID)
				}
			}, getTestOptions())
		})
	}
}

func TestDeviceServiceDefault_UpdateDevice(t *testing.T) {
	tests := []struct {
		name               string
		userID             uint
		deviceID           uint
		newName            string
		newIP              string
		expectError        bool
		errorMsg           string
		createConflictUser bool
	}{
		{
			name:        "successful device update",
			userID:      1,
			deviceID:    1,
			newName:     "Updated Device",
			newIP:       "192.168.1.200",
			expectError: false,
		},
		{
			name:        "device not found",
			userID:      1,
			deviceID:    999,
			newName:     "Updated Device",
			newIP:       "192.168.1.200",
			expectError: true,
			errorMsg:    "device not found",
		},
		{
			name:        "invalid IP address",
			userID:      1,
			deviceID:    1,
			newName:     "Updated Device",
			newIP:       testInvalidIP,
			expectError: true,
			errorMsg:    "invalid IP address",
		},
		{
			name:               "IP address already exists globally",
			userID:             1,
			deviceID:           1,
			newName:            "Updated Device",
			newIP:              "192.168.1.101", // This will be used by another user
			expectError:        true,
			errorMsg:           "device with IP address 192.168.1.101 already exists",
			createConflictUser: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
				// Arrange
				deviceSvc := core.GetService[pluginCore.DeviceService](ctx, pluginCore.DEVICE_SERVICE)
				require.NotNil(tb, deviceSvc)

				// Create a device to update (unless testing not found)
				shouldCreateDevice := tt.deviceID == 1
				if shouldCreateDevice {
					createdDevice, err := deviceSvc.CreateDevice(context.Background(), tt.userID, testDeviceName1, testIPv4)
					require.NoError(tb, err)
					tt.deviceID = createdDevice.ID
				}

				// For the IP conflict test, create a device with the conflicting IP for another user
				if tt.createConflictUser {
					_, err := deviceSvc.CreateDevice(context.Background(), 2, "Other User Device", "192.168.1.101")
					require.NoError(tb, err)
				}

				// Act
				device, err := deviceSvc.UpdateDevice(context.Background(), tt.userID, tt.deviceID, tt.newName, tt.newIP)

				// Assert
				if tt.expectError {
					assert.Error(tb, err)
					assert.Contains(tb, err.Error(), tt.errorMsg)
					assert.Nil(tb, device)
				} else {
					assert.NoError(tb, err)
					assert.NotNil(tb, device)
					assert.Equal(tb, tt.userID, device.UserID)
					assert.Equal(tb, tt.newName, device.Name)
					assert.Equal(tb, tt.newIP, device.IPAddress)
					assert.Equal(tb, tt.deviceID, device.ID)
				}
			}, getTestOptions())
		})
	}
}

func TestDeviceServiceDefault_UpdateDeviceName(t *testing.T) {
	tests := []struct {
		name        string
		userID      uint
		deviceID    uint
		newName     string
		expectError bool
		errorMsg    string
	}{
		{
			name:        "successful device name update",
			userID:      1,
			deviceID:    1,
			newName:     "Updated Device Name",
			expectError: false,
		},
		{
			name:        "device not found",
			userID:      1,
			deviceID:    999,
			newName:     "Updated Device Name",
			expectError: true,
			errorMsg:    "device not found",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
				// Arrange
				deviceSvc := core.GetService[pluginCore.DeviceService](ctx, pluginCore.DEVICE_SERVICE)
				require.NotNil(tb, deviceSvc)

				// Create a device to update (unless testing not found)
				shouldCreateDevice := tt.deviceID == 1 && !tt.expectError
				if shouldCreateDevice {
					createdDevice, err := deviceSvc.CreateDevice(context.Background(), tt.userID, testDeviceName1, testIPv4)
					require.NoError(tb, err)
					tt.deviceID = createdDevice.ID
				}

				// Act
				device, err := deviceSvc.UpdateDeviceName(context.Background(), tt.userID, tt.deviceID, tt.newName)

				// Assert
				if tt.expectError {
					assert.Error(tb, err)
					assert.Contains(tb, err.Error(), tt.errorMsg)
					assert.Nil(tb, device)
				} else {
					assert.NoError(tb, err)
					assert.NotNil(tb, device)
					assert.Equal(tb, tt.userID, device.UserID)
					assert.Equal(tb, tt.newName, device.Name)
					assert.Equal(tb, testIPv4, device.IPAddress) // IP should remain unchanged
					assert.Equal(tb, tt.deviceID, device.ID)
				}
			}, getTestOptions())
		})
	}
}

func TestDeviceService_GetDevice(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		deviceSvc := core.GetService[pluginCore.DeviceService](ctx, pluginCore.DEVICE_SERVICE)
		require.NotNil(tb, deviceSvc)

		testUserID := uint(1)

		// Create a device to retrieve
		createdDevice, err := deviceSvc.CreateDevice(context.Background(), testUserID, testDeviceName1, testIPv4)
		require.NoError(tb, err)

		tests := []struct {
			name        string
			userID      uint
			deviceID    uint
			expectError bool
			errorMsg    string
		}{
			{
				name:        "successful device retrieval",
				userID:      testUserID,
				deviceID:    createdDevice.ID,
				expectError: false,
			},
			{
				name:        "device not found - wrong user",
				userID:      999,
				deviceID:    createdDevice.ID,
				expectError: true,
				errorMsg:    "device not found",
			},
			{
				name:        "device not found",
				userID:      testUserID,
				deviceID:    999,
				expectError: true,
				errorMsg:    "device not found",
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				// Act
				device, err := deviceSvc.GetDevice(context.Background(), tt.userID, tt.deviceID)

				// Assert
				if tt.expectError {
					assert.Error(t, err)
					assert.Contains(t, err.Error(), tt.errorMsg)
					assert.Nil(t, device)
				} else {
					assert.NoError(t, err)
					assert.NotNil(t, device)
					assert.Equal(t, testUserID, device.UserID)
					assert.Equal(t, createdDevice.Name, device.Name)
					assert.Equal(t, createdDevice.IPAddress, device.IPAddress)
					assert.Equal(t, createdDevice.ID, device.ID)
				}
			})
		}
	}, getTestOptions())
}

func TestDeviceServiceDefault_ListDevices(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		deviceSvc := core.GetService[pluginCore.DeviceService](ctx, pluginCore.DEVICE_SERVICE)
		require.NotNil(tb, deviceSvc)

		userID1 := uint(1)
		userID2 := uint(2)

		// Create test devices for different users
		_, err := deviceSvc.CreateDevice(context.Background(), userID1, testDeviceName1, testIPv4)
		require.NoError(tb, err)

		_, err = deviceSvc.CreateDevice(context.Background(), userID1, testDeviceName2, testIPv6)
		require.NoError(tb, err)

		_, err = deviceSvc.CreateDevice(context.Background(), userID2, testDeviceName1+" User2", "192.168.1.102")
		require.NoError(tb, err)

		tests := []struct {
			name          string
			userID        uint
			filters       []queryutil.CrudFilter
			sorts         []queryutil.Sort
			pagination    queryutil.Pagination
			expectedCount int
			expectedTotal int64
		}{
			{
				name:          "list all devices for user 1",
				userID:        userID1,
				filters:       []queryutil.CrudFilter{},
				sorts:         []queryutil.Sort{},
				pagination:    queryutil.Pagination{Start: 0, End: 10, PageSize: 10, Mode: "server"},
				expectedCount: 2,
				expectedTotal: 2,
			},
			{
				name:          "list all devices for user 2",
				userID:        userID2,
				filters:       []queryutil.CrudFilter{},
				sorts:         []queryutil.Sort{},
				pagination:    queryutil.Pagination{Start: 0, End: 10, PageSize: 10, Mode: "server"},
				expectedCount: 1,
				expectedTotal: 1,
			},
			{
				name:   "filter by name",
				userID: userID1,
				filters: []queryutil.CrudFilter{
					queryutil.Equal("name", testDeviceName1),
				},
				sorts:         []queryutil.Sort{},
				pagination:    queryutil.Pagination{Start: 0, End: 10, PageSize: 10, Mode: "server"},
				expectedCount: 1,
				expectedTotal: 1,
			},
			{
				name:   "filter by IP address",
				userID: userID1,
				filters: []queryutil.CrudFilter{
					queryutil.Equal("ip_address", testIPv4),
				},
				sorts:         []queryutil.Sort{},
				pagination:    queryutil.Pagination{Start: 0, End: 10, PageSize: 10, Mode: "server"},
				expectedCount: 1,
				expectedTotal: 1,
			},
			{
				name:    "sort by name",
				userID:  userID1,
				filters: []queryutil.CrudFilter{},
				sorts: []queryutil.Sort{
					{
						Field: "name",
						Order: "asc",
					},
				},
				pagination:    queryutil.Pagination{Start: 0, End: 10, PageSize: 10, Mode: "server"},
				expectedCount: 2,
				expectedTotal: 2,
			},
			{
				name:          "pagination limit",
				userID:        userID1,
				filters:       []queryutil.CrudFilter{},
				sorts:         []queryutil.Sort{},
				pagination:    queryutil.Pagination{Start: 0, End: 0, PageSize: 1, Mode: "server"},
				expectedCount: 1,
				expectedTotal: 2,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				// Act
				devices, total, err := deviceSvc.ListDevices(context.Background(), tt.userID, tt.filters, tt.sorts, tt.pagination)

				// Assert
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedTotal, total)
				assert.Len(t, devices, tt.expectedCount)

				// Verify all devices belong to the correct user
				for _, device := range devices {
					assert.Equal(t, tt.userID, device.UserID)
				}

				// Verify sorting when specified
				if len(tt.sorts) > 0 && tt.sorts[0].Field == "name" && tt.sorts[0].Order == "asc" && len(devices) > 1 {
					assert.True(t, devices[0].Name <= devices[1].Name)
				}
			})
		}
	}, getTestOptions())
}

func TestDeviceServiceDefault_DeleteDevice(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		deviceSvc := core.GetService[pluginCore.DeviceService](ctx, pluginCore.DEVICE_SERVICE)
		require.NotNil(tb, deviceSvc)

		testUserID := uint(1)

		// Create a device to delete
		createdDevice, err := deviceSvc.CreateDevice(context.Background(), testUserID, testDeviceName1, testIPv4)
		require.NoError(tb, err)

		// Create another device for different user
		otherUserDevice, err := deviceSvc.CreateDevice(context.Background(), 2, testDeviceName2, testIPv6)
		require.NoError(tb, err)

		tests := []struct {
			name        string
			userID      uint
			deviceID    uint
			expectError bool
		}{
			{
				name:        "successful device deletion",
				userID:      testUserID,
				deviceID:    createdDevice.ID,
				expectError: false,
			},
			{
				name:        "idempotent deletion of non-existent device",
				userID:      testUserID,
				deviceID:    999,
				expectError: false,
			},
			{
				name:        "cannot delete other user's device",
				userID:      testUserID,
				deviceID:    otherUserDevice.ID,
				expectError: true,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				// Act
				err := deviceSvc.DeleteDevice(context.Background(), tt.userID, tt.deviceID)

				// Assert
				if tt.expectError {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)
				}
			})
		}

		// Verify the created device is actually deleted
		_, err = deviceSvc.GetDevice(context.Background(), testUserID, createdDevice.ID)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "device not found")
	}, getTestOptions())
}

func TestDeviceService_GetDeviceByIPAddress(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		deviceSvc := core.GetService[pluginCore.DeviceService](ctx, pluginCore.DEVICE_SERVICE)
		require.NotNil(tb, deviceSvc)

		testUserID := uint(1)

		// Create a device to retrieve
		createdDevice, err := deviceSvc.CreateDevice(context.Background(), testUserID, testDeviceName1, testIPv4)
		require.NoError(tb, err)

		// Create device for another user with different IP
		_, err = deviceSvc.CreateDevice(context.Background(), 2, testDeviceName2, "192.168.1.103")
		require.NoError(tb, err)

		tests := []struct {
			name        string
			userID      uint
			ipAddress   string
			expectError bool
			errorMsg    string
		}{
			{
				name:        "successful device retrieval by IP",
				userID:      testUserID,
				ipAddress:   testIPv4,
				expectError: false,
			},
			{
				name:        "device not found by IP - wrong user",
				userID:      999,
				ipAddress:   testIPv4,
				expectError: true,
				errorMsg:    "device not found",
			},
			{
				name:        "device not found by IP - non-existent IP",
				userID:      testUserID,
				ipAddress:   "192.168.1.999",
				expectError: true,
				errorMsg:    "device not found",
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				// Act
				device, err := deviceSvc.GetDeviceByIPAddress(context.Background(), tt.userID, tt.ipAddress)

				// Assert
				if tt.expectError {
					assert.Error(t, err)
					assert.Contains(t, err.Error(), tt.errorMsg)
					assert.Nil(t, device)
				} else {
					assert.NoError(t, err)
					assert.NotNil(t, device)
					assert.Equal(t, testUserID, device.UserID)
					assert.Equal(t, createdDevice.Name, device.Name)
					assert.Equal(t, createdDevice.IPAddress, device.IPAddress)
					assert.Equal(t, createdDevice.ID, device.ID)
				}
			})
		}
	}, getTestOptions())
}
