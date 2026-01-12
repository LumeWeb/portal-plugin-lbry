package protocol

import (
	"context"
	"encoding/hex"
	"testing"

	"go.lumeweb.com/portal-plugin-lbry/internal"
	"go.lumeweb.com/portal/db/models"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.lumeweb.com/liblbry/protocol"
	lbrystream "go.lumeweb.com/liblbry/stream"
	pluginMocks "go.lumeweb.com/portal-plugin-lbry/core/mocks"
	pluginDb "go.lumeweb.com/portal-plugin-lbry/internal/db"
	coreTesting "go.lumeweb.com/portal/core/testing"
	coreMocks "go.lumeweb.com/portal/core/testing/mocks"
	"gorm.io/gorm"
)

func TestNewReflectorStore(t *testing.T) {
	runBlobStoreTest(t, func(tb testing.TB, ctx coreTesting.TestContext) {
		// Act
		store, err := NewReflectorStore(ctx)

		// Assert
		assert.NoError(t, err)
		assert.NotNil(t, store)
		assert.Equal(t, REFLECTOR_STORE_NAME, store.Name())
	})
}

func TestReflectorStore_List(t *testing.T) {
	runBlobStoreTest(t, func(tb testing.TB, ctx coreTesting.TestContext) {
		// Arrange
		store, err := NewReflectorStore(ctx)
		require.NoError(tb, err)

		// Act
		results, err := store.List(t.Context(), 0, 10)

		// Assert
		assert.NoError(t, err)
		assert.NotNil(t, results)
		assert.Empty(t, results) // Should return empty slice for no-op implementation
	})
}

func TestReflectorStore_ExtractUserIDFromContext_ReflectorSource(t *testing.T) {
	runBlobStoreTest(t, func(tb testing.TB, ctx coreTesting.TestContext) {
		// Arrange
		store, err := NewReflectorStore(ctx)
		require.NoError(tb, err)

		// Test case structure
		type testCase struct {
			name        string
			ctx         context.Context
			expectedID  uint
			shouldError bool
		}

		testCases := []testCase{
			{
				name:        "reflector source with IP",
				ctx:         context.WithValue(context.Background(), protocol.SourceContextKey, protocol.SourceReflector),
				expectedID:  0, // Will fail because no IP address
				shouldError: true,
			},
			{
				name:        "peer source",
				ctx:         context.WithValue(context.Background(), protocol.SourceContextKey, protocol.SourcePeer),
				expectedID:  0, // Will fail because not reflector
				shouldError: true,
			},
			{
				name:        "no source",
				ctx:         context.Background(),
				expectedID:  0, // Will fail because no source
				shouldError: true,
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				// Act
				result := store.extractUserIDFromContext(tc.ctx)

				// Assert
				assert.Equal(t, tc.expectedID, result)
			})
		}
	})
}

func TestReflectorStore_ExtractUserIDFromContext_WithDeviceLookup(t *testing.T) {
	runBlobStoreTest(t, func(tb testing.TB, ctx coreTesting.TestContext) {
		// Arrange
		store, err := NewReflectorStore(ctx)
		require.NoError(tb, err)

		// Create mock device service
		mockDeviceService := pluginMocks.NewMockDeviceService(tb)
		store.deviceSvc = mockDeviceService

		testIPAddress := "192.168.1.100"
		testUserID := uint(123)
		testDevice := &pluginDb.Device{
			Model: gorm.Model{
				ID: 1,
			},
			UserID:    testUserID,
			Name:      "test-device",
			IPAddress: testIPAddress,
		}

		// Set up mock expectation for device lookup
		mockDeviceService.EXPECT().GetDeviceByIPAddress(mock.Anything, testIPAddress).
			Return(testDevice, nil).Maybe()

		// Create context with reflector source and IP address
		ctxWithSource := context.WithValue(context.Background(), protocol.SourceContextKey, protocol.SourceReflector)
		ctxWithIP := context.WithValue(ctxWithSource, protocol.IPAddressContextKey, testIPAddress)

		// Act
		result := store.extractUserIDFromContext(ctxWithIP)

		// Assert
		assert.Equal(t, testUserID, result)
	})
}

func TestReflectorStore_Put(t *testing.T) {
	runBlobStoreTest(t, func(tb testing.TB, ctx coreTesting.TestContext) {
		// Arrange
		store, err := NewReflectorStore(ctx)
		require.NoError(tb, err)

		// Create mock services
		mockStorage := coreMocks.NewMockStorageService(tb)
		mockDeviceService := pluginMocks.NewMockDeviceService(tb)
		mockUploadService := pluginMocks.NewMockUploadService(tb)
		store.storageSvc = mockStorage
		store.deviceSvc = mockDeviceService
		store.uploadSvc = mockUploadService

		testData := []byte("test reflector blob data")
		testHash := "a2f1841bb9c5f3b583ac3b8c07ee1a5bf9cc48923721c30d5ca6318615776c284e8936d72fa4db7fdda2e4e9598b1e6c"
		userID := uint(123)
		testIPAddress := "192.168.1.100"

		// Set up mock expectations
		mockStorage.EXPECT().S3TemporaryUpload(mock.Anything, mock.Anything, mock.AnythingOfType("uint64"), mock.Anything, mock.AnythingOfType("func(*core.S3TempUploadOptions)")).
			Return("upload_id_123", nil).Once()

		// Set up mock expectation for MarkPendingBlobAsReceived
		mockUploadService.EXPECT().MarkPendingBlobAsReceived(mock.Anything, userID, uint(1), mock.AnythingOfType("*stream.BlobInfo")).
			Return(nil).Once()

		testDevice := &pluginDb.Device{
			Model: gorm.Model{
				ID: 1,
			},
			UserID:    userID,
			Name:      "test-device",
			IPAddress: testIPAddress,
		}
		mockDeviceService.EXPECT().GetDeviceByIPAddress(mock.Anything, testIPAddress).
			Return(testDevice, nil).Times(2)

		// Create context with reflector source and IP address
		ctxWithSource := context.WithValue(context.Background(), protocol.SourceContextKey, protocol.SourceReflector)
		ctxWithIP := context.WithValue(ctxWithSource, protocol.IPAddressContextKey, testIPAddress)

		// Act
		err = store.Put(ctxWithIP, testHash, testData)

		// Assert
		assert.NoError(t, err)
	})
}

func TestReflectorStore_Put_TerminatingBlob(t *testing.T) {
	runBlobStoreTest(t, func(tb testing.TB, ctx coreTesting.TestContext) {
		// Arrange
		store, err := NewReflectorStore(ctx)
		require.NoError(tb, err)

		// Create mock services
		mockStorage := coreMocks.NewMockStorageService(tb)
		mockDeviceService := pluginMocks.NewMockDeviceService(tb)
		mockUploadService := pluginMocks.NewMockUploadService(tb)
		store.storageSvc = mockStorage
		store.deviceSvc = mockDeviceService
		store.uploadSvc = mockUploadService

		testData := []byte{} // Empty data for terminating blob
		testHash := ""       // Empty hash for terminating blob
		userID := uint(123)
		testIPAddress := "192.168.1.100"

		// Set up mock expectations - storage should NOT be called for terminating blob
		// But upload service should be called to mark as received
		mockUploadService.EXPECT().MarkPendingBlobAsReceived(mock.Anything, userID, uint(1), mock.AnythingOfType("*stream.BlobInfo")).
			Return(nil).Once()

		testDevice := &pluginDb.Device{
			Model: gorm.Model{
				ID: 1,
			},
			UserID:    userID,
			Name:      "test-device",
			IPAddress: testIPAddress,
		}
		mockDeviceService.EXPECT().GetDeviceByIPAddress(mock.Anything, testIPAddress).
			Return(testDevice, nil).Twice()

		// Create context with reflector source and IP address
		ctxWithSource := context.WithValue(context.Background(), protocol.SourceContextKey, protocol.SourceReflector)
		ctxWithIP := context.WithValue(ctxWithSource, protocol.IPAddressContextKey, testIPAddress)

		// Act
		err = store.Put(ctxWithIP, testHash, testData)

		// Assert
		assert.NoError(t, err)

		// Verify that storage was NOT called (terminating blob should skip storage)
		// Since we didn't set up any storage expectations, the test would fail if storage was called
	})
}

func TestReflectorStore_IsTerminatingBlob(t *testing.T) {
	runBlobStoreTest(t, func(tb testing.TB, ctx coreTesting.TestContext) {
		// Arrange
		store, err := NewReflectorStore(ctx)
		require.NoError(tb, err)

		userID := uint(123)

		// Test cases
		testCases := []struct {
			name           string
			hash           string
			expectedResult bool
		}{
			{
				name:           "empty hash is terminating",
				hash:           "",
				expectedResult: true,
			},
			{
				name:           "non-empty hash is not terminating",
				hash:           "some_hash_value",
				expectedResult: false,
			},
			{
				name:           "zero hash is not terminating",
				hash:           "0000000000000000",
				expectedResult: false,
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				// Act
				result := store.isTerminatingBlob(userID, tc.hash)

				// Assert
				assert.Equal(t, tc.expectedResult, result)
			})
		}
	})
}

func TestReflectorStore_Get(t *testing.T) {
	runBlobStoreTest(t, func(tb testing.TB, ctx coreTesting.TestContext) {
		// Arrange
		store, err := NewReflectorStore(ctx)
		require.NoError(tb, err)

		// Create mock services
		mockStorage := coreMocks.NewMockStorageService(tb)
		mockDeviceService := pluginMocks.NewMockDeviceService(tb)
		store.storageSvc = mockStorage
		store.deviceSvc = mockDeviceService

		testData := []byte("test reflector blob data")
		testHash := "test_hash_456"
		userID := uint(456)
		testIPAddress := "192.168.1.100"

		// Set up mock expectations
		mockStorage.EXPECT().S3GetTemporaryUpload(mock.Anything, mock.Anything, "456/test_hash_456").
			Return(internal.NewReadSeekCloser(testData), nil).Once()

		testDevice := &pluginDb.Device{
			Model: gorm.Model{
				ID: 1,
			},
			UserID:    userID,
			Name:      "test-device",
			IPAddress: testIPAddress,
		}
		mockDeviceService.EXPECT().GetDeviceByIPAddress(mock.Anything, testIPAddress).
			Return(testDevice, nil).Once()

		// Create context with reflector source and IP address
		ctxWithSource := context.WithValue(context.Background(), protocol.SourceContextKey, protocol.SourceReflector)
		ctxWithIP := context.WithValue(ctxWithSource, protocol.IPAddressContextKey, testIPAddress)

		// Act
		result, err := store.Get(ctxWithIP, testHash)

		// Assert
		assert.NoError(t, err)
		assert.Equal(t, testData, result)
	})
}

func TestReflectorStore_Delete(t *testing.T) {
	runBlobStoreTest(t, func(tb testing.TB, ctx coreTesting.TestContext) {
		// Arrange
		store, err := NewReflectorStore(ctx)
		require.NoError(tb, err)

		// Create mock services
		mockStorage := coreMocks.NewMockStorageService(tb)
		mockDeviceService := pluginMocks.NewMockDeviceService(tb)
		store.storageSvc = mockStorage
		store.deviceSvc = mockDeviceService

		testHash := "test_hash_789"
		userID := uint(789)
		testIPAddress := "192.168.1.100"

		// Set up mock expectations
		mockStorage.EXPECT().S3DeleteTemporaryUpload(mock.Anything, mock.Anything, "789/test_hash_789").
			Return(nil).Once()

		testDevice := &pluginDb.Device{
			Model: gorm.Model{
				ID: 1,
			},
			UserID:    userID,
			Name:      "test-device",
			IPAddress: testIPAddress,
		}
		mockDeviceService.EXPECT().GetDeviceByIPAddress(mock.Anything, testIPAddress).
			Return(testDevice, nil).Once()

		// Create context with reflector source and IP address
		ctxWithSource := context.WithValue(context.Background(), protocol.SourceContextKey, protocol.SourceReflector)
		ctxWithIP := context.WithValue(ctxWithSource, protocol.IPAddressContextKey, testIPAddress)

		// Act
		err = store.Delete(ctxWithIP, testHash)

		// Assert
		assert.NoError(t, err)
	})
}

func TestReflectorStore_PutSD(t *testing.T) {
	runBlobStoreTest(t, func(tb testing.TB, ctx coreTesting.TestContext) {
		// Arrange
		store, err := NewReflectorStore(ctx)
		require.NoError(tb, err)

		// Create mock services
		mockUploadService := pluginMocks.NewMockUploadService(tb)
		mockDeviceService := pluginMocks.NewMockDeviceService(tb)
		mockWorkflowService := coreMocks.NewMockWorkflowService(tb)
		store.uploadSvc = mockUploadService
		store.deviceSvc = mockDeviceService
		store.workflowSvc = mockWorkflowService

		// Create test SD blob data with at least one blob for validation
		// Use valid hex-encoded blob hash from test data
		blobHashBytes, _ := hex.DecodeString("a2f1841bb9c5f3b583ac3b8c07ee1a5bf9cc48923721c30d5ca6318615776c284e8936d72fa4db7fdda2e4e9598b1e6c")
		ivBytes, _ := hex.DecodeString("30303030303030303030303030303031")

		sdBlob := lbrystream.SDBlob{
			StreamName:        "test-stream",
			StreamType:        "file",
			SuggestedFileName: "test-file.txt",
			Key:               []byte("test-key"),
			StreamHash:        []byte("test-stream-hash"),
			BlobInfos: []lbrystream.BlobInfo{
				{
					Length:   1024,
					BlobNum:  0,
					BlobHash: blobHashBytes,
					IV:       ivBytes,
				},
			},
		}

		sdBlobBytes, err := sdBlob.ToBlob()
		require.NoError(t, err)

		testHash := "test_sd_hash_123"
		userID := uint(123)
		deviceID := uint(1)
		testIPAddress := "192.168.1.100"

		// Set up mock expectations for device lookup (called twice - once for user ID, once for device ID)
		testDevice := &pluginDb.Device{
			Model: gorm.Model{
				ID: deviceID,
			},
			UserID:    userID,
			Name:      "test-device",
			IPAddress: testIPAddress,
		}
		mockDeviceService.EXPECT().GetDeviceByIPAddress(mock.Anything, testIPAddress).
			Return(testDevice, nil).Twice()

		// Set up mock expectation for GetPendingStream (should return nil to indicate no existing stream)
		mockUploadService.EXPECT().GetPendingStream(mock.Anything, userID, testHash).
			Return(nil, gorm.ErrRecordNotFound).Once()

		// Set up mock expectation for StorePendingStream
		mockUploadService.EXPECT().StorePendingStream(mock.Anything, userID, deviceID, mock.AnythingOfType("*stream.SDBlob"), testHash).
			Return(uint(456), nil).Once()

		// Set up mock expectation for StartWorkflow
		mockWorkflowService.EXPECT().StartWorkflow(mock.Anything, "lbry.reflector.assembly", mock.Anything, mock.Anything, mock.Anything).
			Return((*models.Request)(nil), nil).Once()

		// Create context with reflector source and IP address
		ctxWithSource := context.WithValue(context.Background(), protocol.SourceContextKey, protocol.SourceReflector)
		ctxWithIP := context.WithValue(ctxWithSource, protocol.IPAddressContextKey, testIPAddress)

		// Act
		err = store.PutSD(ctxWithIP, testHash, sdBlobBytes)

		// Assert
		assert.NoError(t, err)
	})
}

// createValidReflectorContext creates a valid context with reflector source and IP address
// This helper function sets up a context that will pass user ID validation but fail SD blob validation
func createValidReflectorContext() context.Context {
	testIPAddress := "192.168.1.100"
	ctxWithSource := context.WithValue(context.Background(), protocol.SourceContextKey, protocol.SourceReflector)
	ctxWithIP := context.WithValue(ctxWithSource, protocol.IPAddressContextKey, testIPAddress)
	return ctxWithIP
}

func TestReflectorStore_PutSD_ErrorCases(t *testing.T) {
	runBlobStoreTest(t, func(tb testing.TB, ctx coreTesting.TestContext) {
		// Arrange
		store, err := NewReflectorStore(ctx)
		require.NoError(tb, err)

		testCases := []struct {
			name        string
			ctx         context.Context
			data        []byte
			expectError bool
			errorMsg    string
		}{
			{
				name:        "no user ID in context",
				ctx:         context.Background(),
				data:        []byte("invalid sd blob"),
				expectError: true,
				errorMsg:    "user ID not found in context",
			},
			{
				name:        "reflector source without IP address",
				ctx:         context.WithValue(context.Background(), protocol.SourceContextKey, protocol.SourceReflector),
				data:        []byte("invalid sd blob"),
				expectError: true,
				errorMsg:    "user ID not found in context",
			},
			{
				name:        "valid context but invalid SD blob data",
				ctx:         createValidReflectorContext(),
				data:        []byte("this is not a valid SD blob - just random text"),
				expectError: true,
				errorMsg:    "invalid SD blob",
			},
			{
				name:        "valid context but malformed SD blob binary data",
				ctx:         createValidReflectorContext(),
				data:        []byte{0x00, 0x01, 0x02, 0x03}, // Invalid binary data
				expectError: true,
				errorMsg:    "invalid SD blob",
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				// Set up mock expectations for valid context cases
				if tc.name == "valid context but invalid SD blob data" || tc.name == "valid context but malformed SD blob binary data" {
					// Mock device service for user ID lookup
					mockDeviceService := pluginMocks.NewMockDeviceService(t)
					store.deviceSvc = mockDeviceService

					testDevice := &pluginDb.Device{
						Model: gorm.Model{
							ID: 1,
						},
						UserID:    123,
						Name:      "test-device",
						IPAddress: "192.168.1.100",
					}
					mockDeviceService.EXPECT().GetDeviceByIPAddress(mock.Anything, "192.168.1.100").
						Return(testDevice, nil).Once()
				}

				// Act
				err := store.PutSD(tc.ctx, "test_hash", tc.data)

				// Assert
				if tc.expectError {
					assert.Error(t, err)
					assert.Contains(t, err.Error(), tc.errorMsg)
				} else {
					assert.NoError(t, err)
				}
			})
		}
	})
}

func TestReflectorStore_Has(t *testing.T) {
	runBlobStoreTest(t, func(tb testing.TB, ctx coreTesting.TestContext) {
		// Arrange
		store, err := NewReflectorStore(ctx)
		require.NoError(tb, err)

		// Create mock services
		mockStorage := coreMocks.NewMockStorageService(tb)
		mockDeviceService := pluginMocks.NewMockDeviceService(tb)
		store.storageSvc = mockStorage
		store.deviceSvc = mockDeviceService

		testHash := "test_hash_123"
		userID := uint(123)
		testIPAddress := "192.168.1.100"

		// Set up mock expectations
		mockStorage.EXPECT().S3TemporaryUploadExists(mock.Anything, mock.Anything, "123/test_hash_123").
			Return(true, nil).Once()

		testDevice := &pluginDb.Device{
			Model: gorm.Model{
				ID: 1,
			},
			UserID:    userID,
			Name:      "test-device",
			IPAddress: testIPAddress,
		}
		mockDeviceService.EXPECT().GetDeviceByIPAddress(mock.Anything, testIPAddress).
			Return(testDevice, nil).Once()

		// Create context with reflector source and IP address
		ctxWithSource := context.WithValue(context.Background(), protocol.SourceContextKey, protocol.SourceReflector)
		ctxWithIP := context.WithValue(ctxWithSource, protocol.IPAddressContextKey, testIPAddress)

		// Act
		exists, err := store.Has(ctxWithIP, testHash)

		// Assert
		assert.NoError(t, err)
		assert.True(t, exists)
	})
}

func TestReflectorStore_Has_NotExisting(t *testing.T) {
	runBlobStoreTest(t, func(tb testing.TB, ctx coreTesting.TestContext) {
		// Arrange
		store, err := NewReflectorStore(ctx)
		require.NoError(tb, err)

		// Create mock services
		mockStorage := coreMocks.NewMockStorageService(tb)
		mockDeviceService := pluginMocks.NewMockDeviceService(tb)
		store.storageSvc = mockStorage
		store.deviceSvc = mockDeviceService

		testHash := "test_hash_456"
		userID := uint(456)
		testIPAddress := "192.168.1.100"

		// Set up mock expectations - blob doesn't exist
		mockStorage.EXPECT().S3TemporaryUploadExists(mock.Anything, mock.Anything, "456/test_hash_456").
			Return(false, nil).Once()

		testDevice := &pluginDb.Device{
			Model: gorm.Model{
				ID: 1,
			},
			UserID:    userID,
			Name:      "test-device",
			IPAddress: testIPAddress,
		}
		mockDeviceService.EXPECT().GetDeviceByIPAddress(mock.Anything, testIPAddress).
			Return(testDevice, nil).Once()

		// Create context with reflector source and IP address
		ctxWithSource := context.WithValue(context.Background(), protocol.SourceContextKey, protocol.SourceReflector)
		ctxWithIP := context.WithValue(ctxWithSource, protocol.IPAddressContextKey, testIPAddress)

		// Act
		exists, err := store.Has(ctxWithIP, testHash)

		// Assert
		assert.NoError(t, err)
		assert.False(t, exists)
	})
}

func TestReflectorStore_Has_NoUserID(t *testing.T) {
	runBlobStoreTest(t, func(tb testing.TB, ctx coreTesting.TestContext) {
		// Arrange
		store, err := NewReflectorStore(ctx)
		require.NoError(tb, err)

		testHash := "test_hash_789"

		testCases := []struct {
			name string
			ctx  context.Context
		}{
			{
				name: "no context values",
				ctx:  context.Background(),
			},
			{
				name: "reflector source without IP",
				ctx:  context.WithValue(context.Background(), protocol.SourceContextKey, protocol.SourceReflector),
			},
			{
				name: "peer source with IP",
				ctx: func() context.Context {
					ctxWithSource := context.WithValue(context.Background(), protocol.SourceContextKey, protocol.SourcePeer)
					return context.WithValue(ctxWithSource, protocol.IPAddressContextKey, "192.168.1.100")
				}(),
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				// Act
				exists, err := store.Has(tc.ctx, testHash)

				// Assert
				assert.Error(t, err)
				assert.False(t, exists)
				assert.Contains(t, err.Error(), "user ID not found in context for ReflectorStore Has operation")
			})
		}
	})
}

func TestReflectorStore_Has_StorageError(t *testing.T) {
	runBlobStoreTest(t, func(tb testing.TB, ctx coreTesting.TestContext) {
		// Arrange
		store, err := NewReflectorStore(ctx)
		require.NoError(tb, err)

		// Create mock services
		mockStorage := coreMocks.NewMockStorageService(tb)
		mockDeviceService := pluginMocks.NewMockDeviceService(tb)
		store.storageSvc = mockStorage
		store.deviceSvc = mockDeviceService

		testHash := "test_hash_error"
		userID := uint(999)
		testIPAddress := "192.168.1.100"

		// Set up mock expectations - storage service returns error
		mockStorage.EXPECT().S3TemporaryUploadExists(mock.Anything, mock.Anything, "999/test_hash_error").
			Return(false, assert.AnError).Once()

		testDevice := &pluginDb.Device{
			Model: gorm.Model{
				ID: 1,
			},
			UserID:    userID,
			Name:      "test-device",
			IPAddress: testIPAddress,
		}
		mockDeviceService.EXPECT().GetDeviceByIPAddress(mock.Anything, testIPAddress).
			Return(testDevice, nil).Once()

		// Create context with reflector source and IP address
		ctxWithSource := context.WithValue(context.Background(), protocol.SourceContextKey, protocol.SourceReflector)
		ctxWithIP := context.WithValue(ctxWithSource, protocol.IPAddressContextKey, testIPAddress)

		// Act
		exists, err := store.Has(ctxWithIP, testHash)

		// Assert
		assert.Error(t, err)                                              // Should now propagate the storage error
		assert.False(t, exists)                                           // Should return false when storage service errors
		assert.Contains(t, err.Error(), "failed to check blob existence") // Verify wrapped error message
	})
}

func TestReflectorStore_Has_DeviceLookupError(t *testing.T) {
	runBlobStoreTest(t, func(tb testing.TB, ctx coreTesting.TestContext) {
		// Arrange
		store, err := NewReflectorStore(ctx)
		require.NoError(tb, err)

		// Create mock services
		mockDeviceService := pluginMocks.NewMockDeviceService(tb)
		store.deviceSvc = mockDeviceService

		testHash := "test_hash_device_error"
		testIPAddress := "192.168.1.100"

		// Set up mock expectations - device service returns error
		mockDeviceService.EXPECT().GetDeviceByIPAddress(mock.Anything, testIPAddress).
			Return(nil, assert.AnError).Once()

		// Create context with reflector source and IP address
		ctxWithSource := context.WithValue(context.Background(), protocol.SourceContextKey, protocol.SourceReflector)
		ctxWithIP := context.WithValue(ctxWithSource, protocol.IPAddressContextKey, testIPAddress)

		// Act
		exists, err := store.Has(ctxWithIP, testHash)

		// Assert
		assert.Error(t, err)    // Should now return error when device lookup fails
		assert.False(t, exists) // Should return false when device lookup fails
		assert.Contains(t, err.Error(), "user ID not found in context for ReflectorStore Has operation")
	})
}

func TestReflectorStore_markBlobAsReceived_InvalidHash(t *testing.T) {
	runBlobStoreTest(t, func(tb testing.TB, ctx coreTesting.TestContext) {
		// Arrange
		store, err := NewReflectorStore(ctx)
		require.NoError(tb, err)

		userID := uint(123)
		invalidHash := "invalid-hash"

		// Act & Assert
		err = store.markBlobAsReceived(t.Context(), userID, invalidHash, 100)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to decode blob hash")
	})
}
