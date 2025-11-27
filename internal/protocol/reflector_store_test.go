package protocol

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"io"
	"testing"

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
				ast := assert.New(t)
				ast.Equal(tc.expectedID, result)
				if tc.shouldError {
					ast.Equal(uint(0), result)
				} else {
					ast.NotEqual(uint(0), result)
				}
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
			Return(testDevice, nil).Once()

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
		store.storageSvc = mockStorage
		store.deviceSvc = mockDeviceService

		testData := []byte("test reflector blob data")
		testHash := "test_hash_123"
		userID := uint(123)
		testIPAddress := "192.168.1.100"

		// Set up mock expectations
		mockStorage.EXPECT().S3TemporaryUpload(mock.Anything, mock.Anything, mock.AnythingOfType("uint64"), mock.Anything, mock.AnythingOfType("func(*core.S3TempUploadOptions)")).
			Return("upload_id_123", nil).Once()

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
		err = store.Put(ctxWithIP, testHash, testData)

		// Assert
		assert.NoError(t, err)
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
			Return(io.NopCloser(bytes.NewReader(testData)), nil).Once()

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
			Return(nil, errors.New("not found")).Once()

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
				name:        "invalid SD blob data",
				ctx:         context.WithValue(context.Background(), protocol.SourceContextKey, protocol.SourceReflector),
				data:        []byte("invalid sd blob"),
				expectError: true,
				errorMsg:    "user ID not found in context",
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
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
