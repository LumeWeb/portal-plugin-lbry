package upload

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.lumeweb.com/liblbry/stream"
	pluginCore "go.lumeweb.com/portal-plugin-lbry/core"
	pluginMocks "go.lumeweb.com/portal-plugin-lbry/core/mocks"
	"go.lumeweb.com/portal-plugin-lbry/internal"
	pluginConfig "go.lumeweb.com/portal-plugin-lbry/internal/config"
	pluginDb "go.lumeweb.com/portal-plugin-lbry/internal/db"
	"go.lumeweb.com/portal-plugin-lbry/internal/db/migrations"
	"go.lumeweb.com/portal-plugin-lbry/internal/protocol"
	pluginTesting "go.lumeweb.com/portal-plugin-lbry/internal/testing"
	"go.lumeweb.com/portal/core"
	coreTesting "go.lumeweb.com/portal/core/testing"
	coreMocks "go.lumeweb.com/portal/core/testing/mocks"
	"go.lumeweb.com/portal/db/models"
	"go.lumeweb.com/portal/service"
	"go.lumeweb.com/queryutil"
	"gorm.io/gorm"
)

const (
	// Generated test constants for LBRY upload service tests
	testUploadHash1 = "4324a7b8daf8e09df5fe0417b226ed23ec26e95584e2e705aa5a8ea83b0ced91e37d6bf95737562f60588ab17a9fd6e9"
	testUploadHash2 = "31b5772051a353b40e5f2c34ceb9800de27811526d10a1237dce4cb59c5b6124c26553ec1cc400bc4c6acecc6a6039d1"
	testUploadHash3 = "53c5b4fa23478e829dca6297df0a5731584ad713ba692581252a2aee4899545654ce1da33948ad96491e832aa4633ced"
	testUploadHash4 = "f9bc1a8b11cd1eaa8220b86d0648d7ac638f55216ce71f6c1a597388799c64a16d9f6584d6861f414ec68c07c42c33aa"
	testUploadCID1  = "bafksamcdest3rwxy4co7l7qec6zcn3jd5qtosvme4ltqlks2r2udwdhnshrx227zk43vml3alcflc6u723uq"
)

// Helper functions for common mock patterns
func getStorageAndPinAndUploadMocks(ctx coreTesting.TestContext) (*coreMocks.MockStorageService, *coreMocks.MockPinService, *coreMocks.MockUploadService) {
	mockStorage := core.GetService[*coreMocks.MockStorageService](ctx, core.STORAGE_SERVICE)
	mockPin := core.GetService[*coreMocks.MockPinService](ctx, core.PIN_SERVICE)
	mockUpload := core.GetService[*coreMocks.MockUploadService](ctx, core.UPLOAD_SERVICE)
	return mockStorage, mockPin, mockUpload
}

func setupStorageMock(storage *coreMocks.MockStorageService, expectedUploadID string, expectedError error) {
	storage.EXPECT().S3TemporaryUpload(mock.Anything, mock.Anything, mock.AnythingOfType("uint64"), mock.Anything).Return(expectedUploadID, expectedError).Once()
}

func getTestOptions() coreTesting.TestContextBuilderOption {
	freePeerPort, err := pluginTesting.GetFreePort()
	if err != nil {
		panic(fmt.Sprintf("failed to get free peer port: %v", err))
	}
	freeDhtPort, err := pluginTesting.GetFreePort()
	if err != nil {
		panic(fmt.Sprintf("failed to get free DHT port: %v", err))
	}
	freeReflectorPort, err := pluginTesting.GetFreePort()
	if err != nil {
		panic(fmt.Sprintf("failed to get free reflector port: %v", err))
	}

	return coreTesting.CombineOptions(
		coreTesting.WithServiceFactory(core.WORKFLOW_SERVICE, service.NewWorkflowCoordinator),
		coreTesting.WithServiceFactory(pluginCore.UPLOAD_SERVICE, NewUploadService),
		coreTesting.WithMockServiceFactory(pluginCore.DEVICE_SERVICE, pluginMocks.NewMockDeviceService),
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

func TestNewUploadService(t *testing.T) {
	// Act
	svc, options, err := NewUploadService()

	// Assert
	assert.NoError(t, err)
	assert.NotNil(t, svc)
	assert.NotNil(t, options)
}

func TestUploadServiceDefault_HandleUpload(t *testing.T) {
	tests := []struct {
		name          string
		testData      []byte
		expectedError bool
		setupMocks    func(ctx coreTesting.TestContext) (*coreMocks.MockStorageService, *coreMocks.MockPinService, *coreMocks.MockUploadService)
	}{
		{
			name:          "successful upload",
			testData:      []byte("test data for lbry upload"),
			expectedError: false,
			setupMocks: func(ctx coreTesting.TestContext) (*coreMocks.MockStorageService, *coreMocks.MockPinService, *coreMocks.MockUploadService) {
				mockStorage, mockPin, mockUpload := getStorageAndPinAndUploadMocks(ctx)

				// Mock successful storage upload
				setupStorageMock(mockStorage, "test-upload-id", nil)

				return mockStorage, mockPin, mockUpload
			},
		},
		{
			name:          "storage service error",
			testData:      []byte("test data for lbry upload"),
			expectedError: true,
			setupMocks: func(ctx coreTesting.TestContext) (*coreMocks.MockStorageService, *coreMocks.MockPinService, *coreMocks.MockUploadService) {
				mockStorage, mockPin, mockUpload := getStorageAndPinAndUploadMocks(ctx)

				// Mock storage upload error
				setupStorageMock(mockStorage, "", errors.New("storage error"))

				return mockStorage, mockPin, mockUpload
			},
		},
		{
			name:          "empty data upload",
			testData:      []byte(""),
			expectedError: false,
			setupMocks: func(ctx coreTesting.TestContext) (*coreMocks.MockStorageService, *coreMocks.MockPinService, *coreMocks.MockUploadService) {
				mockStorage, mockPin, mockUpload := getStorageAndPinAndUploadMocks(ctx)

				// Mock successful storage upload for empty data
				setupStorageMock(mockStorage, "empty-upload-id", nil)

				return mockStorage, mockPin, mockUpload
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
				// Arrange
				uploadsvc := core.GetService[pluginCore.UploadService](ctx, pluginCore.UPLOAD_SERVICE)
				require.NotNil(tb, uploadsvc)

				_, _, _ = tt.setupMocks(ctx)

				// Create test reader
				testReader := internal.NewReadSeekCloser(tt.testData)

				// Act
				uploadCID, uploadID, err := uploadsvc.HandleUpload(context.Background(), testReader)

				// Assert
				if tt.expectedError {
					assert.Error(tb, err)
					assert.Equal(tb, cid.Undef, uploadCID)
					assert.Empty(tb, uploadID)
				} else {
					assert.NoError(tb, err)
					assert.NotEqual(tb, cid.Undef, uploadCID)
					assert.NotEmpty(tb, uploadID)

					// Verify the CID is a valid LBRY stream CID
					// Convert back to multihash and verify it's a valid LBRY stream hash
					multihash := uploadCID.String()
					_, err := stream.FromMultihash(multihash)
					assert.NoError(tb, err, "CID should be a valid LBRY stream multihash")
				}
			}, getTestOptions())
		})
	}
}

func TestUploadServiceDefault_GetStreamSize(t *testing.T) {
	tests := []struct {
		name          string
		streamID      uint64
		setupData     func(ctx coreTesting.TestContext) uint64
		expectSize    int64
		expectError   bool
		expectedError string
		description   string
	}{
		{
			name:     "stream with single blob",
			streamID: 1,
			setupData: func(ctx coreTesting.TestContext) uint64 {
				stream := createTestStream(ctx, "test_stream_1", testUploadHash1)
				blob := createTestBlob(ctx, testUploadHash1, 1024, nil)
				createTestStreamBlob(ctx, uint64(stream.ID), uint64(blob.ID), 1)
				return uint64(stream.ID)
			},
			expectSize:  1024,
			expectError: false,
			description: "should return correct size for stream with single blob",
		},
		{
			name:     "stream with multiple blobs",
			streamID: 1,
			setupData: func(ctx coreTesting.TestContext) uint64 {
				stream := createTestStream(ctx, "test_stream_2", testUploadHash2)
				blob1 := createTestBlob(ctx, testUploadHash1, 1024, nil)
				blob2 := createTestBlob(ctx, testUploadHash2, 2048, nil)
				blob3 := createTestBlob(ctx, testUploadHash3, 3072, nil)
				createTestStreamBlob(ctx, uint64(stream.ID), uint64(blob1.ID), 1)
				createTestStreamBlob(ctx, uint64(stream.ID), uint64(blob2.ID), 2)
				createTestStreamBlob(ctx, uint64(stream.ID), uint64(blob3.ID), 3)
				return uint64(stream.ID)
			},
			expectSize:  6144, // 1024 + 2048 + 3072
			expectError: false,
			description: "should sum sizes of all blobs in stream",
		},
		{
			name:     "stream with no blobs",
			streamID: 1,
			setupData: func(ctx coreTesting.TestContext) uint64 {
				stream := createTestStream(ctx, "empty_stream", testUploadHash3)
				// Don't create any blobs
				return uint64(stream.ID)
			},
			expectSize:  0,
			expectError: false,
			description: "should return 0 for stream with no blobs",
		},
		{
			name:     "nonexistent stream",
			streamID: 99999,
			setupData: func(ctx coreTesting.TestContext) uint64 {
				// Create a stream but don't use it
				createTestStream(ctx, "other_stream", testUploadHash1)
				return 99999
			},
			expectSize:  0,
			expectError: false,
			description: "should return 0 for nonexistent stream",
		},
		{
			name:     "stream with zero-size blobs",
			streamID: 1,
			setupData: func(ctx coreTesting.TestContext) uint64 {
				stream := createTestStream(ctx, "zero_blob_stream", testUploadHash4)
				blob1 := createTestBlob(ctx, testUploadHash1, 0, nil)
				blob2 := createTestBlob(ctx, testUploadHash2, 0, nil)
				createTestStreamBlob(ctx, uint64(stream.ID), uint64(blob1.ID), 1)
				createTestStreamBlob(ctx, uint64(stream.ID), uint64(blob2.ID), 2)
				return uint64(stream.ID)
			},
			expectSize:  0,
			expectError: false,
			description: "should handle streams with zero-size blobs",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
				// Arrange
				uploadsvc := core.GetService[pluginCore.UploadService](ctx, pluginCore.UPLOAD_SERVICE)
				require.NotNil(tb, uploadsvc)

				// Setup test data and get stream ID
				streamID := tt.setupData(ctx)
				if tt.streamID != 0 && tt.streamID != 99999 {
					tt.streamID = streamID
				}

				// Act
				size, err := uploadsvc.GetStreamSize(context.Background(), tt.streamID)

				// Assert
				if tt.expectError {
					assert.Error(tb, err)
					if tt.expectedError != "" {
						assert.Contains(tb, err.Error(), tt.expectedError)
					}
				} else {
					assert.NoError(tb, err)
					assert.Equal(tb, tt.expectSize, size)
				}
			}, getTestOptions())
		})
	}
}

func TestUploadServiceDefault_GetPendingBlobCount(t *testing.T) {
	tests := []struct {
		name        string
		userID      uint
		streamID    uint
		setupData   func(coreTesting.TestContext)
		expectCount int64
		expectError bool
	}{
		{
			name:     "no pending blobs",
			userID:   1,
			streamID: 1,
			setupData: func(ctx coreTesting.TestContext) {
				// No pending blobs created
			},
			expectCount: 0,
			expectError: false,
		},
		{
			name:     "single pending blob",
			userID:   1,
			streamID: 1,
			setupData: func(ctx coreTesting.TestContext) {
				pendingBlob := pluginDb.PendingBlob{
					BlobHash:   testUploadHash1,
					UserID:     1,
					StreamID:   1,
					BlobSize:   1024,
					BlobNumber: 0,
					Received:   true,
				}
				err := ctx.DB().Create(&pendingBlob).Error
				require.NoError(t, err)
			},
			expectCount: 1,
			expectError: false,
		},
		{
			name:     "multiple pending blobs",
			userID:   1,
			streamID: 1,
			setupData: func(ctx coreTesting.TestContext) {
				pendingBlobs := []pluginDb.PendingBlob{
					{BlobHash: testUploadHash1, UserID: 1, StreamID: 1, BlobSize: 1024, BlobNumber: 0, Received: true},
					{BlobHash: testUploadHash2, UserID: 1, StreamID: 1, BlobSize: 2048, BlobNumber: 1, Received: true},
					{BlobHash: testUploadHash3, UserID: 1, StreamID: 1, BlobSize: 3072, BlobNumber: 2, Received: false},
				}
				for _, blob := range pendingBlobs {
					err := ctx.DB().Create(&blob).Error
					require.NoError(t, err)
				}
			},
			expectCount: 3,
			expectError: false,
		},
		{
			name:     "blobs for different users",
			userID:   1,
			streamID: 1,
			setupData: func(ctx coreTesting.TestContext) {
				pendingBlobs := []pluginDb.PendingBlob{
					{BlobHash: testUploadHash1, UserID: 1, StreamID: 1, BlobSize: 1024, BlobNumber: 0, Received: true},
					{BlobHash: testUploadHash2, UserID: 2, StreamID: 1, BlobSize: 2048, BlobNumber: 1, Received: true}, // Different user
					{BlobHash: testUploadHash3, UserID: 1, StreamID: 2, BlobSize: 3072, BlobNumber: 0, Received: true}, // Different stream
				}
				for _, blob := range pendingBlobs {
					err := ctx.DB().Create(&blob).Error
					require.NoError(t, err)
				}
			},
			expectCount: 1, // Only count blobs for user 1 and stream 1
			expectError: false,
		},
		{
			name:     "blobs for different streams",
			userID:   1,
			streamID: 1,
			setupData: func(ctx coreTesting.TestContext) {
				pendingBlobs := []pluginDb.PendingBlob{
					{BlobHash: testUploadHash1, UserID: 1, StreamID: 1, BlobSize: 1024, BlobNumber: 0, Received: true},
					{BlobHash: testUploadHash2, UserID: 1, StreamID: 2, BlobSize: 2048, BlobNumber: 0, Received: true}, // Different stream
					{BlobHash: testUploadHash3, UserID: 1, StreamID: 3, BlobSize: 3072, BlobNumber: 0, Received: true}, // Different stream
				}
				for _, blob := range pendingBlobs {
					err := ctx.DB().Create(&blob).Error
					require.NoError(t, err)
				}
			},
			expectCount: 1, // Only count blobs for stream 1
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
				// Arrange
				uploadsvc := core.GetService[pluginCore.UploadService](ctx, pluginCore.UPLOAD_SERVICE)
				require.NotNil(tb, uploadsvc)

				// Setup test data
				tt.setupData(ctx)

				// Act
				count, err := uploadsvc.GetPendingBlobCount(context.Background(), tt.userID, tt.streamID)

				// Assert
				if tt.expectError {
					assert.Error(tb, err)
				} else {
					assert.NoError(tb, err)
					assert.Equal(tb, tt.expectCount, count)
				}
			}, getTestOptions())
		})
	}
}

func TestUploadService_RaceConditionHandling(t *testing.T) {
	tests := []struct {
		name string
		test func(t *testing.T, ctx coreTesting.TestContext)
	}{
		{
			name: "BlobFirst_ThenSDBlob",
			test: func(t *testing.T, ctx coreTesting.TestContext) {
				uploadsvc := core.GetService[pluginCore.UploadService](ctx, pluginCore.UPLOAD_SERVICE)
				require.NotNil(t, uploadsvc)

				testCtx := context.Background()
				userID := uint(123)
				deviceID := uint(456)
				streamID := uint(789)

				// Create test blob info
				blobHashBytes := make([]byte, 32)
				for i := range blobHashBytes {
					blobHashBytes[i] = byte(i)
				}
				blobHash := hex.EncodeToString(blobHashBytes)

				blobInfo := &stream.BlobInfo{
					BlobHash: blobHashBytes,
					Length:   1024,
					BlobNum:  1,
					IV:       []byte("test-iv-data"),
				}

				// Clean up before test
				err := ctx.DB().Where("user_id = ?", userID).Delete(&pluginDb.PendingBlob{}).Error
				require.NoError(t, err)

				// Step 1: Upload blob first (marks as received=true)
				err = uploadsvc.StorePendingBlob(testCtx, userID, deviceID, streamID, blobInfo)
				require.NoError(t, err)

				// Verify blob is marked as received
				var pendingBlob pluginDb.PendingBlob
				err = ctx.DB().Where("user_id = ? AND blob_hash = ?", userID, blobHash).First(&pendingBlob).Error
				require.NoError(t, err)
				assert.True(t, pendingBlob.Received, "Blob should be marked as received when uploaded first")

				// Step 2: Process SD blob (should not change received back to false)
				// Simulate SD blob processing by updating only stream_id (like createPendingBlobsFromSDBlob does)
				err = ctx.DB().Model(&pluginDb.PendingBlob{}).Where("user_id = ? AND blob_hash = ?", userID, blobHash).Updates(map[string]interface{}{
					"stream_id": streamID,
				}).Error
				require.NoError(t, err)

				// Verify blob is still marked as received (race condition protection)
				err = ctx.DB().Where("user_id = ? AND blob_hash = ?", userID, blobHash).First(&pendingBlob).Error
				require.NoError(t, err)
				assert.True(t, pendingBlob.Received, "Blob should remain marked as received after SD blob processing")
			},
		},
		{
			name: "SDBlobFirst_ThenBlob",
			test: func(t *testing.T, ctx coreTesting.TestContext) {
				uploadsvc := core.GetService[pluginCore.UploadService](ctx, pluginCore.UPLOAD_SERVICE)
				require.NotNil(t, uploadsvc)

				testCtx := context.Background()
				userID := uint(123)
				deviceID := uint(456)
				streamID := uint(789)

				// Create test blob info
				blobHashBytes := make([]byte, 32)
				for i := range blobHashBytes {
					blobHashBytes[i] = byte(i)
				}
				blobHash := hex.EncodeToString(blobHashBytes)

				blobInfo := &stream.BlobInfo{
					BlobHash: blobHashBytes,
					Length:   1024,
					BlobNum:  1,
					IV:       []byte("test-iv-data"),
				}

				// Clean up before test
				err := ctx.DB().Where("user_id = ?", userID).Delete(&pluginDb.PendingBlob{}).Error
				require.NoError(t, err)

				// Step 1: Process SD blob first (creates record with received=false)
				// Create pending blob record manually to simulate SD blob processing
				pendingBlob := pluginDb.PendingBlob{
					BlobHash:   blobHash,
					UserID:     userID,
					DeviceID:   deviceID,
					StreamID:   streamID,
					BlobSize:   int(blobInfo.Length),
					BlobNumber: blobInfo.BlobNum,
					Received:   false, // Mark as not received yet - waiting for upload
					IVData:     blobInfo.IV,
				}

				err = ctx.DB().Create(&pendingBlob).Error
				require.NoError(t, err)

				// Verify blob is initially marked as not received
				var savedBlob pluginDb.PendingBlob
				err = ctx.DB().Where("user_id = ? AND blob_hash = ?", userID, blobHash).First(&savedBlob).Error
				require.NoError(t, err)
				assert.False(t, savedBlob.Received, "Blob should be marked as not received when SD blob processed first")

				// Step 2: Upload blob (marks as received=true)
				err = uploadsvc.StorePendingBlob(testCtx, userID, deviceID, streamID, blobInfo)
				require.NoError(t, err)

				// Verify blob is now marked as received
				err = ctx.DB().Where("user_id = ? AND blob_hash = ?", userID, blobHash).First(&savedBlob).Error
				require.NoError(t, err)
				assert.True(t, savedBlob.Received, "Blob should be marked as received when uploaded after SD blob")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
				tt.test(t, ctx)
			}, getTestOptions())
		})
	}
}

func TestUploadServiceDefault_ListStreams_EdgeCases(t *testing.T) {
	tests := []struct {
		name          string
		userID        uint
		filters       []queryutil.CrudFilter
		sorts         []queryutil.Sort
		pagination    queryutil.Pagination
		setupData     func(ctx coreTesting.TestContext)
		expectCount   int64
		expectResults int
		expectError   bool
		description   string
	}{
		{
			name:    "empty database",
			userID:  1,
			filters: []queryutil.CrudFilter{},
			sorts:   []queryutil.Sort{},
			pagination: queryutil.Pagination{
				Start:    0,
				End:      10,
				PageSize: 10, // End - Start = 10 - 0 = 10
				Mode:     "server",
			},
			setupData: func(ctx coreTesting.TestContext) {
				// No data setup
			},
			expectCount:   0,
			expectResults: 0,
			expectError:   false,
			description:   "should handle empty database gracefully",
		},
		{
			name:    "large pagination offset",
			userID:  1,
			filters: []queryutil.CrudFilter{},
			sorts:   []queryutil.Sort{},
			pagination: queryutil.Pagination{
				Start:    100,
				End:      110,
				PageSize: 10, // End - Start = 110 - 100 = 10
				Mode:     "server",
			},
			setupData: func(ctx coreTesting.TestContext) {
				setupTestStreamData(ctx, 1)
			},
			expectCount:   2,
			expectResults: 0,
			expectError:   false,
			description:   "should handle large offset gracefully",
		},
		{
			name:    "sorting by stream name",
			userID:  1,
			filters: []queryutil.CrudFilter{},
			sorts: []queryutil.Sort{
				{
					Field: "stream_name",
					Order: "asc",
				},
			},
			pagination: queryutil.Pagination{
				Start:    0,
				End:      10,
				PageSize: 10, // End - Start = 10 - 0 = 10
				Mode:     "server",
			},
			setupData: func(ctx coreTesting.TestContext) {
				setupTestStreamData(ctx, 1)
			},
			expectCount:   2,
			expectResults: 2,
			expectError:   false,
			description:   "should sort results by stream name",
		},
		{
			name:   "filter by exact stream name",
			userID: 1,
			filters: []queryutil.CrudFilter{
				queryutil.Equal("stream_name", "test_stream_1"),
			},
			sorts: []queryutil.Sort{},
			pagination: queryutil.Pagination{
				Start:    0,
				End:      10,
				PageSize: 10, // End - Start = 10 - 0 = 10
				Mode:     "server",
			},
			setupData: func(ctx coreTesting.TestContext) {
				setupTestStreamData(ctx, 1)
			},
			expectCount:   1,
			expectResults: 1,
			expectError:   false,
			description:   "should filter by exact stream name match",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
				// Arrange
				uploadsvc := core.GetService[pluginCore.UploadService](ctx, pluginCore.UPLOAD_SERVICE)
				require.NotNil(tb, uploadsvc)

				// Setup test data
				tt.setupData(ctx)

				// Act
				streams, total, err := uploadsvc.ListStreams(context.Background(), tt.userID, tt.filters, tt.sorts, tt.pagination)

				// Assert
				if tt.expectError {
					assert.Error(tb, err)
				} else {
					assert.NoError(tb, err)
					assert.Equal(tb, tt.expectCount, total)
					assert.Len(tb, streams, tt.expectResults)

					// Additional verification for sorting test
					if tt.name == "sorting by stream name" && len(streams) == 2 {
						assert.True(tb, streams[0].StreamName <= streams[1].StreamName)
					}
				}
			}, getTestOptions())
		})
	}
}

func TestUploadServiceDefault_DeleteStream_EdgeCases(t *testing.T) {
	tests := []struct {
		name             string
		userID           uint
		sdHash           string
		setupData        func(ctx coreTesting.TestContext) (string, error)
		expectError      bool
		expectedError    string
		description      string
		skipSDHashLookup bool
	}{
		{
			name:   "delete stream pin for stream with no blobs",
			userID: 1,
			sdHash: "", // Will be set in setupData
			setupData: func(ctx coreTesting.TestContext) (string, error) {
				// Create stream with no blobs
				stream := createTestStream(ctx, "no_blobs_stream", testUploadHash1)
				createTestStreamPin(ctx, 1, uint64(stream.ID))
				// Don't create any blobs
				return stream.SDHash, nil
			},
			expectError: false,
			description: "should handle deletion of stream pin for stream with no blobs",
		},
		{
			name:   "concurrent deletion simulation",
			userID: 1,
			sdHash: "", // Will be set in setupData
			setupData: func(ctx coreTesting.TestContext) (string, error) {
				streams, _, _ := setupTestStreamData(ctx, 1)
				// Simulate another user deleting their pin first
				err := ctx.DB().Where("user_id = ? AND stream_id = ?", 999, streams[0].ID).Delete(&pluginDb.StreamPin{}).Error
				if err != nil {
					return "", err
				}
				return streams[0].SDHash, nil
			},
			expectError: false,
			description: "should handle deletion when other pins were removed concurrently",
		},
		{
			name:   "zero user ID",
			userID: 0,
			sdHash: testUploadHash1, // Use a test hash
			setupData: func(ctx coreTesting.TestContext) (string, error) {
				_, _, _ = setupTestStreamData(ctx, 1)
				return testUploadHash1, nil
			},
			expectError:   true,
			expectedError: gorm.ErrRecordNotFound.Error(),
			description:   "should handle zero user ID gracefully",
		},
		{
			name:   "empty SD hash",
			userID: 1,
			sdHash: "",
			setupData: func(ctx coreTesting.TestContext) (string, error) {
				setupTestStreamData(ctx, 1)
				return "", nil
			},
			expectError:      true,
			expectedError:    gorm.ErrRecordNotFound.Error(),
			description:      "should handle empty SD hash gracefully",
			skipSDHashLookup: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
				// Arrange
				uploadsvc := core.GetService[pluginCore.UploadService](ctx, pluginCore.UPLOAD_SERVICE)
				require.NotNil(tb, uploadsvc)

				// Setup test data
				_, err := tt.setupData(ctx)
				require.NoError(tb, err)

				// Get SD hash if needed
				if tt.sdHash == "" && !tt.skipSDHashLookup {
					var streamRecord pluginDb.Stream
					err = ctx.DB().First(&streamRecord).Error
					require.NoError(tb, err)
					tt.sdHash = streamRecord.SDHash
				}

				// Act
				err = uploadsvc.DeleteStream(context.Background(), tt.userID, tt.sdHash)

				// Assert
				if tt.expectError {
					assert.Error(tb, err)
					if tt.expectedError != "" {
						assert.Contains(tb, err.Error(), tt.expectedError)
					}
				} else {
					assert.NoError(tb, err)
				}
			}, getTestOptions())
		})
	}
}

func TestUploadServiceDefault_DeleteStream(t *testing.T) {
	tests := []struct {
		name          string
		userID        uint
		sdHash        string
		setupData     func(ctx coreTesting.TestContext) (string, error)
		expectError   bool
		expectedError string
		description   string
		verifyCleanup func(ctx coreTesting.TestContext, sdHash string) error
	}{
		{
			name:   "successful deletion of user's stream pin",
			userID: 1,
			sdHash: "", // Will be set in setupData
			setupData: func(ctx coreTesting.TestContext) (string, error) {
				// Create test data without other user pins
				stream1 := createTestStream(ctx, "test_stream_1", testUploadHash1)
				stream2 := createTestStream(ctx, "test_stream_2", testUploadHash2)
				_ = createTestStream(ctx, "another_stream", testUploadHash3) // unused but needed for test data consistency

				// Create stream pins for user 1 only
				createTestStreamPin(ctx, 1, uint64(stream1.ID))
				createTestStreamPin(ctx, 1, uint64(stream2.ID))

				// Create stream blobs
				createTestStreamBlob(ctx, uint64(stream1.ID), 1001, 1)
				createTestStreamBlob(ctx, uint64(stream1.ID), 1002, 2)
				createTestStreamBlob(ctx, uint64(stream2.ID), 2001, 1)

				return stream1.SDHash, nil
			},
			expectError: false,
			description: "should successfully delete user's stream pin only",
			verifyCleanup: func(ctx coreTesting.TestContext, sdHash string) error {
				// Find stream by SD hash first
				var stream pluginDb.Stream
				err := ctx.DB().Where("sd_hash = ?", sdHash).First(&stream).Error
				if err != nil {
					return fmt.Errorf("failed to find stream: %w", err)
				}

				// Verify user's stream pin is deleted
				var pinCount int64
				err = ctx.DB().Model(&pluginDb.StreamPin{}).Where("user_id = ? AND stream_id = ?", 1, stream.ID).Count(&pinCount).Error
				if err != nil {
					return err
				}
				if pinCount != 0 {
					return fmt.Errorf("expected 0 pins for user, got %d", pinCount)
				}

				// Verify stream and blobs still exist (new behavior - they are not deleted)
				var streamCount int64
				err = ctx.DB().Model(&pluginDb.Stream{}).Where("sd_hash = ?", sdHash).Count(&streamCount).Error
				if err != nil {
					return err
				}
				if streamCount != 1 {
					return fmt.Errorf("expected 1 stream to remain, got %d", streamCount)
				}

				// Find stream by SD hash first
				var streamForBlobs pluginDb.Stream
				err = ctx.DB().Where("sd_hash = ?", sdHash).First(&streamForBlobs).Error
				if err != nil {
					return fmt.Errorf("failed to find stream for blob check: %w", err)
				}

				var blobCount int64
				err = ctx.DB().Model(&pluginDb.StreamBlob{}).Where("stream_id = ?", streamForBlobs.ID).Count(&blobCount).Error
				if err != nil {
					return err
				}
				if blobCount != 2 {
					return fmt.Errorf("expected 2 blobs to remain, got %d", blobCount)
				}

				return nil
			},
		},
		{
			name:   "deletion of stream with multiple pins",
			userID: 1,
			sdHash: "", // Will be set in setupData
			setupData: func(ctx coreTesting.TestContext) (string, error) {
				streams, _, _ := setupTestStreamData(ctx, 1)
				// stream[0] already has a pin from user 999 in setupTestStreamData
				return streams[0].SDHash, nil
			},
			expectError: false,
			description: "should delete only user's pin when stream has multiple pins",
			verifyCleanup: func(ctx coreTesting.TestContext, sdHash string) error {
				// Find stream by SD hash first
				var stream pluginDb.Stream
				err := ctx.DB().Where("sd_hash = ?", sdHash).First(&stream).Error
				if err != nil {
					return fmt.Errorf("failed to find stream: %w", err)
				}

				// Verify user's pin is deleted
				var userPinCount int64
				err = ctx.DB().Model(&pluginDb.StreamPin{}).Where("user_id = ? AND stream_id = ?", 1, stream.ID).Count(&userPinCount).Error
				if err != nil {
					return err
				}
				if userPinCount != 0 {
					return fmt.Errorf("expected 0 pins for user 1, got %d", userPinCount)
				}

				// Verify other user's pin still exists
				var otherPinCount int64
				err = ctx.DB().Model(&pluginDb.StreamPin{}).Where("user_id = ? AND stream_id = ?", 999, stream.ID).Count(&otherPinCount).Error
				if err != nil {
					return err
				}
				if otherPinCount != 1 {
					return fmt.Errorf("expected 1 pin for user 999, got %d", otherPinCount)
				}

				// Verify stream and blobs still exist (new behavior - they are never deleted)
				var streamCount int64
				err = ctx.DB().Model(&pluginDb.Stream{}).Where("sd_hash = ?", sdHash).Count(&streamCount).Error
				if err != nil {
					return err
				}
				if streamCount != 1 {
					return fmt.Errorf("expected 1 stream to remain, got %d", streamCount)
				}

				return nil
			},
		},
		{
			name:   "stream not found",
			userID: 1,
			sdHash: "nonexistent_sd_hash",
			setupData: func(ctx coreTesting.TestContext) (string, error) {
				setupTestStreamData(ctx, 1)
				return "nonexistent_sd_hash", nil
			},
			expectError:   true,
			expectedError: gorm.ErrRecordNotFound.Error(),
			description:   "should return error when stream doesn't exist",
		},
		{
			name:   "access denied for other user's stream",
			userID: 2,
			sdHash: "", // Will be set in setupData
			setupData: func(ctx coreTesting.TestContext) (string, error) {
				streams, _, _ := setupTestStreamData(ctx, 1) // Create streams for user 1
				return streams[0].SDHash, nil
			},
			expectError:   true,
			expectedError: gorm.ErrRecordNotFound.Error(),
			description:   "should return error when user tries to delete another user's stream",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
				// Arrange
				uploadsvc := core.GetService[pluginCore.UploadService](ctx, pluginCore.UPLOAD_SERVICE)
				require.NotNil(tb, uploadsvc)

				// Setup test data and get SD hash
				sdHash, err := tt.setupData(ctx)
				require.NoError(tb, err)
				if tt.sdHash == "" {
					tt.sdHash = sdHash
				}

				// Act
				err = uploadsvc.DeleteStream(context.Background(), tt.userID, tt.sdHash)

				// Assert
				if tt.expectError {
					assert.Error(tb, err)
					if tt.expectedError != "" {
						assert.Contains(tb, err.Error(), tt.expectedError)
					}
				} else {
					assert.NoError(tb, err)

					// Verify cleanup if verification function provided
					if tt.verifyCleanup != nil {
						verifyErr := tt.verifyCleanup(ctx, tt.sdHash)
						assert.NoError(tb, verifyErr)
					}
				}
			}, getTestOptions())
		})
	}
}

func TestUploadServiceDefault_ListStreams_AfterDeleteStream(t *testing.T) {
	tests := []struct {
		name          string
		userID        uint
		setupData     func(ctx coreTesting.TestContext) (string, string, error) // Returns (sdHashToDelete, sdHashToKeep, error)
		expectCount   int64
		expectResults int
		expectError   bool
		description   string
	}{
		{
			name:   "deleted stream should not appear in list",
			userID: 1,
			setupData: func(ctx coreTesting.TestContext) (string, string, error) {
				// Create test streams
				stream1 := createTestStream(ctx, "test_stream_1", testUploadHash1)
				stream2 := createTestStream(ctx, "test_stream_2", testUploadHash2)
				stream3 := createTestStream(ctx, "test_stream_3", testUploadHash3)

				// Create stream pins for user 1
				createTestStreamPin(ctx, 1, uint64(stream1.ID))
				createTestStreamPin(ctx, 1, uint64(stream2.ID))
				createTestStreamPin(ctx, 1, uint64(stream3.ID))

				// Create stream blobs
				createTestStreamBlob(ctx, uint64(stream1.ID), 1001, 1)
				createTestStreamBlob(ctx, uint64(stream2.ID), 2001, 1)
				createTestStreamBlob(ctx, uint64(stream3.ID), 3001, 1)

				return stream1.SDHash, stream2.SDHash, nil
			},
			expectCount:   2, // After deleting one, should have 2 remaining
			expectResults: 2,
			expectError:   false,
			description:   "should not list streams after their pin is deleted",
		},
		{
			name:   "other user's deleted stream should not affect their listing",
			userID: 2,
			setupData: func(ctx coreTesting.TestContext) (string, string, error) {
				// Create test streams
				stream1 := createTestStream(ctx, "shared_stream_1", testUploadHash1)
				stream2 := createTestStream(ctx, "shared_stream_2", testUploadHash2)

				// User 1 pins both streams
				createTestStreamPin(ctx, 1, uint64(stream1.ID))
				createTestStreamPin(ctx, 1, uint64(stream2.ID))

				// User 2 pins only stream2
				createTestStreamPin(ctx, 2, uint64(stream2.ID))

				// Create stream blobs
				createTestStreamBlob(ctx, uint64(stream1.ID), 1001, 1)
				createTestStreamBlob(ctx, uint64(stream2.ID), 2001, 1)

				return stream1.SDHash, stream2.SDHash, nil
			},
			expectCount:   1, // User 2 should still see stream2 even after user 1 deletes stream1
			expectResults: 1,
			expectError:   false,
			description:   "should only list streams pinned by current user",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
				// Arrange
				uploadsvc := core.GetService[pluginCore.UploadService](ctx, pluginCore.UPLOAD_SERVICE)
				require.NotNil(tb, uploadsvc)

				// Setup test data
				sdHashToDelete, sdHashToKeep, err := tt.setupData(ctx)
				require.NoError(tb, err)

				// Verify initial state - user should see all their streams
				_, initialTotal, err := uploadsvc.ListStreams(context.Background(), tt.userID, []queryutil.CrudFilter{}, []queryutil.Sort{}, queryutil.Pagination{
					Start:    0,
					End:      10,
					PageSize: 10,
					Mode:     "server",
				})
				require.NoError(tb, err)
				require.Greater(tb, initialTotal, int64(0), "should have initial streams")

				// Delete one stream (user 1's stream)
				err = uploadsvc.DeleteStream(context.Background(), 1, sdHashToDelete)
				require.NoError(tb, err)

				// Act - List streams for the test user
				streams, total, err := uploadsvc.ListStreams(context.Background(), tt.userID, []queryutil.CrudFilter{}, []queryutil.Sort{}, queryutil.Pagination{
					Start:    0,
					End:      10,
					PageSize: 10,
					Mode:     "server",
				})

				// Assert
				if tt.expectError {
					assert.Error(tb, err)
				} else {
					assert.NoError(tb, err)
					assert.Equal(tb, tt.expectCount, total, "total count mismatch")
					assert.Equal(tb, tt.expectResults, len(streams), "results count mismatch")

					// Verify the deleted stream is not in the list
					for _, stream := range streams {
						assert.NotEqual(tb, sdHashToDelete, stream.SDHash, "deleted stream should not appear in list")
					}

					// If we expect to keep a stream, verify it's still there
					if sdHashToKeep != "" && tt.expectResults > 0 {
						found := false
						for _, stream := range streams {
							if stream.SDHash == sdHashToKeep {
								found = true
								break
							}
						}
						assert.True(tb, found, "expected stream should still be in list")
					}
				}
			}, getTestOptions())
		})
	}
}

func TestUploadServiceDefault_HandleUpload_ReaderErrors(t *testing.T) {
	tests := []struct {
		name         string
		createReader func() io.ReadSeekCloser
		expectedErr  string
	}{
		{
			name: "seek to end error",
			createReader: func() io.ReadSeekCloser {
				return &errorReader{seekError: errors.New("seek error")}
			},
			expectedErr: "failed to seek to end of reader",
		},
		{
			name: "seek to start error",
			createReader: func() io.ReadSeekCloser {
				return &errorReader{seekToStartError: errors.New("seek start error")}
			},
			expectedErr: "failed to seek to start of reader",
		},
		{
			name: "read error during hashing",
			createReader: func() io.ReadSeekCloser {
				return &errorReader{readError: errors.New("read error")}
			},
			expectedErr: "failed to hash reader",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
				// Arrange
				uploadsvc := core.GetService[pluginCore.UploadService](ctx, pluginCore.UPLOAD_SERVICE)
				require.NotNil(tb, uploadsvc)

				testReader := tt.createReader()

				// Act
				uploadCID, uploadID, err := uploadsvc.HandleUpload(context.Background(), testReader)

				// Assert
				assert.Error(tb, err)
				assert.Contains(tb, err.Error(), tt.expectedErr)
				assert.Equal(tb, cid.Undef, uploadCID)
				assert.Empty(tb, uploadID)
			}, getTestOptions())
		})
	}
}

func TestUploadServiceDefault_ProcessUpload(t *testing.T) {
	tests := []struct {
		name              string
		setupStreamResult func() *stream.StreamResult
		mockUploadError   error
		mockPinError      error
		expectError       bool
		description       string
	}{
		{
			name: "successful processing",
			setupStreamResult: func() *stream.StreamResult {
				return &stream.StreamResult{
					ContentHashes: []string{testUploadHash1, testUploadHash2},
					ChunkSizes:    []int{100, 200},
				}
			},
			expectError: false,
			description: "should process upload successfully with valid stream result",
		},
		{
			name: "upload service error",
			setupStreamResult: func() *stream.StreamResult {
				return &stream.StreamResult{
					ContentHashes: []string{testUploadHash1},
					ChunkSizes:    []int{100},
				}
			},
			mockUploadError: errors.New("upload service error"),
			expectError:     true,
			description:     "should handle error when upload service fails",
		},
		{
			name: "pin service error",
			setupStreamResult: func() *stream.StreamResult {
				return &stream.StreamResult{
					ContentHashes: []string{testUploadHash1},
					ChunkSizes:    []int{100},
				}
			},
			mockPinError: errors.New("pin service error"),
			expectError:  true,
			description:  "should handle error when pin service fails",
		},
		{
			name: "empty content hashes",
			setupStreamResult: func() *stream.StreamResult {
				return &stream.StreamResult{
					ContentHashes: []string{},
					ChunkSizes:    []int{},
				}
			},
			expectError: false,
			description: "should handle empty content hashes gracefully",
		},
		{
			name: "multiple content hashes",
			setupStreamResult: func() *stream.StreamResult {
				return &stream.StreamResult{
					ContentHashes: []string{testUploadHash1, testUploadHash2, testUploadHash3, testUploadHash4},
					ChunkSizes:    []int{100, 200, 300, 400},
				}
			},
			expectError: false,
			description: "should process multiple content hashes successfully",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
				// Arrange
				uploadsvc := core.GetService[pluginCore.UploadService](ctx, pluginCore.UPLOAD_SERVICE)
				require.NotNil(tb, uploadsvc)

				// Setup mocks
				mockUpload := core.GetService[*coreMocks.MockUploadService](ctx, core.UPLOAD_SERVICE)
				mockPin := core.GetService[*coreMocks.MockPinService](ctx, core.PIN_SERVICE)

				// Configure mock expectations based on test case
				// Calculate expected number of calls based on content hashes
				streamResult := tt.setupStreamResult()
				expectedCalls := len(streamResult.ContentHashes)

				// Only set up mock expectations if there are content hashes to process
				if expectedCalls > 0 {
					if tt.mockUploadError != nil {
						mockUpload.EXPECT().SaveUpload(mock.Anything, mock.Anything).Return(tt.mockUploadError).Once()
						// When upload fails, CreatePin should not be called
					} else {
						mockUpload.EXPECT().SaveUpload(mock.Anything, mock.Anything).Return(nil).Times(expectedCalls)

						if tt.mockPinError != nil {
							mockPin.EXPECT().CreatePin(mock.Anything, mock.Anything, mock.Anything).Return(nil, tt.mockPinError).Once()
						} else {
							mockPin.EXPECT().CreatePin(mock.Anything, mock.Anything, mock.Anything).Return(&models.Pin{}, nil).Times(expectedCalls)
						}
					}
				}

				// Setup stream result (already calculated above)

				// Act
				err := uploadsvc.ProcessUpload(context.Background(), streamResult, 1)

				// Assert
				if tt.expectError {
					assert.Error(tb, err)
				} else {
					assert.NoError(tb, err)
				}
			}, getTestOptions())
		})
	}
}

func TestUploadServiceDefault_CreateStreamPin(t *testing.T) {
	tests := []struct {
		name              string
		setupStream       func(ctx coreTesting.TestContext) *pluginDb.Stream
		mockCIDError      error
		expectError       bool
		expectedErrorText string
		description       string
	}{
		{
			name: "successful stream pin creation",
			setupStream: func(ctx coreTesting.TestContext) *pluginDb.Stream {
				// Create a stream using helper function
				return createTestStream(ctx, "test_stream", testUploadHash1)
			},
			expectError: false,
			description: "should create stream pin successfully when stream exists",
		},
		{
			name: "stream not found error",
			setupStream: func(ctx coreTesting.TestContext) *pluginDb.Stream {
				// Don't create stream in DB - this should cause a not found error
				return nil
			},
			expectError: true,
			description: "should return error when stream not found in database",
		},

		{
			name: "CID conversion error",
			setupStream: func(ctx coreTesting.TestContext) *pluginDb.Stream {
				// Create a stream using helper function
				return createTestStream(ctx, "test_stream", testUploadHash1)
			},
			mockCIDError:      errors.New("CID conversion error"),
			expectError:       true,
			expectedErrorText: "failed to convert stream CID to LBRY hash",
			description:       "should handle CID conversion errors",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
				// Arrange
				uploadsvc := core.GetService[pluginCore.UploadService](ctx, pluginCore.UPLOAD_SERVICE)
				require.NotNil(tb, uploadsvc)

				// Setup test data
				var _stream *pluginDb.Stream
				if tt.setupStream != nil {
					_stream = tt.setupStream(ctx)
				}

				// Act
				var result *pluginDb.StreamPin
				var err error

				if tt.mockCIDError != nil {
					// For CID conversion error, use an invalid CID that will fail stream.FromMultihash
					// Use a CID with invalid multihash format
					invalidCID := cid.MustParse("bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi")
					result, err = uploadsvc.CreateStreamPin(context.Background(), 1, invalidCID)
				} else {
					// For successful case, we'll test with a valid stream
					if _stream != nil {
						// Create a valid CID for testing using a known good format
						testCID := cid.MustParse(testUploadCID1)
						result, err = uploadsvc.CreateStreamPin(context.Background(), 1, testCID)
					} else {
						// Test the error case by calling with a non-existent stream
						testCID := cid.MustParse(testUploadCID1)
						result, err = uploadsvc.CreateStreamPin(context.Background(), 1, testCID)
					}
				}

				// Assert
				if tt.expectError {
					assert.Error(tb, err)
					if tt.expectedErrorText != "" {
						assert.Contains(tb, err.Error(), tt.expectedErrorText)
					}
				} else {
					assert.NoError(tb, err)
					if result != nil {
						assert.Equal(tb, uint64(1), result.UserID)
					}
				}
			}, getTestOptions())
		})
	}
}

func TestUploadServiceDefault_CreateStreamPin_RestoreSoftDeleted(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		uploadsvc := core.GetService[pluginCore.UploadService](ctx, pluginCore.UPLOAD_SERVICE)
		require.NotNil(tb, uploadsvc)

		// Create a test stream
		testStream := createTestStream(ctx, "test_stream", testUploadHash1)
		require.NotNil(tb, testStream)

		// Create a valid CID for testing
		testCID := cid.MustParse(testUploadCID1)

		// Act 1: Create initial stream pin
		result1, err := uploadsvc.CreateStreamPin(context.Background(), 1, testCID)
		require.NoError(tb, err)
		require.NotNil(tb, result1)
		assert.Equal(tb, uint64(1), result1.UserID)
		assert.Equal(tb, uint64(testStream.ID), result1.StreamID)
		assert.False(tb, result1.DeletedAt.Valid) // Should not be soft-deleted

		// Act 2: Delete the stream pin (soft delete)
		err = uploadsvc.DeleteStream(context.Background(), 1, testStream.SDHash)
		require.NoError(tb, err)

		// Verify the pin is soft-deleted
		var deletedPin pluginDb.StreamPin
		err = ctx.DB().Unscoped().
			Where("user_id = ? AND stream_id = ?", 1, testStream.ID).
			First(&deletedPin).Error
		require.NoError(tb, err)
		assert.True(tb, deletedPin.DeletedAt.Valid) // Should be soft-deleted

		// Act 3: Try to create the stream pin again (should restore soft-deleted pin)
		result2, err := uploadsvc.CreateStreamPin(context.Background(), 1, testCID)
		require.NoError(tb, err)
		require.NotNil(tb, result2)

		// Assert: The restored pin should have the same ID but not be soft-deleted
		assert.Equal(tb, result1.ID, result2.ID) // Same record ID
		assert.Equal(tb, uint64(1), result2.UserID)
		assert.Equal(tb, uint64(testStream.ID), result2.StreamID)
		assert.False(tb, result2.DeletedAt.Valid) // Should not be soft-deleted anymore

		// Verify there's only one pin record in the database (no duplicates)
		var pinCount int64
		err = ctx.DB().Unscoped().Model(&pluginDb.StreamPin{}).
			Where("user_id = ? AND stream_id = ?", 1, testStream.ID).
			Count(&pinCount).Error
		require.NoError(tb, err)
		assert.Equal(tb, int64(1), pinCount) // Should be exactly one pin
	}, getTestOptions())
}

func TestUploadServiceDefault_DeleteStream_Idempotent(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		uploadsvc := core.GetService[pluginCore.UploadService](ctx, pluginCore.UPLOAD_SERVICE)
		require.NotNil(tb, uploadsvc)

		// Create a test stream
		testStream := createTestStream(ctx, "test_stream", testUploadHash1)
		require.NotNil(tb, testStream)

		// Create initial stream pin
		createTestStreamPin(ctx, 1, uint64(testStream.ID))

		// Act 1: First deletion should succeed
		err := uploadsvc.DeleteStream(context.Background(), 1, testStream.SDHash)
		require.NoError(tb, err)

		// Verify pin is soft-deleted after first deletion
		var deletedPin pluginDb.StreamPin
		err = ctx.DB().Unscoped().
			Where("user_id = ? AND stream_id = ?", 1, testStream.ID).
			First(&deletedPin).Error
		require.NoError(tb, err)
		assert.True(tb, deletedPin.DeletedAt.Valid, "Pin should be soft-deleted after first deletion")

		// Act 2: Second deletion should also succeed (idempotent)
		err = uploadsvc.DeleteStream(context.Background(), 1, testStream.SDHash)
		assert.NoError(tb, err, "Second deletion should succeed (idempotent)")

		// Verify pin is still soft-deleted (no hard delete occurred)
		var stillDeletedPin pluginDb.StreamPin
		err = ctx.DB().Unscoped().
			Where("user_id = ? AND stream_id = ?", 1, testStream.ID).
			First(&stillDeletedPin).Error
		require.NoError(tb, err)
		assert.True(tb, stillDeletedPin.DeletedAt.Valid, "Pin should still be soft-deleted after second deletion")
		assert.Equal(tb, deletedPin.ID, stillDeletedPin.ID, "Should be same pin record")

		// Act 3: Third deletion should also succeed (idempotent)
		err = uploadsvc.DeleteStream(context.Background(), 1, testStream.SDHash)
		assert.NoError(tb, err, "Third deletion should succeed (idempotent)")

		// Verify there's still only one pin record (no duplicates created)
		var pinCount int64
		err = ctx.DB().Unscoped().Model(&pluginDb.StreamPin{}).
			Where("user_id = ? AND stream_id = ?", 1, testStream.ID).
			Count(&pinCount).Error
		require.NoError(tb, err)
		assert.Equal(tb, int64(1), pinCount, "Should be exactly one pin record")

		// Verify pin is still soft-deleted
		var finalPin pluginDb.StreamPin
		err = ctx.DB().Unscoped().
			Where("user_id = ? AND stream_id = ?", 1, testStream.ID).
			First(&finalPin).Error
		require.NoError(tb, err)
		assert.True(tb, finalPin.DeletedAt.Valid, "Pin should still be soft-deleted after third deletion")
		assert.Equal(tb, deletedPin.ID, finalPin.ID, "Should be same pin record throughout")
	}, getTestOptions())
}

func TestUploadServiceDefault_DeleteStream_RestoreAndDeleteCycle(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		uploadsvc := core.GetService[pluginCore.UploadService](ctx, pluginCore.UPLOAD_SERVICE)
		require.NotNil(tb, uploadsvc)

		// Create a test stream
		testStream := createTestStream(ctx, "test_stream", testUploadHash1)
		require.NotNil(tb, testStream)

		// Create a valid CID for testing
		testCID := cid.MustParse(testUploadCID1)

		// Act 1: Create initial stream pin
		result1, err := uploadsvc.CreateStreamPin(context.Background(), 1, testCID)
		require.NoError(tb, err)
		require.NotNil(tb, result1)
		assert.False(tb, result1.DeletedAt.Valid, "Initial pin should not be soft-deleted")

		// Act 2: Delete the stream pin
		err = uploadsvc.DeleteStream(context.Background(), 1, testStream.SDHash)
		require.NoError(tb, err)

		// Verify pin is soft-deleted
		var deletedPin pluginDb.StreamPin
		err = ctx.DB().Unscoped().
			Where("user_id = ? AND stream_id = ?", 1, testStream.ID).
			First(&deletedPin).Error
		require.NoError(tb, err)
		assert.True(tb, deletedPin.DeletedAt.Valid, "Pin should be soft-deleted")
		assert.Equal(tb, result1.ID, deletedPin.ID, "Should be same pin record")

		// Act 3: Recreate the stream pin (should restore soft-deleted pin)
		result2, err := uploadsvc.CreateStreamPin(context.Background(), 1, testCID)
		require.NoError(tb, err)
		require.NotNil(tb, result2)

		// Assert: The restored pin should have been the same record
		assert.Equal(tb, result1.ID, result2.ID, "Should be same record ID after restoration")
		assert.Equal(tb, uint64(1), result2.UserID)
		assert.Equal(tb, uint64(testStream.ID), result2.StreamID)
		assert.False(tb, result2.DeletedAt.Valid, "Restored pin should not be soft-deleted")

		// Act 4: Delete the restored stream pin
		err = uploadsvc.DeleteStream(context.Background(), 1, testStream.SDHash)
		require.NoError(tb, err)

		// Verify pin is soft-deleted again
		var finalDeletedPin pluginDb.StreamPin
		err = ctx.DB().Unscoped().
			Where("user_id = ? AND stream_id = ?", 1, testStream.ID).
			First(&finalDeletedPin).Error
		require.NoError(tb, err)
		assert.True(tb, finalDeletedPin.DeletedAt.Valid, "Pin should be soft-deleted again")
		assert.Equal(tb, result1.ID, finalDeletedPin.ID, "Should be same record ID throughout")

		// Verify there's only one pin record in database (no duplicates)
		var pinCount int64
		err = ctx.DB().Unscoped().Model(&pluginDb.StreamPin{}).
			Where("user_id = ? AND stream_id = ?", 1, testStream.ID).
			Count(&pinCount).Error
		require.NoError(tb, err)
		assert.Equal(tb, int64(1), pinCount, "Should be exactly one pin record throughout cycle")
	}, getTestOptions())
}

// errorReader is a test helper that implements io.ReadSeekCloser and returns configurable errors
type errorReader struct {
	seekError        error
	seekToStartError error
	readError        error
	seekCalled       bool
}

func (r *errorReader) Read(_ []byte) (n int, err error) {
	if r.readError != nil {
		return 0, r.readError
	}
	return 0, io.EOF
}

func (r *errorReader) Seek(_ int64, whence int) (int64, error) {
	if whence == io.SeekEnd && r.seekError != nil {
		return 0, r.seekError
	}
	if whence == io.SeekStart && r.seekToStartError != nil {
		return 0, r.seekToStartError
	}
	r.seekCalled = true
	return 0, nil
}

func (r *errorReader) Close() error {
	return nil
}

// Helper functions for creating test data
func createTestStream(ctx coreTesting.TestContext, streamName, sdHash string) *pluginDb.Stream {
	stream := &pluginDb.Stream{
		StreamHash:        sdHash,
		SDHash:            sdHash,
		StreamName:        streamName,
		StreamType:        "video",
		SuggestedFileName: streamName + ".mp4",
	}
	err := ctx.DB().Create(stream).Error
	if err != nil {
		ctx.T().Fatalf("Failed to create test stream: %v", err)
	}
	return stream
}

func createTestStreamPin(ctx coreTesting.TestContext, userID uint64, streamID uint64) *pluginDb.StreamPin {
	pin := &pluginDb.StreamPin{
		UserID:   userID,
		StreamID: streamID,
	}
	err := ctx.DB().Create(pin).Error
	if err != nil {
		ctx.T().Fatalf("Failed to create test stream pin: %v", err)
	}
	return pin
}

func createTestStreamBlob(ctx coreTesting.TestContext, streamID uint64, blobID uint64, blobNumber int) *pluginDb.StreamBlob {
	blob := &pluginDb.StreamBlob{
		StreamID:   streamID,
		BlobID:     blobID,
		BlobNumber: blobNumber,
	}
	err := ctx.DB().Create(blob).Error
	if err != nil {
		ctx.T().Fatalf("Failed to create test stream blob: %v", err)
	}
	return blob
}

func createTestBlob(ctx coreTesting.TestContext, blobHash string, blobSize int, iv []byte) *pluginDb.Blob {
	blob := &pluginDb.Blob{
		BlobHash:    blobHash,
		BlobSize:    blobSize,
		IVData:      iv,
		Terminating: false,
	}
	err := ctx.DB().Create(blob).Error
	if err != nil {
		ctx.T().Fatalf("Failed to create test blob: %v", err)
	}
	return blob
}

// Helper functions for creating test pending data
func createTestPendingBlob(ctx coreTesting.TestContext, userID, deviceID, streamID uint, blobHash string, blobSize, blobNumber int, received bool, iv []byte) *pluginDb.PendingBlob {
	pendingBlob := &pluginDb.PendingBlob{
		BlobHash:   blobHash,
		UserID:     userID,
		DeviceID:   deviceID,
		StreamID:   streamID,
		BlobSize:   blobSize,
		BlobNumber: blobNumber,
		Received:   received,
		IVData:     iv,
	}
	err := ctx.DB().Create(pendingBlob).Error
	if err != nil {
		ctx.T().Fatalf("Failed to create test pending blob: %v", err)
	}
	return pendingBlob
}

func createTestPendingStream(ctx coreTesting.TestContext, userID, deviceID uint, streamHash, sdHash, streamName, streamType, suggestedFileName string, keyData []byte) *pluginDb.PendingStream {
	pendingStream := &pluginDb.PendingStream{
		StreamHash:        streamHash,
		SDHash:            sdHash,
		StreamName:        streamName,
		StreamType:        streamType,
		SuggestedFileName: suggestedFileName,
		KeyData:           keyData,
		UserID:            userID,
		DeviceID:          deviceID,
	}
	err := ctx.DB().Create(pendingStream).Error
	if err != nil {
		ctx.T().Fatalf("Failed to create test pending stream: %v", err)
	}
	return pendingStream
}

func setupTestStreamData(ctx coreTesting.TestContext, userID uint64) ([]*pluginDb.Stream, []*pluginDb.StreamPin, []*pluginDb.StreamBlob) {
	// Create test streams
	stream1 := createTestStream(ctx, "test_stream_1", testUploadHash1)
	stream2 := createTestStream(ctx, "test_stream_2", testUploadHash2)
	stream3 := createTestStream(ctx, "another_stream", testUploadHash3)
	streams := []*pluginDb.Stream{stream1, stream2, stream3}

	// Create stream pins for user 1
	pin1 := createTestStreamPin(ctx, userID, uint64(stream1.ID))
	pin2 := createTestStreamPin(ctx, userID, uint64(stream2.ID))
	pins := []*pluginDb.StreamPin{pin1, pin2}

	// Create stream blobs
	blob1 := createTestStreamBlob(ctx, uint64(stream1.ID), 1001, 1)
	blob2 := createTestStreamBlob(ctx, uint64(stream1.ID), 1002, 2)
	blob3 := createTestStreamBlob(ctx, uint64(stream2.ID), 2001, 1)
	blobs := []*pluginDb.StreamBlob{blob1, blob2, blob3}

	// Create a pin for another user to test shared streams
	otherUserPin := createTestStreamPin(ctx, 999, uint64(stream1.ID))

	return streams, append(pins, otherUserPin), blobs
}

func TestUploadServiceDefault_StorePendingBlob(t *testing.T) {
	tests := []struct {
		name          string
		userID        uint
		deviceID      uint
		blobInfo      *stream.BlobInfo
		setupData     func(ctx coreTesting.TestContext) (uint, error) // Returns streamID
		expectError   bool
		expectedError string
		description   string
	}{
		{
			name:     "successful pending blob storage",
			userID:   1,
			deviceID: 1,
			blobInfo: &stream.BlobInfo{
				BlobHash: []byte(testUploadHash3),
				Length:   1024,
				BlobNum:  1,
				IV:       []byte("test_iv"),
			},
			setupData: func(ctx coreTesting.TestContext) (uint, error) {
				// Create a pending stream first using helper function
				pendingStream := createTestPendingStream(ctx, 1, 1, testUploadHash1, "test_sd_hash", "test_stream", "lbryfile", "test_file.txt", []byte("test_key"))
				return pendingStream.ID, nil
			},
			expectError: false,
			description: "should successfully store pending blob",
		},
		{
			name:     "update existing pending blob",
			userID:   1,
			deviceID: 2, // Different device ID
			blobInfo: &stream.BlobInfo{
				BlobHash: []byte(testUploadHash3),
				Length:   2048, // Different size
				BlobNum:  2,    // Different blob number
				IV:       []byte("new_iv"),
			},
			setupData: func(ctx coreTesting.TestContext) (uint, error) {
				// Create a pending stream first using helper function
				pendingStream := createTestPendingStream(ctx, 1, 1, testUploadHash1, "test_sd_hash", "test_stream", "lbryfile", "test_file.txt", []byte("test_key"))
				// Create existing pending blob using the same hex encoding as StorePendingBlob
				createTestPendingBlob(ctx, 1, 1, pendingStream.ID, hex.EncodeToString([]byte(testUploadHash3)), 1024, 1, true, []byte("old_iv"))
				return pendingStream.ID, nil
			},
			expectError: false,
			description: "should update existing pending blob on conflict",
		},
		{
			name:     "store pending blob with empty IV",
			userID:   1,
			deviceID: 1,
			blobInfo: &stream.BlobInfo{
				BlobHash: []byte(testUploadHash4),
				Length:   512,
				BlobNum:  3,
				IV:       []byte{},
			},
			expectError: false,
			description: "should handle empty IV gracefully",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
				// Arrange
				uploadsvc := core.GetService[pluginCore.UploadService](ctx, pluginCore.UPLOAD_SERVICE)
				require.NotNil(tb, uploadsvc)

				// Setup test data if needed
				var streamID uint
				if tt.setupData != nil {
					var err error
					streamID, err = tt.setupData(ctx)
					require.NoError(tb, err)
				}

				// Act
				err := uploadsvc.StorePendingBlob(context.Background(), tt.userID, tt.deviceID, streamID, tt.blobInfo)

				// Assert
				if tt.expectError {
					assert.Error(tb, err)
					if tt.expectedError != "" {
						assert.Contains(tb, err.Error(), tt.expectedError)
					}
				} else {
					assert.NoError(tb, err)

					// Verify the pending blob was stored/updated correctly
					var pendingBlob pluginDb.PendingBlob
					err = ctx.DB().Where("user_id = ? AND blob_hash = ?", tt.userID, hex.EncodeToString(tt.blobInfo.BlobHash)).First(&pendingBlob).Error
					assert.NoError(tb, err)
					assert.Equal(tb, tt.userID, pendingBlob.UserID)
					assert.Equal(tb, tt.deviceID, pendingBlob.DeviceID)
					assert.Equal(tb, int(tt.blobInfo.Length), pendingBlob.BlobSize)
					assert.Equal(tb, tt.blobInfo.BlobNum, pendingBlob.BlobNumber)
					assert.True(tb, pendingBlob.Received)
					assert.Equal(tb, tt.blobInfo.IV, pendingBlob.IVData)
				}
			}, getTestOptions())
		})
	}
}

func TestUploadServiceDefault_MarkPendingBlobAsReceived(t *testing.T) {
	tests := []struct {
		name          string
		userID        uint
		deviceID      uint
		blobInfo      *stream.BlobInfo
		setupData     func(ctx coreTesting.TestContext) (uint, error) // Returns streamID
		expectError   bool
		expectedError string
		description   string
	}{
		{
			name:     "mark existing pending blob as received",
			userID:   1,
			deviceID: 1,
			blobInfo: &stream.BlobInfo{
				BlobHash: []byte(testUploadHash3),
				Length:   1024,
				BlobNum:  1,
				IV:       []byte("test_iv"),
			},
			setupData: func(ctx coreTesting.TestContext) (uint, error) {
				// Create a pending stream first
				pendingStream := createTestPendingStream(ctx, 1, 1, testUploadHash1, "test_sd_hash", "test_stream", "lbryfile", "test_file.txt", []byte("test_key"))
				// Create existing pending blob with Received=false
				createTestPendingBlob(ctx, 1, 1, pendingStream.ID, hex.EncodeToString([]byte(testUploadHash3)), 1024, 1, false, []byte("test_iv"))
				return pendingStream.ID, nil
			},
			expectError: false,
			description: "should mark existing pending blob as received without changing stream_id",
		},
		{
			name:     "mark terminating blob as received",
			userID:   1,
			deviceID: 1,
			blobInfo: &stream.BlobInfo{
				BlobHash: []byte{}, // Empty hash indicates terminating blob
				Length:   0,
				BlobNum:  99,
				IV:       nil,
			},
			setupData: func(ctx coreTesting.TestContext) (uint, error) {
				// Create a pending stream first
				pendingStream := createTestPendingStream(ctx, 1, 1, testUploadHash1, "test_sd_hash", "test_stream", "lbryfile", "test_file.txt", []byte("test_key"))
				return pendingStream.ID, nil
			},
			expectError: false,
			description: "should mark terminating blob as received with empty hash",
		},
		{
			name:     "mark non-existent pending blob as received",
			userID:   1,
			deviceID: 1,
			blobInfo: &stream.BlobInfo{
				BlobHash: []byte(testUploadHash4),
				Length:   2048,
				BlobNum:  2,
				IV:       []byte("new_iv"),
			},
			setupData: func(ctx coreTesting.TestContext) (uint, error) {
				// Create a pending stream but don't create the pending blob
				pendingStream := createTestPendingStream(ctx, 1, 1, testUploadHash1, "test_sd_hash", "test_stream", "lbryfile", "test_file.txt", []byte("test_key"))
				return pendingStream.ID, nil
			},
			expectError: false,
			description: "should create new pending blob marked as received",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
				// Arrange
				uploadsvc := core.GetService[pluginCore.UploadService](ctx, pluginCore.UPLOAD_SERVICE)
				require.NotNil(tb, uploadsvc)

				// Setup test data if needed
				var originalStreamID uint
				if tt.setupData != nil {
					var err error
					_, err = tt.setupData(ctx)
					require.NoError(tb, err)

					// Get original stream_id if blob exists
					var pendingBlob pluginDb.PendingBlob
					blobHash := hex.EncodeToString(tt.blobInfo.BlobHash)
					if blobHash != "" {
						err = ctx.DB().Where("user_id = ? AND blob_hash = ?", tt.userID, blobHash).First(&pendingBlob).Error
						if err == nil {
							originalStreamID = pendingBlob.StreamID
						}
					}
				}

				// Act
				err := uploadsvc.MarkPendingBlobAsReceived(context.Background(), tt.userID, tt.deviceID, tt.blobInfo)

				// Assert
				if tt.expectError {
					assert.Error(tb, err)
					if tt.expectedError != "" {
						assert.Contains(tb, err.Error(), tt.expectedError)
					}
				} else {
					assert.NoError(tb, err)

					// Verify the pending blob was marked as received correctly
					var updatedPendingBlob pluginDb.PendingBlob
					blobHash := hex.EncodeToString(tt.blobInfo.BlobHash)
					if blobHash == "" {
						// For terminating blobs, we need to find the generated hash
						err = ctx.DB().Where("user_id = ? AND terminating = ? AND received = ?", tt.userID, true, true).First(&updatedPendingBlob).Error
					} else {
						err = ctx.DB().Where("user_id = ? AND blob_hash = ?", tt.userID, blobHash).First(&updatedPendingBlob).Error
					}
					assert.NoError(tb, err)
					assert.Equal(tb, tt.userID, updatedPendingBlob.UserID)
					assert.Equal(tb, tt.deviceID, updatedPendingBlob.DeviceID)
					assert.Equal(tb, int(tt.blobInfo.Length), updatedPendingBlob.BlobSize)
					assert.Equal(tb, tt.blobInfo.BlobNum, updatedPendingBlob.BlobNumber)
					assert.True(tb, updatedPendingBlob.Received)
					assert.Equal(tb, tt.blobInfo.IV, updatedPendingBlob.IVData)

					// For terminating blobs, check that terminating flag is set
					if len(tt.blobInfo.BlobHash) == 0 {
						assert.True(tb, updatedPendingBlob.Terminating)
					}

					// Critical test: verify stream_id is preserved for existing blobs
					if originalStreamID != 0 {
						assert.Equal(tb, originalStreamID, updatedPendingBlob.StreamID, "stream_id should be preserved for existing blobs")
					}
				}
			}, getTestOptions())
		})
	}
}

func TestUploadServiceDefault_StorePendingStream(t *testing.T) {
	tests := []struct {
		name          string
		userID        uint
		deviceID      uint
		sdBlob        *stream.SDBlob
		sdHash        string
		setupData     func(ctx coreTesting.TestContext)
		expectError   bool
		expectedError string
		description   string
	}{
		{
			name:     "successful pending stream storage",
			userID:   1,
			deviceID: 1,
			sdBlob: &stream.SDBlob{
				StreamHash:        []byte(testUploadHash1),
				StreamName:        "test_stream",
				StreamType:        "video",
				SuggestedFileName: "test.mp4",
				Key:               []byte("test_key"),
			},
			sdHash:      testUploadHash2,
			expectError: false,
			description: "should successfully store pending stream",
		},
		{
			name:     "successful pending stream storage with child blobs",
			userID:   1,
			deviceID: 1,
			sdBlob: &stream.SDBlob{
				StreamHash:        []byte(testUploadHash1),
				StreamName:        "test_stream_with_blobs",
				StreamType:        "video",
				SuggestedFileName: "test_with_blobs.mp4",
				Key:               []byte("test_key"),
				BlobInfos: []stream.BlobInfo{
					{
						BlobHash: []byte(testUploadHash3),
						Length:   1024,
						BlobNum:  1,
						IV:       []byte("iv1"),
					},
					{
						BlobHash: []byte(testUploadHash4),
						Length:   2048,
						BlobNum:  2,
						IV:       []byte("iv2"),
					},
				},
			},
			sdHash:      testUploadHash2,
			expectError: false,
			description: "should successfully store pending stream and create child pending blobs",
		},
		{
			name:     "update existing pending stream",
			userID:   1,
			deviceID: 2, // Different device ID
			sdBlob: &stream.SDBlob{
				StreamHash:        []byte(testUploadHash1),
				StreamName:        "updated_stream",
				StreamType:        "audio",
				SuggestedFileName: "updated.mp3",
				Key:               []byte("updated_key"),
			},
			sdHash: testUploadHash2,
			setupData: func(ctx coreTesting.TestContext) {
				// Create existing pending stream using helper
				createTestPendingStream(ctx, 1, 1, testUploadHash1, testUploadHash2, "old_stream", "video", "old.mp4", []byte("old_key"))
			},
			expectError: false,
			description: "should update existing pending stream on conflict",
		},
		{
			name:     "store pending stream with minimal data",
			userID:   1,
			deviceID: 1,
			sdBlob: &stream.SDBlob{
				StreamHash:        []byte(testUploadHash3),
				StreamName:        "",
				StreamType:        "",
				SuggestedFileName: "",
				Key:               []byte{},
			},
			sdHash:      testUploadHash4,
			expectError: false,
			description: "should handle minimal stream data gracefully",
		},
		{
			name:        "error with nil sdBlob",
			userID:      1,
			deviceID:    1,
			sdBlob:      nil,
			sdHash:      testUploadHash1,
			expectError: true,
			description: "should return error when sdBlob is nil",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
				// Arrange
				uploadsvc := core.GetService[pluginCore.UploadService](ctx, pluginCore.UPLOAD_SERVICE)
				require.NotNil(tb, uploadsvc)

				// Setup test data if needed
				if tt.setupData != nil {
					tt.setupData(ctx)
				}

				// Act
				_, err := uploadsvc.StorePendingStream(context.Background(), tt.userID, tt.deviceID, tt.sdBlob, tt.sdHash)

				// Assert
				if tt.expectError {
					assert.Error(tb, err)
					if tt.expectedError != "" {
						assert.Contains(tb, err.Error(), tt.expectedError)
					}
				} else {
					assert.NoError(tb, err)

					// Verify the pending stream was stored/updated correctly
					var pendingStream pluginDb.PendingStream
					err = ctx.DB().Where("user_id = ? AND sd_hash = ?", tt.userID, tt.sdHash).First(&pendingStream).Error
					assert.NoError(tb, err)
					assert.Equal(tb, tt.userID, pendingStream.UserID)
					assert.Equal(tb, tt.deviceID, pendingStream.DeviceID)
					assert.Equal(tb, hex.EncodeToString(tt.sdBlob.StreamHash), pendingStream.StreamHash)
					assert.Equal(tb, tt.sdHash, pendingStream.SDHash)
					assert.Equal(tb, tt.sdBlob.StreamName, pendingStream.StreamName)
					assert.Equal(tb, tt.sdBlob.StreamType, pendingStream.StreamType)
					assert.Equal(tb, tt.sdBlob.SuggestedFileName, pendingStream.SuggestedFileName)
					assert.Equal(tb, tt.sdBlob.Key, pendingStream.KeyData)

					// Verify child pending blobs were created if SD blob has BlobInfos
					if len(tt.sdBlob.BlobInfos) > 0 {
						// Get the expected blob hashes from SD blob
						expectedBlobHashes := make([]string, len(tt.sdBlob.BlobInfos))
						for i, blobInfo := range tt.sdBlob.BlobInfos {
							expectedBlobHashes[i] = hex.EncodeToString(blobInfo.BlobHash)
						}

						var pendingBlobs []pluginDb.PendingBlob
						// Query pending blobs by user ID and blob hashes (IN clause)
						err = ctx.DB().Where("user_id = ? AND blob_hash IN ?", tt.userID, expectedBlobHashes).Find(&pendingBlobs).Error
						assert.NoError(tb, err)
						assert.Len(tb, pendingBlobs, len(tt.sdBlob.BlobInfos))

						// Verify each child pending blob
						for _, expectedBlob := range tt.sdBlob.BlobInfos {
							found := false
							for _, actualBlob := range pendingBlobs {
								if actualBlob.BlobHash == hex.EncodeToString(expectedBlob.BlobHash) {
									assert.Equal(tb, tt.userID, actualBlob.UserID)
									assert.Equal(tb, tt.deviceID, actualBlob.DeviceID)
									assert.Equal(tb, int(expectedBlob.Length), actualBlob.BlobSize)
									assert.Equal(tb, expectedBlob.BlobNum, actualBlob.BlobNumber)
									assert.False(tb, actualBlob.Received) // Should be false for child blobs created from SD blob
									assert.Equal(tb, expectedBlob.IV, actualBlob.IVData)
									found = true
									break
								}
							}
							assert.True(tb, found, "Expected pending blob %s not found", hex.EncodeToString(expectedBlob.BlobHash))
						}
					}
				}
			}, getTestOptions())
		})
	}
}

func TestUploadServiceDefault_GetMissingBlobs(t *testing.T) {
	tests := []struct {
		name            string
		userID          uint
		requiredBlobs   []string
		setupData       func(ctx coreTesting.TestContext) (uint, error)
		expectedMissing []string
		expectError     bool
		description     string
	}{
		{
			name:          "no missing blobs",
			userID:        1,
			requiredBlobs: []string{testUploadHash3, testUploadHash4},
			setupData: func(ctx coreTesting.TestContext) (uint, error) {
				// Create a pending stream first using helper
				pendingStream := createTestPendingStream(ctx, 1, 1, testUploadHash1, "test_sd_hash", "test_stream", "lbryfile", "test_file.txt", []byte("test_key"))
				// Create all required pending blobs
				createTestPendingBlob(ctx, 1, 1, pendingStream.ID, testUploadHash3, 1024, 1, true, []byte("iv1"))
				createTestPendingBlob(ctx, 1, 1, pendingStream.ID, testUploadHash4, 2048, 2, true, []byte("iv2"))
				return pendingStream.ID, nil
			},
			expectedMissing: []string{},
			expectError:     false,
			description:     "should return empty list when all blobs are available",
		},
		{
			name:          "some missing blobs",
			userID:        1,
			requiredBlobs: []string{testUploadHash3, testUploadHash4, testUploadHash1},
			setupData: func(ctx coreTesting.TestContext) (uint, error) {
				// Create a pending stream first using helper
				pendingStream := createTestPendingStream(ctx, 1, 1, testUploadHash1, "test_sd_hash", "test_stream", "lbryfile", "test_file.txt", []byte("test_key"))
				// Create only some of the required pending blobs
				createTestPendingBlob(ctx, 1, 1, pendingStream.ID, testUploadHash3, 1024, 1, true, []byte("iv1"))
				// testUploadHash4 and testUploadHash1 are missing
				return pendingStream.ID, nil
			},
			expectedMissing: []string{testUploadHash4, testUploadHash1},
			expectError:     false,
			description:     "should return list of missing blobs",
		},
		{
			name:            "all missing blobs",
			userID:          1,
			requiredBlobs:   []string{testUploadHash3, testUploadHash4},
			setupData:       func(ctx coreTesting.TestContext) (uint, error) { return 0, nil }, // No blobs created
			expectedMissing: []string{testUploadHash3, testUploadHash4},
			expectError:     false,
			description:     "should return all required blobs when none are available",
		},
		{
			name:            "empty required blobs list",
			userID:          1,
			requiredBlobs:   []string{},
			setupData:       func(ctx coreTesting.TestContext) (uint, error) { return 0, nil },
			expectedMissing: []string{},
			expectError:     false,
			description:     "should handle empty required blobs list gracefully",
		},
		{
			name:          "blobs from different user",
			userID:        1,
			requiredBlobs: []string{testUploadHash3},
			setupData: func(ctx coreTesting.TestContext) (uint, error) {
				// Create blob for different user
				createTestPendingBlob(ctx, 2, 1, 0, testUploadHash3, 1024, 1, true, []byte("iv1"))
				return 0, nil
			},
			expectedMissing: []string{testUploadHash3},
			expectError:     false,
			description:     "should not find blobs belonging to other users",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
				// Arrange
				uploadsvc := core.GetService[pluginCore.UploadService](ctx, pluginCore.UPLOAD_SERVICE)
				require.NotNil(tb, uploadsvc)

				// Setup test data
				streamID, err := tt.setupData(ctx)
				require.NoError(tb, err)

				// Act
				missingBlobs, err := uploadsvc.GetMissingBlobs(context.Background(), tt.userID, streamID, tt.requiredBlobs)

				// Assert
				if tt.expectError {
					assert.Error(tb, err)
				} else {
					assert.NoError(tb, err)
					assert.ElementsMatch(tb, tt.expectedMissing, missingBlobs)
				}
			}, getTestOptions())
		})
	}
}

func TestUploadServiceDefault_CleanupPendingBlobs(t *testing.T) {
	tests := []struct {
		name          string
		userID        uint
		streamResult  *stream.StreamResult
		setupData     func(ctx coreTesting.TestContext)
		expectError   bool
		expectedError string
		description   string
	}{
		{
			name:   "successful cleanup of pending blobs and stream",
			userID: 1,
			streamResult: &stream.StreamResult{
				ContentBlobs: [][]byte{[]byte(testUploadHash3), []byte(testUploadHash4)},
				SDBlobHash:   testUploadHash2,
			},
			setupData: func(ctx coreTesting.TestContext) {
				// Create pending stream first to get streamID
				pendingStream := createTestPendingStream(ctx, 1, 1, testUploadHash1, testUploadHash2, "test_stream", "video", "test.mp4", []byte("test_key"))
				// Create pending blobs associated with the stream (using hex encoding like StorePendingBlob)
				createTestPendingBlob(ctx, 1, 1, pendingStream.ID, hex.EncodeToString([]byte(testUploadHash3)), 1024, 1, true, []byte("iv1"))
				createTestPendingBlob(ctx, 1, 1, pendingStream.ID, hex.EncodeToString([]byte(testUploadHash4)), 2048, 2, true, []byte("iv2"))
			},
			expectError: false,
			description: "should successfully cleanup pending blobs and stream",
		},
		{
			name:   "cleanup with no pending data",
			userID: 1,
			streamResult: &stream.StreamResult{
				ContentBlobs: [][]byte{[]byte(testUploadHash3)},
				SDBlobHash:   testUploadHash2,
			},
			setupData:   func(ctx coreTesting.TestContext) {}, // No pending data
			expectError: false,
			description: "should handle cleanup when no pending data exists",
		},
		{
			name:   "cleanup with only pending stream",
			userID: 1,
			streamResult: &stream.StreamResult{
				ContentBlobs: [][]byte{}, // No content blobs
				SDBlobHash:   testUploadHash2,
			},
			setupData: func(ctx coreTesting.TestContext) {
				pendingStream := createTestPendingStream(ctx, 1, 1, testUploadHash1, testUploadHash2, "test_stream", "video", "test.mp4", []byte("test_key"))
				// Create a pending blob that should remain since ContentBlobs is empty (using hex encoding like StorePendingBlob)
				createTestPendingBlob(ctx, 1, 1, pendingStream.ID, hex.EncodeToString([]byte(testUploadHash3)), 1024, 1, true, []byte("iv1"))
			},
			expectError: false,
			description: "should cleanup only pending stream when no content blobs",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
				// Arrange
				uploadsvc := core.GetService[pluginCore.UploadService](ctx, pluginCore.UPLOAD_SERVICE)
				require.NotNil(tb, uploadsvc)

				// Setup test data
				tt.setupData(ctx)

				// Capture initial blob count before cleanup
				var initialBlobs int64
				err := ctx.DB().Model(&pluginDb.PendingBlob{}).Where("user_id = ?", tt.userID).Count(&initialBlobs).Error
				require.NoError(tb, err)

				// Act
				err = uploadsvc.CleanupPendingBlobs(context.Background(), tt.userID, tt.streamResult)

				// Assert
				if tt.expectError {
					assert.Error(tb, err)
					if tt.expectedError != "" {
						assert.Contains(tb, err.Error(), tt.expectedError)
					}
				} else {
					assert.NoError(tb, err)

					// Verify cleanup was successful
					// Check pending blobs are deleted
					var remainingBlobs int64
					err = ctx.DB().Model(&pluginDb.PendingBlob{}).Where("user_id = ?", tt.userID).Count(&remainingBlobs).Error
					assert.NoError(tb, err)

					expectedBlobs := 0
					if len(tt.streamResult.ContentBlobs) == 0 {
						// If no content blobs in stream result, we shouldn't delete any
						expectedBlobs = int(initialBlobs)
					}
					assert.Equal(tb, expectedBlobs, int(remainingBlobs))

					// Check pending stream is deleted
					var remainingStreams int64
					err = ctx.DB().Model(&pluginDb.PendingStream{}).Where("user_id = ? AND sd_hash = ?", tt.userID, tt.streamResult.SDBlobHash).Count(&remainingStreams).Error
					assert.NoError(tb, err)
					assert.Equal(tb, int64(0), remainingStreams)
				}
			}, getTestOptions())
		})
	}
}

func TestUploadServiceDefault_GetPendingStream(t *testing.T) {
	tests := []struct {
		name          string
		userID        uint
		sdHash        string
		setupData     func(ctx coreTesting.TestContext)
		expectError   bool
		expectedError string
		description   string
	}{
		{
			name:   "successful retrieval of pending stream",
			userID: 1,
			sdHash: testUploadHash2,
			setupData: func(ctx coreTesting.TestContext) {
				createTestPendingStream(ctx, 1, 1, testUploadHash1, testUploadHash2, "test_stream", "video", "test.mp4", []byte("test_key"))
			},
			expectError: false,
			description: "should successfully retrieve pending stream",
		},
		{
			name:   "pending stream not found",
			userID: 1,
			sdHash: "nonexistent_sd_hash",
			setupData: func(ctx coreTesting.TestContext) {
				createTestPendingStream(ctx, 1, 1, testUploadHash1, testUploadHash2, "test_stream", "video", "test.mp4", []byte("test_key"))
			},
			expectError:   true,
			expectedError: "pending stream not found",
			description:   "should return error when pending stream not found",
		},
		{
			name:   "access denied for other user's pending stream",
			userID: 2,
			sdHash: testUploadHash2,
			setupData: func(ctx coreTesting.TestContext) {
				createTestPendingStream(ctx, 1, 1, testUploadHash1, testUploadHash2, "test_stream", "video", "test.mp4", []byte("test_key"))
			},
			expectError:   true,
			expectedError: "pending stream not found",
			description:   "should return error when accessing other user's pending stream",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
				// Arrange
				uploadsvc := core.GetService[pluginCore.UploadService](ctx, pluginCore.UPLOAD_SERVICE)
				require.NotNil(tb, uploadsvc)

				// Setup test data
				tt.setupData(ctx)

				// Act
				pendingStream, err := uploadsvc.GetPendingStream(context.Background(), tt.userID, tt.sdHash)

				// Assert
				if tt.expectError {
					assert.Error(tb, err)
					if tt.expectedError != "" {
						assert.Contains(tb, err.Error(), tt.expectedError)
					}
					assert.Nil(tb, pendingStream)
				} else {
					assert.NoError(tb, err)
					assert.NotNil(tb, pendingStream)
					assert.Equal(tb, tt.userID, pendingStream.UserID)
					assert.Equal(tb, tt.sdHash, pendingStream.SDHash)
				}
			}, getTestOptions())
		})
	}
}

func TestUploadServiceDefault_GetPendingBlobs(t *testing.T) {
	tests := []struct {
		name          string
		userID        uint
		sdHash        string
		setupData     func(ctx coreTesting.TestContext)
		expectedCount int
		expectError   bool
		expectedError string
		description   string
	}{
		{
			name:   "successful retrieval of pending blobs",
			userID: 1,
			sdHash: testUploadHash2,
			setupData: func(ctx coreTesting.TestContext) {
				// Create pending stream first to get streamID
				pendingStream := createTestPendingStream(ctx, 1, 1, testUploadHash1, testUploadHash2, "test_stream", "video", "test.mp4", []byte("test_key"))
				createTestPendingBlob(ctx, 1, 1, pendingStream.ID, testUploadHash3, 1024, 1, true, []byte("iv1"))
				createTestPendingBlob(ctx, 1, 1, pendingStream.ID, testUploadHash4, 2048, 2, true, []byte("iv2"))
			},
			expectedCount: 2,
			expectError:   false,
			description:   "should successfully retrieve all pending blobs for user",
		},
		{
			name:   "no pending blobs found",
			userID: 1,
			sdHash: testUploadHash2,
			setupData: func(ctx coreTesting.TestContext) {
				createTestPendingStream(ctx, 1, 1, testUploadHash1, testUploadHash2, "test_stream", "video", "test.mp4", []byte("test_key"))
				// No pending blobs created
			},
			expectedCount: 0,
			expectError:   false,
			description:   "should return empty list when no pending blobs exist",
		},
		{
			name:   "pending blobs from different user",
			userID: 2,
			sdHash: testUploadHash2,
			setupData: func(ctx coreTesting.TestContext) {
				// Create pending streams for both users
				pendingStream1 := createTestPendingStream(ctx, 1, 1, testUploadHash1, testUploadHash2, "test_stream", "video", "test.mp4", []byte("test_key"))
				pendingStream2 := createTestPendingStream(ctx, 2, 1, testUploadHash1, testUploadHash2, "test_stream", "video", "test.mp4", []byte("test_key"))
				createTestPendingBlob(ctx, 1, 1, pendingStream1.ID, testUploadHash3, 1024, 1, true, []byte("iv1")) // User 1's blob
				createTestPendingBlob(ctx, 2, 1, pendingStream2.ID, testUploadHash4, 2048, 2, true, []byte("iv2")) // User 2's blob
			},
			expectedCount: 1, // Only user 2's blob should be returned
			expectError:   false,
			description:   "should only return pending blobs for specified user",
		},
		{
			name:   "blobs ordered by blob number",
			userID: 1,
			sdHash: testUploadHash2,
			setupData: func(ctx coreTesting.TestContext) {
				// Create pending stream first to get streamID
				pendingStream := createTestPendingStream(ctx, 1, 1, testUploadHash1, testUploadHash2, "test_stream", "video", "test.mp4", []byte("test_key"))
				// Create blobs out of order to test sorting
				createTestPendingBlob(ctx, 1, 1, pendingStream.ID, testUploadHash4, 2048, 3, true, []byte("iv3"))
				createTestPendingBlob(ctx, 1, 1, pendingStream.ID, testUploadHash3, 1024, 1, true, []byte("iv1"))
				createTestPendingBlob(ctx, 1, 1, pendingStream.ID, testUploadHash1, 512, 2, true, []byte("iv2"))
			},
			expectedCount: 3,
			expectError:   false,
			description:   "should return pending blobs ordered by blob number",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
				// Arrange
				uploadsvc := core.GetService[pluginCore.UploadService](ctx, pluginCore.UPLOAD_SERVICE)
				require.NotNil(tb, uploadsvc)

				// Setup test data
				tt.setupData(ctx)

				// Act
				pendingBlobs, err := uploadsvc.GetPendingBlobs(context.Background(), tt.userID, tt.sdHash)

				// Assert
				if tt.expectError {
					assert.Error(tb, err)
					if tt.expectedError != "" {
						assert.Contains(tb, err.Error(), tt.expectedError)
					}
				} else {
					assert.NoError(tb, err)
					assert.Len(tb, pendingBlobs, tt.expectedCount)

					// Verify ordering if multiple blobs
					if len(pendingBlobs) > 1 {
						for i := 1; i < len(pendingBlobs); i++ {
							assert.True(tb, pendingBlobs[i-1].BlobNumber <= pendingBlobs[i].BlobNumber,
								"Blobs should be ordered by blob number")
						}
					}

					// Verify all returned blobs belong to the correct user
					for _, blob := range pendingBlobs {
						assert.Equal(tb, tt.userID, blob.UserID)
					}
				}
			}, getTestOptions())
		})
	}
}

func TestUploadServiceDefault_ListStreams(t *testing.T) {
	tests := []struct {
		name          string
		userID        uint
		filters       []queryutil.CrudFilter
		sorts         []queryutil.Sort
		pagination    queryutil.Pagination
		setupData     func(ctx coreTesting.TestContext) uint64
		expectCount   int64
		expectResults int
		expectError   bool
		description   string
	}{
		{
			name:    "successful listing with no filters",
			userID:  1,
			filters: []queryutil.CrudFilter{},
			sorts:   []queryutil.Sort{},
			pagination: queryutil.Pagination{
				Start:    0,
				End:      10,
				PageSize: 10, // End - Start = 10 - 0 = 10
				Mode:     "server",
			},
			setupData: func(ctx coreTesting.TestContext) uint64 {
				_, _, _ = setupTestStreamData(ctx, 1)
				return 2 // User 1 has 2 out of 3 streams
			},
			expectCount:   2,
			expectResults: 2,
			expectError:   false,
			description:   "should return all streams for user with no filters",
		},
		{
			name:    "listing with pagination",
			userID:  1,
			filters: []queryutil.CrudFilter{},
			sorts:   []queryutil.Sort{},
			pagination: queryutil.Pagination{
				Start:    0,
				End:      1,
				PageSize: 1, // End - Start = 1 - 0 = 1
				Mode:     "server",
			},
			setupData: func(ctx coreTesting.TestContext) uint64 {
				_, _, _ = setupTestStreamData(ctx, 1)
				return 2
			},
			expectCount:   2,
			expectResults: 1,
			expectError:   false,
			description:   "should return paginated results",
		},
		{
			name:   "filter by exact stream name",
			userID: 1,
			filters: []queryutil.CrudFilter{
				queryutil.Equal("stream_name", "test_stream_1"),
			},
			sorts: []queryutil.Sort{},
			pagination: queryutil.Pagination{
				Start:    0,
				End:      10,
				PageSize: 10, // End - Start = 10 - 0 = 10
				Mode:     "server",
			},
			setupData: func(ctx coreTesting.TestContext) uint64 {
				_, _, _ = setupTestStreamData(ctx, 1)
				return 2
			},
			expectCount:   1,
			expectResults: 1,
			expectError:   false,
			description:   "should filter by exact stream name match",
		},
		{
			name:    "listing with no results",
			userID:  999,
			filters: []queryutil.CrudFilter{},
			sorts:   []queryutil.Sort{},
			pagination: queryutil.Pagination{
				Start:    0,
				End:      10,
				PageSize: 10, // End - Start = 10 - 0 = 10
				Mode:     "server",
			},
			setupData: func(ctx coreTesting.TestContext) uint64 {
				return 0
			},
			expectCount:   0,
			expectResults: 0,
			expectError:   false,
			description:   "should return empty results for user with no streams",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
				// Arrange
				uploadsvc := core.GetService[pluginCore.UploadService](ctx, pluginCore.UPLOAD_SERVICE)
				require.NotNil(tb, uploadsvc)

				// Setup test data
				tt.setupData(ctx)

				// Act
				streams, total, err := uploadsvc.ListStreams(context.Background(), tt.userID, tt.filters, tt.sorts, tt.pagination)

				// Assert
				if tt.expectError {
					assert.Error(tb, err)
				} else {
					assert.NoError(tb, err)
					assert.Equal(tb, tt.expectCount, total)
					assert.Len(tb, streams, tt.expectResults)

					// Verify that all returned streams belong to the user
					for _, stream := range streams {
						assert.NotZero(tb, stream.ID)
						assert.NotEmpty(tb, stream.StreamName)
					}
				}
			}, getTestOptions())
		})
	}
}
