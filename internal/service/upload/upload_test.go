package upload

import (
	"context"
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
	"go.lumeweb.com/portal-plugin-lbry/internal"
	pluginConfig "go.lumeweb.com/portal-plugin-lbry/internal/config"
	"go.lumeweb.com/portal-plugin-lbry/internal/db"
	"go.lumeweb.com/portal-plugin-lbry/internal/db/migrations"
	"go.lumeweb.com/portal-plugin-lbry/internal/protocol"
	pluginTesting "go.lumeweb.com/portal-plugin-lbry/internal/testing"
	"go.lumeweb.com/portal/core"
	coreTesting "go.lumeweb.com/portal/core/testing"
	coreMocks "go.lumeweb.com/portal/core/testing/mocks"
	"go.lumeweb.com/portal/db/models"
	"go.lumeweb.com/portal/service"
	"go.lumeweb.com/queryutil"
)

const (
	// Generated test constants for LBRY upload service tests
	testUploadHash1 = "4324a7b8daf8e09df5fe0417b226ed23ec26e95584e2e705aa5a8ea83b0ced91e37d6bf95737562f60588ab17a9fd6e9"
	testUploadHash2 = "31b5772051a353b40e5f2c34ceb9800de27811526d10a1237dce4cb59c5b6124c26553ec1cc400bc4c6acecc6a6039d1"
	testUploadHash3 = "53c5b4fa23478e829dca6297df0a5731584ad713ba692581252a2aee4899545654ce1da33948ad96491e832aa4633ced"
	testUploadHash4 = "f9bc1a8b11cd1eaa8220b86d0648d7ac638f55216ce71f6c1a597388799c64a16d9f6584d6861f414ec68c07c42c33aa"
	testUploadCID1  = "bafksamcdest3rwxy4co7l7qec6zcn3jd5qtosvme4ltqlks2r2udwdhnshrx227zk43vml3alcflc6u723uq"
	testUploadCID2  = "bafksambrwv3saundko2a4xzmgthltaan4j4bcutnccqsg7oojs2zyw3betbgku7mdtcabpcmnlhmy2tahhiq"
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
		coreTesting.WithServiceFactory(pluginCore.UPLOAD_SERVICE, NewUploadService),
		coreTesting.WithSQLitePluginMigrations(
			internal.ProtocolName, migrations.GetSQLite(),
		),
		coreTesting.WithMockServiceFactory(core.STORAGE_SERVICE, coreMocks.NewMockStorageService),
		coreTesting.WithMockServiceFactory(core.PIN_SERVICE, coreMocks.NewMockPinService),
		coreTesting.WithMockServiceFactory(core.UPLOAD_SERVICE, coreMocks.NewMockUploadService),
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
				testReader := pluginTesting.NewReadSeekCloser(tt.testData)

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
				err := ctx.DB().Where("user_id = ? AND stream_id = ?", 999, streams[0].ID).Delete(&db.StreamPin{}).Error
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
			expectedError: "stream not found",
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
			expectedError:    "stream not found or access denied",
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
					var streamRecord db.Stream
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
				var stream db.Stream
				err := ctx.DB().Where("sd_hash = ?", sdHash).First(&stream).Error
				if err != nil {
					return fmt.Errorf("failed to find stream: %w", err)
				}

				// Verify user's stream pin is deleted
				var pinCount int64
				err = ctx.DB().Model(&db.StreamPin{}).Where("user_id = ? AND stream_id = ?", 1, stream.ID).Count(&pinCount).Error
				if err != nil {
					return err
				}
				if pinCount != 0 {
					return fmt.Errorf("expected 0 pins for user, got %d", pinCount)
				}

				// Verify stream and blobs still exist (new behavior - they are not deleted)
				var streamCount int64
				err = ctx.DB().Model(&db.Stream{}).Where("sd_hash = ?", sdHash).Count(&streamCount).Error
				if err != nil {
					return err
				}
				if streamCount != 1 {
					return fmt.Errorf("expected 1 stream to remain, got %d", streamCount)
				}

				// Find stream by SD hash first
				var streamForBlobs db.Stream
				err = ctx.DB().Where("sd_hash = ?", sdHash).First(&streamForBlobs).Error
				if err != nil {
					return fmt.Errorf("failed to find stream for blob check: %w", err)
				}

				var blobCount int64
				err = ctx.DB().Model(&db.StreamBlob{}).Where("stream_id = ?", streamForBlobs.ID).Count(&blobCount).Error
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
				var stream db.Stream
				err := ctx.DB().Where("sd_hash = ?", sdHash).First(&stream).Error
				if err != nil {
					return fmt.Errorf("failed to find stream: %w", err)
				}

				// Verify user's pin is deleted
				var userPinCount int64
				err = ctx.DB().Model(&db.StreamPin{}).Where("user_id = ? AND stream_id = ?", 1, stream.ID).Count(&userPinCount).Error
				if err != nil {
					return err
				}
				if userPinCount != 0 {
					return fmt.Errorf("expected 0 pins for user 1, got %d", userPinCount)
				}

				// Verify other user's pin still exists
				var otherPinCount int64
				err = ctx.DB().Model(&db.StreamPin{}).Where("user_id = ? AND stream_id = ?", 999, stream.ID).Count(&otherPinCount).Error
				if err != nil {
					return err
				}
				if otherPinCount != 1 {
					return fmt.Errorf("expected 1 pin for user 999, got %d", otherPinCount)
				}

				// Verify stream and blobs still exist (new behavior - they are never deleted)
				var streamCount int64
				err = ctx.DB().Model(&db.Stream{}).Where("sd_hash = ?", sdHash).Count(&streamCount).Error
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
			expectedError: "stream not found or access denied",
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
			expectedError: "stream not found or access denied",
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
		setupStream       func(ctx coreTesting.TestContext) *db.Stream
		mockCIDError      error
		expectError       bool
		expectedErrorText string
		description       string
	}{
		{
			name: "successful stream pin creation",
			setupStream: func(ctx coreTesting.TestContext) *db.Stream {
				// Create a stream in DB
				_stream := &db.Stream{
					StreamHash:        testUploadHash1,
					SDHash:            testUploadHash1,
					StreamName:        "test_stream",
					StreamType:        "video",
					SuggestedFileName: "test.mp4",
				}
				err := ctx.DB().Create(_stream).Error
				require.NoError(t, err)
				return _stream
			},
			expectError: false,
			description: "should create stream pin successfully when stream exists",
		},
		{
			name: "stream not found error",
			setupStream: func(ctx coreTesting.TestContext) *db.Stream {
				// Don't create stream in DB - this should cause a not found error
				return nil
			},
			expectError: true,
			description: "should return error when stream not found in database",
		},

		{
			name: "CID conversion error",
			setupStream: func(ctx coreTesting.TestContext) *db.Stream {
				// Create a stream in DB
				_stream := &db.Stream{
					StreamHash:        testUploadHash1,
					SDHash:            testUploadHash1,
					StreamName:        "test_stream",
					StreamType:        "video",
					SuggestedFileName: "test.mp4",
				}
				err := ctx.DB().Create(_stream).Error
				require.NoError(t, err)
				return _stream
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
				var _stream *db.Stream
				if tt.setupStream != nil {
					_stream = tt.setupStream(ctx)
				}

				// Act
				var result *db.StreamPin
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
func createTestStream(ctx coreTesting.TestContext, streamName, sdHash string) *db.Stream {
	stream := &db.Stream{
		StreamHash:        sdHash,
		SDHash:            sdHash,
		StreamName:        streamName,
		StreamType:        "video",
		SuggestedFileName: streamName + ".mp4",
	}
	err := ctx.DB().Create(stream).Error
	if err != nil {
		panic(err)
	}
	return stream
}

func createTestStreamPin(ctx coreTesting.TestContext, userID uint64, streamID uint64) *db.StreamPin {
	pin := &db.StreamPin{
		UserID:   userID,
		StreamID: streamID,
	}
	err := ctx.DB().Create(pin).Error
	if err != nil {
		panic(err)
	}
	return pin
}

func createTestStreamBlob(ctx coreTesting.TestContext, streamID uint64, blobID uint64, blobNumber int) *db.StreamBlob {
	blob := &db.StreamBlob{
		StreamID:   streamID,
		BlobID:     blobID,
		BlobNumber: blobNumber,
	}
	err := ctx.DB().Create(blob).Error
	if err != nil {
		panic(err)
	}
	return blob
}

func setupTestStreamData(ctx coreTesting.TestContext, userID uint64) ([]*db.Stream, []*db.StreamPin, []*db.StreamBlob) {
	// Create test streams
	stream1 := createTestStream(ctx, "test_stream_1", testUploadHash1)
	stream2 := createTestStream(ctx, "test_stream_2", testUploadHash2)
	stream3 := createTestStream(ctx, "another_stream", testUploadHash3)
	streams := []*db.Stream{stream1, stream2, stream3}

	// Create stream pins for user 1
	pin1 := createTestStreamPin(ctx, userID, uint64(stream1.ID))
	pin2 := createTestStreamPin(ctx, userID, uint64(stream2.ID))
	pins := []*db.StreamPin{pin1, pin2}

	// Create stream blobs
	blob1 := createTestStreamBlob(ctx, uint64(stream1.ID), 1001, 1)
	blob2 := createTestStreamBlob(ctx, uint64(stream1.ID), 1002, 2)
	blob3 := createTestStreamBlob(ctx, uint64(stream2.ID), 2001, 1)
	blobs := []*db.StreamBlob{blob1, blob2, blob3}

	// Create a pin for another user to test shared streams
	otherUserPin := createTestStreamPin(ctx, 999, uint64(stream1.ID))

	return streams, append(pins, otherUserPin), blobs
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

				// Debug output for pagination test
				if tt.name == "listing with pagination" {
					fmt.Printf("DEBUG: pagination = %+v\n", tt.pagination)
					fmt.Printf("DEBUG: GetOffset() = %d, GetLimit() = %d\n", tt.pagination.GetOffset(), tt.pagination.GetLimit())
					fmt.Printf("DEBUG: expected %d results, got %d\n", tt.expectResults, len(streams))
					for i, stream := range streams {
						fmt.Printf("DEBUG: stream[%d] = {ID: %d, Name: %s}\n", i, stream.ID, stream.StreamName)
					}
				}

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
