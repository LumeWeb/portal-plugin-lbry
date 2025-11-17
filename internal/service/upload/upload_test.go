package upload

import (
	"context"
	"errors"
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
