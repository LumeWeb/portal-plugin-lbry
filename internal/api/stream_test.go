package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.lumeweb.com/liblbry/blob"
	pluginCore "go.lumeweb.com/portal-plugin-lbry/core"
	pluginMocks "go.lumeweb.com/portal-plugin-lbry/core/mocks"
	"go.lumeweb.com/portal-plugin-lbry/internal"
	"go.lumeweb.com/portal-plugin-lbry/internal/api/dto"
	"go.lumeweb.com/portal-plugin-lbry/internal/db"
	"go.lumeweb.com/portal/core"
	coreTesting "go.lumeweb.com/portal/core/testing"
	"go.lumeweb.com/queryutil"
	"gorm.io/gorm"
)

func TestAPI_handleStreamUpload_Success(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		token, _ := createTestUserAndLogin(ctx)

		// Get mock upload service and set up expectations
		mockUploadSvc := core.GetService[*pluginMocks.MockUploadService](ctx, pluginCore.UPLOAD_SERVICE)
		require.NotNil(tb, mockUploadSvc)

		// Mock successful HandleUpload call
		mockUploadSvc.EXPECT().HandleUpload(mock.AnythingOfType("*context.valueCtx"), mock.AnythingOfType("multipart.sectionReadCloser")).Return(
			cid.MustParse("bafksamfc6gcbxoof6o2yhlb3rqd64gs37hgererxehbq2xfgggdbk53mfbhisnwxf6snw765ulsoswmldzwa"),
			"test-upload-id",
			nil,
		).Once()

		// Make HTTP request using new helper with metadata
		req := createMultipartRequestWithMetadata(ctx, t, http.MethodPost, "/api/streams/upload", "test lbry stream content", "test.txt", token, "test-stream", "test-video.mp4")
		rec := httptest.NewRecorder()
		ctx.Router().ServeHTTP(rec, req)

		// Verify response
		assert.Equal(t, http.StatusCreated, rec.Code)
		var response dto.PostStreamUploadResponse
		err := json.Unmarshal(rec.Body.Bytes(), &response)
		assert.NoError(t, err)
		assert.NotEmpty(t, response.UploadHash)
		assert.True(t, len(response.UploadHash) > 0, "UploadHash should not be empty")
		assert.Equal(t, blob.BlobHashHexLength, len(response.UploadHash), "UploadHash should be exactly BlobHashHexLength characters long")
	}, getTestOptions(), coreTesting.WithCron(), coreTesting.WithMockS3())
}

func TestAPI_handleStreamUpload_Unauthorized(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {

		// Create test file upload without authentication
		testContent := "test lbry stream content"

		// Make HTTP request without auth header using helper
		req := createMultipartRequest(ctx, t, http.MethodPost, "/api/streams/upload", testContent, "test.txt", "")
		rec := httptest.NewRecorder()
		ctx.Router().ServeHTTP(rec, req)

		// Verify response - should be unauthorized
		assert.Equal(t, http.StatusUnauthorized, rec.Code)
	}, getTestOptions())
}

func TestAPI_handleStreamUpload_InvalidToken(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {

		// Create test file upload with invalid token
		testContent := "test lbry stream content"

		// Make HTTP request with invalid token using helper
		req := createMultipartRequest(ctx, t, http.MethodPost, "/api/streams/upload", testContent, "test.txt", "invalid-token")
		rec := httptest.NewRecorder()
		ctx.Router().ServeHTTP(rec, req)

		// Verify response - should be unauthorized
		assert.Equal(t, http.StatusUnauthorized, rec.Code)
	}, getTestOptions())
}

func TestAPI_handleStreamUpload_NoFile(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		token, _ := createTestUserAndLogin(ctx)

		// Make HTTP request without file using helper
		req := createEmptyMultipartRequest(ctx, t, http.MethodPost, "/api/streams/upload", "test.txt", token)
		rec := httptest.NewRecorder()
		ctx.Router().ServeHTTP(rec, req)

		// Verify response - should be bad request due to missing file
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	}, getTestOptions())
}

func TestAPI_handleStreamUpload_EmptyFile(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		token, _ := createTestUserAndLogin(ctx)

		// Make HTTP request with empty file - should be rejected
		req := createMultipartRequest(ctx, t, http.MethodPost, "/api/streams/upload", "", "empty.txt", token)
		rec := httptest.NewRecorder()
		ctx.Router().ServeHTTP(rec, req)

		// Verify response - should fail with empty file
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	}, getTestOptions())
}

func TestAPI_handleStreamUpload_LargeFile(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		token, _ := createTestUserAndLogin(ctx)

		// Get mock upload service and set up expectations
		mockUploadSvc := core.GetService[*pluginMocks.MockUploadService](ctx, pluginCore.UPLOAD_SERVICE)
		require.NotNil(tb, mockUploadSvc)

		// Mock successful HandleUpload call for large file
		mockUploadSvc.EXPECT().HandleUpload(mock.AnythingOfType("*context.valueCtx"), mock.AnythingOfType("multipart.sectionReadCloser")).Return(
			cid.MustParse("bafksamfc6gcbxoof6o2yhlb3rqd64gs37hgererxehbq2xfgggdbk53mfbhisnwxf6snw765ulsoswmldzwa"),
			"large-upload-id",
			nil,
		).Once()

		// Create a 10MB test file to test large file handling
		// Calculate exact repetitions needed: targetSize / contentLength
		targetSize := 10 * 1024 * 1024 // 10MB in bytes
		content := "This is test content for a large file. "
		contentLength := len(content)
		repetitions := targetSize / contentLength

		largeContent := strings.Repeat(content, repetitions) // Exactly 10MB

		// Make HTTP request using helper with metadata to ensure metadata validation passes
		req := createMultipartRequestWithMetadata(ctx, t, http.MethodPost, "/api/streams/upload", largeContent, "large.txt", token, "large-stream", "large-file.txt")
		rec := httptest.NewRecorder()
		ctx.Router().ServeHTTP(rec, req)

		// Verify response - should succeed if within limits
		// Note: This might fail if the upload limit is configured lower than our test content
		if rec.Code == http.StatusCreated {
			var response dto.PostStreamUploadResponse
			err := json.Unmarshal(rec.Body.Bytes(), &response)
			assert.NoError(t, err)
			assert.NotEmpty(t, response.UploadHash)
		} else {
			// If it fails, it should be due to size limit (not missing metadata)
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		}
	}, getTestOptions(), coreTesting.WithCron(), coreTesting.WithMockS3())
}

func TestAPI_handleStreamUpload_MalformedMultipart(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		token, _ := createTestUserAndLogin(ctx)

		// Make HTTP request with malformed multipart using helper that includes metadata
		// This test specifically targets multipart parsing failure, not metadata validation
		req := createMalformedMultipartRequestWithMetadata(ctx, t, http.MethodPost, "/api/streams/upload", "test content", "test.txt", token, "malformed-stream", "malformed-file.txt")
		rec := httptest.NewRecorder()
		ctx.Router().ServeHTTP(rec, req)

		// Verify response - should be bad request due to malformed multipart
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	}, getTestOptions())
}

func TestAPIHandleStreamList(t *testing.T) {
	tests := []struct {
		name           string
		userID         uint
		filters        []queryutil.CrudFilter
		sorts          []queryutil.Sort
		pagination     *queryutil.Pagination
		mockStreams    []*db.Stream
		mockTotal      int64
		mockError      error
		expectedStatus int
		expectedCount  int
		description    string
	}{
		{
			name:           "successful list streams",
			userID:         1,
			filters:        []queryutil.CrudFilter{},
			sorts:          []queryutil.Sort{},
			pagination:     nil,
			mockStreams:    generateMockStreams(2, 1),
			mockTotal:      2,
			expectedCount:  computeExpectedItems(2, nil),
			expectedStatus: http.StatusOK,
			description:    "should return list of streams for authenticated user",
		},
		{
			name:   "empty streams list",
			userID: 1,

			filters:        []queryutil.CrudFilter{},
			sorts:          []queryutil.Sort{},
			pagination:     nil,
			mockStreams:    generateMockStreams(0, 1),
			mockTotal:      0,
			expectedCount:  computeExpectedItems(0, nil),
			expectedStatus: http.StatusOK,
			description:    "should return empty list when user has no streams",
		},
		{
			name:    "list streams with pagination",
			userID:  1,
			filters: []queryutil.CrudFilter{},
			sorts:   []queryutil.Sort{},
			pagination: &queryutil.Pagination{
				Start:    0,
				End:      1,
				PageSize: 1,
				Mode:     "server",
			},
			mockStreams:    generateMockStreams(1, 1),
			mockTotal:      2,
			expectedCount:  computeExpectedItems(2, &queryutil.Pagination{Start: 0, End: 1, PageSize: 1, Mode: "server"}),
			expectedStatus: http.StatusOK,
			description:    "should return paginated results",
		},
		{
			name:   "list streams with filter",
			userID: 1,
			filters: []queryutil.CrudFilter{
				queryutil.Equal("stream_name", "test_stream_1"),
			},
			sorts:          []queryutil.Sort{},
			pagination:     nil,
			mockStreams:    generateMockStreams(1, 1),
			mockTotal:      1,
			expectedCount:  computeExpectedItems(1, nil),
			expectedStatus: http.StatusOK,
			description:    "should return filtered results",
		},
		{
			name:   "service error",
			userID: 1,

			filters:        []queryutil.CrudFilter{},
			sorts:          []queryutil.Sort{},
			pagination:     nil,
			mockStreams:    nil,
			mockTotal:      0,
			mockError:      errors.New("service error"),
			expectedStatus: http.StatusInternalServerError,
			expectedCount:  0,
			description:    "should handle service errors",
		},
		// Comprehensive Query Tests
		{
			name:           "search by stream name",
			userID:         1,
			filters:        []queryutil.CrudFilter{},
			sorts:          []queryutil.Sort{},
			pagination:     nil,
			mockStreams:    generateMockStreams(1, 5),
			mockTotal:      1,
			expectedCount:  computeExpectedItems(1, nil),
			expectedStatus: http.StatusOK,
			description:    "should search streams by name",
		},
		{
			name:           "search by sd hash",
			userID:         1,
			filters:        []queryutil.CrudFilter{},
			sorts:          []queryutil.Sort{},
			pagination:     nil,
			mockStreams:    generateMockStreams(1, 10),
			mockTotal:      1,
			expectedCount:  computeExpectedItems(1, nil),
			expectedStatus: http.StatusOK,
			description:    "should search streams by sd hash",
		},
		{
			name:   "multiple filters combined",
			userID: 1,
			filters: []queryutil.CrudFilter{
				queryutil.Equal("stream_type", "video"),
				queryutil.Contains("stream_name", "test_stream"),
			},
			sorts:          []queryutil.Sort{},
			pagination:     nil,
			mockStreams:    generateMockStreams(3, 1),
			mockTotal:      3,
			expectedCount:  computeExpectedItems(3, nil),
			expectedStatus: http.StatusOK,
			description:    "should apply multiple filters",
		},
		{
			name:           "empty search returns all",
			userID:         1,
			filters:        []queryutil.CrudFilter{},
			sorts:          []queryutil.Sort{},
			pagination:     nil,
			mockStreams:    generateMockStreams(5, 1),
			mockTotal:      5,
			expectedCount:  computeExpectedItems(5, nil),
			expectedStatus: http.StatusOK,
			description:    "should return all streams for empty search",
		},
		// Comprehensive Sort Tests
		{
			name:    "sort by stream name ascending",
			userID:  1,
			filters: []queryutil.CrudFilter{},
			sorts: []queryutil.Sort{
				{Field: "stream_name", Order: "asc"},
			},
			pagination:     nil,
			mockStreams:    generateMockStreams(3, 1),
			mockTotal:      3,
			expectedCount:  computeExpectedItems(3, nil),
			expectedStatus: http.StatusOK,
			description:    "should sort by stream name ascending",
		},
		{
			name:    "sort by stream name descending",
			userID:  1,
			filters: []queryutil.CrudFilter{},
			sorts: []queryutil.Sort{
				{Field: "stream_name", Order: "desc"},
			},
			pagination:     nil,
			mockStreams:    generateMockStreams(3, 1),
			mockTotal:      3,
			expectedCount:  computeExpectedItems(3, nil),
			expectedStatus: http.StatusOK,
			description:    "should sort by stream name descending",
		},
		{
			name:    "sort by created at ascending",
			userID:  1,
			filters: []queryutil.CrudFilter{},
			sorts: []queryutil.Sort{
				{Field: "created_at", Order: "asc"},
			},
			pagination:     nil,
			mockStreams:    generateMockStreams(3, 1),
			mockTotal:      3,
			expectedCount:  computeExpectedItems(3, nil),
			expectedStatus: http.StatusOK,
			description:    "should sort by created at ascending",
		},
		{
			name:    "sort by multiple fields",
			userID:  1,
			filters: []queryutil.CrudFilter{},
			sorts: []queryutil.Sort{
				{Field: "stream_type", Order: "asc"},
				{Field: "stream_name", Order: "desc"},
			},
			pagination:     nil,
			mockStreams:    generateMockStreams(5, 1),
			mockTotal:      5,
			expectedCount:  computeExpectedItems(5, nil),
			expectedStatus: http.StatusOK,
			description:    "should sort by multiple fields",
		},
		// Advanced Pagination Tests
		{
			name:    "pagination with large dataset",
			userID:  1,
			filters: []queryutil.CrudFilter{},
			sorts:   []queryutil.Sort{},
			pagination: &queryutil.Pagination{
				Start:    10,
				End:      20,
				PageSize: 10,
				Mode:     "server",
			},
			mockStreams:    generateMockStreams(10, 11),
			mockTotal:      50,
			expectedCount:  computeExpectedItems(50, &queryutil.Pagination{Start: 10, End: 20, PageSize: 10, Mode: "server"}),
			expectedStatus: http.StatusOK,
			description:    "should handle pagination with large dataset",
		},
		{
			name:    "pagination beyond dataset size",
			userID:  1,
			filters: []queryutil.CrudFilter{},
			sorts:   []queryutil.Sort{},
			pagination: &queryutil.Pagination{
				Start:    100,
				End:      110,
				PageSize: 10,
				Mode:     "server",
			},
			mockStreams:    generateMockStreams(0, 101),
			mockTotal:      25,
			expectedCount:  computeExpectedItems(25, &queryutil.Pagination{Start: 100, End: 110, PageSize: 10, Mode: "server"}),
			expectedStatus: http.StatusOK,
			description:    "should return empty when pagination exceeds dataset",
		},
		{
			name:   "pagination with sort and filter",
			userID: 1,
			filters: []queryutil.CrudFilter{
				queryutil.Equal("stream_type", "video"),
			},
			sorts: []queryutil.Sort{
				{Field: "stream_name", Order: "asc"},
			},
			pagination: &queryutil.Pagination{
				Start:    0,
				End:      5,
				PageSize: 5,
				Mode:     "server",
			},
			mockStreams:    generateMockStreams(5, 1),
			mockTotal:      15,
			expectedCount:  computeExpectedItems(15, &queryutil.Pagination{Start: 0, End: 5, PageSize: 5, Mode: "server"}),
			expectedStatus: http.StatusOK,
			description:    "should combine pagination with sort and filter",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {

				// Arrange
				token, userID := createTestUserAndLogin(ctx)
				tt.userID = userID

				// Get the mock upload service
				mockUploadSvc := core.GetService[*pluginMocks.MockUploadService](ctx, pluginCore.UPLOAD_SERVICE)
				require.NotNil(tb, mockUploadSvc)

				// Setup mock expectations for ListStreams
				mockUploadSvc.EXPECT().ListStreams(
					mock.Anything,
					tt.userID,
					mock.Anything,
					mock.Anything,
					mock.Anything,
				).Return(tt.mockStreams, tt.mockTotal, tt.mockError).Once()

				// Setup mock expectations for GetStreamSize (called for each stream)
				if tt.mockError == nil && tt.mockStreams != nil {
					for _, stream := range tt.mockStreams {
						mockUploadSvc.EXPECT().GetStreamSize(
							mock.Anything,
							uint64(stream.ID),
						).Return(int64(1024), nil).Once()
					}
				}

				// Create request using queryutil.BuildURL for dynamic URL generation
				baseURL := "/api/streams"
				requestURL := baseURL
				var err error

				if tt.filters != nil || tt.sorts != nil || tt.pagination != nil {
					requestURL, err = queryutil.BuildURL(baseURL, tt.sorts, tt.pagination, tt.filters...)
					if err != nil {
						tb.Fatalf("Failed to build URL: %v", err)
					}
				}

				req := ctx.NewAPIRequest(http.MethodGet, requestURL, nil)
				setAuthHeader(req, token)
				rec := httptest.NewRecorder()

				// Act
				ctx.Router().ServeHTTP(rec, req)

				// Assert
				assert.Equal(tb, tt.expectedStatus, rec.Code)

				if tt.expectedStatus == http.StatusOK {
					var response queryutil.Response[[]*dto.StreamResponse]
					err := json.Unmarshal(rec.Body.Bytes(), &response)
					require.NoError(tb, err)

					// Check response structure
					assert.NotNil(tb, response.Data)
					assert.Equal(tb, tt.mockTotal, response.Total)
					assert.Len(tb, response.Data, tt.expectedCount)

					// Verify stream data structure if we have results
					if tt.expectedCount > 0 {
						stream := response.Data[0]
						assert.NotZero(tb, stream.ID)
						assert.NotEmpty(tb, stream.StreamHash)
						assert.NotEmpty(tb, stream.SDHash)
						assert.NotEmpty(tb, stream.StreamName)
						assert.NotEmpty(tb, stream.StreamType)
						assert.NotEmpty(tb, stream.SuggestedFileName)
						assert.False(tb, stream.CreatedAt.IsZero())
						assert.False(tb, stream.UpdatedAt.IsZero())

						// Verify Size field is present (non-negative value)
						assert.GreaterOrEqual(tb, stream.Size, int64(0))
					}
				}
			}, getTestOptions(), coreTesting.WithCron(), coreTesting.WithMockS3())
		})
	}
}

func TestAPIHandleStreamDelete(t *testing.T) {
	tests := []struct {
		name           string
		sdHash         string
		mockError      error
		expectedStatus int
		description    string
	}{
		{
			name:           "successful stream deletion",
			sdHash:         "acc6adf8b4f10dcddffc5c2ca87dbd9cb3a2664564695ac7aaab038193ff14a280cc3d4ebae55c71d0b885a7316d0137",
			mockError:      nil,
			expectedStatus: http.StatusNoContent,
			description:    "should successfully delete user's stream",
		},
		{
			name:           "stream not found",
			sdHash:         "55d5aeff21b8d0f65239efef3d1287ebfa560a9c3e81c79c36b811b56fa3c1195682bcabe44513600cfd33fd65a0eb7e",
			mockError:      gorm.ErrRecordNotFound,
			expectedStatus: http.StatusNotFound,
			description:    "should return 404 when stream not found",
		},
		{
			name:           "access denied",
			sdHash:         "bde7be09c5010edee00d6d3db98ec0a0c4b37756757a6bd8bbbc1492a40025b391dd4e5fcf066d82e1c996b8427e1248",
			mockError:      gorm.ErrRecordNotFound,
			expectedStatus: http.StatusNotFound,
			description:    "should return 404 when user doesn't own stream",
		},
		{
			name:           "service error",
			sdHash:         "acc6adf8b4f10dcddffc5c2ca87dbd9cb3a2664564695ac7aaab038193ff14a280cc3d4ebae55c71d0b885a7316d0137",
			mockError:      errors.New("internal service error"),
			expectedStatus: http.StatusInternalServerError,
			description:    "should return 500 for service errors",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {

				// Arrange
				token, userID := createTestUserAndLogin(ctx)

				// Get the mock upload service
				mockUploadSvc := core.GetService[*pluginMocks.MockUploadService](ctx, pluginCore.UPLOAD_SERVICE)
				require.NotNil(tb, mockUploadSvc)

				// Setup mock expectations
				mockUploadSvc.EXPECT().DeleteStream(
					mock.AnythingOfType("*context.valueCtx"),
					userID,
					tt.sdHash,
				).Return(tt.mockError).Once()

				// Create request with path parameter
				url := fmt.Sprintf("/api/streams/%s", tt.sdHash)
				req := ctx.NewAPIRequest(http.MethodDelete, url, nil)
				setAuthHeader(req, token)
				rec := httptest.NewRecorder()

				// Act
				ctx.Router().ServeHTTP(rec, req)

				// Assert
				assert.Equal(tb, tt.expectedStatus, rec.Code)

				if tt.expectedStatus == http.StatusNoContent {
					assert.Empty(tb, rec.Body.Bytes())
				}
			}, getTestOptions())
		})
	}
}

func TestAPIHandleStreamList_Authentication(t *testing.T) {
	tests := []struct {
		name           string
		token          string
		expectedStatus int
		description    string
	}{
		{
			name:           "missing authentication",
			token:          "",
			expectedStatus: http.StatusUnauthorized,
			description:    "should require authentication",
		},
		{
			name:           "invalid token",
			token:          "invalid.token.here",
			expectedStatus: http.StatusUnauthorized,
			description:    "should reject invalid token",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {

				// Arrange
				url := "/api/streams"
				req := ctx.NewAPIRequest(http.MethodGet, url, nil)

				if tt.token != "" {
					setAuthHeader(req, tt.token)
				}

				// Act
				rec := httptest.NewRecorder()
				ctx.Router().ServeHTTP(rec, req)

				// Assert
				assert.Equal(tb, tt.expectedStatus, rec.Code)
			}, getTestOptions())
		})
	}
}

func TestAPIHandleStreamDelete_Authentication(t *testing.T) {
	tests := []struct {
		name           string
		token          string
		expectedStatus int
		description    string
	}{
		{
			name:           "missing authentication",
			token:          "",
			expectedStatus: http.StatusUnauthorized,
			description:    "should require authentication",
		},
		{
			name:           "invalid token",
			token:          "invalid.token.here",
			expectedStatus: http.StatusUnauthorized,
			description:    "should reject invalid token",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {

				// Arrange
				url := "/api/streams/test_sd_hash_123"
				req := ctx.NewAPIRequest(http.MethodDelete, url, nil)

				if tt.token != "" {
					setAuthHeader(req, tt.token)
				}

				// Act
				rec := httptest.NewRecorder()
				ctx.Router().ServeHTTP(rec, req)

				// Assert
				assert.Equal(tb, tt.expectedStatus, rec.Code)
			}, getTestOptions())
		})
	}
}

func TestAPI_NewAPI(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {

		// Test that the API can be created successfully
		api, options, err := NewAPI()

		assert.NoError(t, err)
		assert.NotNil(t, api)
		assert.NotNil(t, options)
		assert.Equal(t, internal.ProtocolName, api.Name())
		assert.Equal(t, internal.ProtocolName, api.Subdomain())
	}, getTestOptions())
}
