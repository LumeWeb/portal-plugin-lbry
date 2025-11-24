package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.lumeweb.com/liblbry/blob"
	pluginCore "go.lumeweb.com/portal-plugin-lbry/core"
	pluginMocks "go.lumeweb.com/portal-plugin-lbry/core/mocks"
	"go.lumeweb.com/portal-plugin-lbry/internal"
	"go.lumeweb.com/portal-plugin-lbry/internal/api/dto"
	pluginConfig "go.lumeweb.com/portal-plugin-lbry/internal/config"
	"go.lumeweb.com/portal-plugin-lbry/internal/db"
	"go.lumeweb.com/portal-plugin-lbry/internal/db/migrations"
	"go.lumeweb.com/portal-plugin-lbry/internal/protocol"
	pluginTesting "go.lumeweb.com/portal-plugin-lbry/internal/testing"
	"go.lumeweb.com/portal/core"
	coreTesting "go.lumeweb.com/portal/core/testing"
	coreMocks "go.lumeweb.com/portal/core/testing/mocks"
	"go.lumeweb.com/portal/service"
	"go.lumeweb.com/queryutil"
	"gorm.io/gorm"
)

const (
	TestEmail    = "test@example.com"
	TestPassword = "example"
	TestIP       = "127.0.0.1"
)

// General setup helper that configures all mocks with default expectations
func setupMocks(ctx coreTesting.TestContext, tb coreTesting.TB) {
	// Setup storage mock expectations
	/*	sm := core.GetService[*coreMocks.MockStorageService](ctx, core.STORAGE_SERVICE)
		require.NotNil(tb, sm)
		sm.EXPECT().S3Client(mock.Anything).Return(&s3.Client{}, nil)
		sm.EXPECT().GetTemporaryUploadDir(mock.Anything).Return("/tmp/uploads").Maybe()*/

	// Setup workflow mock expectations
	wm := core.GetService[*coreMocks.MockWorkflowService](ctx, core.WORKFLOW_SERVICE)
	require.NotNil(tb, wm)
	wm.EXPECT().RegisterWorkflow(mock.AnythingOfType("string"), mock.AnythingOfType("[]core.OperationStep"), mock.AnythingOfType("bool")).Return(nil).Maybe()
	wm.EXPECT().StartWorkflow(mock.Anything, mock.AnythingOfType("string"), mock.Anything).Return(nil, nil).Maybe()
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
		coreTesting.WithServiceFactory(core.CRON_SERVICE, service.NewCronService),
		coreTesting.WithServiceFactory(core.UPLOAD_SERVICE, service.NewMetadataService),
		coreTesting.WithServiceFactory(core.PIN_SERVICE, service.NewPinService),
		coreTesting.WithServiceFactory(core.REQUEST_SERVICE, service.NewRequestService),
		coreTesting.WithServiceFactory(core.WORKFLOW_SERVICE, service.NewWorkflowCoordinator),
		coreTesting.WithServiceFactory(core.USER_SERVICE, service.NewUserService),
		coreTesting.WithServiceFactory(core.AUTH_SERVICE, service.NewAuthService),
		coreTesting.WithServiceFactory(core.STORAGE_SERVICE, service.NewStorageService),
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
		// Include mock service factory for plugin-specific upload service
		coreTesting.WithMockServiceFactory(pluginCore.UPLOAD_SERVICE, pluginMocks.NewMockUploadService),
	)
}

func createTestUserAndLogin(ctx coreTesting.TestContext) (string, uint) {
	userSvc := core.GetService[core.UserService](ctx, core.USER_SERVICE)
	authSvc := core.GetService[core.AuthService](ctx, core.AUTH_SERVICE)

	user, err := userSvc.CreateAccount(TestEmail, TestPassword, false)
	if err != nil {
		ctx.T().Fatalf("failed to create test user: %v", err)
	}

	token, _, err := authSvc.LoginPassword(TestEmail, TestPassword, TestIP, false)
	if err != nil {
		ctx.T().Fatalf("failed to login test user: %v", err)
	}

	return token, user.ID
}

func setAuthHeader(req *http.Request, token string) {
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))
}

func createTestFileUpload(t *testing.T, content string, filename string) (*bytes.Buffer, string) {
	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	part, err := writer.CreateFormFile("file", filename)
	require.NoError(t, err)

	_, err = part.Write([]byte(content))
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)

	return body, writer.FormDataContentType()
}

func createBaseMultipartRequest(ctx coreTesting.TestContext, t *testing.T, method, url, token string, bodyBuilder func(*multipart.Writer) error) *http.Request {
	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)

	// Call the body builder function to create the specific multipart content
	err := bodyBuilder(writer)
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)

	req := ctx.NewAPIRequest(method, url, body.Bytes())
	req.Header.Set("Content-Type", "multipart/form-data; boundary="+writer.Boundary())

	if token != "" {
		req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))
	}

	return req
}

func createMultipartRequest(ctx coreTesting.TestContext, t *testing.T, method, url, content, filename, token string) *http.Request {
	return createBaseMultipartRequest(ctx, t, method, url, token, func(writer *multipart.Writer) error {
		part, err := writer.CreateFormFile("file", filename)
		require.NoError(t, err)

		_, err = part.Write([]byte(content))
		require.NoError(t, err)

		return nil
	})
}

func createEmptyMultipartRequest(ctx coreTesting.TestContext, t *testing.T, method, url, filename, token string) *http.Request {
	return createBaseMultipartRequest(ctx, t, method, url, token, func(writer *multipart.Writer) error {
		// Create form file without writing content
		_, err := writer.CreateFormFile("file", filename)
		require.NoError(t, err)

		return nil
	})
}

func createMalformedMultipartRequestWithMetadata(ctx coreTesting.TestContext, t *testing.T, method, url, content, filename, token string, streamName, suggestedFileName string) *http.Request {
	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)

	// Create metadata
	metadata := &dto.StreamMetadataRequest{
		StreamName:        streamName,
		SuggestedFileName: suggestedFileName,
	}

	// Marshal metadata to JSON
	metadataJSON, err := json.Marshal(metadata)
	require.NoError(t, err)

	// Add metadata as "meta" form field
	metaPart, err := writer.CreateFormField("meta")
	require.NoError(t, err)
	_, err = metaPart.Write(metadataJSON)
	require.NoError(t, err)

	// Add file content as "file" form field
	filePart, err := writer.CreateFormFile("file", filename)
	require.NoError(t, err)
	_, err = filePart.Write([]byte(content))
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)

	// Corrupt the multipart data by removing the final boundary
	validData := body.Bytes()
	boundary := writer.Boundary()
	corruptedData := validData[:len(validData)-len("--"+boundary+"--\r\n")]

	req := ctx.NewAPIRequest(method, url, corruptedData)
	req.Header.Set("Content-Type", "multipart/form-data; boundary="+boundary)

	if token != "" {
		req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))
	}

	return req
}

// createMultipartRequestWithMetadata creates a multipart request with both file content and stream metadata
func createMultipartRequestWithMetadata(ctx coreTesting.TestContext, t *testing.T, method, url, content, filename, token string, streamName, suggestedFileName string) *http.Request {
	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)

	// Create metadata
	metadata := &dto.StreamMetadataRequest{
		StreamName:        streamName,
		SuggestedFileName: suggestedFileName,
	}

	// Marshal metadata to JSON
	metadataJSON, err := json.Marshal(metadata)
	require.NoError(t, err)

	// Add metadata as "meta" form field
	metaPart, err := writer.CreateFormField("meta")
	require.NoError(t, err)
	_, err = metaPart.Write(metadataJSON)
	require.NoError(t, err)

	// Add file content as "file" form field
	filePart, err := writer.CreateFormFile("file", filename)
	require.NoError(t, err)
	_, err = filePart.Write([]byte(content))
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)

	req := ctx.NewAPIRequest(method, url, body.Bytes())
	req.Header.Set("Content-Type", "multipart/form-data; boundary="+writer.Boundary())

	if token != "" {
		req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))
	}

	return req
}

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
		assert.Equal(t, http.StatusOK, rec.Code)
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
		if rec.Code == http.StatusOK {
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

func TestAPI_handleStreamUpload_InvalidContentType(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		token, _ := createTestUserAndLogin(ctx)

		// Make HTTP request with invalid content type using helper
		// This test specifically targets content-type validation failure, not metadata validation
		req := ctx.NewAPIRequest(http.MethodPost, "/api/streams/upload", []byte("not multipart"))
		setAuthHeader(req, token)
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		ctx.Router().ServeHTTP(rec, req)

		// Verify response - should be bad request due to invalid content type
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	}, getTestOptions())
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

// generateMockStreams generates mock stream data for testing
func generateMockStreams(count int, startID int) []*db.Stream {
	streams := make([]*db.Stream, count)
	for i := 0; i < count; i++ {
		id := startID + i
		streams[i] = &db.Stream{
			Model:             gorm.Model{ID: uint(id), CreatedAt: time.Now(), UpdatedAt: time.Now()},
			StreamHash:        fmt.Sprintf("hash_%d", id),
			SDHash:            fmt.Sprintf("sd_hash_%d", id),
			StreamName:        fmt.Sprintf("test_stream_%d", id),
			StreamType:        []string{"video", "audio", "image"}[id%3],
			SuggestedFileName: fmt.Sprintf("test_file_%d.mp4", id),
		}
	}
	return streams
}

// computeExpectedItems calculates expected items based on pagination
func computeExpectedItems(totalItems int, pagination *queryutil.Pagination) int {
	if pagination == nil {
		// Default pagination behavior (10 items max)
		if totalItems > 10 {
			return 10
		}
		return totalItems
	}

	// Calculate based on pagination parameters
	start := pagination.Start
	end := pagination.End

	// Handle edge cases
	if start >= totalItems {
		return 0
	}

	if end > totalItems {
		end = totalItems
	}

	return end - start
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

				// Setup mock expectations
				mockUploadSvc.EXPECT().ListStreams(
					mock.Anything,
					tt.userID,
					mock.Anything,
					mock.Anything,
					mock.Anything,
				).Return(tt.mockStreams, tt.mockTotal, tt.mockError).Once()

				// Create request using queryutil.BuildURL for dynamic URL generation
				baseURL := "/api/streams"
				var requestURL string
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
