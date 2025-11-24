package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.lumeweb.com/liblbry/blob"
	pluginCore "go.lumeweb.com/portal-plugin-lbry/core"
	"go.lumeweb.com/portal-plugin-lbry/internal"
	"go.lumeweb.com/portal-plugin-lbry/internal/api/dto"
	pluginConfig "go.lumeweb.com/portal-plugin-lbry/internal/config"
	"go.lumeweb.com/portal-plugin-lbry/internal/db/migrations"
	"go.lumeweb.com/portal-plugin-lbry/internal/protocol"
	"go.lumeweb.com/portal-plugin-lbry/internal/service/upload"
	pluginTesting "go.lumeweb.com/portal-plugin-lbry/internal/testing"
	"go.lumeweb.com/portal/core"
	coreTesting "go.lumeweb.com/portal/core/testing"
	coreMocks "go.lumeweb.com/portal/core/testing/mocks"
	"go.lumeweb.com/portal/service"
)

const (
	TestEmail    = "test@example.com"
	TestPassword = "example"
	TestIP       = "127.0.0.1"
)

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
		coreTesting.WithServiceFactory(pluginCore.UPLOAD_SERVICE, upload.NewUploadService),
		coreTesting.WithAPI(internal.ProtocolName, NewAPI),
		coreTesting.WithAPIID(internal.ProtocolName),
		coreTesting.WithProtocol(internal.ProtocolName, protocol.NewProtocol),
		coreTesting.WithConfig("plugin.lbry.protocol.peer_port", uint(freePeerPort)),
		coreTesting.WithConfig("plugin.lbry.protocol.dht_port", uint(freeDhtPort)),
		coreTesting.WithConfig("plugin.lbry.protocol.reflector_port", uint(freeReflectorPort)),
		coreTesting.WithProtocolConfig(internal.ProtocolName, pluginConfig.ProtocolConfig{
			PeerPort:      uint(freePeerPort),
			DHTPort:       uint(freeDhtPort),
			ReflectorPort: uint(freeReflectorPort),
		}),
		coreTesting.WithSQLitePluginMigrations(
			internal.ProtocolName, migrations.GetSQLite(),
		),
		coreTesting.WithCron(),
		coreTesting.WithMockServiceFactory(core.STORAGE_SERVICE, coreMocks.NewMockStorageService),
		coreTesting.WithMockServiceFactory(core.PIN_SERVICE, coreMocks.NewMockPinService),
		coreTesting.WithMockServiceFactory(core.UPLOAD_SERVICE, coreMocks.NewMockUploadService),
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

func createMalformedMultipartRequest(ctx coreTesting.TestContext, t *testing.T, method, url, content, filename, token string) *http.Request {
	body, contentType := createTestFileUpload(t, content, filename)

	// Corrupt the multipart data by removing the final boundary
	validData := body.Bytes()
	boundary := strings.Split(contentType, "boundary=")[1]
	corruptedData := validData[:len(validData)-len("--"+boundary+"--\r\n")]

	req := ctx.NewAPIRequest(method, url, corruptedData)
	req.Header.Set("Content-Type", contentType)

	if token != "" {
		req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))
	}

	return req
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
	}, getTestOptions(), coreTesting.WithMockS3())
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
	}, getTestOptions(), coreTesting.WithMockS3())
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
	}, getTestOptions(), coreTesting.WithMockS3())
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
	}, getTestOptions(), coreTesting.WithMockS3())
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
	}, getTestOptions(), coreTesting.WithMockS3())
}

func TestAPI_handleStreamUpload_LargeFile(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		token, _ := createTestUserAndLogin(ctx)

		// Create a large test file (larger than typical limits)
		largeContent := strings.Repeat("This is test content for a large file. ", 10000) // ~400KB

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
	}, getTestOptions(), coreTesting.WithMockS3())
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
	}, getTestOptions(), coreTesting.WithMockS3())
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
	}, getTestOptions(), coreTesting.WithMockS3())
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
