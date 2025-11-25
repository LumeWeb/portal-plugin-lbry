package tests

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.lumeweb.com/liblbry"
	"go.lumeweb.com/liblbry/blob/transfer"
	"go.lumeweb.com/liblbry/blob/transfer/peer_transfer"
	"go.lumeweb.com/liblbry/client"
	"go.lumeweb.com/liblbry/protocol"
	"go.lumeweb.com/liblbry/storage/memory"
	pluginCore "go.lumeweb.com/portal-plugin-lbry/core"
	"go.lumeweb.com/portal-plugin-lbry/internal"
	"go.lumeweb.com/portal-plugin-lbry/internal/api"
	"go.lumeweb.com/portal-plugin-lbry/internal/api/dto"
	pluginConfig "go.lumeweb.com/portal-plugin-lbry/internal/config"
	"go.lumeweb.com/portal/core"
	coreTesting "go.lumeweb.com/portal/core/testing"
	"go.lumeweb.com/portal/db/models"
	"go.lumeweb.com/queryutil"
)

func TestIntegration_UploadAndDownload(t *testing.T) {
	coreTesting.RunTestCaseWithComponents(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		testContent := TestStreamContent
		filename := "integration-test-file.txt"
		streamName := "integration-test-stream"
		suggestedFileName := "integration-test-video.mp4"

		// Create test user and get auth token
		token, userID := createTestUserAndLogin(ctx)

		// Get services needed for the test
		uploadSvc := core.GetService[pluginCore.UploadService](ctx, pluginCore.UPLOAD_SERVICE)
		require.NotNil(tb, uploadSvc, "Upload service should be available")

		// Upload file via POST API
		uploadHash, _ := uploadFileViaAPI(tb, ctx, token, testContent, filename, streamName, suggestedFileName)
		require.NotEmpty(tb, uploadHash, "Upload hash should not be empty")

		// Wait for workflow completion using realistic polling
		waitForWorkflowCompletion(tb, ctx, userID, uploadHash)

		// Verify stream was created via API
		streamRecord := verifyStreamViaAPI(tb, ctx, token, uploadHash)
		require.NotNil(tb, streamRecord, "Stream should be created and accessible via API")

		// Test blob download
		downloadedContent, err := testBlobDownload(ctx, streamRecord.SDHash)
		require.NoError(tb, err, "Blob download should succeed")
		require.Equal(tb, testContent, downloadedContent, "Downloaded content should match uploaded content")
	}, coreTesting.TestComponents(coreTesting.ComponentDB, coreTesting.ComponentCron),
		coreTesting.CombineOptions(
			GetCommonTestOptions(),
			GetDbTestOptions(),
			coreTesting.WithAPI(internal.ProtocolName, api.NewAPI),
			coreTesting.WithAPIConfig(internal.ProtocolName, &pluginConfig.APIConfig{}),
		))
}

func uploadFileViaAPI(tb coreTesting.TB, ctx coreTesting.TestContext, token, content, filename, streamName, suggestedFileName string) (string, *dto.PostStreamUploadResponse) {
	// Create multipart request with metadata
	body := &bytes.Buffer{}
	writer := createMultipartWriter(tb, body, content, filename, streamName, suggestedFileName)

	// Create HTTP request
	req := ctx.NewAPIRequest(http.MethodPost, "/api/streams/upload", body.Bytes())
	req.Header.Set("Content-Type", "multipart/form-data; boundary="+writer.Boundary())
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))

	// Execute request
	rec := httptest.NewRecorder()
	ctx.Router().ServeHTTP(rec, req)

	// Verify response
	require.Equal(tb, http.StatusCreated, rec.Code, "Upload should succeed")

	var response dto.PostStreamUploadResponse
	err := json.Unmarshal(rec.Body.Bytes(), &response)
	require.NoError(tb, err, "Response should be valid JSON")
	require.NotEmpty(tb, response.UploadHash, "Upload hash should not be empty")

	return response.UploadHash, &response
}

func createMultipartWriter(tb coreTesting.TB, body *bytes.Buffer, content, filename, streamName, suggestedFileName string) *multipart.Writer {
	writer := multipart.NewWriter(body)

	// Add metadata
	metadata := &dto.StreamMetadataRequest{
		StreamName:        streamName,
		SuggestedFileName: suggestedFileName,
	}

	metadataJSON, err := json.Marshal(metadata)
	require.NoError(tb, err, "Metadata should marshal to JSON")

	metaPart, err := writer.CreateFormField("meta")
	require.NoError(tb, err, "Should create metadata form field")
	_, err = metaPart.Write(metadataJSON)
	require.NoError(tb, err, "Should write metadata")

	// Add file
	filePart, err := writer.CreateFormFile("file", filename)
	require.NoError(tb, err, "Should create file form field")
	_, err = filePart.Write([]byte(content))
	require.NoError(tb, err, "Should write file content")

	err = writer.Close()
	require.NoError(tb, err, "Should close multipart writer")

	return writer
}

func waitForWorkflowCompletion(tb coreTesting.TB, ctx coreTesting.TestContext, userID uint, uploadHash string) {
	// Get services needed for checking workflow completion
	workflowSvc := core.GetService[core.WorkflowService](ctx, core.WORKFLOW_SERVICE)
	require.NotNil(tb, workflowSvc, "Workflow service should be available")

	uploadSvc := core.GetService[pluginCore.UploadService](ctx, pluginCore.UPLOAD_SERVICE)
	require.NotNil(tb, uploadSvc, "Upload service should be available")

	// First verify that a workflow was actually created
	_, initialTotal, err := workflowSvc.ListWorkflowInstances(
		ctx,
		userID,
		queryutil.Filters(),
		[]queryutil.Sort{},
		queryutil.Pagination{Start: 0, End: 10},
	)
	require.NoError(tb, err, "Should be able to list workflow instances")
	require.Greater(tb, initialTotal, int64(0), "At least one workflow should have been created")

	// Wait for workflow completion with realistic timing
	maxWait := 30 * time.Second
	pollInterval := 1 * time.Second
	startTime := time.Now()

	for time.Since(startTime) < maxWait {
		// List active workflow instances for user (should be 0 when done)
		_, workflowTotal, err := workflowSvc.ListWorkflowInstances(
			ctx,
			userID,
			queryutil.Filters(
				queryutil.NotEqual("status", string(models.RequestStatusCompleted)),
			),
			[]queryutil.Sort{},
			queryutil.Pagination{
				Start: 0,
				End:   10,
			},
		)
		require.NoError(tb, err, "Should be able to list workflow instances")

		// Workflow is complete when:
		// No active workflow instances (workflowTotal == 0)
		if workflowTotal == 0 {
			tb.Logf("Workflow completed successfully after %v (active workflows: %d)",
				time.Since(startTime), workflowTotal)
			return
		}

		tb.Logf("Waiting for workflow completion... (%v elapsed, active workflows: %d)",
			time.Since(startTime), workflowTotal)
		time.Sleep(pollInterval)
	}

	tb.Fatalf("Workflow did not complete within %v", maxWait)
}

func verifyStreamViaAPI(tb coreTesting.TB, ctx coreTesting.TestContext, token, uploadHash string) *dto.StreamResponse {
	req := ctx.NewAPIRequest(http.MethodGet, "/api/streams", nil)
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))

	// Execute request
	rec := httptest.NewRecorder()
	ctx.Router().ServeHTTP(rec, req)

	// Verify response
	require.Equal(tb, http.StatusOK, rec.Code, "Stream should be accessible via API")

	// Parse the response to check if it's a valid list response
	var response queryutil.Response[[]*dto.StreamResponse]
	err := json.Unmarshal(rec.Body.Bytes(), &response)
	require.NoError(tb, err, "Response should be valid JSON")

	// Return first stream from the list
	require.NotEmpty(tb, response.Data, "Should have at least one stream")

	return response.Data[0]
}

func createTestBlobAcquirer(ctx coreTesting.TestContext) (client.StreamAcquirer, error) {
	t := ctx.T()
	logger := ctx.Logger().Logger

	// Create memory storage for caching blobs
	memoryStore := memory.NewMemoryStore()

	// Create DHT node for peer discovery on port 4445
	dhtNode, err := protocol.NewDHTNodeWithDefaults(
		protocol.WithDHTLogger(logger),
		protocol.WithDHTAddress("127.0.0.1:4445"),
		protocol.WithDHTSeedNodes([]string{"0.0.0.1"}),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create DHT node: %w", err)
	}

	protoCfg := core.GetProtocolConfig[*pluginConfig.ProtocolConfig](ctx, internal.ProtocolName)

	// Create peer transfer with DHT discovery using DefaultPeerClientFactory
	peerTransfer, err := peer_transfer.NewPeerTransfer(
		dhtNode,
		protocol.DefaultPeerClientFactory(),
		peer_transfer.WithPeerTransferLogger(logger),
		peer_transfer.WithPeerTransferTimeout(30*time.Second),
		peer_transfer.WithPeerTransferMaxPeers(5),
		peer_transfer.WithPeerTransferFixedPeers([]string{fmt.Sprintf("127.0.0.1:%d", protoCfg.PeerPort)}),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create peer transfer: %w", err)
	}

	// Start the components
	err = dhtNode.Start()
	if err != nil {
		return nil, fmt.Errorf("failed to start dht: %w", err)
	}

	peerTransfer.Start()
	// Create blob acquirer with peer transfer and memory storage
	transfers := []transfer.Transfer{peerTransfer}
	acquirer, err := liblbry.NewBlobAcquirer(transfers, memoryStore)
	if err != nil {
		return nil, fmt.Errorf("failed to create blob acquirer: %w", err)
	}

	streamAcquirer, err := client.NewStreamAcquirerBuilder(logger).WithAcquirer(acquirer).WithStore(memoryStore).WithFactory(client.NewStreamAcquirerFactory(logger)).Build()
	require.NoError(t, err)

	t.Cleanup(func() {
		peerTransfer.Stop()
		dhtNode.Shutdown()
	})

	return streamAcquirer, nil
}

func testBlobDownload(ctx coreTesting.TestContext, streamHash string) (string, error) {
	tb := ctx.T()
	tb.Logf("Testing blob download for stream with hash: %s", streamHash)

	// Verify we can access the stream hash
	require.NotEmpty(tb, streamHash, "Stream hash should not be empty")

	// Create a blob acquirer with DHT, peer transfer, and memory storage
	acquirer, err := createTestBlobAcquirer(ctx)
	require.NoError(tb, err, "Failed to create blob acquirer")

	// For this test, we'll try to download the stream hash as a blob
	reader, err := acquirer.GetStream(ctx.GetContext(), streamHash, client.WithAcquireVerification(true))
	if err != nil {
		tb.Logf("Failed to acquire blob %s: %v", streamHash, err)
		return "", err
	}
	defer func() {
		if closeErr := reader.Close(); closeErr != nil {
			tb.Logf("Warning: failed to close reader for blob %s: %v", streamHash, closeErr)
		}
	}()

	// Read all data from the reader
	data, err := io.ReadAll(reader)
	if err != nil {
		tb.Logf("Failed to read data from blob %s: %v", streamHash, err)
		return "", err
	}

	tb.Logf("Successfully downloaded blob %s, size: %d bytes", streamHash, len(data))
	return string(data), nil
}

// Helper function to create test user and login (reused from api_test.go)
func createTestUserAndLogin(ctx coreTesting.TestContext) (string, uint) {
	userSvc := core.GetService[core.UserService](ctx, core.USER_SERVICE)
	authSvc := core.GetService[core.AuthService](ctx, core.AUTH_SERVICE)

	user, err := userSvc.CreateAccount(TestUserEmail, TestUserPassword, false)
	if err != nil {
		ctx.T().Fatalf("failed to create test user: %v", err)
	}

	token, _, err := authSvc.LoginPassword(TestUserEmail, TestUserPassword, TestSourceIP, false)
	if err != nil {
		ctx.T().Fatalf("failed to login test user: %v", err)
	}

	return token, user.ID
}
