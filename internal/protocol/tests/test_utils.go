package tests

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net"
	"net/http"
	"net/http/httptest"
	"time"

	"github.com/stretchr/testify/require"
	"go.lumeweb.com/liblbry"
	"go.lumeweb.com/liblbry/blob/transfer"
	"go.lumeweb.com/liblbry/blob/transfer/peer_transfer"
	"go.lumeweb.com/liblbry/client"
	"go.lumeweb.com/liblbry/protocol"
	"go.lumeweb.com/liblbry/storage/memory"
	"go.lumeweb.com/portal-plugin-lbry/internal"
	"go.lumeweb.com/portal-plugin-lbry/internal/api/dto"
	pluginConfig "go.lumeweb.com/portal-plugin-lbry/internal/config"
	"go.lumeweb.com/portal-plugin-lbry/internal/info"
	pluginTesting "go.lumeweb.com/portal-plugin-lbry/internal/testing"
	"go.lumeweb.com/portal/core"
	coreTesting "go.lumeweb.com/portal/core/testing"
	"go.lumeweb.com/portal/db/models"
	"go.lumeweb.com/portal/service"
	"go.lumeweb.com/queryutil"
)

func GetCoreTestOptions() []coreTesting.TestContextBuilderOption {
	return []coreTesting.TestContextBuilderOption{
		coreTesting.WithStatefulMockRenterService(),
		coreTesting.WithServiceFactory(core.UPLOAD_SERVICE, service.NewMetadataService),
		coreTesting.WithServiceFactory(core.PIN_SERVICE, service.NewPinService),
		coreTesting.WithServiceFactory(core.STORAGE_SERVICE, service.NewStorageService),
		coreTesting.WithServiceFactory(core.REQUEST_SERVICE, service.NewRequestService),
		coreTesting.WithServiceFactory(core.WORKFLOW_SERVICE, service.NewWorkflowCoordinator),
		coreTesting.WithServiceFactory(core.USER_SERVICE, service.NewUserService),
		coreTesting.WithServiceFactory(core.AUTH_SERVICE, service.NewAuthService),
		coreTesting.WithServiceFactory(core.TUS_SERVICE, service.NewTUSService),
	}
}

func GetPluginTestOptions() []coreTesting.TestContextBuilderOption {
	var freePeerPort, _ = pluginTesting.GetFreePort()
	var freeDhtPort, _ = pluginTesting.GetFreePort()
	var freeReflectorPort, _ = pluginTesting.GetFreePort()

	return []coreTesting.TestContextBuilderOption{
		coreTesting.WithPlugins(info.GetPluginInfo()),

		coreTesting.WithConfig("plugin.lbry.protocol.peer_port", uint(freePeerPort)),
		coreTesting.WithConfig("plugin.lbry.protocol.dht_port", uint(freeDhtPort)),
		coreTesting.WithConfig("plugin.lbry.protocol.reflector_port", uint(freeReflectorPort)),
		coreTesting.WithConfig("plugin.lbry.protocol.fixed_peers", []string{"s1.lbry.network:4444"}),
		coreTesting.WithAPIID(internal.ProtocolName),
		coreTesting.WithProtocolConfig(internal.ProtocolName, pluginConfig.ProtocolConfig{
			PeerPort:      uint(freePeerPort),
			DHTPort:       uint(freeDhtPort),
			ReflectorPort: uint(freeReflectorPort),
		}),
		coreTesting.WithMockS3(),
	}
}

func GetCommonTestOptions() []coreTesting.TestContextBuilderOption {
	return []coreTesting.TestContextBuilderOption{coreTesting.CombineOptions(GetCoreTestOptions(), GetPluginTestOptions())}
}

func GetIntegrationTestOptions() []coreTesting.TestContextBuilderOption {
	return []coreTesting.TestContextBuilderOption{
		coreTesting.CombineOptions(
			GetCommonTestOptions(),
		),

		coreTesting.WithAPIConfig(internal.ProtocolName, &pluginConfig.APIConfig{}),
	}
}

// uploadFileViaAPI uploads a file via the POST API and returns the upload hash
func uploadFileViaAPI(tb coreTesting.TB, ctx coreTesting.TestContext, token, content, filename, streamName, suggestedFileName string) (string, error) {
	tb.Helper()

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
	if rec.Code != http.StatusCreated {
		return "", fmt.Errorf("upload failed with status code: %d", rec.Code)
	}

	var response dto.PostStreamUploadResponse
	err := json.Unmarshal(rec.Body.Bytes(), &response)
	if err != nil {
		return "", fmt.Errorf("response should be valid JSON: %w", err)
	}

	if response.UploadHash == "" {
		return "", fmt.Errorf("upload hash should not be empty")
	}

	return response.UploadHash, nil
}

// createMultipartWriter creates a multipart writer with metadata and file content
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

// createTestBlobAcquirer creates a test blob acquirer with DHT, peer transfer, and memory storage
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
		peer_transfer.WithPeerTransferFixedPeers([]string{net.JoinHostPort("127.0.0.1", fmt.Sprintf("%d", protoCfg.PeerPort))}),
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

// testBlobDownload tests downloading a blob by stream hash
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

// createTestUserAndLogin creates a test user and returns auth token and user ID
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

// waitForWorkflowCompletion waits for the workflow to complete
func waitForWorkflowCompletion(tb coreTesting.TB, ctx coreTesting.TestContext, userID uint) {
	// Get services needed for checking workflow completion
	workflowSvc := core.GetService[core.WorkflowService](ctx, core.WORKFLOW_SERVICE)
	require.NotNil(tb, workflowSvc, "Workflow service should be available")

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
