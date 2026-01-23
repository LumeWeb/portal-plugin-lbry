package tests

import (
	"bytes"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"go.lumeweb.com/liblbry/protocol"
	"go.lumeweb.com/liblbry/stream"
	"go.lumeweb.com/portal-plugin-lbry/internal"
	pluginConfig "go.lumeweb.com/portal-plugin-lbry/internal/config"
	"go.lumeweb.com/portal/core"
	coreTesting "go.lumeweb.com/portal/core/testing"
)

// TestIntegration_ReflectorUpload tests the complete flow of:
// 1. Generating a random stream using liblbry stream creator
// 2. Uploading it to the reflector server (port 5666)
// 3. Verifying the operation gets processed
// 4. Downloading and verifying the stream
func TestIntegration_ReflectorUpload(t *testing.T) {
	coreTesting.RunTestCaseWithComponents(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		streamName := "reflector-test-stream"
		suggestedFileName := "reflector-test-file.txt"

		// Create test user and get auth token
		token, userID := createTestUserAndLogin(ctx)

		// Create device via API with hardcoded IP
		deviceID, err := createDeviceViaAPI(ctx, token, "test-device", "127.0.0.1")
		require.NoError(tb, err, "Failed to create device via API")
		require.NotEmpty(tb, deviceID, "Device ID should not be empty")

		// Generate random stream data using liblbry stream creator
		content, streamResult := generateRandomStream(tb, streamName, suggestedFileName)

		// Upload stream to reflector server
		uploadHash, err := uploadStreamToReflector(ctx, streamResult)
		require.NoError(tb, err, "Failed to upload stream to reflector")
		require.NotEmpty(tb, uploadHash, "Upload hash should not be empty")

		// Wait for workflow completion
		waitForWorkflowCompletion(tb, ctx, userID)

		// Verify stream via API
		streamResponse := verifyStreamViaAPI(tb, ctx, token)
		require.NotNil(tb, streamResponse, "Stream should be accessible via API")
		require.NotEmpty(tb, streamResponse.StreamHash, "Stream hash should not be empty")

		sdHash := streamResponse.SDHash

		// Verify we can download the stream using the existing infrastructure
		downloadedContent, err := testBlobDownload(ctx, sdHash)
		require.NoError(tb, err, "Failed to download stream after reflector upload")
		require.Equal(tb, string(content), downloadedContent, "Downloaded content should match original")
	}, coreTesting.TestComponents(coreTesting.ComponentDB, coreTesting.ComponentCron), GetIntegrationTestOptions()...)
}

// generateRandomStream creates a random stream using liblbry stream creator
func generateRandomStream(tb testing.TB, streamName, suggestedFileName string) ([]byte, *stream.StreamResult) {
	tb.Helper()

	// Generate random test data (1KB for testing)
	contentSize := 1024
	content := make([]byte, contentSize)
	_, err := rand.Read(content)
	require.NoError(tb, err, "Failed to generate random content")

	// Create stream creator
	streamCreator := stream.NewStreamCreator()

	// Create SD blob with metadata
	sdBlob := &stream.SDBlob{
		StreamName:        streamName,
		SuggestedFileName: suggestedFileName,
		StreamType:        stream.StreamTypeLBRYFile,
	}

	// Prepare stream options
	var streamOpts []stream.StreamOption

	// Add SD blob
	sdBlobBytes, err := sdBlob.ToBlob()
	require.NoError(tb, err, "Failed to serialize SD blob")
	streamOpts = append(streamOpts, stream.WithExistingSDBlob(sdBlobBytes))

	// Create stream from random content
	reader := bytes.NewReader(content)
	streamResult, err := streamCreator.CreateStream(reader, int64(len(content)), streamOpts...)
	require.NoError(tb, err, "Failed to create stream")
	require.NotNil(tb, streamResult, "Stream result should not be nil")
	require.NotEmpty(tb, streamResult.SDBlobHash, "SD blob hash should not be empty")

	return content, streamResult
}

// uploadStreamToReflector uploads a stream to the reflector server
func uploadStreamToReflector(ctx coreTesting.TestContext, streamResult *stream.StreamResult) (string, error) {
	tb := ctx.T()
	tb.Logf("Uploading stream to reflector server")

	// Create reflector client
	client := protocol.NewReflectorClient(
		protocol.WithReflectorClientLogger(ctx.Logger().Logger),
	)

	protoCfg := core.GetProtocolConfig[*pluginConfig.ProtocolConfig](ctx, internal.ProtocolName)

	// Connect to reflector server
	err := client.Connect(net.JoinHostPort("127.0.0.1", fmt.Sprintf("%d", protoCfg.ReflectorPort)))
	if err != nil {
		return "", fmt.Errorf("failed to connect to reflector server: %w", err)
	}
	defer func() {
		if err := client.Close(); err != nil {
			tb.Logf("Error closing reflector client: %v", err)
		}
	}()

	// Upload SD blob
	sdBlobBytes, err := streamResult.SDBlob.ToBlob()
	if err != nil {
		return "", fmt.Errorf("failed to serialize SD blob: %w", err)
	}

	err = client.SendSDBlob(streamResult.SDBlobHash, sdBlobBytes)
	if err != nil {
		return "", fmt.Errorf("failed to upload SD blob: %w", err)
	}

	// Upload content blobs
	for i, blob := range streamResult.ContentBlobs {
		contentHash := streamResult.ContentHashes[i]
		err = client.SendBlob(contentHash, blob)
		if err != nil {
			return "", fmt.Errorf("failed to upload content blob %d: %w", i, err)
		}
	}

	// Return the SD blob hash as the upload hash
	return streamResult.SDBlobHash, nil
}

// createDeviceViaAPI creates a device via the API endpoint
func createDeviceViaAPI(ctx coreTesting.TestContext, token, name, ipAddress string) (string, error) {
	// Create the device request payload
	requestBody := map[string]any{
		"name":       name,
		"ip_address": ipAddress,
	}

	jsonBody, err := json.Marshal(requestBody)
	if err != nil {
		return "", fmt.Errorf("failed to marshal device request: %w", err)
	}

	// Create request using test context
	req := ctx.NewAPIRequest(http.MethodPost, "/api/devices", jsonBody)
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))
	req.Header.Set("Content-Type", "application/json")

	// Execute request
	rec := httptest.NewRecorder()
	ctx.Router().ServeHTTP(rec, req)

	// Check response status
	if rec.Code != http.StatusCreated {
		return "", fmt.Errorf("failed to create device: status code %d", rec.Code)
	}

	// Parse the response to get the device ID
	var response struct {
		ID uint `json:"id"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		return "", fmt.Errorf("failed to decode device response: %w", err)
	}

	return fmt.Sprintf("%d", response.ID), nil
}
