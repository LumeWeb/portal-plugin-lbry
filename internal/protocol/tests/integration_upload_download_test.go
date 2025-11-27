package tests

import (
	"testing"

	"github.com/stretchr/testify/require"
	pluginCore "go.lumeweb.com/portal-plugin-lbry/core"
	"go.lumeweb.com/portal/core"
	coreTesting "go.lumeweb.com/portal/core/testing"
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
		uploadHash, err := uploadFileViaAPI(tb, ctx, token, testContent, filename, streamName, suggestedFileName)
		require.NoError(tb, err, "Upload should succeed")
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
	}, coreTesting.TestComponents(coreTesting.ComponentDB, coreTesting.ComponentCron), GetIntegrationTestOptions()...)
}
