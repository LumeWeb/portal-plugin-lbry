package tests

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	pluginCore "go.lumeweb.com/portal-plugin-lbry/core"
	"go.lumeweb.com/portal-plugin-lbry/internal"
	pluginDb "go.lumeweb.com/portal-plugin-lbry/internal/db"
	"go.lumeweb.com/portal-plugin-lbry/internal/protocol"
	pluginTesting "go.lumeweb.com/portal-plugin-lbry/internal/testing"
	"go.lumeweb.com/portal/core"
	coreTesting "go.lumeweb.com/portal/core/testing"
)

// seedTestStream creates a test stream in the database and returns its SD hash
// This function ensures the test has a known, controlled stream to retrieve
// without depending on external network state or hardcoded hashes
func seedTestStream(tb coreTesting.TB, ctx coreTesting.TestContext, userID uint64) string {
	// Create test data that simulates an LBRY stream
	testData := []byte(TestStreamContent)

	// Get the upload service
	uploadService := core.GetService[pluginCore.UploadService](ctx, pluginCore.UPLOAD_SERVICE)
	require.NotNil(tb, uploadService, "Upload service should be available")

	// Create a temporary upload using the upload service
	reqCtx := context.Background()
	uploadCID, uploadID, err := uploadService.HandleUpload(reqCtx, pluginTesting.NewReadSeekCloser(testData))
	require.NoError(tb, err)
	require.NotEmpty(tb, uploadID)

	// Create a WorkflowTest instance to process the upload
	wfTest := coreTesting.NewWorkflowTest(ctx)

	// Get the protocol for operation name
	proto := core.GetProtocol(internal.ProtocolName)
	require.NotNil(tb, proto)

	// Start the upload workflow to create the stream
	uploadReq := wfTest.StartOperationWorkflow(core.PostUploadOperationName(proto.Name()),
		core.WithWorkflowStorageHash(core.NewStorageHashFromRawMultihash(uploadCID.Hash())),
		core.WithWorkflowUserID(userID),
		core.WithWorkflowSourceIP(TestSourceIP),
		core.WithWorkflowStructData(protocol.PostUploadWorkflowData{
			UploadID: uploadID,
			Size:     int64(len(testData)),
		}, "json"),
	)

	// Execute the upload workflow
	wfTest.ExecuteWorkflowStep(uploadReq)
	wfTest.CompleteWorkflowStep(uploadReq)

	// Verify the upload succeeded
	wfTest.AssertOperationSuccess(uploadReq)

	// Get the stream record from the database to find the SD hash
	db := ctx.DB()
	var streamRecord pluginDb.Stream
	err = db.Where("upload_hash = ?", uploadID).First(&streamRecord).Error
	require.NoError(tb, err, "Stream should be created in database")
	require.NotEmpty(tb, streamRecord.SDHash, "Stream should have an SD hash")

	return streamRecord.SDHash
}

func TestRetrieveOperationHandler_Execute_Integration(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {

		// Create a test user first
		userSvc := core.GetService[core.UserService](ctx, core.USER_SERVICE)
		require.NotNil(tb, userSvc, "User service should be available")

		testUser, err := userSvc.CreateAccount(TestUserEmail, TestUserPassword, false)
		require.NoError(tb, err)

		// Seed a test stream to retrieve - this ensures we have a known, controlled SD hash
		// without depending on external network state or hardcoded values
		testSDHash := seedTestStream(tb, ctx, testUser.ID)

		// Parse the seeded SD hash as a CID
		_cid, err := internal.LBRYHashToCID(testSDHash)
		require.NoError(tb, err)

		// Get the upload service (which handles stream pins)
		uploadService := core.GetService[pluginCore.UploadService](ctx, pluginCore.UPLOAD_SERVICE)
		require.NotNil(tb, uploadService, "Upload service should be available")

		// Create a WorkflowTest instance
		wfTest := coreTesting.NewWorkflowTest(ctx)

		// Get the operation name
		operationName := core.RetrieveOperationName(internal.ProtocolName)

		// Start the workflow with the seeded SD hash
		req := wfTest.StartOperationWorkflow(operationName,
			core.WithWorkflowStorageHash(core.NewStorageHashFromRawMultihash(_cid.Hash())),
			core.WithWorkflowUserID(testUser.ID),
			core.WithWorkflowSourceIP(TestSourceIP))

		// Execute the workflow step
		wfTest.ExecuteWorkflowStep(req)
		wfTest.CompleteWorkflowStep(req)

		// Assertions
		wfTest.AssertOperationSuccess(req)
		wfTest.AssertOperationStatusMessageContains(req, "retrieved")
		wfTest.AssertOperationStatusProgress(req, 100)

	},
		coreTesting.CombineOptions(
			GetCommonTestOptions(), GetDbTestOptions()),
	)
}
