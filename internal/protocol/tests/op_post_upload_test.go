package tests

import (
	"context"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/require"
	pluginCore "go.lumeweb.com/portal-plugin-lbry/core"
	"go.lumeweb.com/portal-plugin-lbry/internal"
	pluginDb "go.lumeweb.com/portal-plugin-lbry/internal/db"
	"go.lumeweb.com/portal-plugin-lbry/internal/protocol"
	"go.lumeweb.com/portal/core"
	coreTesting "go.lumeweb.com/portal/core/testing"
)

func TestPostUploadOperationHandler_Execute_Integration(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		// Create test data that simulates an LBRY stream
		testData := []byte(TestStreamContent)

		proto := core.GetProtocol(internal.ProtocolName)
		require.NotNil(tb, proto)

		// Create a test user account
		userSvc := core.GetService[core.UserService](ctx, core.USER_SERVICE)
		reqCtx := context.Background()
		testUser, err := userSvc.CreateAccount(reqCtx, TestUserEmail, TestUserPassword, false)
		require.NoError(tb, err)

		// Get the upload service
		uploadService := core.GetService[pluginCore.UploadService](ctx, pluginCore.UPLOAD_SERVICE)
		require.NotNil(tb, uploadService)

		// Create a temporary upload using the upload service
		uploadCID, uploadID, err := uploadService.HandleUpload(reqCtx, internal.NewReadSeekCloser(testData))
		require.NoError(tb, err)
		require.NotEmpty(tb, uploadID)
		require.NotEqual(tb, cid.Undef, uploadCID)

		// Create a WorkflowTest instance
		wfTest := coreTesting.NewWorkflowTest(ctx)

		// Start the workflow with the upload hash
		req := wfTest.StartOperationWorkflow(core.PostUploadOperationName(proto.Name()),
			core.WithWorkflowStorageHash(core.NewStorageHashFromRawMultihash(uploadCID.Hash())),
			core.WithWorkflowUserID(testUser.ID),
			core.WithWorkflowSourceIP(TestSourceIP),
			core.WithWorkflowStructData(protocol.PostUploadWorkflowData{
				UploadID: uploadID,
				Size:     int64(len(testData)),
			}, "json"),
		)

		// Act
		// Execute the workflow step
		wfTest.ExecuteWorkflowStep(req)
		wfTest.CompleteWorkflowStep(req)

		// Assert
		// Assertions
		wfTest.AssertOperationSuccess(req)
		wfTest.AssertOperationStatusMessageContains(req, "Upload processed successfully")
		wfTest.AssertOperationStatusProgress(req, 100)

		// Verify that the stream was created in the database
		db := ctx.DB()
		var streamRecord pluginDb.Stream
		err = db.First(&streamRecord).Error
		require.NoError(tb, err)

		var streamPin pluginDb.StreamPin
		err = db.Where("user_id = ?", testUser.ID).First(&streamPin).Error
		require.NoError(tb, err)

	},
		coreTesting.CombineOptions(GetCommonTestOptions()),
	)
}
