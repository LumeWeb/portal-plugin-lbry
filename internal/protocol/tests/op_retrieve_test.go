package tests

import (
	"testing"

	"github.com/stretchr/testify/require"
	pluginCore "go.lumeweb.com/portal-plugin-lbry/core"
	"go.lumeweb.com/portal-plugin-lbry/internal"
	"go.lumeweb.com/portal/core"
	coreTesting "go.lumeweb.com/portal/core/testing"
)

// TestSDHash is a sample SD hash for testing purposes
// This should be replaced with a real test SD hash when available
const TestSDHash = "acc6adf8b4f10dcddffc5c2ca87dbd9cb3a2664564695ac7aaab038193ff14a280cc3d4ebae55c71d0b885a7316d0137"

func TestRetrieveOperationHandler_Execute_Integration(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {

		// Parse the test SD hash as a CID
		_cid, err := internal.LBRYHashToCID(TestSDHash)
		require.NoError(tb, err)

		// Get the upload service (which handles stream pins)
		uploadService := core.GetService[pluginCore.UploadService](ctx, pluginCore.UPLOAD_SERVICE)
		require.NotNil(tb, uploadService, "Upload service should be available")

		// Create a WorkflowTest instance
		wfTest := coreTesting.NewWorkflowTest(ctx)

		// Create a test user
		userSvc := core.GetService[core.UserService](ctx, core.USER_SERVICE)
		require.NotNil(tb, userSvc, "User service should be available")

		testUser, err := userSvc.CreateAccount(TestUserEmail, TestUserPassword, false)
		require.NoError(tb, err)

		// Get the operation name
		operationName := core.RetrieveOperationName(internal.ProtocolName)

		// Start the workflow with the SD hash
		req := wfTest.StartOperationWorkflow(operationName,
			core.WithWorkflowStorageHash(core.NewStorageHashFromRawMultihash(_cid.Hash())),
			core.WithWorkflowUserID(testUser.ID),
			core.WithWorkflowSourceIP(TestSourceIP))

		// Execute the workflow step
		wfTest.ExecuteWorkflowStep(req)
		wfTest.CompleteWorkflowStep(req)

		// Assertions
		wfTest.AssertOperationSuccess(req)
		wfTest.AssertOperationStatusMessageContains(req, "Content retrieved from LBRY network")
		wfTest.AssertOperationStatusProgress(req, 100)

	},
		coreTesting.CombineOptions(
			GetCommonTestOptions()),
	)
}
