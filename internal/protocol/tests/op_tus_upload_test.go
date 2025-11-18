package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/tus/tusd/v2/pkg/handler"
	pluginCore "go.lumeweb.com/portal-plugin-lbry/core"
	"go.lumeweb.com/portal-plugin-lbry/internal"
	pluginTesting "go.lumeweb.com/portal-plugin-lbry/internal/testing"
	"go.lumeweb.com/portal/core"
	coreTesting "go.lumeweb.com/portal/core/testing"
)

func TestTUSUploadOperationHandler_Execute_Integration(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// --- Test Setup ---
		// Initialize services and test dependencies
		wfTest := coreTesting.NewWorkflowTest(ctx)
		tusService := core.GetService[core.TUSService](ctx, core.TUS_SERVICE)
		storageSvc := core.GetService[core.StorageService](ctx, core.STORAGE_SERVICE)
		userSvc := core.GetService[core.UserService](ctx, core.USER_SERVICE)
		proto := core.GetProtocol(internal.ProtocolName)

		// Validate test context setup
		require.NotNil(tb, tusService, "TUS service should be available")
		require.NotNil(tb, storageSvc, "Storage service should be available")
		require.NotNil(tb, userSvc, "User service should be available")
		require.NotNil(tb, proto, "Protocol should be available")

		// --- Test Data Preparation ---
		// Create test data that simulates an LBRY stream
		testData := []byte(TestTUSStreamContent)

		// Create test user
		testUser, err := userSvc.CreateAccount(TestUserEmail, TestUserPassword, false)
		require.NoError(tb, err)

		// --- TUS Upload Setup ---
		objectId := uuid.New().String()
		uploadId := uuid.New().String()
		fullId := fmt.Sprintf("%s+%s", objectId, uploadId)
		uploaderIp := TestSourceIP

		// Create a temporary upload to get the hash using the upload service
		reqCtx := context.Background()
		uploadSvc := core.GetService[pluginCore.UploadService](ctx, pluginCore.UPLOAD_SERVICE)
		uploadCID, tempUploadID, err := uploadSvc.HandleUpload(reqCtx, pluginTesting.NewReadSeekCloser(testData))
		require.NoError(tb, err)
		require.NotEmpty(tb, tempUploadID)

		storageHash := core.NewStorageHashFromMultihash(uploadCID.Hash(), uploadCID.Type(), nil)

		// Create TUS upload with the upload hash
		tusUpload, err := tusService.CreateUpload(
			ctx,
			storageHash,
			fullId,
			testUser.ID,
			uploaderIp,
			proto.(core.StorageProtocol),
		)
		require.NoError(tb, err)

		err = tusService.UploadProcessing(ctx, proto.(core.StorageProtocol), tusUpload.TUSUploadID)
		require.NoError(tb, err)

		// --- Storage Upload ---
		// Upload file info
		fileInfo := handler.FileInfo{
			ID:   objectId,
			Size: int64(len(testData)),
			MetaData: map[string]string{
				"stream_name":         "test-tus-stream",
				"suggested_file_name": "test-tus-video.mp4",
			},
		}
		infoBytes := mustMarshal(tb, fileInfo)
		infoData := io.NopCloser(bytes.NewReader(infoBytes))
		err = storageSvc.S3MultipartUpload(
			ctx,
			infoData,
			ctx.Config().Config().Core.Storage.S3.BufferBucket,
			storageSvc.GetTemporaryUploadPath(proto.(core.StorageProtocol), fmt.Sprintf("%s.info", objectId)),
			uint64(len(infoBytes)),
		)
		require.NoError(tb, err)

		// Upload the actual file data
		err = storageSvc.S3MultipartUpload(
			ctx,
			pluginTesting.NewReadSeekCloser(testData),
			ctx.Config().Config().Core.Storage.S3.BufferBucket,
			storageSvc.GetTemporaryUploadPath(proto.(core.StorageProtocol), objectId),
			uint64(len(testData)),
		)
		require.NoError(tb, err)

		// --- Workflow Execution ---
		wf := wfTest.NewOperationWorkflow(core.TUSUploadOperationName(internal.ProtocolName))
		wfTest.MustConvertRequestToWorkflow(
			tusUpload.GetRequestID(),
			wf,
			0,
			core.WithWorkflowStorageHash(storageHash),
			core.WithWorkflowUserID(testUser.ID),
			core.WithWorkflowSourceIP(uploaderIp),
		)

		req := wfTest.GetRequest(tusUpload.GetRequestID())
		wfTest.ExecuteWorkflowStep(req)
		wfTest.CompleteWorkflowStep(req)

		// --- Assertions ---
		wfTest.AssertOperationSuccess(req)
		wfTest.AssertOperationStatusMessageContains(req, "Successfully completed")
		wfTest.AssertOperationStatusProgress(req, 100)
	},
		coreTesting.CombineOptions(GetTUSUploadTestOptions()),
	)
}

func mustMarshal(tb coreTesting.TB, v interface{}) []byte {
	data, err := json.Marshal(v)
	require.NoError(tb, err)
	return data
}
