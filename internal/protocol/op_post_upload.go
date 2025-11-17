package protocol

import (
	"context"
	"fmt"
	"io"

	"go.lumeweb.com/liblbry/server"
	"go.lumeweb.com/liblbry/stream"
	pluginCore "go.lumeweb.com/portal-plugin-lbry/core"
	"go.lumeweb.com/portal-plugin-lbry/internal"
	"go.lumeweb.com/portal/core"
	"go.lumeweb.com/portal/db/models"
	"go.uber.org/zap"
)

// PostUploadOperationHandler handles post-upload processing
type PostUploadOperationHandler struct {
	core.OperationHelper
}

func (h *PostUploadOperationHandler) ValidateRequest(_ context.Context, req *models.Request) error {
	if req.Hash == nil {
		return fmt.Errorf("upload hash is required")
	}
	if req.UserID == nil {
		return fmt.Errorf("user ID is required")
	}
	if *req.UserID == 0 {
		return fmt.Errorf("user ID cannot be zero")
	}
	return nil
}

func (h *PostUploadOperationHandler) Execute(ctx context.Context, req *models.Request) error {
	var workflow PostUploadWorkflowData
	err := h.StructuredWorkflowData(req.ID, &workflow)
	if err != nil {
		return err
	}

	// Safely assert protocol to *Protocol
	proto, ok := h.Protocol().(*Protocol)
	if !ok {
		return fmt.Errorf("failed to cast protocol to *Protocol")
	}

	// Safely assert node to server.BlobManager
	blobManager, ok := proto.Node().(server.BlobManager)
	if !ok {
		return fmt.Errorf("failed to cast node to server.BlobManager")
	}

	storageSvc := core.GetService[core.StorageService](h.Context(), core.STORAGE_SERVICE)
	// Get the upload from storage service
	upload, err := storageSvc.S3GetTemporaryUpload(ctx, h.Protocol().(core.StorageProtocol), workflow.UploadID)
	if err != nil {
		return fmt.Errorf("failed to get upload: %w", err)
	}
	defer func(upload io.ReadCloser) {
		err = upload.Close()
		if err != nil {
			h.Logger().Error("failed to close upload", zap.Error(err))
		}
	}(upload)

	manifestCreator := stream.NewManifestCreator()
	streamCreator := stream.NewStreamCreator(manifestCreator)

	// Prepare stream options - include metadata if provided
	var streamOpts []stream.StreamOption

	// Add chunk handler for blob management
	streamOpts = append(streamOpts, stream.WithChunkHandler(func(chunk stream.Chunk) error {
		return blobManager.AddBlob(chunk.Hash, chunk.Data)
	}))

	// Add SD handler to apply metadata if provided
	if workflow.Meta != nil {
		sdBlob := &stream.SDBlob{}

		if workflow.Meta.StreamName != "" {
			sdBlob.StreamName = workflow.Meta.StreamName
		}
		if workflow.Meta.SuggestedFileName != "" {
			sdBlob.SuggestedFileName = workflow.Meta.SuggestedFileName
		}

		blob, err := sdBlob.ToBlob()
		if err != nil {
			return fmt.Errorf("failed to serialize SD blob: %w", err)
		}

		streamOpts = append(streamOpts,
			stream.WithExistingSDBlob(blob))
	}

	streamResult, err := streamCreator.CreateStream(upload, workflow.Size, streamOpts...)
	if err != nil {
		return fmt.Errorf("failed to create stream: %w", err)
	}

	sdBlob, err := streamResult.SDBlob.ToBlob()
	if err != nil {
		return fmt.Errorf("failed to convert SDBlob to blob: %w", err)
	}

	err = blobManager.AddSDBlob(streamResult.SDBlobHash, sdBlob)
	if err != nil {
		return fmt.Errorf("failed to add SDBlob to blob manager: %w", err)
	}

	userID := *req.UserID
	uploadSvc := core.GetService[pluginCore.UploadService](h.Context(), pluginCore.UPLOAD_SERVICE)

	if uploadSvc == nil {
		h.Logger().Error("Upload service not available")
		return fmt.Errorf("upload service not available")
	}

	// Process all CIDs to create upload and core pin records
	err = uploadSvc.ProcessUpload(ctx, streamResult, userID)
	if err != nil {
		return fmt.Errorf("failed to process upload: %w", err)
	}

	// Convert SD blob hash to CID
	sdCid, err := internal.LBRYHashToCID(streamResult.SDBlobHash)
	if err != nil {
		return fmt.Errorf("failed to convert SD blob hash to CID: %w", err)
	}

	// Create stream pin record
	_, err = uploadSvc.CreateStreamPin(ctx, userID, sdCid)
	if err != nil {
		return fmt.Errorf("failed to create root pin: %w", err)
	}

	return nil
}

func (h *PostUploadOperationHandler) GetStatus(_ context.Context, _ *models.Request) (*core.RequestStatus, error) {
	return &core.RequestStatus{
		ProgressPercent: 100,
		Message:         "Upload processed successfully",
	}, nil
}

func (h *PostUploadOperationHandler) Cleanup(ctx context.Context, req *models.Request) error {
	// Load workflow to get UploadID
	var workflow PostUploadWorkflowData
	err := h.StructuredWorkflowData(req.ID, &workflow)
	if err != nil {
		return fmt.Errorf("failed to load workflow data: %w", err)
	}

	// Delete temporary upload
	storageSvc := core.GetService[core.StorageService](h.Context(), core.STORAGE_SERVICE)
	if storageSvc == nil {
		return fmt.Errorf("storage service not available")
	}

	err = storageSvc.S3DeleteTemporaryUpload(ctx, h.Protocol().(core.StorageProtocol), workflow.UploadID)
	if err != nil {
		return fmt.Errorf("failed to delete temporary upload: %w", err)
	}
	return nil
}

func NewPostUploadOperation(ctx core.Context) core.Operation {
	return core.NewPostUploadOperation(internal.ProtocolName,
		&PostUploadOperationHandler{
			OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
		},
	)
}
