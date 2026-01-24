package protocol

import (
	"context"
	"fmt"

	"go.lumeweb.com/portal-plugin-lbry/internal"
	"go.lumeweb.com/portal-plugin-lbry/internal/protocol/util"
	"go.lumeweb.com/portal/core"
	"go.lumeweb.com/portal/db/models"
	"go.uber.org/zap"
)

// PostUploadOperationHandler handles post-upload processing
type PostUploadOperationHandler struct {
	core.OperationHelper
}

func (h *PostUploadOperationHandler) ValidateRequest(_ context.Context, req *models.Request) error {
	return ValidateRequestWithHash(req)
}

func (h *PostUploadOperationHandler) Execute(ctx context.Context, req *models.Request) error {
	var workflow PostUploadWorkflowData
	err := h.StructuredWorkflowData(req.ID, &workflow)
	if err != nil {
		return err
	}

	// Initialize progress tracker with manual mode for simple milestones
	tracker, err := h.NewProgressTracker(req.ID, core.ProgressModeManual, func(cfg *core.ProgressTrackerConfig) {
		cfg.MessageProvider = h.NewDefaultProgressMessageProvider(core.OpTypeUpload)
	})
	if err != nil {
		return fmt.Errorf("failed to initialize progress tracker: %w", err)
	}

	if err := tracker.Initialize(); err != nil {
		return fmt.Errorf("failed to initialize tracker: %w", err)
	}

	// Set initial progress
	if err := tracker.SetProgress(10); err != nil {
		h.Logger().Warn("Failed to update progress", zap.Error(err))
	}

	// Get storage service using shared utility
	storageSvc, err := GetStorageService(h.Context())
	if err != nil {
		return err
	}

	// Set progress - preparing
	if err := tracker.SetProgress(30); err != nil {
		h.Logger().Warn("Failed to update progress", zap.Error(err))
	}

	// Create upload processor
	processor := NewUploadProcessor(h.Context())

	// Cast to storage protocol with type safety
	storageProtocol, err := util.CastToStorageProtocol(h.Protocol())
	if err != nil {
		return err
	}

	// Create post upload source
	source := NewPostUploadSource(
		workflow.UploadID,
		workflow.Size,
		workflow.Meta,
		storageSvc,
		storageProtocol,
	)

	// Set progress - processing
	if err := tracker.SetProgress(60); err != nil {
		h.Logger().Warn("Failed to update progress", zap.Error(err))
	}

	// Process the upload using shared processor
	_, err = processor.ProcessStreamUpload(ctx, source, uint64(*req.UserID))
	if err != nil {
		return err
	}

	// Set progress - completing
	if err := tracker.SetProgress(90); err != nil {
		h.Logger().Warn("Failed to update progress", zap.Error(err))
	}

	// Complete
	if err := tracker.SetProgress(100); err != nil {
		h.Logger().Warn("Failed to update progress", zap.Error(err))
	}

	return nil
}

func (h *PostUploadOperationHandler) GetStatus(_ context.Context, req *models.Request) (*core.RequestStatus, error) {
	return h.GetStatusFromWorkflowData(req.ID, req)
}

func (h *PostUploadOperationHandler) Cleanup(ctx context.Context, req *models.Request) error {
	// Load workflow to get UploadID
	var workflow PostUploadWorkflowData
	err := h.StructuredWorkflowData(req.ID, &workflow)
	if err != nil {
		return fmt.Errorf("failed to load workflow data: %w", err)
	}

	// Delete temporary upload using shared utility
	storageSvc, err := GetStorageService(h.Context())
	if err != nil {
		return err
	}

	// Cast to storage protocol with type safety
	storageProtocol, err := util.CastToStorageProtocol(h.Protocol())
	if err != nil {
		return err
	}

	err = storageSvc.S3DeleteTemporaryUpload(ctx, storageProtocol, workflow.UploadID)
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
