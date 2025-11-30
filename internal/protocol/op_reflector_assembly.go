package protocol

import (
	"context"
	"encoding/hex"
	"fmt"
	"sort"
	"time"

	"github.com/samber/lo"
	"go.lumeweb.com/liblbry/server"
	lbrystream "go.lumeweb.com/liblbry/stream"
	pluginCore "go.lumeweb.com/portal-plugin-lbry/core"
	"go.lumeweb.com/portal-plugin-lbry/internal"
	pluginDb "go.lumeweb.com/portal-plugin-lbry/internal/db"
	"go.lumeweb.com/portal/core"
	"go.lumeweb.com/portal/db/models"
	"go.uber.org/zap"
)

// ReflectorAssemblyOperationHandler handles the reflector assembly workflow operation
type ReflectorAssemblyOperationHandler struct {
	core.OperationHelper
}

// NewReflectorAssemblyOperationHandler creates a new ReflectorAssemblyOperationHandler
func NewReflectorAssemblyOperationHandler(ctx core.Context) *ReflectorAssemblyOperationHandler {
	return &ReflectorAssemblyOperationHandler{
		OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
	}
}

// withUserIDContext injects the UserID into the context for ReflectorStore operations
func withUserIDContext(ctx context.Context, userID uint) context.Context {
	return context.WithValue(ctx, userIDContextKey{}, userID)
}

// Execute executes the reflector assembly operation
func (h *ReflectorAssemblyOperationHandler) ValidateRequest(_ context.Context, req *models.Request) error {
	return ValidateRequestWithHash(req)
}

func (h *ReflectorAssemblyOperationHandler) Execute(ctx context.Context, req *models.Request) error {
	var workflow ReflectorAssemblyWorkflowData
	err := h.StructuredWorkflowData(req.ID, &workflow)
	if err != nil {
		return fmt.Errorf("failed to load workflow data: %w", err)
	}

	h.Logger().Debug("Executing reflector assembly workflow",
		zap.Uint("user_id", *req.UserID),
		zap.String("sd_blob_hash", workflow.SDBlobHash))

	// Initialize progress
	workflow.Progress = 10
	err = h.UpdateWorkflowDataStruct(req.ID, workflow)
	if err != nil {
		h.Logger().Warn("Failed to update progress", zap.Error(err))
	}

	// Get upload service
	uploadService := core.GetService[pluginCore.UploadService](h.Context(), pluginCore.UPLOAD_SERVICE)
	if uploadService == nil {
		return fmt.Errorf("upload service not found")
	}

	// Update progress - starting assembly
	workflow.Progress = 30
	err = h.UpdateWorkflowDataStruct(req.ID, workflow)
	if err != nil {
		h.Logger().Warn("Failed to update progress", zap.Error(err))
	}

	// Attempt to assemble the stream
	err = h.checkAndAssembleStream(ctx, uploadService, *req.UserID, workflow.SDBlobHash)
	if err != nil {
		h.Logger().Info("Stream assembly not yet ready",
			zap.Uint("user_id", *req.UserID),
			zap.String("sd_blob_hash", workflow.SDBlobHash),
			zap.Error(err))

		// Return error to trigger workflow retry
		return fmt.Errorf("stream assembly not ready: %w", err)
	}

	// Update progress - completed
	workflow.Progress = 100
	err = h.UpdateWorkflowDataStruct(req.ID, workflow)
	if err != nil {
		h.Logger().Warn("Failed to update progress", zap.Error(err))
	}

	h.Logger().Info("Successfully completed reflector assembly workflow",
		zap.Uint("user_id", *req.UserID),
		zap.String("sd_blob_hash", workflow.SDBlobHash))

	return nil
}

// checkAndAssembleStream checks if all required blobs are available and assembles the stream
func (h *ReflectorAssemblyOperationHandler) checkAndAssembleStream(
	ctx context.Context,
	uploadService pluginCore.UploadService,
	userID uint,
	sdBlobHash string,
) error {
	// Get pending stream metadata from upload service
	pendingStream, err := uploadService.GetPendingStream(ctx, userID, sdBlobHash)
	if err != nil {
		return fmt.Errorf("failed to get pending stream: %w", err)
	}

	h.Logger().Debug("Checking stream assembly progress",
		zap.Uint("user_id", userID),
		zap.String("sd_blob_hash", sdBlobHash),
		zap.Int("total_blobs_expected", pendingStream.TotalBlobs))

	// Get SD blob data from temporary storage
	storageSvc, err := GetStorageService(h.Context())
	if err != nil {
		return fmt.Errorf("failed to get storage service: %w", err)
	}

	storageProtocol, err := GetStorageProtocol()
	if err != nil {
		return fmt.Errorf("failed to get storage protocol: %w", err)
	}

	// Reconstruct SD blob from pending stream metadata
	sdBlob, err := h.buildSDBlob(ctx, uploadService, pendingStream)
	if err != nil {
		return fmt.Errorf("failed to reconstruct SD blob from pending stream: %w", err)
	}

	// Get list of required blob hashes
	requiredBlobs := lo.Map(sdBlob.BlobInfos, func(item lbrystream.BlobInfo, index int) string {
		return hex.EncodeToString(item.BlobHash)
	})

	// Check if we have all required blobs
	missingBlobs, err := uploadService.GetMissingBlobs(ctx, userID, pendingStream.ID, requiredBlobs)
	if err != nil {
		return fmt.Errorf("failed to check missing blobs: %w", err)
	}

	// Get count of pending blobs for this stream
	pendingCount, err := uploadService.GetPendingBlobCount(ctx, userID, pendingStream.ID)
	if err != nil {
		return fmt.Errorf("failed to count pending blobs: %w", err)
	}

	// Validate total vs pending independently
	expectedTotal := pendingStream.TotalBlobs
	pendingTotal := int(pendingCount)

	// Check for mismatch between expected total and SD blob requirements
	if expectedTotal != len(requiredBlobs) {
		h.Logger().Warn("Total blobs count mismatch between pending stream and SD blob",
			zap.Uint("user_id", userID),
			zap.String("sd_blob_hash", sdBlobHash),
			zap.Int("expected_total_from_db", expectedTotal),
			zap.Int("required_from_sd_blob", len(requiredBlobs)))
		// Use the SD blob count as the authoritative source
		expectedTotal = len(requiredBlobs)
	}

	// Check if we have missing blobs OR if pending count doesn't match expected
	if len(missingBlobs) > 0 || pendingTotal != expectedTotal {
		progressPercent := 0
		if expectedTotal > 0 {
			progressPercent = int(float64(pendingTotal) / float64(expectedTotal) * 100)
		}

		h.Logger().Info("Stream not ready for assembly, will retry",
			zap.Uint("user_id", userID),
			zap.String("sd_blob_hash", sdBlobHash),
			zap.Int("expected_total", expectedTotal),
			zap.Int("pending_total", pendingTotal),
			zap.Int("missing_from_required", len(missingBlobs)),
			zap.Int("progress_percent", progressPercent),
			zap.Strings("missing_blob_hashes", missingBlobs))
		return fmt.Errorf("stream not ready: pending %d/%d blobs, missing %d required blobs",
			pendingTotal, expectedTotal, len(missingBlobs))
	}

	// Assemble the stream
	err = h.assembleStream(ctx, uploadService, storageSvc, storageProtocol, userID, sdBlob)
	if err != nil {
		return fmt.Errorf("failed to assemble stream: %w", err)
	}

	h.Logger().Info("Successfully assembled stream",
		zap.Uint("user_id", userID),
		zap.String("sd_blob_hash", sdBlobHash),
		zap.Int("expected_total", expectedTotal),
		zap.Int("pending_total", pendingTotal),
		zap.Int("blob_count", len(requiredBlobs)))

	return nil
}

// assembleStream assembles the stream from temporary blobs and moves to permanent storage
func (h *ReflectorAssemblyOperationHandler) assembleStream(ctx context.Context, uploadService pluginCore.UploadService, storageSvc core.StorageService, storageProtocol core.StorageProtocol, userID uint, sdBlob *lbrystream.SDBlob) error {
	// Inject UserID into context for ReflectorStore operations
	ctx = withUserIDContext(ctx, userID)
	// Get protocol and blob manager
	protocolInstance, _, err := GetProtocolWithValidation()
	if err != nil {
		return fmt.Errorf("failed to get protocol: %w", err)
	}

	// Get ReflectorStore from protocol for reading pending blobs
	reflectorStore := protocolInstance.ReflectorStore()
	if reflectorStore == nil {
		return fmt.Errorf("reflector store not initialized")
	}

	sdBlobHash := sdBlob.HashHex()

	blobManager, ok := protocolInstance.Node().(server.BlobManager)
	if !ok {
		return fmt.Errorf("protocol node does not implement server.BlobManager interface")
	}

	sdBlobBytes, err := sdBlob.ToBlob()
	if err != nil {
		return fmt.Errorf("failed to serialize SD blob: %w", err)
	}

	// Store SD blob in blob manager first
	err = blobManager.AddSDBlob(ctx, sdBlobHash, sdBlobBytes)
	if err != nil {
		return fmt.Errorf("failed to add SD blob %s to blob manager: %w", sdBlobHash, err)
	}

	h.Logger().Debug("Successfully stored SD blob in blob manager",
		zap.String("sd_blob_hash", sdBlobHash),
		zap.Int("sd_blob_size", len(sdBlobBytes)))

	// Move only regular blobs (not SD blob) from temporary to permanent storage
	for _, blobInfo := range sdBlob.BlobInfos {
		// Get blob from ReflectorStore for reading pending blobs
		if len(blobInfo.BlobHash) == 0 {
			h.Logger().Debug("Skipping terminating/empty blob",
				zap.String("sd_blob_hash", sdBlobHash))
			continue
		}

		blobHash := hex.EncodeToString(blobInfo.BlobHash)

		data, err := reflectorStore.Get(ctx, blobHash)
		if err != nil {
			return fmt.Errorf("failed to get blob %s from reflector store: %w", blobHash, err)
		}

		// Store blob in blob manager using AddBlob
		err = blobManager.AddBlob(ctx, blobHash, data)
		if err != nil {
			return fmt.Errorf("failed to add blob %s to blob manager: %w", blobHash, err)
		}

		h.Logger().Debug("Successfully stored blob in blob manager",
			zap.String("blob_hash", blobHash),
			zap.Int("blob_size", len(data)))
	}

	// Build stream result from SD blob
	streamResult := h.buildStreamResult(sdBlob)

	// Move from pending to active using ProcessStreamResult
	err = ProcessStreamResult(ctx, h.Context(), streamResult, userID)
	if err != nil {
		return fmt.Errorf("failed to move pending stream to active: %w", err)
	}

	// Cleanup pending blobs from database
	err = uploadService.CleanupPendingBlobs(ctx, userID, streamResult)
	if err != nil {
		return fmt.Errorf("failed to cleanup pending blobs: %w", err)
	}

	// Cleanup regular blobs from ReflectorStore
	for _, blobInfo := range sdBlob.BlobInfos {
		if len(blobInfo.BlobHash) == 0 {
			h.Logger().Debug("Skipping terminating/empty blob",
				zap.String("sd_blob_hash", sdBlobHash))
			continue
		}
		err = reflectorStore.Delete(ctx, hex.EncodeToString(blobInfo.BlobHash))
		if err != nil {
			h.Logger().Warn("Failed to delete blob from reflector store",
				zap.Uint("user_id", userID),
				zap.String("blob_hash", hex.EncodeToString(blobInfo.BlobHash)),
				zap.Error(err))
			// Continue with cleanup even if deletion fails
		}
	}

	return nil
}

// GetStatus returns the status of the reflector assembly operation
func (h *ReflectorAssemblyOperationHandler) GetStatus(_ context.Context, req *models.Request) (*core.RequestStatus, error) {
	status := &core.RequestStatus{
		State:     req.Status,
		UpdatedAt: time.Now(),
	}

	// Try to get workflow data for progress
	var workflow ReflectorAssemblyWorkflowData
	err := h.StructuredWorkflowData(req.ID, &workflow)

	// Set default status based on request status
	switch req.Status {
	case models.RequestStatusPending:
		status.ProgressPercent = 0
		status.Message = "Waiting to start reflector assembly"
	case models.RequestStatusProcessing:
		if err == nil {
			status.ProgressPercent = float64(workflow.Progress)
		} else {
			status.ProgressPercent = 25 // Fallback progress
		}
		status.Message = "Assembling stream from uploaded blobs"
	case models.RequestStatusCompleted:
		status.ProgressPercent = 100
		status.Message = "Stream assembly completed successfully"
	case models.RequestStatusFailed:
		status.ProgressPercent = 0
		status.Message = "Stream assembly failed"
	default:
		if err == nil {
			status.ProgressPercent = float64(workflow.Progress)
		} else {
			status.ProgressPercent = 0
		}
		status.Message = "Assembly in progress"
	}

	return status, nil
}

// Cleanup handles cleanup for the reflector assembly operation
// buildStreamResult creates a StreamResult from an SDBlob
func (h *ReflectorAssemblyOperationHandler) buildStreamResult(sdBlob *lbrystream.SDBlob) *lbrystream.StreamResult {
	// Extract content hashes and chunk sizes from blob infos using lo.Map
	contentHashes := lo.Map(sdBlob.BlobInfos, func(item lbrystream.BlobInfo, index int) string {
		return hex.EncodeToString(item.BlobHash)
	})
	chunkSizes := lo.Map(sdBlob.BlobInfos, func(item lbrystream.BlobInfo, index int) int {
		return item.Length
	})
	contentBlobs := lo.Map(sdBlob.BlobInfos, func(item lbrystream.BlobInfo, index int) []byte {
		return item.BlobHash
	})

	return &lbrystream.StreamResult{
		SDBlob:        sdBlob,
		SDBlobHash:    sdBlob.HashHex(),
		StreamHash:    hex.EncodeToString(sdBlob.StreamHash),
		ContentHashes: contentHashes,
		ChunkSizes:    chunkSizes,
		ContentBlobs:  contentBlobs,
		TotalChunks:   len(sdBlob.BlobInfos),
	}
}

// buildSDBlob reconstructs an SD blob from pending stream metadata
func (h *ReflectorAssemblyOperationHandler) buildSDBlob(ctx context.Context, uploadService pluginCore.UploadService, pendingStream *pluginDb.PendingStream) (*lbrystream.SDBlob, error) {
	// Decode stream hash from hex string
	streamHashBytes, err := hex.DecodeString(pendingStream.StreamHash)
	if err != nil {
		return nil, fmt.Errorf("failed to decode stream hash %q: %w", pendingStream.StreamHash, err)
	}

	// Build the SDBlob structure from pending stream metadata
	sdBlob := lbrystream.SDBlob{
		StreamName:        pendingStream.StreamName,
		StreamType:        pendingStream.StreamType,
		SuggestedFileName: pendingStream.SuggestedFileName,
		Key:               pendingStream.KeyData,
		StreamHash:        streamHashBytes,
	}

	// Get associated blobs using upload service
	blobInfos, err := h.getBlobInfosFromPendingBlobs(ctx, uploadService, pendingStream.UserID, pendingStream.SDHash)
	if err != nil {
		return nil, fmt.Errorf("failed to get associated blobs for pending stream %q: %w", pendingStream.SDHash, err)
	}

	// Sort blob infos by blob number to maintain order
	sort.Slice(blobInfos, func(i, j int) bool {
		return blobInfos[i].BlobNum < blobInfos[j].BlobNum
	})

	sdBlob.BlobInfos = blobInfos

	return &sdBlob, nil
}

// getBlobInfosFromPendingBlobs retrieves blob information using upload service
func (h *ReflectorAssemblyOperationHandler) getBlobInfosFromPendingBlobs(ctx context.Context, uploadService pluginCore.UploadService, userID uint, sdHash string) ([]lbrystream.BlobInfo, error) {
	pendingBlobs, err := uploadService.GetPendingBlobs(ctx, userID, sdHash)
	if err != nil {
		return nil, fmt.Errorf("failed to get pending blobs for user %d and SD hash %s: %w", userID, sdHash, err)
	}

	var blobInfos []lbrystream.BlobInfo
	for _, pendingBlob := range pendingBlobs {
		var blobHashBytes []byte

		// For terminating blobs, always use empty hash in blob lists
		if pendingBlob.Terminating {
			blobHashBytes = []byte{} // Empty hash for terminating blobs
		} else {
			// Convert blob hash from hex string to bytes for non-terminating blobs
			var err error
			blobHashBytes, err = hex.DecodeString(pendingBlob.BlobHash)
			if err != nil {
				h.Logger().Warn("Failed to decode pending blob hash from hex string",
					zap.String("blob_hash", pendingBlob.BlobHash),
					zap.Uint("user_id", userID),
					zap.Error(err))
				continue
			}
		}

		// Create BlobInfo using the IV data from the pending blob
		// The IV data is available from when the blob was initially uploaded
		blobInfo := lbrystream.BlobInfo{
			Length:   pendingBlob.BlobSize,
			BlobNum:  pendingBlob.BlobNumber, // Use blob number from database
			BlobHash: blobHashBytes,
			IV:       pendingBlob.IVData,
		}
		blobInfos = append(blobInfos, blobInfo)
	}

	return blobInfos, nil
}

// Cleanup handles cleanup for the reflector assembly operation.
// Note: Cleanup is handled within assembleStream after successful processing,
// so this method is intentionally a no-op.
func (h *ReflectorAssemblyOperationHandler) Cleanup(ctx context.Context, req *models.Request) error {
	return nil
}
