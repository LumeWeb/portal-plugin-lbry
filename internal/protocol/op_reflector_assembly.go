package protocol

import (
	"context"
	"encoding/hex"
	"fmt"
	"sort"

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

// Reflector assembly step constants
const (
	assemblyStepCheckAssembly  = "check_assembly"
	assemblyStepAssembleStream = "assemble_stream"
	assemblyStepCleanup        = "cleanup"
)

// ReflectorAssemblyProgress holds progress state for assembly
type ReflectorAssemblyProgress struct {
	sdBlobHash     string
	totalBlobs     int
	tracker        *core.ProgressTracker
	helper         *core.ProgressTrackerHelper
	assemblyHelper *core.ProgressTrackerHelper
}

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

	// Get upload service
	uploadService := core.GetService[pluginCore.UploadService](h.Context(), pluginCore.UPLOAD_SERVICE)
	if uploadService == nil {
		return fmt.Errorf("upload service not found")
	}

	// Get pending stream for validation
	pendingStream, err := uploadService.GetPendingStream(ctx, *req.UserID, workflow.SDBlobHash)
	if err != nil {
		return fmt.Errorf("failed to get pending stream: %w", err)
	}

	h.Logger().Debug("Retrieved pending stream for progress tracking",
		zap.Uint("user_id", *req.UserID),
		zap.String("sd_blob_hash", workflow.SDBlobHash),
		zap.Int("pending_stream_total_blobs", pendingStream.TotalBlobs),
		zap.Bool("has_terminating_blob", pendingStream.TerminatingBlobNumber != nil))

	// Initialize progress tracker with weighted steps
	tracker, err := h.NewProgressTracker(req.ID, core.ProgressModeWeighted, func(cfg *core.ProgressTrackerConfig) {
		cfg.Steps = []core.ProgressStep{
			{
				Name:        assemblyStepCheckAssembly,
				Description: "Checking stream assembly progress",
				Weight:      5,
			},
			{
				Name:        assemblyStepAssembleStream,
				Description: "Assembling stream from uploaded blobs",
				Weight:      90,
			},
			{
				Name:        assemblyStepCleanup,
				Description: "Cleaning up temporary blobs",
				Weight:      5,
			},
		}
		cfg.MessageProvider = h.NewDefaultProgressMessageProvider(core.OpTypeUpload)
	})
	if err != nil {
		return fmt.Errorf("failed to initialize progress tracker: %w", err)
	}

	if err = tracker.Initialize(); err != nil {
		return fmt.Errorf("failed to initialize tracker: %w", err)
	}

	// Create progress state (totalBlobs will be set in checkAndAssembleStream)
	progress := &ReflectorAssemblyProgress{
		sdBlobHash: workflow.SDBlobHash,
		tracker:    tracker,
		helper:     core.NewProgressTrackerHelper(tracker, h.Context()),
	}

	// Attempt to assemble the stream
	err = h.checkAndAssembleStream(ctx, uploadService, *req.UserID, workflow.SDBlobHash, progress)
	if err != nil {
		h.Logger().Info("Stream assembly not yet ready",
			zap.Uint("user_id", *req.UserID),
			zap.String("sd_blob_hash", workflow.SDBlobHash),
			zap.Error(err))

		// Return error to trigger workflow retry
		return fmt.Errorf("stream assembly not ready: %w", err)
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
	progress *ReflectorAssemblyProgress,
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

	// Count non-terminating blobs for sub-tracker work units
	nonTerminatingBlobCount := 0
	for _, blobInfo := range sdBlob.BlobInfos {
		if len(blobInfo.BlobHash) > 0 {
			nonTerminatingBlobCount++
		}
	}

	// Set totalBlobs in progress state for sub-tracker creation
	progress.totalBlobs = nonTerminatingBlobCount

	h.Logger().Debug("SD blob info",
		zap.Uint("user_id", userID),
		zap.String("sd_blob_hash", sdBlobHash),
		zap.Int("sd_blob_info_count", len(sdBlob.BlobInfos)),
		zap.Int("non_terminating_blob_count", nonTerminatingBlobCount),
		zap.Int("pending_stream_total_blobs", pendingStream.TotalBlobs),
		zap.Bool("has_terminating_blob", pendingStream.TerminatingBlobNumber != nil))

	// Create sub-tracker for blob assembly (work units mode)
	assemblySubTracker, err := progress.tracker.CreateSubTrackerForStep(assemblyStepAssembleStream, core.ProgressModeWorkUnits, func(cfg *core.ProgressTrackerConfig) {
		cfg.TotalWorkUnits = nonTerminatingBlobCount
	})
	if err != nil {
		return fmt.Errorf("failed to create assembly sub-tracker: %w", err)
	}
	// Initialize the sub-tracker
	if err = assemblySubTracker.Initialize(); err != nil {
		return fmt.Errorf("failed to initialize assembly sub-tracker: %w", err)
	}
	progress.assemblyHelper = core.NewProgressTrackerHelper(assemblySubTracker, h.Context())
	h.Logger().Debug("Successfully created and initialized assembly sub-tracker",
		zap.Uint("user_id", userID),
		zap.String("sd_blob_hash", sdBlobHash),
		zap.Int("total_work_units", nonTerminatingBlobCount))

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

	// Account for terminating blob in pending total since it's not stored in pending_blobs table
	if pendingStream.TerminatingBlobNumber != nil {
		pendingTotal++
	}

	// Check for mismatch between expected total and SD blob requirements
	if expectedTotal != len(requiredBlobs) {
		hasTerminatingBlob := pendingStream.TerminatingBlobNumber != nil
		h.Logger().Debug("Total blobs count mismatch detected",
			zap.Uint("user_id", userID),
			zap.String("sd_blob_hash", sdBlobHash),
			zap.Int("pending_stream_total_blobs", expectedTotal),
			zap.Int("sd_blob_info_count", len(sdBlob.BlobInfos)),
			zap.Int("required_blobs_count", len(requiredBlobs)),
			zap.Bool("has_terminating_blob", hasTerminatingBlob))
		return fmt.Errorf("total blobs count mismatch between pending stream (%d) and SD blob (%d) for sd_hash %q (has_terminating_blob: %v)",
			expectedTotal, len(sdBlob.BlobInfos), sdBlobHash, hasTerminatingBlob)
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

	// Step 1: Check assembly complete
	if err = progress.helper.RunStep(assemblyStepCheckAssembly, 100, func() error {
		return nil
	}); err != nil {
		return err
	}

	if err = progress.helper.RunStep(assemblyStepAssembleStream, 100, func() error {
		return h.assembleStream(ctx, uploadService, storageSvc, storageProtocol, userID, sdBlob, progress)
	}); err != nil {
		return fmt.Errorf("failed to assemble stream: %w", err)
	}

	// Step 3: Cleanup
	if err = progress.helper.RunStep(assemblyStepCleanup, 100, func() error {
		return nil
	}); err != nil {
		return err
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
func (h *ReflectorAssemblyOperationHandler) assembleStream(ctx context.Context, uploadService pluginCore.UploadService, storageSvc core.StorageService, storageProtocol core.StorageProtocol, userID uint, sdBlob *lbrystream.SDBlob, progress *ReflectorAssemblyProgress) error {
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
	unitIndex := 0
	for _, blobInfo := range sdBlob.BlobInfos {
		// Get blob from ReflectorStore for reading pending blobs
		if len(blobInfo.BlobHash) == 0 {
			h.Logger().Debug("Skipping terminating/empty blob",
				zap.String("sd_blob_hash", sdBlobHash))
			continue
		}

		blobHash := hex.EncodeToString(blobInfo.BlobHash)

		h.Logger().Debug("Processing blob",
			zap.Uint("user_id", userID),
			zap.String("sd_blob_hash", sdBlobHash),
			zap.String("blob_hash", blobHash),
			zap.Int("blob_num", blobInfo.BlobNum),
			zap.Int("unit_index", unitIndex))

		processErr := progress.assemblyHelper.RunWorkUnit(unitIndex, func() error {
			// Check if blob exists in temporary storage before attempting to retrieve it
			exists, err := reflectorStore.Has(ctx, blobHash)
			if err != nil {
				return fmt.Errorf("failed to check blob existence for %s: %w", blobHash, err)
			}
			if !exists {
				return fmt.Errorf("blob %s not found in temporary storage", blobHash)
			}

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
			return nil
		})
		if processErr != nil {
			return processErr
		}
		unitIndex++
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
	return h.GetStatusFromWorkflowData(req.ID, req)
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
	blobInfos, err := h.getBlobInfosFromPendingBlobs(ctx, uploadService, pendingStream.UserID, pendingStream.SDHash, pendingStream.TerminatingBlobNumber, pendingStream.TerminatingBlobIV)
	if err != nil {
		return nil, fmt.Errorf("failed to get associated blobs for pending stream %q: %w", pendingStream.SDHash, err)
	}

	sdBlob.BlobInfos = blobInfos

	return &sdBlob, nil
}

// getBlobInfosFromPendingBlobs retrieves blob information using upload service
func (h *ReflectorAssemblyOperationHandler) getBlobInfosFromPendingBlobs(ctx context.Context, uploadService pluginCore.UploadService, userID uint, sdHash string, terminatingBlobNum *int, terminatingBlobIV []byte) ([]lbrystream.BlobInfo, error) {
	pendingBlobs, err := uploadService.GetPendingBlobs(ctx, userID, sdHash)
	if err != nil {
		return nil, fmt.Errorf("failed to get pending blobs for user %d and SD hash %s: %w", userID, sdHash, err)
	}

	var blobInfos []lbrystream.BlobInfo
	for _, pendingBlob := range pendingBlobs {
		// Convert blob hash from hex string to bytes
		blobHashBytes, err := hex.DecodeString(pendingBlob.BlobHash)
		if err != nil {
			h.Logger().Warn("Failed to decode pending blob hash from hex string",
				zap.String("blob_hash", pendingBlob.BlobHash),
				zap.Uint("user_id", userID),
				zap.Error(err))
			continue
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

	// Add virtual terminating blob if the stream has one
	if terminatingBlobNum != nil {
		blobInfos = append(blobInfos, lbrystream.BlobInfo{
			Length:   0,
			BlobNum:  *terminatingBlobNum,
			BlobHash: []byte{},          // Empty hash for terminating blob
			IV:       terminatingBlobIV, // Use stored terminating blob IV, or empty if not set
		})
	}

	// Sort blob infos by blob number to maintain correct order
	sort.Slice(blobInfos, func(i, j int) bool {
		return blobInfos[i].BlobNum < blobInfos[j].BlobNum
	})

	return blobInfos, nil
}

// Cleanup handles cleanup for the reflector assembly operation.
// Note: Cleanup is handled within assembleStream after successful processing,
// so this method is intentionally a no-op.
func (h *ReflectorAssemblyOperationHandler) Cleanup(ctx context.Context, req *models.Request) error {
	return nil
}
