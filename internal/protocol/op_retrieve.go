package protocol

import (
	"context"
	"fmt"

	"github.com/ipfs/go-cid"
	"go.lumeweb.com/liblbry/server"
	"go.lumeweb.com/liblbry/stream"
	"go.lumeweb.com/portal-plugin-lbry/internal"
	"go.lumeweb.com/portal/core"
	"go.lumeweb.com/portal/db/models"
	"go.uber.org/zap"
)

// Retrieve operation step constants
const (
	retrieveStepAcquireSDBlob = "acquire_sd_blob"
	retrieveStepStoreSDBlob   = "store_sd_blob"
	retrieveStepProcessStream = "process_stream"
)

// RetrieveOperationHandler handles fetching content from the LBRY network
type RetrieveOperationHandler struct {
	core.OperationHelper
}

func (h *RetrieveOperationHandler) ValidateRequest(_ context.Context, req *models.Request) error {
	return ValidateRequest(req)
}

func (h *RetrieveOperationHandler) Execute(ctx context.Context, req *models.Request) error {
	// Initialize progress tracker with weighted steps
	tracker, err := h.NewProgressTracker(req.ID, core.ProgressModeWeighted, func(cfg *core.ProgressTrackerConfig) {
		cfg.Steps = []core.ProgressStep{
			{
				Name:        retrieveStepAcquireSDBlob,
				Description: "Acquiring SD blob from LBRY network",
				Weight:      30,
			},
			{
				Name:        retrieveStepStoreSDBlob,
				Description: "Storing SD blob locally",
				Weight:      40,
			},
			{
				Name:        retrieveStepProcessStream,
				Description: "Processing stream result",
				Weight:      30,
			},
		}
		cfg.MessageProvider = h.NewDefaultProgressMessageProvider(core.OpTypeRetrieve)
	})
	if err != nil {
		return fmt.Errorf("failed to initialize progress tracker: %w", err)
	}

	if err = tracker.Initialize(); err != nil {
		return fmt.Errorf("failed to initialize tracker: %w", err)
	}

	helper := core.NewProgressTrackerHelper(tracker, h.Context())

	// Request validation is handled by ValidateRequest method

	// Get protocol and node using shared utility
	_, proto, err := GetProtocolWithValidation()
	if err != nil {
		return err
	}

	// Create CID and convert to LBRY hash
	cidObj := cid.NewCidV1(cid.Raw, req.Hash)
	lbryHash, err := stream.FromMultihash(cidObj.String())
	if err != nil {
		return fmt.Errorf("failed to convert CID %q to LBRY hash: %w", cidObj.String(), err)
	}

	h.Logger().Debug("Retrieving LBRY stream",
		zap.Stringer("sd_hash", req.Hash))

	// Cast to BlobManager with safety check
	blobManager, ok := proto.(server.BlobManager)
	if !ok {
		return fmt.Errorf("protocol node does not implement server.BlobManager interface")
	}

	var blob *stream.StreamResult

	// Step 1: Acquire SD blob
	if err = helper.RunStep(retrieveStepAcquireSDBlob, 100, func() error {
		var acquireErr error
		blob, acquireErr = blobManager.AcquireSDBlob(ctx, lbryHash)
		if acquireErr != nil {
			return fmt.Errorf("failed to acquire SD blob for hash %q: %w", lbryHash, acquireErr)
		}

		// Log successful acquisition for debugging
		h.Logger().Debug("Successfully acquired SD blob",
			zap.String("lbry_hash", lbryHash),
			zap.String("sd_blob_hash", blob.SDBlobHash),
			zap.String("stream_hash", blob.StreamHash),
			zap.Int("total_chunks", blob.TotalChunks))

		h.Logger().Debug("Created stream result from SD blob",
			zap.String("sd_hash", lbryHash),
			zap.Int("content_hashes_count", len(blob.ContentHashes)),
			zap.Int("chunk_sizes_count", len(blob.ChunkSizes)))
		return nil
	}); err != nil {
		return err
	}

	// Step 2: Store the SD blob in the local blob store to create the stream record
	if err = helper.RunStep(retrieveStepStoreSDBlob, 100, func() error {
		sdBlobBytes, convErr := blob.SDBlob.ToBlob()
		if convErr != nil {
			return fmt.Errorf("failed to convert SDBlob to blob: %w", convErr)
		}

		err := blobManager.AddSDBlob(ctx, blob.SDBlobHash, sdBlobBytes)
		if err != nil {
			return fmt.Errorf("failed to add SDBlob to blob manager: %w", err)
		}

		h.Logger().Debug("Successfully stored SD blob in local blob store",
			zap.String("sd_hash", blob.SDBlobHash))
		return nil
	}); err != nil {
		return err
	}

	// Step 3: Process the stream result using shared utility
	if err = helper.RunStep(retrieveStepProcessStream, 100, func() error {
		err := ProcessStreamResult(ctx, h.Context(), blob, *req.UserID)
		if err != nil {
			return err
		}

		h.Logger().Info("Successfully processed retrieved stream",
			zap.String("sd_hash", lbryHash),
			zap.Uint("user_id", *req.UserID),
			zap.Int("processed_blobs", len(blob.ContentHashes)))
		return nil
	}); err != nil {
		return err
	}

	return nil
}

func (h *RetrieveOperationHandler) GetStatus(_ context.Context, req *models.Request) (*core.RequestStatus, error) {
	return h.GetStatusFromWorkflowData(req.ID, req)
}

func (h *RetrieveOperationHandler) Cleanup(_ context.Context, _ *models.Request) error {
	// No cleanup needed for retrieve operation
	return nil
}

func NewRetrieveOperation(ctx core.Context) core.Operation {
	return core.NewRetrieveOperation(internal.ProtocolName, &RetrieveOperationHandler{
		OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
	})
}
