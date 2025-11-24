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

// RetrieveOperationHandler handles fetching content from the LBRY network
type RetrieveOperationHandler struct {
	core.OperationHelper
}

func (h *RetrieveOperationHandler) ValidateRequest(_ context.Context, req *models.Request) error {
	return ValidateRequest(req)
}

func (h *RetrieveOperationHandler) Execute(ctx context.Context, req *models.Request) error {
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

	// Acquire SD blob
	blob, err := blobManager.AcquireSDBlob(ctx, lbryHash)
	if err != nil {
		return fmt.Errorf("failed to acquire SD blob for hash %q: %w", lbryHash, err)
	}

	// Log successful acquisition for debugging
	h.Logger().Debug("Successfully acquired SD blob",
		zap.String("lbry_hash", lbryHash),
		zap.Any("blob", blob))

	h.Logger().Debug("Created stream result from SD blob",
		zap.String("sd_hash", lbryHash),
		zap.Int("content_hashes_count", len(blob.ContentHashes)),
		zap.Int("chunk_sizes_count", len(blob.ChunkSizes)))

	// Store the SD blob in the local blob store to create the stream record
	sdBlobBytes, err := blob.SDBlob.ToBlob()
	if err != nil {
		return fmt.Errorf("failed to convert SDBlob to blob: %w", err)
	}

	err = blobManager.AddSDBlob(blob.SDBlobHash, sdBlobBytes)
	if err != nil {
		return fmt.Errorf("failed to add SDBlob to blob manager: %w", err)
	}

	h.Logger().Debug("Successfully stored SD blob in local blob store",
		zap.String("sd_hash", blob.SDBlobHash))

	// Process the stream result using shared utility
	err = ProcessStreamResult(ctx, h.Context(), blob, blob.SDBlobHash, *req.UserID)
	if err != nil {
		return err
	}

	h.Logger().Info("Successfully processed retrieved stream",
		zap.String("sd_hash", lbryHash),
		zap.Uint("user_id", *req.UserID),
		zap.Int("processed_blobs", len(blob.ContentHashes)))

	return nil
}

func (h *RetrieveOperationHandler) GetStatus(_ context.Context, req *models.Request) (*core.RequestStatus, error) {
	// For now just return a simple status since retrieval is synchronous
	status := &core.RequestStatus{
		ProgressPercent: 100,
	}

	if req.Status == models.RequestStatusCompleted {
		status.Message = "Content retrieved from LBRY network"
		status.ProgressPercent = 100
	}

	return status, nil
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
