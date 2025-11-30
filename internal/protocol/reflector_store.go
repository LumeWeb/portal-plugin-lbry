package protocol

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"

	"go.lumeweb.com/liblbry/protocol"
	lbrystream "go.lumeweb.com/liblbry/stream"
	pluginCore "go.lumeweb.com/portal-plugin-lbry/core"
	"go.lumeweb.com/portal-plugin-lbry/internal"
	"go.lumeweb.com/portal/core"
	"go.uber.org/zap"
)

// userIDContextKey is a private context key for storing user ID
type userIDContextKey struct{}

const REFLECTOR_STORE_NAME = "reflector"

// ReflectorStore implements a minimal blob store for pending blobs during upload workflow
// It delegates to the upload service API and uses S3 temporary storage
type ReflectorStore struct {
	logger      *core.Logger
	storageSvc  core.StorageService
	proto       core.StorageProtocol
	deviceSvc   pluginCore.DeviceService
	uploadSvc   pluginCore.UploadService
	workflowSvc core.WorkflowService
}

func (rs *ReflectorStore) PutSD(ctx context.Context, hash string, data []byte) error {
	// Extract user ID from context
	userID := rs.extractUserIDFromContext(ctx)
	if userID == 0 {
		return fmt.Errorf("user ID not found in context for ReflectorStore PutSD operation")
	}

	// Validate SD blob using the dedicated validation function
	if err := lbrystream.ValidateSDBlob(data); err != nil {
		return fmt.Errorf("invalid SD blob %q: %w", hash, err)
	}

	// Parse the SD blob data after validation
	sdBlob := lbrystream.SDBlob{}
	if err := sdBlob.FromBlob(data); err != nil {
		return fmt.Errorf("failed to parse SD blob %q: %w", hash, err)
	}

	// Check if pending stream already exists to avoid creating multiple workflow instances
	existingPendingStream, err := rs.uploadSvc.GetPendingStream(ctx, userID, hash)
	if err == nil && existingPendingStream != nil {
		rs.logger.Debug("Pending stream already exists, skipping workflow creation",
			zap.String("sd_hash", hash),
			zap.Uint("user_id", userID),
			zap.Uint("existing_pending_stream_id", existingPendingStream.ID))
		return nil
	}

	// Get device ID from context (needed for StorePendingStream)
	deviceID, err := rs.extractDeviceIDFromContext(ctx)
	if err != nil {
		return fmt.Errorf("failed to extract device ID from context: %w", err)
	}

	// Store the SD blob metadata as a pending stream using upload service
	// Note: SD blobs go to SQL database only, not to temporary storage like regular blobs
	streamID, err := rs.uploadSvc.StorePendingStream(ctx, userID, deviceID, &sdBlob, hash)
	if err != nil {
		return fmt.Errorf("failed to store pending stream for SD blob %q: %w", hash, err)
	}

	rs.logger.Debug("Successfully stored SD blob as pending stream",
		zap.String("sd_hash", hash),
		zap.Uint("user_id", userID),
		zap.Uint("device_id", deviceID),
		zap.Uint("stream_id", streamID),
		zap.String("stream_name", sdBlob.StreamName),
		zap.String("stream_hash", hex.EncodeToString(sdBlob.StreamHash)),
		zap.Int("blob_count", len(sdBlob.BlobInfos)))

	// Start reflector assembly workflow for background processing
	workflowOptions := []core.WorkflowOption{
		// Pass the SD hash as workflow data for tracking
		core.WithWorkflowStructData(&ReflectorAssemblyWorkflowData{
			SDBlobHash: hash,
			Progress:   0,
		}, "json"),
		// Associate the workflow with the authenticated user
		core.WithWorkflowUserID(userID),
		// Specify the protocol (LBRY) for protocol-specific processing
		core.WithWorkflowProtocol(internal.ProtocolName),
	}

	// Convert SD hash to storage hash for content addressing
	storageHash, err := internal.LBRYHashToStorageHash(hash)
	if err != nil {
		rs.logger.Warn("Failed to convert SD hash to storage hash", zap.String("sd_hash", hash), zap.Error(err))
		// Continue without storage hash - workflow can still proceed
	} else {
		// Add storage hash option if conversion succeeded
		workflowOptions = append(workflowOptions, core.WithWorkflowStorageHash(storageHash))
	}

	_, err = rs.workflowSvc.StartWorkflow(ctx, REFLECTOR_ASSEMBLY_WORKFLOW, workflowOptions...)
	if err != nil {
		rs.logger.Error("Failed to start reflector assembly workflow",
			zap.String("sd_hash", hash),
			zap.Uint("user_id", userID),
			zap.Error(err))
		return fmt.Errorf("failed to start reflector assembly workflow: %w", err)
	} else {
		rs.logger.Debug("Successfully started reflector assembly workflow",
			zap.String("sd_hash", hash),
			zap.Uint("user_id", userID))
	}

	return nil
}

// NewReflectorStore creates a new instance of ReflectorStore
func NewReflectorStore(ctx core.Context) (*ReflectorStore, error) {
	proto, err := GetStorageProtocol()
	if err != nil {
		return nil, err
	}

	storageSvc := core.GetService[core.StorageService](ctx, core.STORAGE_SERVICE)
	if storageSvc == nil {
		return nil, fmt.Errorf("storage service not initialized")
	}

	deviceSvc := core.GetService[pluginCore.DeviceService](ctx, pluginCore.DEVICE_SERVICE)
	if deviceSvc == nil {
		return nil, fmt.Errorf("device service not initialized")
	}

	uploadSvc := core.GetService[pluginCore.UploadService](ctx, pluginCore.UPLOAD_SERVICE)
	if uploadSvc == nil {
		return nil, fmt.Errorf("upload service not initialized")
	}

	workflowSvc := core.GetService[core.WorkflowService](ctx, core.WORKFLOW_SERVICE)
	if workflowSvc == nil {
		return nil, fmt.Errorf("workflow service not initialized")
	}

	return &ReflectorStore{
		logger:      ctx.Logger(),
		storageSvc:  storageSvc,
		proto:       proto,
		deviceSvc:   deviceSvc,
		uploadSvc:   uploadSvc,
		workflowSvc: workflowSvc,
	}, nil
}

// getReflectorBlobPath generates the blob path in the format "{userID}/{blobHash}"
// Based on getBlobPath from op_reflector_assembly.go
func getReflectorBlobPath(userID uint, blobHash string) string {
	return fmt.Sprintf("%d/%s", userID, blobHash)
}

// Has checks if a blob with the given hash exists in temporary storage
func (rs *ReflectorStore) Has(ctx context.Context, hash string) (bool, error) {
	userID := rs.extractUserIDFromContext(ctx)
	if userID == 0 {
		return false, nil
	}

	// Check existence without reading full data
	exists, err := rs.storageSvc.S3TemporaryUploadExists(ctx, rs.proto, getReflectorBlobPath(userID, hash))
	if err != nil {
		// If there's an error checking existence, assume it doesn't exist
		return false, nil
	}

	return exists, nil
}

// isTerminatingBlob checks if a blob is a terminating blob (hash is empty)
// A terminating blob is the last blob in a stream with empty hash
func (rs *ReflectorStore) isTerminatingBlob(userID uint, hash string) bool {
	// If hash is empty, it's a terminating blob
	isTerminating := hash == ""

	if isTerminating {
		rs.logger.Debug("Identified terminating blob",
			zap.String("blob_hash", hash),
			zap.Uint("user_id", userID))
	}

	return isTerminating
}

// Get retrieves a blob by its hash from temporary storage
func (rs *ReflectorStore) Get(ctx context.Context, hash string) ([]byte, error) {
	// For ReflectorStore, we need to determine the user ID and blob path
	// Since this is used in upload workflow, we'll extract user ID from context

	// Extract user ID from context if available
	userID := rs.extractUserIDFromContext(ctx)
	if userID == 0 {
		return nil, fmt.Errorf("user ID not found in context for ReflectorStore Get operation")
	}

	// Check if this is a terminating blob (hash and size are 0)
	isTerminating := rs.isTerminatingBlob(userID, hash)

	if isTerminating {
		rs.logger.Debug("Building terminating blob",
			zap.String("blob_hash", hash),
			zap.Uint("user_id", userID))
		// Return empty byte array for terminating blob
		return []byte{}, nil
	}

	// Get blob from temporary storage
	reader, err := rs.storageSvc.S3GetTemporaryUpload(ctx, rs.proto, getReflectorBlobPath(userID, hash))
	if err != nil {
		return nil, fmt.Errorf("failed to get blob %q from temporary storage: %w", hash, err)
	}
	defer func() {
		if closer, ok := reader.(io.Closer); ok {
			_ = closer.Close()
		}
	}()

	// Read all data from the reader
	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to read blob data for blob %q: %w", hash, err)
	}

	return data, nil
}

// Put stores a blob with the given hash and data to temporary storage
func (rs *ReflectorStore) Put(ctx context.Context, hash string, data []byte) error {
	// Extract user ID from context if available
	userID := rs.extractUserIDFromContext(ctx)
	if userID == 0 {
		return fmt.Errorf("user ID not found in context for ReflectorStore Put operation")
	}

	// Check if this is a terminating blob (empty hash)
	isTerminating := rs.isTerminatingBlob(userID, hash)

	if isTerminating {
		rs.logger.Debug("Skipping storage for terminating blob, but marking as received",
			zap.String("blob_hash", hash),
			zap.Uint("user_id", userID))

		// For terminating blobs, skip storage but still mark as received
		err := rs.markBlobAsReceived(ctx, userID, hash, int64(len(data)))
		if err != nil {
			rs.logger.Warn("Failed to mark terminating blob as received in pending records",
				zap.String("blob_hash", hash),
				zap.Uint("user_id", userID),
				zap.Error(err))
			// Don't fail the operation - just the tracking failed
			return nil
		}

		return nil
	}

	// Store the blob data using the storage service's temporary upload
	uploadID, err := rs.storageSvc.S3TemporaryUpload(ctx, internal.NewReadSeekCloser(data), uint64(len(data)), rs.proto, core.WithS3TempUploadID(getReflectorBlobPath(userID, hash)))
	if err != nil {
		return fmt.Errorf("failed to store blob %q in temporary storage: %w", hash, err)
	}

	rs.logger.Debug("Successfully stored blob in temporary storage",
		zap.String("blob_hash", hash),
		zap.String("upload_id", uploadID),
		zap.Uint("user_id", userID),
		zap.Int("blob_size", len(data)))

	// Update pending blob record to mark as received
	err = rs.markBlobAsReceived(ctx, userID, hash, int64(len(data)))
	if err != nil {
		rs.logger.Warn("Failed to mark blob as received in pending records",
			zap.String("blob_hash", hash),
			zap.Uint("user_id", userID),
			zap.Error(err))
		// Don't fail the upload operation - the blob is stored, just the tracking failed
		return nil
	}

	return nil
}

// List returns a paginated list of blob hashes - no-op for ReflectorStore
// since we don't need listing functionality for upload workflow
func (rs *ReflectorStore) List(_ context.Context, _, _ int) ([]string, error) {
	// ReflectorStore doesn't support listing since it's focused on pending blobs
	// during upload workflow, not general blob management
	rs.logger.Debug("List called on ReflectorStore - returning empty result (no-op)")
	return []string{}, nil
}

// Delete removes a blob by its hash from temporary storage
func (rs *ReflectorStore) Delete(ctx context.Context, hash string) error {
	// Extract user ID from context if available
	userID := rs.extractUserIDFromContext(ctx)
	if userID == 0 {
		return fmt.Errorf("user ID not found in context for ReflectorStore Delete operation")
	}

	// Delete the blob from temporary storage
	err := rs.storageSvc.S3DeleteTemporaryUpload(ctx, rs.proto, getReflectorBlobPath(userID, hash))
	if err != nil {
		return fmt.Errorf("failed to delete blob %q from temporary storage: %w", hash, err)
	}

	rs.logger.Debug("Successfully deleted blob from temporary storage",
		zap.String("blob_hash", hash),
		zap.Uint("user_id", userID))

	return nil
}

// Name returns the name of this blob store
func (rs *ReflectorStore) Name() string {
	return REFLECTOR_STORE_NAME
}

// extractUserIDFromContext extracts user ID from context
// First checks if UserID is directly available in context (shortcut),
// otherwise falls back to looking up device by IP address
func (rs *ReflectorStore) extractUserIDFromContext(ctx context.Context) uint {
	// First check if UserID is directly available in context (shortcut)
	if userID, ok := ctx.Value(userIDContextKey{}).(uint); ok && userID > 0 {
		rs.logger.Debug("Using UserID from context (bypassing IP lookup)",
			zap.Uint("user_id", userID))
		return userID
	}

	// Extract IP address and source from context using liblbry helpers
	ipAddress, hasIP := protocol.GetIPAddressFromContext(ctx)
	source, hasSource := protocol.GetSourceFromContext(ctx)

	// Ensure this is a reflector request
	if !hasSource || source != protocol.SourceReflector {
		rs.logger.Warn("Request source is not reflector",
			zap.String("source", string(source)),
			zap.String("ip_address", ipAddress))
		return 0
	}

	// Must have IP address to identify device
	if !hasIP {
		rs.logger.Warn("No IP address found in context")
		return 0
	}

	// Look up device by IP address to get user ID
	device, err := rs.deviceSvc.GetDeviceByIPAddress(ctx, ipAddress)
	if err != nil {
		rs.logger.Error("Failed to look up device by IP address",
			zap.String("ip_address", ipAddress),
			zap.Error(err))
		return 0
	}

	// Explicit nil check to prevent pointer dereference
	if device == nil {
		rs.logger.Warn("Device lookup returned nil device",
			zap.String("ip_address", ipAddress))
		return 0
	}

	rs.logger.Debug("Successfully looked up user by device IP",
		zap.String("ip_address", ipAddress),
		zap.Uint("device_id", device.ID),
		zap.Uint("user_id", device.UserID))

	return device.UserID
}

// extractDeviceIDFromContext extracts device ID from context by looking up device by IP
func (rs *ReflectorStore) extractDeviceIDFromContext(ctx context.Context) (uint, error) {
	// Extract IP address and source from context using liblbry helpers
	ipAddress, hasIP := protocol.GetIPAddressFromContext(ctx)
	source, hasSource := protocol.GetSourceFromContext(ctx)

	// Ensure this is a reflector request
	if !hasSource || source != protocol.SourceReflector {
		return 0, fmt.Errorf("request source is not reflector")
	}

	// Must have IP address to identify device
	if !hasIP {
		return 0, fmt.Errorf("no IP address found in context")
	}

	// Look up device by IP address to get device ID
	device, err := rs.deviceSvc.GetDeviceByIPAddress(ctx, ipAddress)
	if err != nil {
		return 0, fmt.Errorf("failed to look up device by IP address %q: %w", ipAddress, err)
	}

	// Explicit nil check to prevent pointer dereference
	if device == nil {
		return 0, fmt.Errorf("device not found for IP %q", ipAddress)
	}

	return device.ID, nil
}

// markBlobAsReceived updates the pending blob record to mark it as received
// This handles race conditions by only setting Received=true and never setting it back to false
func (rs *ReflectorStore) markBlobAsReceived(ctx context.Context, userID uint, blobHash string, blobSize int64) error {
	// Get device ID for the update
	deviceID, err := rs.extractDeviceIDFromContext(ctx)
	if err != nil {
		// If we can't get device ID, we can still update the received status
		deviceID = 0
		rs.logger.Debug("Could not extract device ID for pending blob update, using placeholder",
			zap.Uint("user_id", userID),
			zap.String("blob_hash", blobHash),
			zap.Error(err))
	}

	// Create blob info for the update - we need to decode the hash first
	var blobHashBytes []byte
	if blobHash != "" {
		blobHashBytes, err = hex.DecodeString(blobHash)
		if err != nil {
			return fmt.Errorf("failed to decode blob hash %q: %w", blobHash, err)
		}
	}

	blobInfo := &lbrystream.BlobInfo{
		BlobHash: blobHashBytes,
		Length:   int(blobSize),
		BlobNum:  0,   // We don't know the blob number at this point, will be preserved if exists
		IV:       nil, // IV data will be preserved if exists
	}

	// Use upload service to mark as received with race-safe upsert
	err = rs.uploadSvc.MarkPendingBlobAsReceived(ctx, userID, deviceID, blobInfo)
	if err != nil {
		return fmt.Errorf("failed to mark blob %q as received: %w", blobHash, err)
	}

	rs.logger.Debug("Successfully marked blob as received",
		zap.String("blob_hash", blobHash),
		zap.Uint("user_id", userID),
		zap.Uint("device_id", deviceID))

	return nil
}
