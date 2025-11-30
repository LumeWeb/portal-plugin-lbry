package upload

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"

	"github.com/ipfs/go-cid"
	"github.com/samber/lo"
	"go.lumeweb.com/liblbry/blob"
	lbrycrypto "go.lumeweb.com/liblbry/crypto"
	"go.lumeweb.com/liblbry/stream"
	pluginCore "go.lumeweb.com/portal-plugin-lbry/core"
	"go.lumeweb.com/portal-plugin-lbry/internal"
	"go.lumeweb.com/portal-plugin-lbry/internal/db"
	"go.lumeweb.com/portal-plugin-lbry/internal/protocol/util"
	"go.lumeweb.com/portal/core"
	"go.lumeweb.com/portal/db/models"
	"go.lumeweb.com/queryutil"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// UploadServiceDefault implements the UploadServiceDefault interface for LBRY protocol
type UploadServiceDefault struct {
	ctx        core.Context
	db         *gorm.DB
	storage    core.StorageService
	coreUpload core.UploadService
	corePin    core.PinService
	protocol   core.Protocol
	logger     *core.Logger
}

// NewUploadService creates a new LBRY upload service instance
func NewUploadService() (core.Service, []core.ContextBuilderOption, error) {
	service := &UploadServiceDefault{}

	return service, core.ContextOptions(
		core.ContextWithStartupFunc(func(ctx core.Context) error {
			service.ctx = ctx
			service.db = ctx.DB()
			service.storage = core.GetService[core.StorageService](ctx, core.STORAGE_SERVICE)
			service.coreUpload = core.GetService[core.UploadService](ctx, core.UPLOAD_SERVICE)
			service.corePin = core.GetService[core.PinService](ctx, core.PIN_SERVICE)
			service.protocol = core.GetProtocol(internal.ProtocolName)
			service.logger = ctx.Logger()

			if service.storage == nil {
				return fmt.Errorf("storage service not initialized")
			}
			if service.coreUpload == nil {
				return fmt.Errorf("core upload service not initialized")
			}
			if service.corePin == nil {
				return fmt.Errorf("core pin service not initialized")
			}
			if service.protocol == nil {
				return fmt.Errorf("LBRY protocol not initialized")
			}

			return nil
		}),
	), nil
}

// Name returns the service name
func (s *UploadServiceDefault) Name() string {
	return pluginCore.UPLOAD_SERVICE
}

// ID returns the service ID
func (s *UploadServiceDefault) ID() string {
	return pluginCore.UPLOAD_SERVICE
}

// HandleUpload processes an upload and returns the CID and upload ID
func (s *UploadServiceDefault) HandleUpload(ctx context.Context, reader io.ReadSeekCloser) (cid.Cid, string, error) {
	// Get the size of the reader
	size, err := reader.Seek(0, io.SeekEnd)
	if err != nil {
		return cid.Undef, "", fmt.Errorf("failed to seek to end of reader: %w", err)
	}

	// Reset the reader to the beginning
	_, err = reader.Seek(0, io.SeekStart)
	if err != nil {
		return cid.Undef, "", fmt.Errorf("failed to seek to start of reader: %w", err)
	}

	hasher := lbrycrypto.NewHasher()
	streamHash, err := hasher.HashReader(reader)
	if err != nil {
		return cid.Cid{}, "", fmt.Errorf("failed to hash reader: %w", err)
	}

	// Reset the reader to the beginning after hashing
	_, err = reader.Seek(0, io.SeekStart)
	if err != nil {
		return cid.Cid{}, "", fmt.Errorf("failed to seek to start of reader after hashing: %w", err)
	}

	// Create CID from the stream hash using the internal helper
	streamCid, err := internal.LBRYHashToCID(streamHash)
	if err != nil {
		return cid.Cid{}, "", fmt.Errorf("failed to convert stream hash to CID: %w", err)
	}

	// Cast to storage protocol with type safety
	storageProtocol, err := util.CastToStorageProtocol(s.protocol)
	if err != nil {
		return cid.Undef, "", fmt.Errorf("failed to cast protocol to storage protocol: %w", err)
	}
	if err != nil {
		s.logger.Error("Failed to cast protocol to storage protocol", zap.Error(err))
		return cid.Undef, "", fmt.Errorf("failed to cast protocol to storage protocol: %w", err)
	}

	uploadId, err := s.storage.S3TemporaryUpload(ctx, reader, uint64(size), storageProtocol)
	if err != nil {
		s.logger.Error("Failed to store upload data", zap.Error(err))
		return cid.Undef, "", fmt.Errorf("failed to store upload data: %w", err)
	}
	return streamCid, uploadId, nil
}

// ProcessUpload creates upload records for given CIDs
func (s *UploadServiceDefault) ProcessUpload(ctx context.Context, streamResult *stream.StreamResult, userId uint) error {
	// Add defensive guard to ensure content hashes and chunk sizes match
	if len(streamResult.ContentHashes) != len(streamResult.ChunkSizes) {
		return fmt.Errorf("mismatched content hashes (%d) and chunk sizes (%d)", len(streamResult.ContentHashes), len(streamResult.ChunkSizes))
	}

	for index, c := range streamResult.ContentHashes {
		if c == "" {
			s.logger.Debug("Skipping terminating/empty blob",
				zap.String("sd_blob_hash", streamResult.SDBlobHash))
			continue
		}

		_cid, err := internal.LBRYHashToCID(c)
		if err != nil {
			return err
		}

		// Create upload record for this CID
		uploadMeta := &models.Upload{
			UserID:   userId,
			Protocol: s.protocol.Name(),
			Hash:     _cid.Hash(),
			CIDType:  _cid.Type(),
			Size:     uint64(streamResult.ChunkSizes[index]),
		}

		err = s.coreUpload.SaveUpload(ctx, uploadMeta)
		if err != nil {
			s.logger.Error("Failed to save upload record", zap.Error(err), zap.String("cid", _cid.String()))
			return fmt.Errorf("failed to save upload record for %s: %w", _cid.String(), err)
		}

		// Create core pin record for this CID
		pinMeta := &models.Pin{
			UploadID: uploadMeta.ID,
			UserID:   uploadMeta.UserID,
		}

		_, err = s.corePin.CreatePin(ctx, pinMeta, nil)
		if err != nil {
			return fmt.Errorf("failed to create pin record for CID %s: %w", _cid.String(), err)
		}

		s.logger.Debug("Created upload and pin records",
			zap.String("cid", _cid.String()),
			zap.Uint("userID", userId))
	}

	s.logger.Debug("Successfully processed uploads",
		zap.Uint("userID", userId),
		zap.Int("cidCount", len(streamResult.ContentHashes)),
	)

	return nil
}

// StorePendingBlob stores a regular blob in pending state
func (s *UploadServiceDefault) StorePendingBlob(ctx context.Context, userID, deviceID, streamID uint, blobInfo *stream.BlobInfo) error {
	blobHash, isTerminating, hashErr := s.getBlobHashFromInfo(blobInfo)
	if hashErr != nil {
		return fmt.Errorf("failed to generate terminating blob hash: %w", hashErr)
	}

	pendingBlob := db.PendingBlob{
		BlobHash:    blobHash,
		UserID:      userID,
		DeviceID:    deviceID,
		StreamID:    streamID,
		BlobSize:    int(blobInfo.Length),
		BlobNumber:  blobInfo.BlobNum,
		Received:    true, // Mark as received when storing
		Terminating: isTerminating,
		IVData:      blobInfo.IV,
	}

	// Build dynamic updates and conflict columns using shared helper functions
	received := true
	updates := s.buildPendingBlobUpdates(deviceID, &streamID, blobInfo, &received, &isTerminating)
	conflictColumns := s.buildPendingBlobConflictColumns(isTerminating, &streamID)

	err := s.db.WithContext(ctx).Clauses(clause.OnConflict{
		Columns:   conflictColumns,
		DoUpdates: updates,
	}).Create(&pendingBlob).Error

	if err != nil {
		s.logger.Error("Failed to create pending blob record",
			zap.Uint("user_id", userID),
			zap.String("blob_hash", blobHash),
			zap.Bool("terminating", isTerminating),
			zap.Error(err))
		return fmt.Errorf("failed to create pending blob record: %w", err)
	}

	return nil
}

// MarkPendingBlobAsReceived marks an existing pending blob as received without changing other fields
func (s *UploadServiceDefault) MarkPendingBlobAsReceived(ctx context.Context, userID, deviceID uint, blobInfo *stream.BlobInfo) error {
	blobHash, isTerminating, hashErr := s.getBlobHashFromInfo(blobInfo)
	if hashErr != nil {
		return fmt.Errorf("failed to generate terminating blob hash: %w", hashErr)
	}

	var streamID *uint
	var conflictColumns []clause.Column

	// For terminating blobs, we need to find the existing record to get stream_id and blob_number
	if isTerminating {
		terminatingHash, _, _ := s.getBlobHashFromInfo(&stream.BlobInfo{})
		var existingBlob db.PendingBlob
		err := s.db.WithContext(ctx).Where("user_id = ? AND blob_hash = ?",
			userID, terminatingHash).First(&existingBlob).Error
		if err != nil {
			// If not found, we'll create a new record with stream_id=0 (will be updated later)
			if errors.Is(err, gorm.ErrRecordNotFound) {
				zeroStreamID := uint(0)
				streamID = &zeroStreamID
			} else {
				return fmt.Errorf("failed to find existing terminating blob: %w", err)
			}
		} else {
			streamID = &existingBlob.StreamID
			// Use the existing blob number to ensure consistency
			blobInfo.BlobNum = existingBlob.BlobNumber
		}
	}

	pendingBlob := db.PendingBlob{
		BlobHash:    blobHash,
		UserID:      userID,
		DeviceID:    deviceID,
		BlobSize:    int(blobInfo.Length),
		BlobNumber:  blobInfo.BlobNum,
		Received:    true, // Mark as received when storing
		Terminating: isTerminating,
		IVData:      blobInfo.IV,
	}

	// Build dynamic updates and conflict columns using shared helper functions
	received := true
	updates := s.buildPendingBlobUpdates(deviceID, streamID, blobInfo, &received, &isTerminating)
	conflictColumns = s.buildPendingBlobConflictColumns(isTerminating, streamID)

	err := s.db.WithContext(ctx).Clauses(clause.OnConflict{
		Columns:   conflictColumns,
		DoUpdates: updates,
	}).Create(&pendingBlob).Error

	if err != nil {
		s.logger.Error("Failed to mark pending blob as received",
			zap.Uint("user_id", userID),
			zap.String("blob_hash", blobHash),
			zap.Bool("terminating", isTerminating),
			zap.Error(err))
		return fmt.Errorf("failed to mark pending blob as received: %w", err)
	}

	return nil
}

// StorePendingStream stores an SD blob with full stream metadata in pending state
func (s *UploadServiceDefault) StorePendingStream(ctx context.Context, userID, deviceID uint, sdBlob *stream.SDBlob, sdHash string) (uint, error) {
	// Validate input parameters
	if sdBlob == nil {
		return 0, fmt.Errorf("sdBlob cannot be nil")
	}
	if sdHash == "" {
		return 0, fmt.Errorf("sdHash cannot be empty")
	}

	// Convert stream hash bytes to string
	streamHash := hex.EncodeToString(sdBlob.StreamHash)

	pendingStream := db.PendingStream{
		StreamHash:        streamHash,
		SDHash:            sdHash,
		StreamName:        sdBlob.StreamName,
		StreamType:        sdBlob.StreamType,
		SuggestedFileName: sdBlob.SuggestedFileName,
		KeyData:           sdBlob.Key,
		TotalBlobs:        len(sdBlob.BlobInfos),
		UserID:            userID,
		DeviceID:          deviceID,
	}

	err := s.db.WithContext(ctx).Clauses(clause.OnConflict{
		Columns: []clause.Column{{Name: "user_id"}, {Name: "sd_hash"}},
		DoUpdates: clause.Set{
			{Column: clause.Column{Name: "stream_hash"}, Value: pendingStream.StreamHash},
			{Column: clause.Column{Name: "stream_name"}, Value: pendingStream.StreamName},
			{Column: clause.Column{Name: "stream_type"}, Value: pendingStream.StreamType},
			{Column: clause.Column{Name: "suggested_file_name"}, Value: pendingStream.SuggestedFileName},
			{Column: clause.Column{Name: "key_data"}, Value: pendingStream.KeyData},
			{Column: clause.Column{Name: "total_blobs"}, Value: pendingStream.TotalBlobs},
			{Column: clause.Column{Name: "device_id"}, Value: pendingStream.DeviceID},
			{Column: clause.Column{Name: "user_id"}, Value: pendingStream.UserID},
		},
	}).Create(&pendingStream).Error

	if err != nil {
		s.logger.Error("Failed to create pending stream record",
			zap.Uint("user_id", userID),
			zap.String("sd_hash", sdHash),
			zap.Error(err))
		return 0, fmt.Errorf("failed to create pending stream record: %w", err)
	}

	// Auto-create empty pending records for all child blobs referenced in the SD blob
	if len(sdBlob.BlobInfos) > 0 {
		err = s.createPendingBlobsFromSDBlob(ctx, userID, deviceID, pendingStream.ID, sdBlob.BlobInfos)
		if err != nil {
			s.logger.Error("Failed to create pending blob records from SD blob",
				zap.Uint("user_id", userID),
				zap.String("sd_hash", sdHash),
				zap.Error(err))
			return 0, fmt.Errorf("failed to create pending blob records from SD blob: %w", err)
		}

		s.logger.Debug("SD blob and child pending blobs created successfully",
			zap.Uint("user_id", userID),
			zap.String("sd_hash", sdHash),
			zap.Int("child_blob_count", len(sdBlob.BlobInfos)))
	} else {
		s.logger.Debug("SD blob stored successfully (no child blobs)",
			zap.Uint("user_id", userID),
			zap.String("sd_hash", sdHash))
	}

	return pendingStream.ID, nil
}

// getBlobHashFromInfo extracts the blob hash from BlobInfo, handling terminating blobs
// Returns the hash string, isTerminating flag, and any error that occurred during hash generation
func (s *UploadServiceDefault) getBlobHashFromInfo(blobInfo *stream.BlobInfo) (string, bool, error) {
	// Check if this is a terminating blob (empty hash)
	isTerminating := len(blobInfo.BlobHash) == 0

	if isTerminating {
		hash, err := blob.ComputeBlobHashBytes([]byte(internal.TerminatingBlobHash))
		if err != nil {
			return "", true, fmt.Errorf("failed to compute terminating blob hash: %w", err)
		}
		return hex.EncodeToString(hash), true, nil
	} else {
		// Use actual hash for non-terminating blobs
		return hex.EncodeToString(blobInfo.BlobHash), false, nil
	}
}

// buildPendingBlobConflictColumns builds the appropriate conflict columns based on blob type
func (s *UploadServiceDefault) buildPendingBlobConflictColumns(isTerminating bool, streamID *uint) []clause.Column {
	if isTerminating && streamID != nil && *streamID > 0 {
		// For terminating blobs with valid stream_id, use composite key (user_id, stream_id, blob_number)
		return []clause.Column{
			{Name: "user_id"},
			{Name: "stream_id"},
			{Name: "blob_number"},
		}
	} else {
		// For regular blobs or terminating blobs without stream_id, use (user_id, blob_hash) constraint
		return []clause.Column{{Name: "user_id"}, {Name: "blob_hash"}}
	}
}

// buildPendingBlobUpdates builds dynamic DoUpdates clause for pending blob operations
func (s *UploadServiceDefault) buildPendingBlobUpdates(deviceID uint, streamID *uint, blobInfo *stream.BlobInfo, received *bool, terminating *bool) []clause.Assignment {
	var updates []clause.Assignment

	// Always update device_id
	updates = append(updates, clause.Assignment{Column: clause.Column{Name: "device_id"}, Value: deviceID})

	// Update stream_id only if explicitly provided
	if streamID != nil {
		updates = append(updates, clause.Assignment{Column: clause.Column{Name: "stream_id"}, Value: *streamID})
	}

	// Update received status only if explicitly provided
	if received != nil {
		updates = append(updates, clause.Assignment{Column: clause.Column{Name: "received"}, Value: *received})
	}

	// Update terminating status only if explicitly provided
	if terminating != nil {
		updates = append(updates, clause.Assignment{Column: clause.Column{Name: "terminating"}, Value: *terminating})
	}

	// Always update blob_size and blob_number - these are valid even when zero
	// Length=0 is valid for terminating blobs, BlobNum=0 is valid for first blob
	updates = append(updates, clause.Assignment{Column: clause.Column{Name: "blob_size"}, Value: int(blobInfo.Length)})
	updates = append(updates, clause.Assignment{Column: clause.Column{Name: "blob_number"}, Value: blobInfo.BlobNum})
	if blobInfo.IV != nil {
		updates = append(updates, clause.Assignment{Column: clause.Column{Name: "iv_data"}, Value: blobInfo.IV})
	}

	return updates
}

// createPendingBlobsFromSDBlob creates pending blob records from SD blob child blob information
func (s *UploadServiceDefault) createPendingBlobsFromSDBlob(ctx context.Context, userID, deviceID, streamID uint, blobInfos []stream.BlobInfo) error {
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		for _, blobInfo := range blobInfos {
			blobHash, isTerminating, hashErr := s.getBlobHashFromInfo(&blobInfo)
			if hashErr != nil {
				return fmt.Errorf("failed to generate terminating blob hash: %w", hashErr)
			}

			// Create pending blob record with Received=false to indicate it's waiting for upload
			pendingBlob := db.PendingBlob{
				BlobHash:    blobHash,
				UserID:      userID,
				DeviceID:    deviceID,
				StreamID:    streamID,
				BlobSize:    blobInfo.Length,
				BlobNumber:  blobInfo.BlobNum,
				Received:    false, // Mark as not received yet - waiting for upload
				Terminating: isTerminating,
				IVData:      blobInfo.IV,
			}

			// Build dynamic updates and conflict columns using shared helper functions
			updates := s.buildPendingBlobUpdates(deviceID, &streamID, &blobInfo, nil, &isTerminating)
			conflictColumns := s.buildPendingBlobConflictColumns(isTerminating, &streamID)

			err := tx.Clauses(clause.OnConflict{
				Columns:   conflictColumns,
				DoUpdates: updates,
			}).Create(&pendingBlob).Error

			if err != nil {
				return fmt.Errorf("failed to create pending blob record for %s (terminating: %v): %w", blobHash, isTerminating, err)
			}
		}
		return nil
	})
}

// GetMissingBlobs checks which required blobs are not available
func (s *UploadServiceDefault) GetMissingBlobs(ctx context.Context, userID uint, streamID uint, requiredBlobs []string) ([]string, error) {
	// Filter out empty hashes (terminating blobs) before querying database
	// Terminating blobs are handled separately and should not be considered "missing"
	filteredRequiredBlobs := lo.Filter(requiredBlobs, func(hash string, _ int) bool {
		return hash != ""
	})

	// If no non-empty blobs to check, return empty missing list
	if len(filteredRequiredBlobs) == 0 {
		return []string{}, nil
	}

	var availableBlobs []string

	// Check regular pending blobs for this specific stream only
	err := s.db.WithContext(ctx).Model(&db.PendingBlob{}).
		Where("user_id = ? AND stream_id = ? AND blob_hash IN ?", userID, streamID, filteredRequiredBlobs).
		Pluck("blob_hash", &availableBlobs).Error

	if err != nil {
		return nil, err
	}

	// Create set of available blobs
	availableSet := make(map[string]bool)
	for _, hash := range availableBlobs {
		availableSet[hash] = true
	}

	missing := lo.Filter(filteredRequiredBlobs, func(hash string, _ int) bool {
		return !availableSet[hash]
	})

	return missing, nil
}

// GetPendingBlobCount returns the count of pending blobs for a stream
func (s *UploadServiceDefault) GetPendingBlobCount(ctx context.Context, userID uint, streamID uint) (int64, error) {
	var count int64
	err := s.db.WithContext(ctx).
		Model(&db.PendingBlob{}).
		Where("user_id = ? AND stream_id = ?", userID, streamID).
		Count(&count).Error

	if err != nil {
		return 0, fmt.Errorf("failed to count pending blobs: %w", err)
	}

	return count, nil
}

// CleanupPendingBlobs removes pending blob records after successful assembly
func (s *UploadServiceDefault) CleanupPendingBlobs(ctx context.Context, userID uint, streamResult *stream.StreamResult) error {
	// First, find the pending stream by SD hash and user ID to get the stream ID
	var pendingStream db.PendingStream
	findErr := s.db.WithContext(ctx).
		Where("user_id = ? AND sd_hash = ?", userID, streamResult.SDBlobHash).
		First(&pendingStream).Error
	if findErr != nil {
		// If the pending stream is not found, continue with cleanup (no error)
		if errors.Is(findErr, gorm.ErrRecordNotFound) {
			// Continue with cleanup of pending streams by SD hash
		} else {
			// Return error for other database issues
			return fmt.Errorf("failed to find pending stream: %w", findErr)
		}
	}

	// Clean up regular pending blobs associated with this specific stream
	// Only do this if we found a pending stream
	if findErr == nil {
		// If ContentBlobs is empty, don't delete any pending blobs
		if len(streamResult.ContentBlobs) > 0 {
			// Convert ContentBlobs to hex strings for comparison
			contentBlobHashes := make([]string, len(streamResult.ContentBlobs))
			for i, contentBlob := range streamResult.ContentBlobs {
				contentBlobHashes[i] = hex.EncodeToString(contentBlob)
			}

			err := s.db.WithContext(ctx).
				Where("user_id = ? AND stream_id = ? AND blob_hash IN ?", userID, pendingStream.ID, contentBlobHashes).
				Delete(&db.PendingBlob{}).Error
			if err != nil {
				return fmt.Errorf("failed to cleanup pending blobs: %w", err)
			}
		}
	}

	// Clean up pending stream (SD blob) by SD hash
	// This will succeed even if no record exists (no-op)
	err := s.db.WithContext(ctx).
		Where("user_id = ? AND sd_hash = ?", userID, streamResult.SDBlobHash).
		Delete(&db.PendingStream{}).Error
	if err != nil {
		return fmt.Errorf("failed to cleanup pending stream: %w", err)
	}

	return nil
}

// CreateStreamPin creates an LBRY stream pin record
func (s *UploadServiceDefault) CreateStreamPin(ctx context.Context, userId uint, sdCid cid.Cid) (*db.StreamPin, error) {
	// Convert CID to stream hash string
	sdHash := sdCid.String()
	lbryHash, err := stream.FromMultihash(sdHash)
	if err != nil {
		return nil, fmt.Errorf("failed to convert stream CID to LBRY hash: %w", err)
	}

	// Find the stream
	var _stream db.Stream
	if err = s.db.WithContext(ctx).First(&_stream, db.Stream{SDHash: lbryHash}).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, fmt.Errorf("stream not found: %s", sdHash)
		}
		return nil, fmt.Errorf("failed to find stream %s: %w", sdHash, err)
	}

	// Check if pin already exists (idempotence check)
	var existingPin db.StreamPin
	err = s.db.WithContext(ctx).
		Where("user_id = ? AND stream_id = ?", userId, _stream.ID).
		First(&existingPin).Error

	if err == nil {
		// Pin already exists, return existing pin (idempotent operation)
		s.logger.Debug("Stream pin already exists, returning existing pin",
			zap.Uint("userID", userId),
			zap.Uint("streamID", _stream.ID),
			zap.String("sdHash", sdHash))
		return &existingPin, nil
	} else if !errors.Is(err, gorm.ErrRecordNotFound) {
		// Some other error occurred
		return nil, fmt.Errorf("failed to check existing stream pin: %w", err)
	}

	// Create a new StreamPin record
	streamPin := &db.StreamPin{
		UserID:   uint64(userId),
		StreamID: uint64(_stream.ID),
	}

	// Save the StreamPin to the database
	if err = s.db.WithContext(ctx).Create(streamPin).Error; err != nil {
		return nil, fmt.Errorf("failed to create stream pin: %w", err)
	}

	s.logger.Debug("Created new stream pin",
		zap.Uint("userID", userId),
		zap.Uint("streamID", _stream.ID),
		zap.String("sdHash", sdHash))

	return streamPin, nil
}

// ListStreams returns a paginated list of streams for a user
func (s *UploadServiceDefault) ListStreams(ctx context.Context, userID uint, filters []queryutil.CrudFilter, sorts []queryutil.Sort, pagination queryutil.Pagination) ([]*db.Stream, int64, error) {
	var streams []*db.Stream
	var total int64

	// Build the base query with user filter using GORM OOP
	// Join with StreamPin to filter streams that belong to the user
	query := s.db.WithContext(ctx).
		Model(&db.Stream{}).
		Joins("INNER JOIN lbry_stream_pins ON lbry_streams.id = lbry_stream_pins.stream_id").
		Where("lbry_stream_pins.user_id = ?", userID).
		Preload("StreamPin", "lbry_stream_pins.user_id = ?", userID)

	// Apply filters using queryutil helper
	query = queryutil.ApplyFilters(query, filters, nil)

	// Count total records
	countQuery := query
	if err := countQuery.Count(&total).Error; err != nil {
		return nil, 0, fmt.Errorf("failed to count streams: %w", err)
	}

	// Apply sorting using queryutil helper
	query = queryutil.ApplySort(query, sorts)

	// Apply pagination using queryutil helper
	query = queryutil.ApplyPagination(query, pagination)

	// Execute query
	if err := query.Find(&streams).Error; err != nil {
		return nil, 0, fmt.Errorf("failed to fetch streams: %w", err)
	}

	return streams, total, nil
}

// GetPendingStream retrieves pending stream metadata by user ID and SD hash
func (s *UploadServiceDefault) GetPendingStream(ctx context.Context, userID uint, sdHash string) (*db.PendingStream, error) {
	var pendingStream db.PendingStream
	err := s.db.WithContext(ctx).Where("user_id = ? AND sd_hash = ?", userID, sdHash).First(&pendingStream).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, fmt.Errorf("pending stream not found for user %d and SD hash %s", userID, sdHash)
		}
		return nil, fmt.Errorf("failed to get pending stream: %w", err)
	}
	return &pendingStream, nil
}

// GetPendingBlobs retrieves pending blobs for a given SD hash
func (s *UploadServiceDefault) GetPendingBlobs(ctx context.Context, userID uint, sdHash string) ([]*db.PendingBlob, error) {
	// First get the pending stream to find its ID
	var pendingStream db.PendingStream
	err := s.db.WithContext(ctx).Where("user_id = ? AND sd_hash = ?", userID, sdHash).First(&pendingStream).Error
	if err != nil {
		return nil, fmt.Errorf("failed to get pending stream for user %d and SD hash %s: %w", userID, sdHash, err)
	}

	// Now get pending blobs for this specific stream
	var pendingBlobs []*db.PendingBlob
	err = s.db.WithContext(ctx).Where("user_id = ? AND stream_id = ?", userID, pendingStream.ID).Order("blob_number").Find(&pendingBlobs).Error
	if err != nil {
		return nil, fmt.Errorf("failed to get pending blobs for user %d and SD hash %s: %w", userID, sdHash, err)
	}
	return pendingBlobs, nil
}

// DeleteStream removes only the user's stream pin by SD hash
func (s *UploadServiceDefault) DeleteStream(ctx context.Context, userID uint, sdHash string) error {
	if userID == 0 {
		return gorm.ErrRecordNotFound
	}

	// Find the stream by SD hash
	var _stream db.Stream
	err := s.db.WithContext(ctx).Where("sd_hash = ?", sdHash).First(&_stream).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return gorm.ErrRecordNotFound
		}
		return fmt.Errorf("failed to find stream: %w", err)
	}

	// Check if the stream pin exists and belongs to the user
	streamPinQuery := db.StreamPin{
		UserID:   uint64(userID),
		StreamID: uint64(_stream.ID),
	}
	var streamPin db.StreamPin
	err = s.db.WithContext(ctx).
		Where(&streamPinQuery).
		First(&streamPin).Error

	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return gorm.ErrRecordNotFound
		}
		return fmt.Errorf("failed to check stream ownership: %w", err)
	}

	// Delete only the stream pin record
	if err := s.db.WithContext(ctx).
		Where(&streamPinQuery).
		Delete(&db.StreamPin{}).Error; err != nil {
		return fmt.Errorf("failed to delete stream pin: %w", err)
	}

	s.logger.Debug("Deleted stream pin",
		zap.Uint("userID", userID),
		zap.Uint("streamID", _stream.ID),
		zap.String("sdHash", sdHash))

	return nil
}
