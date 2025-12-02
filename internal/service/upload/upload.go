package upload

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/ipfs/go-cid"
	"github.com/samber/lo"
	lbrycrypto "go.lumeweb.com/liblbry/crypto"
	"go.lumeweb.com/liblbry/stream"
	pluginCore "go.lumeweb.com/portal-plugin-lbry/core"
	"go.lumeweb.com/portal-plugin-lbry/internal"
	pluginDb "go.lumeweb.com/portal-plugin-lbry/internal/db"
	"go.lumeweb.com/portal-plugin-lbry/internal/protocol/util"
	"go.lumeweb.com/portal/core"
	"go.lumeweb.com/portal/db"
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

	pendingBlob := pluginDb.PendingBlob{
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
	updates := s.buildPendingBlobUpdates(deviceID, &streamID, blobInfo, &received, &isTerminating, true, true)
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

	// Wrap the entire operation in a database transaction with retry logic to handle lock errors
	return db.RetryableTransaction(s.ctx, s.db, func(tx *gorm.DB) *gorm.DB {
		// Helper function to update existing records using the unique key (user_id, stream_id, blob_number)
		// For StreamID=0, we use a different WHERE clause to handle blobs without stream association
		updateExisting := func(hash string, blobNumber int, streamID uint, updates map[string]any) error {
			var result *gorm.DB
			if streamID == 0 {
				// For blobs without stream association, match on user_id, blob_hash, and blob_number only
				result = tx.Model(&pluginDb.PendingBlob{}).
					Where("user_id = ? AND blob_hash = ? AND blob_number = ? AND stream_id = 0", userID, hash, blobNumber).
					Updates(updates)
			} else {
				// For blobs with stream association, use the full unique key
				result = tx.Model(&pluginDb.PendingBlob{}).
					Where("user_id = ? AND stream_id = ? AND blob_number = ? AND blob_hash = ?", userID, streamID, blobNumber, hash).
					Updates(updates)
			}

			if result.Error != nil {
				return fmt.Errorf("failed to update pending blob: %w", result.Error)
			}
			if result.RowsAffected == 0 {
				return fmt.Errorf("no pending blob matched for update")
			}
			return nil
		}

		// Helper function to create new record with fallback to update
		// This handles race conditions where multiple requests try to create the same record
		createWithFallback := func(pendingBlob pluginDb.PendingBlob, updates map[string]any) error {
			err := tx.Create(&pendingBlob).Error
			if err != nil {
				// Check if this is a duplicate key/conflict error
				if isDuplicateKeyError(err) {
					// Race condition: another request created the record first
					// Try to update the existing record instead
					s.logger.Debug("Race condition detected, updating existing pending blob",
						zap.String("blob_hash", pendingBlob.BlobHash),
						zap.Int("blob_number", pendingBlob.BlobNumber),
						zap.Uint("stream_id", pendingBlob.StreamID))

					// For the update, we need to be careful about blob_number to avoid unique constraint violations
					// If we're updating an existing record, don't try to change blob_number unless it's 0
					updateOnlyFields := updates
					if pendingBlob.BlobNumber > 0 {
						updateOnlyFields = lo.OmitByKeys(updates, []string{"blob_number"})
					}

					return updateExisting(pendingBlob.BlobHash, pendingBlob.BlobNumber, pendingBlob.StreamID, updateOnlyFields)
				}
				// For all other errors, return the original error immediately
				return err
			}
			return nil
		}

		// For terminating blobs, update all pending terminating blobs for the user
		// since they all have the same hash and serve the same purpose
		if isTerminating {
			// Update all terminating blobs for this user to mark them as received
			result := tx.Model(&pluginDb.PendingBlob{}).
				Where("user_id = ? AND blob_hash = ? AND received = ?", userID, blobHash, false).
				Updates(map[string]any{
					"received":  true,
					"device_id": deviceID,
				})

			if result.Error != nil {
				_ = tx.AddError(fmt.Errorf("failed to update terminating blobs: %w", result.Error))
				return tx
			}

			// If no terminating blobs were updated, create a new one
			if result.RowsAffected == 0 {
				pendingBlob := pluginDb.PendingBlob{
					BlobHash:    blobHash,
					UserID:      userID,
					DeviceID:    deviceID,
					StreamID:    0,
					BlobSize:    int(blobInfo.Length),
					BlobNumber:  blobInfo.BlobNum,
					Received:    true,
					Terminating: true,
					IVData:      blobInfo.IV,
				}

				updates := map[string]any{
					"received":  true,
					"device_id": deviceID,
				}

				if err := createWithFallback(pendingBlob, updates); err != nil {
					_ = tx.AddError(fmt.Errorf("failed to create terminating blob: %w", err))
					return tx
				}

				s.logger.Debug("Created new terminating blob as received",
					zap.Uint("user_id", userID),
					zap.String("terminating_hash", blobHash))
			} else {
				s.logger.Debug("Updated all terminating blobs as received",
					zap.Uint("user_id", userID),
					zap.String("terminating_hash", blobHash))
			}

			return nil
		}

		// For regular blobs, find existing records first and update them using their unique key
		// Find all pending blobs with this hash for this user (both received and not received)
		var existingBlobs []pluginDb.PendingBlob
		err := tx.Where("user_id = ? AND blob_hash = ?", userID, blobHash).
			Find(&existingBlobs).Error

		if err != nil {
			_ = tx.AddError(fmt.Errorf("failed to find existing pending blobs: %w", err))
			return tx
		}

		updates := map[string]any{
			"received":  true,
			"device_id": deviceID,
			"blob_size": blobInfo.Length,
		}

		// Only include IV data in updates if it's not nil to preserve existing IV data
		if blobInfo.IV != nil {
			updates["iv_data"] = blobInfo.IV
		}

		// Add blob_number if we have a valid one from blobInfo (allow BlobNum == 0)
		if blobInfo.BlobNum >= 0 {
			updates["blob_number"] = blobInfo.BlobNum
		}

		if len(existingBlobs) > 0 {
			// Split existing blobs into those with received=false and received=true
			var notReceivedBlobs []pluginDb.PendingBlob
			var alreadyReceivedBlobs []pluginDb.PendingBlob

			for _, existingBlob := range existingBlobs {
				if !existingBlob.Received {
					notReceivedBlobs = append(notReceivedBlobs, existingBlob)
				} else {
					alreadyReceivedBlobs = append(alreadyReceivedBlobs, existingBlob)
				}
			}

			// Update blobs with received=false using full updates map
			for _, existingBlob := range notReceivedBlobs {
				if err := updateExisting(blobHash, existingBlob.BlobNumber, existingBlob.StreamID, updates); err != nil {
					_ = tx.AddError(err)
					return tx
				}
			}

			// Update blobs with received=true using updates map without blob_number to avoid unique constraint violations
			updateOnlyFields := lo.OmitByKeys(updates, []string{"blob_number"})
			for _, existingBlob := range alreadyReceivedBlobs {
				if err := updateExisting(blobHash, existingBlob.BlobNumber, existingBlob.StreamID, updateOnlyFields); err != nil {
					_ = tx.AddError(err)
					return tx
				}
			}

			totalUpdated := len(notReceivedBlobs) + len(alreadyReceivedBlobs)
			s.logger.Debug("Updated existing pending blobs as received",
				zap.Uint("user_id", userID),
				zap.String("blob_hash", blobHash),
				zap.Int("not_received_count", len(notReceivedBlobs)),
				zap.Int("already_received_count", len(alreadyReceivedBlobs)),
				zap.Int("total_updated", totalUpdated))
		} else {
			// No existing records found, create a new one
			pendingBlob := pluginDb.PendingBlob{
				BlobHash:    blobHash,
				UserID:      userID,
				DeviceID:    deviceID,
				StreamID:    0,
				BlobSize:    int(blobInfo.Length),
				BlobNumber:  blobInfo.BlobNum,
				Received:    true,
				Terminating: isTerminating,
				IVData:      blobInfo.IV,
			}

			if err := createWithFallback(pendingBlob, updates); err != nil {
				tx.AddError(fmt.Errorf("failed to create pending blob: %w", err))
				return tx
			}

			s.logger.Debug("Created new pending blob as received",
				zap.Uint("user_id", userID),
				zap.String("blob_hash", blobHash))
		}

		return tx
	})
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

	pendingStream := pluginDb.PendingStream{
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
		// Use centralized terminating blob hash generation
		hash := internal.GetTerminatingBlobHash()
		if hash == "" {
			return "", true, fmt.Errorf("failed to generate terminating blob hash")
		}
		return hash, true, nil
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
func (s *UploadServiceDefault) buildPendingBlobUpdates(deviceID uint, streamID *uint, blobInfo *stream.BlobInfo, received *bool, terminating *bool, updateBlobNum, updateSize bool) []clause.Assignment {
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

	// Update blob_size only if requested
	if updateSize {
		updates = append(updates, clause.Assignment{Column: clause.Column{Name: "blob_size"}, Value: int(blobInfo.Length)})
	}

	// Update blob_number only if requested
	if updateBlobNum {
		updates = append(updates, clause.Assignment{Column: clause.Column{Name: "blob_number"}, Value: blobInfo.BlobNum})
	}

	// Always update IV data if present
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
			pendingBlob := pluginDb.PendingBlob{
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
			updates := s.buildPendingBlobUpdates(deviceID, &streamID, &blobInfo, nil, &isTerminating, true, true)
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
	err := s.db.WithContext(ctx).Model(&pluginDb.PendingBlob{}).
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
		Model(&pluginDb.PendingBlob{}).
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
	var pendingStream pluginDb.PendingStream
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
				Delete(&pluginDb.PendingBlob{}).Error
			if err != nil {
				return fmt.Errorf("failed to cleanup pending blobs: %w", err)
			}
		}
	}

	// Clean up pending stream (SD blob) by SD hash
	// This will succeed even if no record exists (no-op)
	err := s.db.WithContext(ctx).
		Where("user_id = ? AND sd_hash = ?", userID, streamResult.SDBlobHash).
		Delete(&pluginDb.PendingStream{}).Error
	if err != nil {
		return fmt.Errorf("failed to cleanup pending stream: %w", err)
	}

	return nil
}

// Helper function to detect duplicate key/conflict errors
func isDuplicateKeyError(err error) bool {
	if err == nil {
		return false
	}

	// Check for GORM's duplicate key error
	if errors.Is(err, gorm.ErrDuplicatedKey) {
		return true
	}

	// Check error message patterns for common duplicate key errors
	errMsg := strings.ToLower(err.Error())
	return strings.Contains(errMsg, "duplicate") ||
		strings.Contains(errMsg, "unique constraint failed") ||
		strings.Contains(errMsg, "unique constraint")
}

// ensureActivePin finds an existing pin (including soft-deleted) and ensures it's active
// Returns the pin if found/restored, or gorm.ErrRecordNotFound if not found
func (s *UploadServiceDefault) ensureActivePin(ctx context.Context, userId, streamID uint, sdHash string) (*pluginDb.StreamPin, error) {
	var pin pluginDb.StreamPin
	err := s.db.WithContext(ctx).
		Unscoped().
		Where("user_id = ? AND stream_id = ?", userId, streamID).
		First(&pin).Error

	if err != nil {
		return nil, err
	}

	// If pin is soft-deleted, restore it
	if pin.DeletedAt.Valid {
		err = s.db.WithContext(ctx).Unscoped().Model(&pin).Update("deleted_at", nil).Error
		if err != nil {
			return nil, fmt.Errorf("failed to restore soft-deleted stream pin: %w", err)
		}

		s.logger.Info("Restored soft-deleted stream pin",
			zap.Uint("userID", userId),
			zap.Uint("streamID", streamID),
			zap.Uint64("pinID", uint64(pin.ID)),
			zap.String("sdHash", sdHash),
			zap.Time("deletedAt", pin.DeletedAt.Time))

		// Reload the pin to reflect the updated state
		err = s.db.WithContext(ctx).First(&pin, pin.ID).Error
		if err != nil {
			return nil, fmt.Errorf("failed to reload restored stream pin: %w", err)
		}
	} else {
		s.logger.Debug("Stream pin already exists, returning existing pin",
			zap.Uint("userID", userId),
			zap.Uint("streamID", streamID),
			zap.Uint64("pinID", uint64(pin.ID)),
			zap.String("sdHash", sdHash))
	}

	return &pin, nil
}

// CreateStreamPin creates an LBRY stream pin record
func (s *UploadServiceDefault) CreateStreamPin(ctx context.Context, userId uint, sdCid cid.Cid) (*pluginDb.StreamPin, error) {
	// Convert CID to stream hash string
	sdHash := sdCid.String()
	lbryHash, err := stream.FromMultihash(sdHash)
	if err != nil {
		return nil, fmt.Errorf("failed to convert stream CID to LBRY hash: %w", err)
	}

	// Find the stream
	var _stream pluginDb.Stream
	if err = s.db.WithContext(ctx).First(&_stream, pluginDb.Stream{SDHash: lbryHash}).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, fmt.Errorf("stream not found: %s", sdHash)
		}
		return nil, fmt.Errorf("failed to find stream %s: %w", sdHash, err)
	}

	// Try to find and ensure an existing pin is active
	existingPin, err := s.ensureActivePin(ctx, userId, _stream.ID, sdHash)
	if err == nil {
		return existingPin, nil
	}
	if !errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, fmt.Errorf("failed to check existing stream pin: %w", err)
	}

	// Create a new StreamPin record
	streamPin := &pluginDb.StreamPin{
		UserID:   uint64(userId),
		StreamID: uint64(_stream.ID),
	}

	// Save the StreamPin to the database with duplicate handling
	if err = s.db.WithContext(ctx).Create(streamPin).Error; err != nil {
		// Handle race condition where another request created the pin concurrently
		if isDuplicateKeyError(err) {
			s.logger.Debug("Stream pin created by concurrent request, fetching existing pin",
				zap.Uint("userID", userId),
				zap.Uint("streamID", _stream.ID),
				zap.String("sdHash", sdHash))

			// Use the helper to find and ensure the pin is active
			existingPin, err := s.ensureActivePin(ctx, userId, _stream.ID, sdHash)
			if err != nil {
				return nil, fmt.Errorf("failed to fetch existing stream pin after duplicate constraint: %w", err)
			}
			return existingPin, nil
		}
		return nil, fmt.Errorf("failed to create stream pin: %w", err)
	}

	s.logger.Debug("Created new stream pin",
		zap.Uint("userID", userId),
		zap.Uint("streamID", _stream.ID),
		zap.String("sdHash", sdHash))

	return streamPin, nil
}

// ListStreams returns a paginated list of streams for a user
func (s *UploadServiceDefault) ListStreams(ctx context.Context, userID uint, filters []queryutil.CrudFilter, sorts []queryutil.Sort, pagination queryutil.Pagination) ([]*pluginDb.Stream, int64, error) {
	var streams []*pluginDb.Stream
	var total int64

	// Build the base query with user filter using GORM OOP
	// Join with StreamPin to filter streams that belong to the user
	// Explicitly exclude soft-deleted stream pins
	query := s.db.WithContext(ctx).
		Model(&pluginDb.Stream{}).
		Joins("INNER JOIN lbry_stream_pins ON lbry_streams.id = lbry_stream_pins.stream_id").
		Where("lbry_stream_pins.user_id = ? AND lbry_stream_pins.deleted_at IS NULL", userID)

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
func (s *UploadServiceDefault) GetPendingStream(ctx context.Context, userID uint, sdHash string) (*pluginDb.PendingStream, error) {
	var pendingStream pluginDb.PendingStream
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
func (s *UploadServiceDefault) GetPendingBlobs(ctx context.Context, userID uint, sdHash string) ([]*pluginDb.PendingBlob, error) {
	// First get the pending stream to find its ID
	var pendingStream pluginDb.PendingStream
	err := s.db.WithContext(ctx).Where("user_id = ? AND sd_hash = ?", userID, sdHash).First(&pendingStream).Error
	if err != nil {
		return nil, fmt.Errorf("failed to get pending stream for user %d and SD hash %s: %w", userID, sdHash, err)
	}

	// Now get pending blobs for this specific stream
	var pendingBlobs []*pluginDb.PendingBlob
	err = s.db.WithContext(ctx).Where("user_id = ? AND stream_id = ?", userID, pendingStream.ID).Order("blob_number").Find(&pendingBlobs).Error
	if err != nil {
		return nil, fmt.Errorf("failed to get pending blobs for user %d and SD hash %s: %w", userID, sdHash, err)
	}
	return pendingBlobs, nil
}

// DeleteStream removes only the user's stream pin by SD hash (idempotent)
func (s *UploadServiceDefault) DeleteStream(ctx context.Context, userID uint, sdHash string) error {
	if userID == 0 {
		return gorm.ErrRecordNotFound
	}

	// Find the stream by SD hash
	var _stream pluginDb.Stream
	err := s.db.WithContext(ctx).Where("sd_hash = ?", sdHash).First(&_stream).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return gorm.ErrRecordNotFound
		}
		return fmt.Errorf("failed to find stream: %w", err)
	}

	// Check if the stream pin exists and belongs to the user (including soft-deleted)
	streamPinQuery := pluginDb.StreamPin{
		UserID:   uint64(userID),
		StreamID: uint64(_stream.ID),
	}
	var streamPin pluginDb.StreamPin
	err = s.db.WithContext(ctx).
		Unscoped(). // Include soft-deleted records to check for restoration
		Where(&streamPinQuery).
		First(&streamPin).Error

	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			// Pin doesn't exist (Unscoped query includes soft-deleted records)
			s.logger.Debug("Stream pin not found",
				zap.Uint("userID", userID),
				zap.Uint("streamID", _stream.ID),
				zap.String("sdHash", sdHash))
			return gorm.ErrRecordNotFound
		}
		return fmt.Errorf("failed to check stream ownership: %w", err)
	}

	// If the pin is already soft-deleted, return success (idempotent)
	if streamPin.DeletedAt.Valid {
		s.logger.Debug("Stream pin already deleted",
			zap.Uint("userID", userID),
			zap.Uint("streamID", _stream.ID),
			zap.String("sdHash", sdHash))
		return nil
	}

	// Delete only the stream pin record (soft delete)
	if err := s.db.WithContext(ctx).
		Where(&streamPinQuery).
		Delete(&pluginDb.StreamPin{}).Error; err != nil {
		return fmt.Errorf("failed to delete stream pin: %w", err)
	}

	s.logger.Debug("Deleted stream pin",
		zap.Uint("userID", userID),
		zap.Uint("streamID", _stream.ID),
		zap.String("sdHash", sdHash))

	return nil
}
