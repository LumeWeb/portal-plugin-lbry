package protocol

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"sort"

	"go.lumeweb.com/portal-plugin-lbry/internal"
	"go.uber.org/zap"
	"gorm.io/gorm/clause"

	"go.lumeweb.com/liblbry/stream"
	pluginDb "go.lumeweb.com/portal-plugin-lbry/internal/db"
	"go.lumeweb.com/portal/core"
	"go.lumeweb.com/portal/service"
	"gorm.io/gorm"
)

const BLOBSTORE_NAME = "sia"

// convertToStorageHash converts LBRY hash to storage hash with consistent error handling
func (bs *BlobStore) convertToStorageHash(hash string) (core.StorageHash, error) {
	storageHash, err := internal.LBRYHashToStorageHash(hash)
	if err != nil {
		return nil, fmt.Errorf("failed to parse storage hash for blob %q: %w", hash, err)
	}
	return storageHash, nil
}

// isTerminatingBlob checks if a hash represents a terminating blob
func (bs *BlobStore) isTerminatingBlob(hash string) bool {
	return hash == "" || hash == internal.GetTerminatingBlobHash()
}

// closeReaderSafely safely closes a reader if it implements io.Closer
func (bs *BlobStore) closeReaderSafely(reader io.Reader) {
	if closer, ok := reader.(io.Closer); ok {
		if err := closer.Close(); err != nil {
			bs.logger.Debug("Failed to close reader", zap.Error(err))
		}
	}
}

// upsertBlobRecord performs a standardized blob upsert operation
func (bs *BlobStore) upsertBlobRecord(ctx context.Context, tx *gorm.DB, blob pluginDb.Blob) error {
	db := tx
	if db == nil {
		db = bs.db.WithContext(ctx)
	} else {
		db = db.WithContext(ctx)
	}

	// For updates, preserve existing IV data if the new blob doesn't have IV data
	// This prevents nuking IV data when blobs are updated without IV information
	doUpdates := clause.Set{
		{Column: clause.Column{Name: "blob_size"}, Value: blob.BlobSize},
	}

	// Only update IV data if it's provided (non-nil and non-empty)
	if len(blob.IVData) > 0 {
		doUpdates = append(doUpdates, clause.Set{
			{Column: clause.Column{Name: "iv_data"}, Value: blob.IVData},
		}...)
	}

	return db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "blob_hash"}},
		DoUpdates: doUpdates,
	}).Create(&blob).Error
}

// validateBlob checks if a blob is valid for processing
func (bs *BlobStore) validateBlob(blob pluginDb.Blob) bool {
	// Non-terminating blobs with data must have both hash and IV
	hasData := blob.BlobSize > 0
	hasMissingMetadata := len(blob.BlobHash) == 0 || len(blob.IVData) == 0

	// Skip blobs that have data but are missing metadata
	if hasData && hasMissingMetadata {
		return false
	}

	// Skip zero-size blobs (non-terminating blobs should have data)
	// Terminating blobs are handled separately in buildBlobInfosFromDb
	if blob.BlobSize == 0 {
		return false
	}

	// Skip blobs with empty hash (invalid)
	if len(blob.BlobHash) == 0 {
		return false
	}

	// Skip blobs with empty IV (invalid for non-terminating blobs)
	if len(blob.IVData) == 0 {
		return false
	}

	return true
}

// BlobStore implements the storage.BlobStore interface for LBRY protocol
type BlobStore struct {
	db         *gorm.DB
	logger     *core.Logger
	storageSvc core.StorageService
	proto      core.StorageProtocol
}

// NewLBRYBlobStore creates a new instance of BlobStore
func NewLBRYBlobStore(ctx core.Context) (*BlobStore, error) {
	proto, err := GetStorageProtocol()
	if err != nil {
		return nil, err
	}

	storageSvc := core.GetService[core.StorageService](ctx, core.STORAGE_SERVICE)
	if storageSvc == nil {
		return nil, fmt.Errorf("storage service not initialized")
	}

	return &BlobStore{
		db:         ctx.DB(),
		logger:     ctx.Logger(),
		storageSvc: storageSvc,
		proto:      proto,
	}, nil
}

// Has checks if a blob with the given hash exists in storage
func (bs *BlobStore) Has(ctx context.Context, hash string) (bool, error) {
	// Terminating blobs are virtual but can be provided as empty data
	if bs.isTerminatingBlob(hash) {
		return true, nil
	}

	// First check if we have metadata in our database
	count, err := bs.hasBlobMetadata(ctx, hash)
	if err != nil {
		return false, err
	}

	// If we have metadata, check if the actual data exists in storage
	if count > 0 {
		// Parse the storage hash using helper
		storageHash, err := bs.convertToStorageHash(hash)
		if err != nil {
			return false, err
		}

		// Try to download the object to verify it exists
		reader, err := bs.storageSvc.DownloadObject(ctx, bs.proto, storageHash, 0)
		if err != nil {
			// Object doesn't exist in storage, but metadata exists
			// This could happen if there was a partial upload
			return false, nil
		}

		// Properly close the reader using helper
		defer bs.closeReaderSafely(reader)

		return true, nil
	}

	return false, nil
}

// Get retrieves a blob by its hash
func (bs *BlobStore) Get(ctx context.Context, hash string) ([]byte, error) {
	// Check if this is an SD blob (stream metadata)
	isSdBlob, streamData, err := bs.isSDBlob(ctx, hash)
	if err != nil {
		return nil, err
	}

	if isSdBlob {
		return streamData, nil
	}

	// Not an SD blob, proceed with regular blob logic
	return bs.getRegularBlob(ctx, hash)
}

// isSDBlob checks if a given hash corresponds to an SD blob (stream metadata)
// Returns a boolean indicating whether it's an SD blob, and the stream data if it is
func (bs *BlobStore) isSDBlob(ctx context.Context, hash string) (bool, []byte, error) {
	// Check if this is an SD blob (stream metadata)
	var _stream pluginDb.Stream
	err := bs.db.WithContext(ctx).Where("sd_hash = ?", hash).First(&_stream).Error
	if err == nil {
		// This is an SD blob, reconstruct it
		data, err := bs.getSDBlob(ctx, &_stream, hash)
		if err != nil {
			return true, nil, err
		}
		return true, data, nil
	} else if !errors.Is(err, gorm.ErrRecordNotFound) {
		// Some other error occurred
		return false, nil, fmt.Errorf("failed to check if hash %q is an SD blob: %w", hash, err)
	}

	return false, nil, nil
}

// getSDBlob handles SD blob retrieval with associated blobs
func (bs *BlobStore) getSDBlob(ctx context.Context, _stream *pluginDb.Stream, hash string) ([]byte, error) {
	// Build the SDBlob structure
	streamHashBytes, err := hex.DecodeString(_stream.StreamHash)
	if err != nil {
		return nil, fmt.Errorf("failed to decode stream hash %q: %w", _stream.StreamHash, err)
	}

	sdBlob := stream.SDBlob{
		StreamName:        _stream.StreamName,
		StreamType:        _stream.StreamType,
		SuggestedFileName: _stream.SuggestedFileName,
		Key:               _stream.KeyData,
		StreamHash:        streamHashBytes,
	}

	// Use shared utility to build blob infos from database
	blobInfos, err := bs.buildBlobInfosFromDb(ctx, _stream.ID)
	if err != nil {
		return nil, fmt.Errorf("failed to get associated blobs for SD stream %q: %w", hash, err)
	}

	// Sort blob infos by blob number to maintain order
	sort.Slice(blobInfos, func(i, j int) bool {
		return blobInfos[i].BlobNum < blobInfos[j].BlobNum
	})

	sdBlob.BlobInfos = blobInfos

	// Set the correct profile by testing both and using the one that matches the SD hash
	if err := internal.SetSDBlobProfileByHash(&sdBlob, hash); err != nil {
		return nil, err
	}

	// Serialize the SD blob
	data, err := sdBlob.ToBlob()
	if err != nil {
		return nil, fmt.Errorf("failed to serialize SD blob %q: %w", hash, err)
	}

	return data, nil
}



// getRegularBlob handles regular blob retrieval
func (bs *BlobStore) getRegularBlob(ctx context.Context, hash string) ([]byte, error) {
	// Check if this is a terminating blob using helper
	if bs.isTerminatingBlob(hash) {
		bs.logger.Debug("Handling terminating blob",
			zap.String("blob_type", "terminating"))
		// Return empty data for terminating blobs (consistent with reflector store behavior)
		return []byte{}, nil
	}

	// First check if we have metadata in our database
	count, err := bs.hasBlobMetadata(ctx, hash)
	if err != nil {
		return nil, fmt.Errorf("failed to check blob existence in database for blob %q: %w", hash, err)
	}

	// If we don't have metadata in the database, the blob doesn't exist
	if count == 0 {
		return nil, fmt.Errorf("blob %q not found in local storage", hash)
	}

	// Parse the storage hash using helper
	storageHash, err := bs.convertToStorageHash(hash)
	if err != nil {
		return nil, err
	}

	// Try to download the blob data from storage
	rc, err := bs.storageSvc.DownloadObject(ctx, bs.proto, storageHash, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to download blob %q: %w", hash, err)
	}

	// Ensure the reader is closed using helper
	defer bs.closeReaderSafely(rc)

	// Read all data from the reader
	b, err := io.ReadAll(rc)
	if err != nil {
		return nil, fmt.Errorf("failed to read blob data for blob %q: %w", hash, err)
	}

	return b, nil
}

// hasBlobMetadata checks if blob metadata exists in the database
func (bs *BlobStore) hasBlobMetadata(ctx context.Context, hash string) (int64, error) {
	var count int64
	err := bs.db.WithContext(ctx).Model(&pluginDb.Blob{}).Where("blob_hash = ?", hash).Count(&count).Error
	return count, err
}

// putBlobData handles the common logic for storing blob data and metadata
func (bs *BlobStore) putBlobData(ctx context.Context, hash string, data []byte) error {
	// Convert the hash using helper
	sh, err := bs.convertToStorageHash(hash)
	if err != nil {
		return err
	}

	// Store the blob data using the storage service
	_, err = bs.storageSvc.UploadObject(ctx, service.NewStorageUploadRequest(
		core.StorageUploadWithProtocol(bs.proto),
		core.StorageUploadWithData(bytes.NewReader(data)),
		core.StorageUploadWithSize(uint64(len(data))),
		core.StorageUploadWithProof(sh),
	))
	if err != nil {
		return fmt.Errorf("failed to store blob %q: %w", hash, err)
	}

	// Update or create blob metadata in the database using helper
	_blob := pluginDb.Blob{
		BlobHash: hash,
		BlobSize: len(data),
	}

	return bs.upsertBlobRecord(ctx, nil, _blob)
}

// Put stores a blob with the given hash and data
func (bs *BlobStore) Put(ctx context.Context, hash string, data []byte) error {
	// Terminating blobs are not stored in the blob table
	if bs.isTerminatingBlob(hash) {
		bs.logger.Debug("Skipping storage of terminating blob",
			zap.String("hash", hash))
		return nil
	}
	return bs.putBlobData(ctx, hash, data)
}

// processStreamBlobs handles the creation/update of stream blob associations
// Terminating blobs are tracked via terminating_blob_number and terminating_blob_iv on the stream instead of as blob records
func (bs *BlobStore) processStreamBlobs(ctx context.Context, streamID uint, blobInfos []stream.BlobInfo) error {
	return bs.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var terminatingBlobNum *int
		var terminatingBlobIV []byte

		// Process each blob in the stream
		for _, blobInfo := range blobInfos {
			// Skip terminating blobs - they will be tracked via terminating_blob_number and terminating_blob_iv
			if len(blobInfo.BlobHash) == 0 {
				if terminatingBlobNum == nil {
					terminatingBlobNum = new(int)
				}
				*terminatingBlobNum = blobInfo.BlobNum
				// Store the IV for the terminating blob
				terminatingBlobIV = blobInfo.IV
				continue
			}

			// Convert blob hash bytes to string
			blobHash := hex.EncodeToString(blobInfo.BlobHash)

			_blob := pluginDb.Blob{
				BlobHash: blobHash,
				BlobSize: int(blobInfo.Length),
				IVData:   blobInfo.IV,
			}

			// Use helper for upsert operation
			err := bs.upsertBlobRecord(ctx, tx, _blob)
			if err != nil {
				return fmt.Errorf("failed to upsert blob %q: %w", blobHash, err)
			}

			// Reload the blob to get the ID after upsert
			err = tx.Where("blob_hash = ?", blobHash).First(&_blob).Error
			if err != nil {
				return fmt.Errorf("failed to reload blob %q: %w", blobHash, err)
			}

			// Create stream blob association
			streamBlob := pluginDb.StreamBlob{
				StreamID:   uint64(streamID),
				BlobID:     uint64(_blob.ID),
				BlobNumber: blobInfo.BlobNum,
			}

			err = tx.Clauses(clause.OnConflict{
				Columns: []clause.Column{{Name: "stream_id"}, {Name: "blob_id"}},
				DoUpdates: clause.Set{
					{Column: clause.Column{Name: "blob_number"}, Value: streamBlob.BlobNumber},
				},
			}).Create(&streamBlob).Error

			if err != nil {
				return fmt.Errorf("failed to create stream blob association: %w", err)
			}
		}

		// Update the stream with terminating blob number and IV if found
		if terminatingBlobNum != nil {
			updates := map[string]any{
				"terminating_blob_number": *terminatingBlobNum,
			}
			if len(terminatingBlobIV) > 0 {
				updates["terminating_blob_iv"] = terminatingBlobIV
			}
			err := tx.Model(&pluginDb.Stream{}).
				Where("id = ?", streamID).
				Updates(updates).Error
			if err != nil {
				return fmt.Errorf("failed to update stream with terminating blob info: %w", err)
			}
		}

		return nil
	})
}

// buildBlobInfosFromDb retrieves and builds blob information from the database
func (bs *BlobStore) buildBlobInfosFromDb(ctx context.Context, streamID uint) ([]stream.BlobInfo, error) {
	// Get the stream to check for terminating blob number
	var _stream pluginDb.Stream
	err := bs.db.WithContext(ctx).Where("id = ?", streamID).First(&_stream).Error
	if err != nil {
		return nil, fmt.Errorf("failed to get stream %d: %w", streamID, err)
	}

	// Get associated blobs for this stream
	var streamBlobs []pluginDb.StreamBlob
	err = bs.db.WithContext(ctx).Where("stream_id = ?", uint64(streamID)).Order("blob_number").Find(&streamBlobs).Error
	if err != nil {
		return nil, fmt.Errorf("failed to get associated blobs for stream %d: %w", streamID, err)
	}

	// Build blob infos from database records
	var blobInfos []stream.BlobInfo

	for _, streamBlob := range streamBlobs {
		// Get the blob details
		var _blob pluginDb.Blob
		err = bs.db.WithContext(ctx).Where("id = ?", streamBlob.BlobID).First(&_blob).Error
		if err != nil {
			// For missing blobs, create a placeholder blob info with empty data
			// This allows SD blob serialization to continue even with missing associated blobs
			bs.logger.Warn("Creating placeholder for missing blob for stream",
				zap.Uint64("blob_id", streamBlob.BlobID),
				zap.Uint64("stream_id", uint64(streamID)))

			// Create a placeholder blob info for the missing blob
			blobInfo := stream.BlobInfo{
				Length:   0, // Missing blobs have no size
				BlobNum:  streamBlob.BlobNumber,
				BlobHash: []byte{}, // Empty hash for missing blobs
				IV:       []byte{}, // Empty IV for missing blobs
			}
			blobInfos = append(blobInfos, blobInfo)
			continue
		}

		// Skip invalid blobs using helper
		if !bs.validateBlob(_blob) {
			bs.logger.Debug("Skipping invalid blob for stream",
				zap.Uint64("blob_id", streamBlob.BlobID),
				zap.Uint64("stream_id", uint64(streamID)),
				zap.String("blob_hash", _blob.BlobHash),
				zap.Int("blob_size", _blob.BlobSize),
				zap.Int("iv_length", len(_blob.IVData)))
			continue
		}

		// Convert the hex string hash to bytes for non-terminating blobs
		blobHashBytes, err := hex.DecodeString(_blob.BlobHash)
		if err != nil {
			bs.logger.Warn("Failed to decode blob hash from hex string",
				zap.String("blob_hash", _blob.BlobHash),
				zap.Uint64("blob_id", streamBlob.BlobID),
				zap.Error(err))
			continue
		}

		blobInfo := stream.BlobInfo{
			Length:   _blob.BlobSize,
			BlobNum:  streamBlob.BlobNumber,
			BlobHash: blobHashBytes,
			IV:       _blob.IVData, // Use stored IV from database
		}
		blobInfos = append(blobInfos, blobInfo)
	}

	// Add virtual terminating blob if the stream has one
	if _stream.TerminatingBlobNumber != nil {
		terminatingIV := []byte{}
		if _stream.TerminatingBlobIV != nil {
			terminatingIV = _stream.TerminatingBlobIV
		}
		blobInfos = append(blobInfos, stream.BlobInfo{
			Length:   0,
			BlobNum:  *_stream.TerminatingBlobNumber,
			BlobHash: []byte{},      // Empty hash for terminating blob
			IV:       terminatingIV, // Use stored terminating blob IV, or empty if not set
		})
	}

	// Sort blob infos by blob number to maintain correct order
	sort.Slice(blobInfos, func(i, j int) bool {
		return blobInfos[i].BlobNum < blobInfos[j].BlobNum
	})

	return blobInfos, nil
}

// PutSD stores an SD blob as a stream in the database
// SD blobs contain stream metadata and should be treated as streams, not regular blobs
func (bs *BlobStore) PutSD(ctx context.Context, hash string, data []byte) error {
	// Parse the SD blob data using the liblbry stream package
	sdBlob := stream.SDBlob{}

	// Use the FromBlob method to parse the blob data into the SDBlob structure
	if err := sdBlob.FromBlob(data); err != nil {
		return fmt.Errorf("failed to parse SD blob: %w", err)
	}

	// Create a stream from the parsed blob data
	// KeyData should contain the encryption key, not the raw data
	_stream := pluginDb.Stream{
		StreamHash:        hex.EncodeToString(sdBlob.StreamHash),
		SDHash:            hash,
		StreamName:        sdBlob.StreamName,
		StreamType:        sdBlob.StreamType,
		SuggestedFileName: sdBlob.SuggestedFileName,
		KeyData:           sdBlob.Key,
	}

	// Use GORM's upsert functionality to handle both creation and updates
	err := bs.db.WithContext(ctx).Clauses(clause.OnConflict{
		Columns: []clause.Column{{Name: "stream_hash"}},
		DoUpdates: clause.Set{
			{Column: clause.Column{Name: "stream_name"}, Value: _stream.StreamName},
			{Column: clause.Column{Name: "stream_type"}, Value: _stream.StreamType},
			{Column: clause.Column{Name: "suggested_file_name"}, Value: _stream.SuggestedFileName},
			{Column: clause.Column{Name: "key_data"}, Value: _stream.KeyData},
		},
	}).Create(&_stream).Error

	if err != nil {
		return fmt.Errorf("failed to upsert SD stream: %w", err)
	}

	// After upsert, retrieve the actual stream ID from the database
	// The in-memory _stream.ID may be unset or stale after OnConflict().Create()
	var actualStream pluginDb.Stream
	err = bs.db.WithContext(ctx).First(&actualStream, "stream_hash = ?", _stream.StreamHash).Error
	if err != nil {
		return fmt.Errorf("failed to retrieve stream after upsert: %w", err)
	}

	// Process child blobs using the shared utility
	if len(sdBlob.BlobInfos) > 0 {
		err = bs.processStreamBlobs(ctx, actualStream.ID, sdBlob.BlobInfos)
		if err != nil {
			return fmt.Errorf("failed to process stream blobs: %w", err)
		}
	}

	return nil
}

// Name returns the name of this blob store
func (bs *BlobStore) Name() string {
	return BLOBSTORE_NAME
}

// List returns a paginated list of blob hashes from streams
func (bs *BlobStore) List(ctx context.Context, offset, limit int) ([]string, error) {
	// Create subquery for regular blobs only
	blobQuery := bs.db.WithContext(ctx).Table("lbry_blobs").Select("blob_hash as hash")

	// Combine queries using UNION ALL
	unionQuery := bs.db.WithContext(ctx).Table("(? UNION ALL ?) as combined", blobQuery, bs.db.WithContext(ctx).Table("lbry_streams").Select("sd_hash as hash")).
		Order("hash").
		Offset(offset).
		Limit(limit)

	// Initialize results slice
	var results []string

	// Execute the query
	err := unionQuery.Scan(&results).Error
	if err != nil {
		return nil, err
	}

	return results, nil
}

// Delete removes a blob by its hash
func (bs *BlobStore) Delete(ctx context.Context, hash string) error {
	// Parse the storage hash using helper
	storageHash, err := bs.convertToStorageHash(hash)
	if err != nil {
		return err
	}

	// Delete the blob data from storage
	err = bs.storageSvc.DeleteObject(ctx, bs.proto, storageHash)
	if err != nil {
		return fmt.Errorf("failed to delete blob from storage: %w", err)
	}

	// Delete the blob metadata from the database
	err = bs.db.WithContext(ctx).Where("blob_hash = ?", hash).Delete(&pluginDb.Blob{}).Error
	if err != nil {
		return fmt.Errorf("failed to delete blob metadata: %w", err)
	}

	return nil
}
