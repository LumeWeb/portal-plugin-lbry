package upload

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/ipfs/go-cid"
	lbrycrypto "go.lumeweb.com/liblbry/crypto"
	"go.lumeweb.com/liblbry/stream"
	pluginCore "go.lumeweb.com/portal-plugin-lbry/core"
	"go.lumeweb.com/portal-plugin-lbry/internal"
	"go.lumeweb.com/portal-plugin-lbry/internal/db"
	"go.lumeweb.com/portal/core"
	"go.lumeweb.com/portal/db/models"
	"go.uber.org/zap"
	"gorm.io/gorm"
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

	uploadId, err := s.storage.S3TemporaryUpload(ctx, reader, uint64(size), s.protocol.(core.StorageProtocol))
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

	// Create a new StreamPin record
	streamPin := &db.StreamPin{
		UserID:   uint64(userId),
		StreamID: uint64(_stream.ID),
	}

	// Save the StreamPin to the database
	if err = s.db.WithContext(ctx).Create(streamPin).Error; err != nil {
		return nil, fmt.Errorf("failed to create stream pin: %w", err)
	}

	return streamPin, nil
}
