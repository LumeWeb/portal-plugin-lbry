package protocol

import (
	"context"
	"fmt"
	"io"

	"go.lumeweb.com/liblbry/server"
	"go.lumeweb.com/liblbry/stream"
	pluginCore "go.lumeweb.com/portal-plugin-lbry/core"
	"go.lumeweb.com/portal-plugin-lbry/internal"
	"go.lumeweb.com/portal/core"
	"go.uber.org/zap"
)

// UploadSource defines the interface for different upload sources
type UploadSource interface {
	GetReader(ctx context.Context, proto core.StorageProtocol) (io.ReadCloser, error)
	GetMetadata() map[string]string
	GetSize() int64
	Close() error
}

// UploadProcessor handles the common stream processing logic
type UploadProcessor struct {
	ctx    core.Context
	logger *core.Logger
}

// NewUploadProcessor creates a new upload processor
func NewUploadProcessor(ctx core.Context) *UploadProcessor {
	return &UploadProcessor{
		ctx:    ctx,
		logger: ctx.Logger(),
	}
}

// ProcessStreamUpload processes an upload using the provided source
func (p *UploadProcessor) ProcessStreamUpload(ctx context.Context, source UploadSource, userID uint64) (*stream.StreamResult, error) {
	defer source.Close()

	// Get protocol from context
	proto := core.GetProtocol(internal.ProtocolName)
	storageProto, ok := proto.(core.StorageProtocol)
	if !ok {
		return nil, fmt.Errorf("protocol does not implement StorageProtocol")
	}

	// Safely assert proto to *Protocol
	pProto, ok := proto.(*Protocol)
	if !ok {
		return nil, fmt.Errorf("protocol is not of type *Protocol")
	}

	// Safely assert node to server.BlobManager
	node := pProto.Node()
	blobManager, ok := node.(server.BlobManager)
	if !ok {
		return nil, fmt.Errorf("failed to cast node to server.BlobManager")
	}

	// Get upload reader
	reader, err := source.GetReader(ctx, storageProto)
	if err != nil {
		return nil, fmt.Errorf("failed to get upload reader: %w", err)
	}
	defer func(reader io.ReadCloser) {
		if reader == nil {
			return
		}
		err = reader.Close()
		if err != nil {
			p.logger.Error("Failed to close upload reader", zap.Error(err))
		}
	}(reader)

	manifestCreator := stream.NewManifestCreator()
	streamCreator := stream.NewStreamCreator(manifestCreator)

	// Prepare stream options - include metadata if provided
	var streamOpts []stream.StreamOption

	// Add chunk handler for blob management
	streamOpts = append(streamOpts, stream.WithChunkHandler(func(chunk stream.Chunk) error {
		return blobManager.AddBlob(chunk.Hash, chunk.Data)
	}))

	// Create SD blob for metadata
	sdBlob := &stream.SDBlob{}

	// Apply metadata from upload source
	metadata := source.GetMetadata()
	applyMetadataToSDBlob(sdBlob, metadata)

	// Add SD handler if we have metadata
	if sdBlob.StreamName != "" || sdBlob.SuggestedFileName != "" {
		blob, err := sdBlob.ToBlob()
		if err != nil {
			return nil, fmt.Errorf("failed to serialize SD blob: %w", err)
		}

		streamOpts = append(streamOpts,
			stream.WithExistingSDBlob(blob))
	}

	// Create stream
	streamResult, err := streamCreator.CreateStream(reader, source.GetSize(), streamOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create stream: %w", err)
	}

	sdBlobBytes, err := streamResult.SDBlob.ToBlob()
	if err != nil {
		return nil, fmt.Errorf("failed to convert SDBlob to blob: %w", err)
	}

	err = blobManager.AddSDBlob(streamResult.SDBlobHash, sdBlobBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to add SDBlob to blob manager: %w", err)
	}

	// Process upload using upload service
	uploadSvc := core.GetService[pluginCore.UploadService](p.ctx, pluginCore.UPLOAD_SERVICE)
	if uploadSvc == nil {
		p.logger.Error("Upload service not available")
		return nil, fmt.Errorf("upload service not available")
	}

	// Process all CIDs to create upload and core pin records
	err = uploadSvc.ProcessUpload(p.ctx, streamResult, uint(userID))
	if err != nil {
		return nil, fmt.Errorf("failed to process upload: %w", err)
	}

	// Convert SD blob hash to CID
	sdCid, err := internal.LBRYHashToCID(streamResult.SDBlobHash)
	if err != nil {
		return nil, fmt.Errorf("failed to convert SD blob hash to CID: %w", err)
	}

	// Create stream pin record
	_, err = uploadSvc.CreateStreamPin(p.ctx, uint(userID), sdCid)
	if err != nil {
		return nil, fmt.Errorf("failed to create root pin: %w", err)
	}

	return streamResult, nil
}
