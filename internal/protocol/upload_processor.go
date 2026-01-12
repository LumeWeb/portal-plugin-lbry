package protocol

import (
	"context"
	"fmt"
	"io"

	"go.lumeweb.com/liblbry/server"
	"go.lumeweb.com/liblbry/stream"
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

	// Get protocol and node using shared utility
	protocol, node, err := GetProtocolWithValidation()
	if err != nil {
		return nil, err
	}

	blobManager, ok := node.(server.BlobManager)
	if !ok {
		return nil, fmt.Errorf("failed to cast node to server.BlobManager")
	}

	// Get upload reader
	reader, err := source.GetReader(ctx, protocol)
	if err != nil {
		return nil, fmt.Errorf("failed to get upload reader: %w", err)
	}
	defer func(r io.ReadCloser) {
		if r == nil {
			return
		}
		if closeErr := r.Close(); closeErr != nil {
			p.logger.Error("Failed to close upload reader", zap.Error(closeErr))
		}
	}(reader)

	streamCreator := stream.NewStreamCreator()

	// Prepare stream options - include metadata if provided
	var streamOpts []stream.StreamOption

	// Add chunk handler for blob management
	streamOpts = append(streamOpts, stream.WithChunkHandler(func(chunk stream.Chunk) error {
		return blobManager.AddBlob(ctx, chunk.Hash, chunk.Data)
	}))

	// Apply metadata from upload source using stream options
	metadata := source.GetMetadata()
	if streamName, ok := metadata["stream_name"]; ok && streamName != "" {
		streamOpts = append(streamOpts, stream.WithStreamName(streamName))
	}
	if suggestedFileName, ok := metadata["suggested_file_name"]; ok && suggestedFileName != "" {
		streamOpts = append(streamOpts, stream.WithSuggestedFileName(suggestedFileName))
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

	err = blobManager.AddSDBlob(ctx, streamResult.SDBlobHash, sdBlobBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to add SDBlob to blob manager: %w", err)
	}

	// Process stream result using shared utility
	// Note: userID is converted from uint64 to uint - this is safe as userID represents a DB PK
	// and conversion will not truncate valid values in this system
	err = ProcessStreamResult(ctx, p.ctx, streamResult, uint(userID))
	if err != nil {
		return nil, err
	}

	return streamResult, nil
}
