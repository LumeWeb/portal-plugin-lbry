package protocol

import (
	"context"
	"fmt"
	"io"

	"go.lumeweb.com/portal/core"
)

// TUSUploadSource implements UploadSource for TUS uploads
type TUSUploadSource struct {
	tusHandler  core.TusHandler
	tusUploadID string
	protocol    core.StorageProtocol
	metadata    map[string]string
	size        int64
}

// NewTUSUploadSource creates a new TUS upload source
func NewTUSUploadSource(tusHandler core.TusHandler, tusUploadID string, protocol core.StorageProtocol) *TUSUploadSource {
	return &TUSUploadSource{
		tusHandler:  tusHandler,
		tusUploadID: tusUploadID,
		protocol:    protocol,
	}
}

// Initialize fetches metadata and size from TUS handler
func (t *TUSUploadSource) Initialize(ctx context.Context) error {
	// Get metadata
	metadata, err := t.tusHandler.GetUploadMetadata(ctx, t.protocol, t.tusUploadID)
	if err != nil {
		return fmt.Errorf("failed to get upload metadata: %w", err)
	}
	t.metadata = metadata

	// Get size
	size, err := t.tusHandler.UploadSize(ctx, t.protocol, t.tusUploadID)
	if err != nil {
		return fmt.Errorf("failed to get upload size: %w", err)
	}
	t.size = int64(size)

	return nil
}

// GetReader returns the upload reader from TUS handler
func (t *TUSUploadSource) GetReader(ctx context.Context, proto core.StorageProtocol) (io.ReadCloser, error) {
	reader, err := t.tusHandler.UploadReader(ctx, t.tusUploadID, proto, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to get upload reader: %w", err)
	}
	return reader, nil
}

// GetMetadata returns the TUS upload metadata
func (t *TUSUploadSource) GetMetadata() map[string]string {
	return t.metadata
}

// GetSize returns the upload size
func (t *TUSUploadSource) GetSize() int64 {
	return t.size
}

// Close is a no-op for TUS upload source (reader is closed by GetReader caller)
func (t *TUSUploadSource) Close() error {
	return nil
}
