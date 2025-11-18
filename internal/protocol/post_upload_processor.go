package protocol

import (
	"context"
	"fmt"
	"io"

	"go.lumeweb.com/portal-plugin-lbry/internal/api/dto"
	"go.lumeweb.com/portal/core"
)

// PostUploadSource implements UploadSource for post upload workflow
type PostUploadSource struct {
	uploadID string
	size     int64
	meta     *dto.StreamMetadataRequest
	storage  core.StorageService
	protocol core.StorageProtocol
}

// NewPostUploadSource creates a new post upload source
func NewPostUploadSource(uploadID string, size int64, meta *dto.StreamMetadataRequest, storage core.StorageService, protocol core.StorageProtocol) *PostUploadSource {
	return &PostUploadSource{
		uploadID: uploadID,
		size:     size,
		meta:     meta,
		storage:  storage,
		protocol: protocol,
	}
}

// GetReader returns the upload reader from storage
func (p *PostUploadSource) GetReader(ctx context.Context, _ core.StorageProtocol) (io.ReadCloser, error) {
	upload, err := p.storage.S3GetTemporaryUpload(ctx, p.protocol, p.uploadID)
	if err != nil {
		return nil, fmt.Errorf("failed to get upload: %w", err)
	}
	return upload, nil
}

// GetMetadata returns metadata as map[string]string
func (p *PostUploadSource) GetMetadata() map[string]string {
	metadata := make(map[string]string)

	if p.meta != nil {
		if p.meta.StreamName != "" {
			metadata["stream_name"] = p.meta.StreamName
		}
		if p.meta.SuggestedFileName != "" {
			metadata["suggested_file_name"] = p.meta.SuggestedFileName
		}
	}

	return metadata
}

// GetSize returns the upload size
func (p *PostUploadSource) GetSize() int64 {
	return p.size
}

// Close is a no-op for post upload source (reader is closed by GetReader caller)
func (p *PostUploadSource) Close() error {
	return nil
}
