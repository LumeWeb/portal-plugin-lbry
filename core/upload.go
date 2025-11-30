package core

import (
	"context"
	"io"

	"github.com/ipfs/go-cid"
	"go.lumeweb.com/liblbry/stream"
	"go.lumeweb.com/portal-plugin-lbry/internal/db"
	portalCore "go.lumeweb.com/portal/core"
	"go.lumeweb.com/queryutil"
)

// UploadService handles stream uploads for LBRY protocol
type UploadService interface {
	portalCore.Service

	// HandleUpload processes an upload and returns the CID and upload ID
	HandleUpload(ctx context.Context, reader io.ReadSeekCloser) (cid.Cid, string, error)

	// ProcessUpload creates upload records for given CIDs
	ProcessUpload(ctx context.Context, result *stream.StreamResult, userId uint) error

	// CreateStreamPin creates an LBRY stream pin record
	CreateStreamPin(ctx context.Context, userId uint, sdCid cid.Cid) (*db.StreamPin, error)

	// ListStreams returns a paginated list of streams for a user
	ListStreams(ctx context.Context, userID uint, filters []queryutil.CrudFilter, sorts []queryutil.Sort, pagination queryutil.Pagination) ([]*db.Stream, int64, error)

	// DeleteStream deletes a stream and all associated data
	DeleteStream(ctx context.Context, userID uint, sdHash string) error

	// StorePendingBlob stores a regular blob in pending state
	StorePendingBlob(ctx context.Context, userID, deviceID, streamID uint, blobInfo *stream.BlobInfo) error

	// StorePendingStream stores an SD blob with full stream metadata in pending state
	StorePendingStream(ctx context.Context, userID, deviceID uint, sdBlob *stream.SDBlob, sdHash string) (uint, error)

	// GetMissingBlobs checks which required blobs are not available
	GetMissingBlobs(ctx context.Context, userID uint, streamID uint, requiredBlobs []string) ([]string, error)

	// GetPendingBlobCount returns the count of pending blobs for a stream
	GetPendingBlobCount(ctx context.Context, userID uint, streamID uint) (int64, error)

	// CleanupPendingBlobs removes pending blob records after successful assembly
	CleanupPendingBlobs(ctx context.Context, userID uint, streamResult *stream.StreamResult) error

	// GetPendingStream retrieves pending stream metadata by user ID and SD hash
	GetPendingStream(ctx context.Context, userID uint, sdHash string) (*db.PendingStream, error)

	// GetPendingBlobs retrieves pending blobs for a given SD hash
	GetPendingBlobs(ctx context.Context, userID uint, sdHash string) ([]*db.PendingBlob, error)

	// MarkPendingBlobAsReceived marks an existing pending blob as received without changing other fields
	MarkPendingBlobAsReceived(ctx context.Context, userID, deviceID uint, blobInfo *stream.BlobInfo) error
}

// Upload service name
const UPLOAD_SERVICE = "lbry.upload"
