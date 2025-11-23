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
}

// Upload service name
const UPLOAD_SERVICE = "lbry.upload"
