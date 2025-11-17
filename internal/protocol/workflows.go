package protocol

import "go.lumeweb.com/portal-plugin-lbry/internal/api/dto"

// LBRY workflow constants
const UPLOAD_WORKFLOW = "lbry.upload"

// PostUploadWorkflowData contains data for post-upload workflow processing
type PostUploadWorkflowData struct {
	UploadID string                 `json:"upload_id"`
	Size     int64                  `json:"size"`
	Meta     *dto.StreamMetadataRequest `json:"meta,omitempty"`
}
