package protocol

import (
	"go.lumeweb.com/portal-plugin-lbry/internal"
	"go.lumeweb.com/portal-plugin-lbry/internal/api/dto"
	"go.lumeweb.com/portal/core"
)

// LBRY workflow constants
const UPLOAD_WORKFLOW = "lbry.upload"
const TUS_UPLOAD_WORKFLOW = "lbry.tus.upload"
const PIN_WORKFLOW = "lbry.pin"
const REFLECTOR_ASSEMBLY_WORKFLOW = "lbry.reflector.assembly"

// LBRY operation constants
const REFLECTOR_ASSEMBLY_OPERATION = "reflector.assembly"

// PostUploadWorkflowData contains data for post-upload workflow processing
type PostUploadWorkflowData struct {
	UploadID string                     `json:"upload_id"`
	Size     int64                      `json:"size"`
	Meta     *dto.StreamMetadataRequest `json:"meta,omitempty"`
}

// ReflectorAssemblyWorkflowData contains data for reflector assembly workflow processing
type ReflectorAssemblyWorkflowData struct {
	SDBlobHash string `json:"sd_blob_hash"`
}

// NewReflectorAssemblyOperation creates a new ReflectorAssemblyOperation
func NewReflectorAssemblyOperation(ctx core.Context) core.Operation {

	return core.NewNamedOperation(
		core.OperationName(internal.ProtocolName, REFLECTOR_ASSEMBLY_OPERATION),
		core.OpTypeUpload,
		NewReflectorAssemblyOperationHandler(ctx),
		"Reassemble Blob Parts",
	)
}
