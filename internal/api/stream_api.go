package api

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/labstack/echo/v4"
	"go.lumeweb.com/httputil"
	mcontext "go.lumeweb.com/portal-middleware/context"
	"go.lumeweb.com/portal-plugin-lbry/internal"
	"go.lumeweb.com/portal-plugin-lbry/internal/api/dto"
	"go.lumeweb.com/portal-plugin-lbry/internal/db"
	"go.lumeweb.com/portal-plugin-lbry/internal/protocol"
	router "go.lumeweb.com/portal-router"
	"go.lumeweb.com/portal/core"
	"go.lumeweb.com/queryutil"
	queryutilhttp "go.lumeweb.com/queryutil/http"
	"gorm.io/gorm"
)

// setupStreamRoutes defines all stream-related routes
func (a *API) setupStreamRoutes() []router.Route {
	// Create reusable schema for StreamResponse
	streamSchema := queryutil.NewSchemaProvider().ForType(&dto.StreamResponse{})

	return []router.Route{
		router.NewRoute(
			http.MethodPost,
			"/streams/upload",
			a.handleStreamUpload,
			router.WithAccess(core.ACCESS_USER_ROLE),
			router.WithSwagger(
				router.WithSummary("Upload a stream"),
				router.WithDescription("Upload a stream to the LBRY network. This endpoint requires authentication and supports file uploads up to the configured limit."),
				router.WithFileUpload("File to upload", true),
				router.WithSuccessResponse(http.StatusCreated, "File uploaded successfully", router.WithJSONContent(&dto.PostStreamUploadResponse{})),
			),
		),
		router.NewRoute(
			http.MethodPost,
			"/streams/pin",
			a.handleStreamPin,
			router.WithAccess(core.ACCESS_USER_ROLE),
			router.WithSwagger(
				router.WithSummary("Pin a stream"),
				router.WithDescription("Pin a stream to keep it available on the LBRY network. This endpoint requires authentication."),
				router.WithRequestBody(&dto.StreamPinRequest{}, "Pin request", true),
				router.WithSuccessResponse(http.StatusCreated, "Stream pin request accepted"),
			),
		),
		router.NewRoute(
			http.MethodGet,
			"/streams",
			a.handleStreamList,
			router.WithAccess(core.ACCESS_USER_ROLE),
			router.WithSwagger(
				router.WithSummary("List streams"),
				router.WithDescription("List all streams for the authenticated user with pagination, filtering, and sorting support."),
				router.WithSchema(streamSchema),
				router.WithFilterParamsFromSchema(streamSchema),
				router.WithSuccessResponse(http.StatusOK, "Streams retrieved successfully", router.WithJSONContent(&queryutil.Response[[]*dto.StreamResponse]{})),
			),
		),
		router.NewRoute(
			http.MethodDelete,
			"/streams/:sd_hash",
			a.handleStreamDelete,
			router.WithAccess(core.ACCESS_USER_ROLE),
			router.WithSwagger(
				router.WithSummary("Delete a stream"),
				router.WithDescription("Delete a stream and all associated data. Only streams owned by the authenticated user can be deleted."),
				router.WithSuccessResponse(http.StatusNoContent, "Stream deleted successfully"),
			),
		),
	}
}

// handleStreamUpload handles stream upload requests
// This function processes file uploads for the LBRY protocol, handling authentication,
// file preparation, upload processing, workflow initiation, and response generation.
func (a *API) handleStreamUpload(c echo.Context) error {
	// Extract request context and authenticate user
	ctx := httputil.Context(c)

	// Extract user ID from the request context for authentication
	user, err := mcontext.GetUserID(ctx.Context)
	if err != nil {
		// If user authentication fails, return an account error
		apiErr := core.NewAccountError(core.ErrKeyLoginFailed, nil)
		_ = ctx.Error(apiErr, apiErr.HttpStatus())
		return nil
	}

	// Parse form data to extract metadata
	meta := c.Request().FormValue("meta")
	if meta == "" {
		// If no metadata provided, return a bad request error
		_ = ctx.Error(NewError(ErrKeyMetadataMissing, nil), http.StatusBadRequest)
		return nil
	}

	// Create a StreamMetadataRequest instance
	metadata := &dto.StreamMetadataRequest{}

	// Parse the JSON from the "meta" form field into the metadata struct
	err = json.Unmarshal([]byte(meta), metadata)
	if err != nil {
		// If parsing fails, return a bad request error
		_ = ctx.Error(NewError(ErrKeyMetadataJSONInvalid, err), http.StatusBadRequest)
		return nil
	}

	// Validate the metadata using ctx.Validate
	err = ctx.Validate(metadata)
	if err != nil {
		// If validation fails, return a bad request error with validation details
		_ = ctx.Error(NewError(ErrKeyMetadataValidationFailed, err), http.StatusBadRequest)
		return nil
	}

	// Prepare file upload with size limits
	// Prepare the file upload with configured size limits from the core configuration
	upload, err := ctx.PrepareFileUpload(int64(a.config.Config().Core.PostUploadLimit))
	if err != nil {
		// If file preparation fails (e.g., file too large), return a bad request error
		_ = ctx.Error(err, http.StatusBadRequest)
		return nil
	}

	// Check if the uploaded file is empty
	if upload.Size == 0 {
		// If file is empty, return a bad request error
		_ = ctx.Error(NewError(ErrKeyEmptyFile, nil), http.StatusBadRequest)
		return nil
	}

	// Set filename from prepared upload if Filename is empty
	if metadata.Filename == "" {
		metadata.Filename = upload.Filename
	}

	// Get the request context for passing to downstream services
	reqCtx := ctx.Request().Context()

	// Handle upload via LBRY upload service
	// Process the actual upload using the LBRY-specific upload service
	// This service handles the LBRY-specific hashing and storage logic
	uploadCID, uploadID, err := a.uploadService.HandleUpload(reqCtx, upload.File)
	if err != nil {
		// If upload processing fails, return the error
		_ = ctx.Error(NewError(ErrKeyUploadFailed, err), http.StatusInternalServerError)
		return nil
	}

	// Initiate upload workflow for post-processing
	// Start a background workflow to handle post-upload processing
	// This includes tasks like metadata extraction, indexing, and notifications
	_, err = a.workflowService.StartWorkflow(reqCtx, protocol.UPLOAD_WORKFLOW,
		// Pass the upload ID and metadata as workflow data for tracking
		core.WithWorkflowStructData(&protocol.PostUploadWorkflowData{
			UploadID: uploadID,
			Size:     int64(upload.Size),
			Meta:     metadata,
		}, "json"),
		// Associate the workflow with the authenticated user
		core.WithWorkflowUserID(user),
		// Record the source IP for audit/logging purposes
		core.WithWorkflowSourceIP(c.RealIP()),
		// Specify the protocol (LBRY) for protocol-specific processing
		core.WithWorkflowProtocol(internal.ProtocolName),
		// Pass the storage hash for content addressing and retrieval
		core.WithWorkflowStorageHash(core.NewStorageHashFromRawMultihash(uploadCID.Hash())),
	)
	if err != nil {
		// If workflow initiation fails, return the error
		_ = ctx.Error(NewError(ErrKeyStreamProcessingFailed, err), http.StatusInternalServerError)
		return nil
	}

	// Generate and return response
	// Return a successful response containing the upload hash (LBRY hash)
	// The client can use this hash to reference and retrieve the uploaded content

	lbryHash, err := internal.CIDToLBRYHash(uploadCID)
	if err != nil {
		// If conversion fails, return the error
		_ = ctx.Error(NewError(ErrKeyInvalidSDHash, err), http.StatusBadRequest)
		return nil
	}

	ctx.Response().WriteHeader(http.StatusCreated)
	return httputil.EncodeResponse(ctx, &dto.PostStreamUploadResponse{}, &dto.PostStreamUploadResponse{
		UploadHash: lbryHash,
	})
}

// handleStreamPin handles stream pin requests
// This function processes pin requests for LBRY streams, handling authentication,
// request validation, and pin management.
func (a *API) handleStreamPin(c echo.Context) error {
	// Extract request context and authenticate user
	ctx := httputil.Context(c)

	// Extract user ID from the request context for authentication
	user, err := mcontext.GetUserID(ctx.Context)
	if err != nil {
		// If user authentication fails, return an account error
		apiErr := core.NewAccountError(core.ErrKeyLoginFailed, nil)
		_ = ctx.Error(apiErr, apiErr.HttpStatus())
		return nil
	}

	// Parse and validate request body using httputil
	var pinRequest dto.StreamPinRequest
	_, ok := httputil.DecodeAndValidateRequest(ctx, &pinRequest)
	if !ok {
		return nil
	}

	multihash, err := internal.LBRYHashToStorageHash(pinRequest.SDHash)
	if err != nil {
		_ = ctx.Error(NewError(ErrKeyInvalidSDHash, err), http.StatusBadRequest)
		return nil
	}

	// Start pin workflow for background processing
	_, err = a.workflowService.StartWorkflow(ctx.Request().Context(), protocol.PIN_WORKFLOW,
		// Associate the workflow with the authenticated user
		core.WithWorkflowUserID(user),
		// Record the source IP
		core.WithWorkflowSourceIP(c.RealIP()),
		// Specify the protocol
		core.WithWorkflowProtocol(internal.ProtocolName),
		// Specify the SD hash
		core.WithWorkflowStorageHash(multihash),
	)
	if err != nil {
		// If workflow initiation fails, return a structured API error
		_ = ctx.Error(NewError(ErrKeyStreamProcessingFailed, err), http.StatusInternalServerError)
		return nil
	}

	return ctx.NoContent(http.StatusCreated)
}

// handleStreamList handles stream list requests
// This function processes list requests for LBRY streams using queryutilhttp.ProcessListRequest
// for standardized HTTP handling with authentication, query parsing, and pagination.
func (a *API) handleStreamList(c echo.Context) error {
	// Extract request context and authenticate user
	ctx := httputil.Context(c)

	// Extract user ID from the request context for authentication
	user, err := mcontext.GetUserID(ctx.Context)
	if err != nil {
		// If user authentication fails, return an account error
		apiErr := core.NewAccountError(core.ErrKeyLoginFailed, nil)
		_ = ctx.Error(apiErr, apiErr.HttpStatus())
		return nil
	}

	// Use queryutilhttp.ProcessListRequest for standardized HTTP handling
	return queryutilhttp.ProcessListRequest(
		c.Response(),
		c.Request(),
		"streams",
		// Create service function that includes user filtering
		func(filters []queryutil.CrudFilter, sorts []queryutil.Sort, pagination queryutil.Pagination) ([]*db.Stream, int64, error) {
			return a.uploadService.ListStreams(ctx.Request().Context(), uint(user), filters, sorts, pagination)
		},
		// Convert domain entities to DTOs
		func(stream *db.Stream) *dto.StreamResponse {
			return &dto.StreamResponse{
				ID:                uint64(stream.ID),
				StreamHash:        stream.StreamHash,
				SDHash:            stream.SDHash,
				StreamName:        stream.StreamName,
				StreamType:        stream.StreamType,
				SuggestedFileName: stream.SuggestedFileName,
				CreatedAt:         stream.CreatedAt,
				UpdatedAt:         stream.UpdatedAt,
			}
		},
		// Configure search and sort options
		queryutilhttp.WithSearchConfig(&queryutil.GlobalSearchConfig{
			SearchableColumns: []string{"stream_name", "sd_hash"},
		}),
	)
}

// handleStreamDelete handles stream deletion requests
// This function processes delete requests for LBRY streams, handling authentication,
// request validation, and stream deletion.
func (a *API) handleStreamDelete(c echo.Context) error {
	// Extract request context and authenticate user
	ctx := httputil.Context(c)

	// Extract user ID from the request context for authentication
	user, err := mcontext.GetUserID(ctx.Context)
	if err != nil {
		// If user authentication fails, return an account error
		apiErr := core.NewAccountError(core.ErrKeyLoginFailed, nil)
		_ = ctx.Error(apiErr, apiErr.HttpStatus())
		return nil
	}

	// Parse and validate request body using httputil
	var deleteRequest dto.StreamDeleteRequest
	_, ok := httputil.DecodeAndValidateRequest(ctx, &deleteRequest)
	if !ok {
		return nil
	}

	// Delete the stream
	err = a.uploadService.DeleteStream(ctx.Request().Context(), user, deleteRequest.SDHash)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			_ = ctx.Error(NewError(ErrKeyStreamNotFound, err), http.StatusNotFound)
		} else {
			_ = ctx.Error(NewError(ErrKeyStreamDeleteFailed, err), http.StatusInternalServerError)
		}
		return nil
	}

	// Return success response
	return ctx.NoContent(http.StatusNoContent)
}
