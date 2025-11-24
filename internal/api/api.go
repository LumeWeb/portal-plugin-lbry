package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/labstack/echo/v4"
	"github.com/tus/tusd/v2/pkg/handler"
	"go.lumeweb.com/httputil"
	lbrycrypto "go.lumeweb.com/liblbry/crypto"
	"go.lumeweb.com/portal-middleware/auth/jwt"
	mcontext "go.lumeweb.com/portal-middleware/context"
	"go.lumeweb.com/portal-middleware/middleware"
	pluginCore "go.lumeweb.com/portal-plugin-lbry/core"
	"go.lumeweb.com/portal-plugin-lbry/internal"
	"go.lumeweb.com/portal-plugin-lbry/internal/api/dto"
	pluginConfig "go.lumeweb.com/portal-plugin-lbry/internal/config"
	"go.lumeweb.com/portal-plugin-lbry/internal/db"
	"go.lumeweb.com/portal-plugin-lbry/internal/protocol"
	"go.lumeweb.com/portal-router"
	"go.lumeweb.com/portal/config"
	"go.lumeweb.com/portal/core"
	"go.lumeweb.com/portal/event"
	"go.lumeweb.com/portal/service"
	"go.lumeweb.com/queryutil"
	queryutilhttp "go.lumeweb.com/queryutil/http"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

var _ core.API = (*API)(nil)
var _ core.APITusHandler = (*API)(nil)

const TUS_HTTP_ROUTE = "/api/streams/upload/tus"

type API struct {
	ctx             core.Context
	config          config.Manager
	logger          *core.Logger
	workflowService core.WorkflowService
	uploadService   pluginCore.UploadService
	tus             core.TusHandler
}

func (a *API) OpenAPIInfo() router.APIInfoDefinition {
	return router.APIInfo().
		Title("LBRY Stream API").
		Description("LBRY Stream API for uploading and managing streams on the LBRY network. Supports stream uploads with metadata management and LBRY network operations.")
}

func NewAPI() (core.API, []core.ContextBuilderOption, error) {
	api := &API{}
	return api, core.ContextOptions(
		core.ContextWithStartupFunc(func(ctx core.Context) error {
			api.ctx = ctx
			api.config = ctx.Config()
			api.logger = ctx.APILogger(api)
			api.workflowService = core.GetService[core.WorkflowService](ctx, core.WORKFLOW_SERVICE)
			api.uploadService = core.GetService[pluginCore.UploadService](ctx, pluginCore.UPLOAD_SERVICE)

			sproto, err := protocol.GetStorageProtocol()
			if err != nil {
				return fmt.Errorf("failed to get storage protocol: %w", err)
			}
			proto := core.GetProtocol(internal.ProtocolName)
			event.OnBootStartupFuncsCompleted(ctx, func(ctx core.Context) error {
				var _tus core.TusHandler
				var err error
				_tus, err = service.CreateTusHandler(ctx, core.TUSHandlerConfig{
					Protocol: proto,
					BasePath: TUS_HTTP_ROUTE,
					CreatedUploadHandler: service.TUSDefaultUploadCreatedHandler(ctx, func(hook handler.HookEvent, uploaderId uint) (core.StorageHash, error) {
						return nil, nil
					}, nil),
					UploadProgressHandler:   service.TUSDefaultUploadProgressHandler(ctx),
					TerminatedUploadHandler: service.TUSDefaultUploadTerminatedHandler(ctx),
					CompletedUploadHandler: service.TUSDefaultUploadCompletedHandler(ctx, nil, protocol.TUS_UPLOAD_WORKFLOW,
						func(handlr core.TusHandler, hook handler.HookEvent) (core.StorageHash, error) {
							upload, err := handlr.UploadReader(ctx, hook.Upload.ID, sproto, 0)
							if err != nil {
								return nil, err
							}
							defer closeUpload(upload, api.logger)

							return getStreamUploadHash(upload, api.logger)
						},
					),
				})

				if err != nil {
					return fmt.Errorf("failed to create tus handler: %w", err)
				}
				api.tus = _tus

				return nil
			})

			return nil
		}),
	), nil
}

func (a *API) Name() string {
	return internal.ProtocolName
}

func (a *API) Subdomain() string {
	return internal.ProtocolName
}

func (a *API) AuthTokenName() string {
	return core.AUTH_TOKEN_NAME
}

func (a *API) GetTusHandler() core.TusHandler {
	return a.tus
}

func (a *API) Config() config.APIConfig {
	return &pluginConfig.APIConfig{}
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
		return err
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
		return err
	}

	// Generate and return response
	// Return a successful response containing the upload hash (LBRY hash)
	// The client can use this hash to reference and retrieve the uploaded content

	lbryHash, err := internal.CIDToLBRYHash(uploadCID)
	if err != nil {
		// If conversion fails, return the error
		return fmt.Errorf("failed to convert CID to LBRY hash: %w", err)
	}

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
		// If workflow initiation fails, return the error
		return err
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

	// Get the upload service
	uploadSvc := core.GetService[pluginCore.UploadService](a.ctx, pluginCore.UPLOAD_SERVICE)
	if uploadSvc == nil {
		_ = ctx.Error(NewError(ErrKeyStreamListFailed, fmt.Errorf("upload service not available")), http.StatusInternalServerError)
		return nil
	}

	// Use queryutilhttp.ProcessListRequest for standardized HTTP handling
	return queryutilhttp.ProcessListRequest(
		c.Response(),
		c.Request(),
		"streams",
		// Create service function that includes user filtering
		func(filters []queryutil.CrudFilter, sorts []queryutil.Sort, pagination queryutil.Pagination) ([]*db.Stream, int64, error) {
			return uploadSvc.ListStreams(ctx.Request().Context(), uint(user), filters, sorts, pagination)
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

	// Get the upload service
	uploadSvc := core.GetService[pluginCore.UploadService](a.ctx, pluginCore.UPLOAD_SERVICE)
	if uploadSvc == nil {
		_ = ctx.Error(NewError(ErrKeyStreamDeleteFailed, fmt.Errorf("upload service not available")), http.StatusInternalServerError)
		return nil
	}

	// Delete the stream
	err = uploadSvc.DeleteStream(ctx.Request().Context(), user, deleteRequest.SDHash)
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

func (a *API) Configure(r router.Router, accessService core.AccessService) error {
	authMw := middleware.AuthMiddleware(a.ctx, middleware.WithAuthPurpose(jwt.PurposeLogin, jwt.PurposeAPI))

	// Create reusable schema for StreamResponse
	streamSchema := queryutil.NewSchemaProvider().ForType(&dto.StreamResponse{})

	// Define routes using the correct pattern
	routes := router.DefineRoutes(
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
				router.WithSummary("Unpin a stream"),
				router.WithDescription("Unpin this stream for the authenticated user. The stream itself and other users' pins remain. Only streams that the user has pinned can be unpinned."),
				router.WithSuccessResponse(http.StatusNoContent, "Stream unpinned successfully"),
			),
		),
	)

	// Register routes with proper error handling
	group, err := r.Group("/api")
	if err != nil {
		return err
	}

	err = router.RegisterRoutes(group, accessService, a.Subdomain(), routes, router.WithMiddlewares(authMw), router.WithCors())
	if err != nil {
		return err
	}

	err = a.tus.SetupRoute(r, a.Subdomain(), true, false, TUS_HTTP_ROUTE)
	if err != nil {
		return err
	}

	return nil
}

func closeUpload(upload io.ReadCloser, logger *core.Logger) {
	err := upload.Close()
	if err != nil {
		logger.Error("Failed to close reader", zap.Error(err))
	}
}

func getStreamUploadHash(upload io.ReadCloser, logger *core.Logger) (core.StorageHash, error) {
	hash, err := lbrycrypto.NewHasher().HashReader(upload)
	if err != nil {
		logger.Error("Failed to hash stream upload", zap.Error(err))
		return nil, fmt.Errorf("failed to hash stream upload: %w", err)
	}

	storageHash, err := internal.LBRYHashToStorageHash(hash)
	if err != nil {
		logger.Error("Failed to convert LBRY hash to storage hash", zap.Error(err), zap.String("hash", hash))
		return nil, fmt.Errorf("failed to convert LBRY hash to storage hash: %w", err)
	}

	return storageHash, nil
}
