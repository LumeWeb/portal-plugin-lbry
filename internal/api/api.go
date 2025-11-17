package api

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v4"
	"go.lumeweb.com/httputil"
	"go.lumeweb.com/portal-middleware/auth/jwt"
	mcontext "go.lumeweb.com/portal-middleware/context"
	"go.lumeweb.com/portal-middleware/middleware"
	pluginCore "go.lumeweb.com/portal-plugin-lbry/core"
	"go.lumeweb.com/portal-plugin-lbry/internal"
	"go.lumeweb.com/portal-plugin-lbry/internal/api/dto"
	pluginConfig "go.lumeweb.com/portal-plugin-lbry/internal/config"
	"go.lumeweb.com/portal-plugin-lbry/internal/protocol"
	"go.lumeweb.com/portal-router"
	"go.lumeweb.com/portal/config"
	"go.lumeweb.com/portal/core"
)

var _ core.API = (*API)(nil)

type API struct {
	ctx             core.Context
	config          config.Manager
	logger          *core.Logger
	workflowService core.WorkflowService
	uploadService   pluginCore.UploadService
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

	// Set filename from prepared upload if SuggestedFileName is empty
	if metadata.SuggestedFileName == "" {
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
	// Return a successful response containing the upload hash (CID)
	// The client can use this hash to reference and retrieve the uploaded content
	return httputil.EncodeResponse(ctx, &dto.PostStreamUploadResponse{}, &dto.PostStreamUploadResponse{
		UploadHash: uploadCID.String(),
	})
}

func (a *API) Configure(r router.Router, accessService core.AccessService) error {
	authMw := middleware.AuthMiddleware(a.ctx, middleware.WithAuthPurpose(jwt.PurposeLogin, jwt.PurposeAPI))

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
				router.WithSuccessResponse(http.StatusOK, "File uploaded successfully", router.WithJSONContent(&dto.PostStreamUploadResponse{})),
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

	return nil
}
