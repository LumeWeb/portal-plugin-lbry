package api

import (
	"fmt"
	"io"

	"github.com/tus/tusd/v2/pkg/handler"
	lbrycrypto "go.lumeweb.com/liblbry/crypto"
	"go.lumeweb.com/portal-middleware/auth/jwt"
	"go.lumeweb.com/portal-middleware/middleware"
	pluginCore "go.lumeweb.com/portal-plugin-lbry/core"
	"go.lumeweb.com/portal-plugin-lbry/internal"
	pluginConfig "go.lumeweb.com/portal-plugin-lbry/internal/config"
	"go.lumeweb.com/portal-plugin-lbry/internal/protocol"
	"go.lumeweb.com/portal-router"
	"go.lumeweb.com/portal/config"
	"go.lumeweb.com/portal/core"
	"go.lumeweb.com/portal/event"
	"go.lumeweb.com/portal/service"
	"go.uber.org/zap"
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
	deviceService   pluginCore.DeviceService
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
			api.deviceService = core.GetService[pluginCore.DeviceService](ctx, pluginCore.DEVICE_SERVICE)

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
func (a *API) Configure(r router.Router, accessService core.AccessService) error {
	authMw := middleware.AuthMiddleware(a.ctx, middleware.WithAuthPurpose(jwt.PurposeLogin, jwt.PurposeAPI))

	// Create the API group
	group, err := r.Group("/api")
	if err != nil {
		return err
	}

	// Setup stream routes
	streamRoutes := a.setupStreamRoutes()
	err = router.RegisterRoutes(group, accessService, a.Subdomain(), streamRoutes, router.WithMiddlewares(authMw), router.WithCors())
	if err != nil {
		return err
	}

	// Setup device routes
	deviceRoutes := a.setupDeviceRoutes()
	err = router.RegisterRoutes(group, accessService, a.Subdomain(), deviceRoutes, router.WithMiddlewares(authMw), router.WithCors())
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
