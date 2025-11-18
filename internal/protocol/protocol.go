package protocol

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"

	"github.com/knadh/koanf/v2"
	"go.lumeweb.com/liblbry"
	"go.lumeweb.com/liblbry/server"
	"go.lumeweb.com/liblbry/stream"
	"go.lumeweb.com/portal-plugin-lbry/internal"
	pluginConfig "go.lumeweb.com/portal-plugin-lbry/internal/config"
	"go.lumeweb.com/portal/config"
	"go.lumeweb.com/portal/core"
	"go.lumeweb.com/portal/db/models"
	"go.lumeweb.com/portal/db/models/data_models"
	"go.lumeweb.com/portal/service"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

var _ core.Protocol = (*Protocol)(nil)
var _ core.StorageProtocol = (*Protocol)(nil)
var _ core.ProtocolPinHandler = (*Protocol)(nil)

type Protocol struct {
	ctx  core.Context
	db   *gorm.DB
	node server.Server
}

func (p Protocol) CreateProtocolPin(_ context.Context, _ uint, _ any) error {
	return nil
}
func (p Protocol) GetProtocolPin(_ context.Context, _ *gorm.DB, _ uint) (any, error) {
	return nil, nil
}

func (p Protocol) UpdateProtocolPin(_ context.Context, _ uint, _ any) error {
	return nil
}

func (p Protocol) DeleteProtocolPin(_ context.Context, _ uint) error {
	return nil
}

func (p Protocol) QueryProtocolPin(_ context.Context, _ any) *gorm.DB {
	return nil
}

func (p Protocol) GetProtocolPinModel() data_models.PinDataModel {
	return nil
}

func (p Protocol) Workflows() []core.WorkflowDefinition {
	return []core.WorkflowDefinition{
		p.newUploadWorkflow(),
		p.newTUSUploadWorkflow(),
	}
}

func (p Protocol) Operations() []core.Operation {
	return []core.Operation{
		NewPostUploadOperation(p.ctx),
		service.NewTUSOperationHandler(p.ctx, p, func(ctx context.Context, helper core.OperationHelper, request *models.Request, tsReq *models.TUSRequest) error {
			// Validate user ID before processing
			if request.UserID == nil || *request.UserID == 0 {
				return fmt.Errorf("user ID is required")
			}
			userID := *request.UserID

			// Get TUS handler
			tusHandler := core.GetAPI(internal.ProtocolName).(core.APITusHandler).GetTusHandler()

			// Create upload processor
			processor := NewUploadProcessor(helper.Context())

			// Create TUS upload source
			source := NewTUSUploadSource(tusHandler, tsReq.TUSUploadID, helper.Protocol().(core.StorageProtocol))

			// Initialize the source to fetch metadata and size
			err := source.Initialize(ctx)
			if err != nil {
				return err
			}

			// Process the upload using shared processor
			_, err = processor.ProcessStreamUpload(ctx, source, uint64(userID))
			if err != nil {
				return err
			}

			return nil
		}),
	}
}

func (p Protocol) Name() string {
	return internal.ProtocolName
}

func (p Protocol) DisplayName() string {
	return internal.ProtocolDisplayName
}

func (p Protocol) EncodeFileName(hash core.StorageHash) string {
	return hash.String()
}

func (p Protocol) Hash(_ io.Reader, _ uint64) (core.StorageHash, error) {
	return nil, errors.New("not implemented")
}

func (p Protocol) Config() config.ProtocolConfig {
	return &pluginConfig.ProtocolConfig{}
}
func NewProtocol() (core.Protocol, []core.ContextBuilderOption, error) {
	proto := &Protocol{}

	opts := core.ContextOptions(
		core.ContextWithStartupFunc(func(ctx core.Context) error {
			proto.ctx = ctx
			proto.db = ctx.DB()

			node, err := buildServer(ctx)
			if err != nil {
				return err
			}

			proto.node = node

			err = node.Start(ctx)
			if err != nil {
				return err
			}

			return nil
		}),
		core.ContextWithExitFunc(func(ctx core.Context) error {
			if proto.node == nil {
				return nil
			}
			err := proto.node.Stop(ctx)
			if err != nil {
				return err
			}

			return nil
		}),
	)

	return proto, opts, nil
}

func (p *Protocol) Node() server.Server {
	return p.node
}

func (p Protocol) newRetryStep(operation string) core.OperationStep {
	return core.OperationStep{
		Operation:       operation,
		FailureBehavior: core.RetryStep,
		ID:              operation,
	}
}

func (p Protocol) newUploadWorkflow() core.WorkflowDefinition {
	return core.WorkflowDefinition{
		Name:                 UPLOAD_WORKFLOW,
		AutoTriggerFirstStep: true,
		Steps: []core.OperationStep{
			p.newRetryStep(core.PostUploadOperationName(p.Name())),
		},
	}
}

func (p Protocol) newTUSUploadWorkflow() core.WorkflowDefinition {
	return core.WorkflowDefinition{
		Name:                 TUS_UPLOAD_WORKFLOW,
		AutoTriggerFirstStep: true,
		Steps: append([]core.OperationStep{
			p.newRetryStep(core.TUSUploadOperationName(p.Name())),
		}),
	}
}

// applyMetadataToSDBlob applies metadata from TUS upload to an SD blob
func applyMetadataToSDBlob(sdBlob *stream.SDBlob, metadata map[string]string) {
	// Apply metadata from TUS upload
	if streamName, ok := metadata["stream_name"]; ok && streamName != "" {
		sdBlob.StreamName = streamName
	}

	if suggestedFileName, ok := metadata["suggested_file_name"]; ok && suggestedFileName != "" {
		sdBlob.SuggestedFileName = suggestedFileName
	}
}

// isPrivateOrReservedIP checks if an IP address is in private or reserved ranges
func isPrivateOrReservedIP(ip net.IP) bool {
	if ip.IsLoopback() || ip.IsLinkLocalUnicast() {
		return true
	}

	// Check IPv4 private and reserved ranges
	if ip4 := ip.To4(); ip4 != nil {
		// RFC1918 private ranges
		// 10.0.0.0/8
		if ip4[0] == 10 {
			return true
		}
		// 172.16.0.0/12 (172.16.0.0 - 172.31.255.255)
		if ip4[0] == 172 && ip4[1] >= 16 && ip4[1] <= 31 {
			return true
		}
		// 192.168.0.0/16
		if ip4[0] == 192 && ip4[1] == 168 {
			return true
		}
		// 100.64.0.0/10 (Carrier-grade NAT)
		if ip4[0] == 100 && ip4[1] >= 64 && ip4[1] <= 127 {
			return true
		}
		// 169.254.0.0/16 (Link-local, already checked but being thorough)
		if ip4[0] == 169 && ip4[1] == 254 {
			return true
		}
	} else {
		// IPv6 private and reserved ranges
		// ::1 (loopback, already checked but being thorough)
		if ip.IsLoopback() {
			return true
		}
		// fc00::/7 (Unique local addresses)
		if ip[0] >= 0xfc && ip[0] <= 0xfd {
			return true
		}
		// fe80::/10 (Link-local, already checked but being thorough)
		if ip[0] == 0xfe && (ip[1]&0xc0) == 0x80 {
			return true
		}
	}

	return false
}

// getFirstPublicIP attempts to get the first public IP address
func getFirstPublicIP() (string, error) {
	// Try to get public IP from local non-loopback interfaces
	interfaces, err := net.Interfaces()
	if err != nil {
		return "", fmt.Errorf("failed to get network interfaces: %w", err)
	}

	// Check if we're in a test environment
	isTestMode := testing.Testing()

	for _, iface := range interfaces {
		// Skip loopback and down interfaces
		if iface.Flags&net.FlagLoopback != 0 || iface.Flags&net.FlagUp == 0 {
			continue
		}

		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}

		for _, addr := range addrs {
			var ip net.IP
			switch v := addr.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			}

			// Skip private, reserved, loopback and link-local addresses
			// unless we're in test mode, then allow private IPs
			if ip == nil || (!isTestMode && isPrivateOrReservedIP(ip)) {
				continue
			}

			// Prefer IPv4 addresses
			if ip.To4() != nil {
				return ip.String(), nil
			}
		}
	}

	return "", fmt.Errorf("no suitable public IP address found")
}

func buildServer(ctx core.Context) (server.Server, error) {
	// Create disk storage factory using the helper from store.go
	factory, err := liblbry.CreateStorageFactoryWithOptions[StoreFactory](WithContext(ctx))
	if err != nil {
		return nil, err
	}

	// Create empty config (unused by CreateStore)
	cfg := koanf.New(".")

	// Create the disk store
	store, err := factory.CreateStore(cfg)
	if err != nil {
		return nil, err
	}

	protoCfg := core.GetProtocolConfig[*pluginConfig.ProtocolConfig](ctx, internal.ProtocolName)

	var seedNodes = pluginConfig.BootstrapPeers

	if protoCfg != nil && len(protoCfg.Peers) > 0 {
		seedNodes = protoCfg.Peers
	}

	// Get the public IP for DHT address
	var publicIP string

	// Use configured PublicIP if set, otherwise auto-detect
	if protoCfg != nil && protoCfg.PublicIP != "" {
		publicIP = protoCfg.PublicIP
		ctx.Logger().Info("Using configured public IP for DHT", zap.String("ip", publicIP))
	} else {
		publicIP, err = getFirstPublicIP()
		if err != nil {
			ctx.Logger().Warn("Failed to get public IP for DHT, using empty address", zap.Error(err))
			publicIP = ""
		} else {
			ctx.Logger().Info("Using auto-detected public IP for DHT", zap.String("ip", publicIP))
		}
	}

	// Construct DHT address with IP and port
	dhtAddress := ""
	if publicIP != "" && protoCfg != nil {
		dhtAddress = net.JoinHostPort(publicIP, fmt.Sprintf("%d", protoCfg.DHTPort))
	}

	builder := server.NewServerBuilder().
		WithStorage(store).
		WithDHT().
		WithDHTAddress(dhtAddress).
		WithDHTSeedNodes(seedNodes...).
		WithLogger(ctx.Logger().Logger)

	if protoCfg != nil {
		builder = builder.
			WithPeer(int(protoCfg.PeerPort)).
			WithReflector(int(protoCfg.ReflectorPort))
	}

	return builder.Build()
}
