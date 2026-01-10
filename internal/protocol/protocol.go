package protocol

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"testing"

	"github.com/knadh/koanf/v2"
	"go.lumeweb.com/liblbry"
	"go.lumeweb.com/liblbry/blob/transfer"
	"go.lumeweb.com/liblbry/blob/transfer/peer_transfer"
	"go.lumeweb.com/liblbry/protocol"
	"go.lumeweb.com/liblbry/server"
	"go.lumeweb.com/liblbry/stream"
	pluginCore "go.lumeweb.com/portal-plugin-lbry/core"
	"go.lumeweb.com/portal-plugin-lbry/internal"
	pluginConfig "go.lumeweb.com/portal-plugin-lbry/internal/config"
	"go.lumeweb.com/portal-plugin-lbry/internal/protocol/util"
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

type accessControl struct {
	ctx           core.Context
	deviceService pluginCore.DeviceService
}

// newAccessControl creates a new accessControl instance with proper error handling
func newAccessControl(ctx core.Context) (*accessControl, error) {
	// Get device service
	deviceSvc := core.GetService[pluginCore.DeviceService](ctx, pluginCore.DEVICE_SERVICE)
	if deviceSvc == nil {
		return nil, fmt.Errorf("device service not available for access control")
	}

	return &accessControl{
		ctx:           ctx,
		deviceService: deviceSvc,
	}, nil
}

func (a *accessControl) Allow(ctx context.Context, _ string, peerIP string) bool {
	logger := a.ctx.Logger()

	// If device service is not available, deny access by default
	if a.deviceService == nil {
		logger.Debug("Device service not available, denying access by default")
		return false
	}

	if src, exist := protocol.GetSourceFromContext(ctx); exist {
		if src == protocol.SourcePeer {
			return true
		}
	}

	// Check if device exists with this IP address
	device, err := a.deviceService.GetDeviceByIPAddress(a.ctx.GetContext(), peerIP)
	if err != nil {
		logger.Error("Error checking device by IP address", zap.String("ip", peerIP), zap.Error(err))
		return false
	}

	// If device exists, allow access
	if device != nil {
		logger.Debug("Allowing access for whitelisted device", zap.String("ip", peerIP), zap.Uint("device_id", device.ID))
		return true
	}

	logger.Debug("Denying access for non-whitelisted IP", zap.String("ip", peerIP))
	return false
}

type Protocol struct {
	*core.BaseComponent
	node           server.Server
	reflectorStore *ReflectorStore
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
		p.newPinWorkflow(),
		p.newUploadWorkflow(),
		p.newTUSUploadWorkflow(),
		p.newReflectorAssemblyWorkflow(),
	}
}

func (p Protocol) Operations() []core.Operation {
	return []core.Operation{
		NewRetrieveOperation(p.Context()),
		NewPostUploadOperation(p.Context()),
		NewReflectorAssemblyOperation(p.Context()),
		service.NewTUSOperationHandler(p.Context(), p, func(ctx context.Context, helper core.OperationHelper, request *models.Request, tsReq *models.TUSRequest) error {
			// Validate request using shared utility
			if err := ValidateRequest(request); err != nil {
				return err
			}
			userID := *request.UserID

			// Get TUS handler
			tusHandler := core.GetAPI(internal.ProtocolName).(core.APITusHandler).GetTusHandler()

			// Create upload processor
			processor := NewUploadProcessor(helper.Context())

			// Cast to storage protocol with type safety
			storageProtocol, err := util.CastToStorageProtocol(helper.Protocol())
			if err != nil {
				return err
			}

			// Create TUS upload source
			source := NewTUSUploadSource(tusHandler, tsReq.TUSUploadID, storageProtocol)

			// Initialize the source to fetch metadata and size
			err = source.Initialize(ctx)
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

func (p Protocol) ID() string {
	return p.Name()
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

func (p Protocol) GetConfig() config.ProtocolConfig {
	return &pluginConfig.ProtocolConfig{}
}
func NewProtocol() (core.Protocol, []core.ContextBuilderOption, error) {
	proto := &Protocol{}

	opts := core.ContextOptions(
		core.ContextWithStartupFunc(func(ctx core.Context) error {
			// Initialize ReflectorStore
			reflectorStore, err := NewReflectorStore(ctx)
			if err != nil {
				return fmt.Errorf("failed to create reflector store: %w", err)
			}
			proto.reflectorStore = reflectorStore

			node, err := buildServer(ctx, reflectorStore)
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

func (p *Protocol) ReflectorStore() *ReflectorStore {
	return p.reflectorStore
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

func (p Protocol) newPinWorkflow() core.WorkflowDefinition {
	return core.WorkflowDefinition{
		Name:                 PIN_WORKFLOW,
		AutoTriggerFirstStep: true,
		Steps: []core.OperationStep{
			p.newRetryStep(core.RetrieveOperationName(p.Name())),
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

func (p Protocol) newReflectorAssemblyWorkflow() core.WorkflowDefinition {
	return core.WorkflowDefinition{
		Name:                 REFLECTOR_ASSEMBLY_WORKFLOW,
		AutoTriggerFirstStep: true,
		Steps: []core.OperationStep{
			p.newRetryStep(core.OperationName(internal.ProtocolName, REFLECTOR_ASSEMBLY_OPERATION)),
		},
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

func buildServer(ctx core.Context, reflectorStore *ReflectorStore) (server.Server, error) {
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

	if protoCfg != nil && len(protoCfg.DHTSeedPeers) > 0 {
		seedNodes = protoCfg.DHTSeedPeers
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

	// Build transfer options with fixed peers if configured
	// Use configured values or defaults
	dhtRetryAttempts := 0
	maxPeers := math.MaxInt

	if protoCfg != nil {
		if protoCfg.TransferDHTRetryAttempts > 0 {
			dhtRetryAttempts = protoCfg.TransferDHTRetryAttempts
		}
		if protoCfg.TransferMaxPeers != -1 {
			maxPeers = protoCfg.TransferMaxPeers
		}
	}

	transferOptions := []transfer.TransferOption{
		peer_transfer.WithPeerTransferDHTRetryAttemptsOption(dhtRetryAttempts),
		peer_transfer.WithPeerTransferMaxPeersOption(maxPeers),
	}

	// Add fixed peers to transfer options if configured
	if protoCfg != nil && len(protoCfg.FixedPeers) > 0 {
		ctx.Logger().Info("Adding fixed peers to transfer configuration",
			zap.Strings("fixed_peers", protoCfg.FixedPeers))
		transferOptions = append(transferOptions, peer_transfer.WithPeerTransferFixedPeersOption(protoCfg.FixedPeers))
	}

	// Build DHT options
	dhtOptions := []protocol.DHTOption{
		protocol.WithDHTSeedNodes(seedNodes),
	}

	// Add full DHT network scan if enabled in config
	if protoCfg != nil && protoCfg.FullDHT {
		ctx.Logger().Info("Full DHT network scan enabled - this may impact startup performance")
		dhtOptions = append(dhtOptions, protocol.WithDHTNetworkScan(true))
	}

	// Add DHT node ID if configured
	if protoCfg != nil && protoCfg.DHTNodeID != "" {
		ctx.Logger().Info("Using configured DHT node ID", zap.String("node_id", protoCfg.DHTNodeID))
		dhtOptions = append(dhtOptions, protocol.WithDHTNodeID(protoCfg.DHTNodeID))
	}

	builder := server.NewServerBuilder().
		WithStorage(store).
		WithDHT().
		WithDHTAddress(dhtAddress).
		WithDHTOptions(dhtOptions...).
		WithDefaultAcquirer().
		WithTransferOptions(transferOptions...).
		WithLogger(ctx.Logger().Logger)

	if protoCfg != nil {
		// Create access control
		accessControlInstance, err := newAccessControl(ctx)
		if err != nil {
			ctx.Logger().Error("Failed to create access control", zap.Error(err))
			return nil, err
		}

		builder = builder.
			WithPeer(int(protoCfg.PeerPort)).
			WithReflector(int(protoCfg.ReflectorPort)).
			WithReflectorStore(reflectorStore).
			WithAccessControl(accessControlInstance)
	}

	return builder.Build()
}
