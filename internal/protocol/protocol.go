package protocol

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"

	"github.com/knadh/koanf/v2"
	"go.lumeweb.com/liblbry"
	"go.lumeweb.com/liblbry/server"
	"go.lumeweb.com/portal-plugin-lbry/internal"
	pluginConfig "go.lumeweb.com/portal-plugin-lbry/internal/config"
	"go.lumeweb.com/portal/config"
	"go.lumeweb.com/portal/core"
	"go.lumeweb.com/portal/db/models/data_models"
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
	}
}

func (p Protocol) Operations() []core.Operation {
	return []core.Operation{
		NewPostUploadOperation(p.ctx),
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

// getFirstPublicIP attempts to get the first public IP address
func getFirstPublicIP() (string, error) {
	// Try to get public IP from local non-loopback interfaces
	interfaces, err := net.Interfaces()
	if err != nil {
		return "", fmt.Errorf("failed to get network interfaces: %w", err)
	}

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

			// Skip loopback and link-local addresses
			if ip == nil || ip.IsLoopback() || ip.IsLinkLocalUnicast() {
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

	// Get the first public IP for DHT address
	publicIP, err := getFirstPublicIP()
	if err != nil {
		ctx.Logger().Warn("Failed to get public IP for DHT, using empty address", zap.Error(err))
		publicIP = ""
	} else {
		ctx.Logger().Info("Using public IP for DHT", zap.String("ip", publicIP))
	}

	// Construct DHT address with IP and port
	dhtAddress := ""
	if publicIP != "" && protoCfg != nil {
		dhtAddress = net.JoinHostPort(publicIP, fmt.Sprintf("%d", protoCfg.DHTPort))
	}

	return server.NewServerBuilder().
		WithStorage(store).
		WithPeer(int(protoCfg.PeerPort)).
		WithReflector(int(protoCfg.ReflectorPort)).
		WithDHT().
		WithDHTAddress(dhtAddress).
		WithDHTSeedNodes(seedNodes...).
		WithLogger(ctx.Logger().Logger).Build()
}
