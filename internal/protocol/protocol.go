package protocol

import (
	"errors"
	"io"

	"github.com/knadh/koanf/v2"
	"go.lumeweb.com/liblbry"
	"go.lumeweb.com/liblbry/server"
	"go.lumeweb.com/portal-plugin-lbry/internal"
	pluginConfig "go.lumeweb.com/portal-plugin-lbry/internal/config"
	"go.lumeweb.com/portal/config"
	"go.lumeweb.com/portal/core"
	"gorm.io/gorm"
)

var _ core.Protocol = (*Protocol)(nil)
var _ core.StorageProtocol = (*Protocol)(nil)

type Protocol struct {
	ctx  core.Context
	db   *gorm.DB
	node server.Server
}

func (p Protocol) Workflows() []core.WorkflowDefinition {
	return []core.WorkflowDefinition{}
}

func (p Protocol) Operations() []core.Operation {
	return []core.Operation{}
}

func (p Protocol) Name() string {
	return internal.ProtocolName
}

func (p Protocol) DisplayName() string {
	return internal.ProtocolDisplayName
}

func (p Protocol) EncodeFileName(_ core.StorageHash) string {
	return ""
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
	return server.NewServerBuilder().
		WithStorage(store).
		WithPeer().
		WithReflector().
		WithDHT().
		WithDHTSeedNodes(seedNodes...).
		WithLogger(ctx.Logger().Logger).Build()
}
