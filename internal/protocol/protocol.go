package protocol

import (
	"errors"
	"io"

	"go.lumeweb.com/portal-plugin-lbry/internal"
	pluginConfig "go.lumeweb.com/portal-plugin-lbry/internal/config"
	"go.lumeweb.com/portal/config"
	"go.lumeweb.com/portal/core"
	"gorm.io/gorm"
)

var _ core.Protocol = (*Protocol)(nil)
var _ core.StorageProtocol = (*Protocol)(nil)

// Helper functions for operation names
func confirmOperationName() string {
	return core.OperationName(internal.ProtocolName, "confirm")
}

type Protocol struct {
	ctx core.Context
	db  *gorm.DB
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

			return nil
		}),
		core.ContextWithExitFunc(func(ctx core.Context) error {
			return nil
		}),
	)

	return proto, opts, nil
}
