package config

import (
	z "github.com/Oudwins/zog"
	"go.lumeweb.com/portal/config"
)

var _ config.Defaults = (*ProtocolConfig)(nil)

type ProtocolConfig struct {
}

func (c ProtocolConfig) Defaults() map[string]any {
	return map[string]any{}
}

func (c ProtocolConfig) Schema() z.ZogSchema {
	return z.Struct(z.Shape{})
}
