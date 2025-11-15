package config

import (
	z "github.com/Oudwins/zog"
	"go.lumeweb.com/portal/config"
)

var _ config.Defaults = (*ProtocolConfig)(nil)

var bootstrapPeers = []string{
	"s1.lbry.network",
}

type ProtocolConfig struct {
	Peers []string `config:"peers"`
}

func (c ProtocolConfig) Defaults() map[string]any {
	return map[string]any{}
}

func (c ProtocolConfig) Schema() z.ZogSchema {
	return z.Struct(z.Shape{
		"Peers": z.Slice(z.String()),
	})
}
