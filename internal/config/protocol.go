package config

import (
	z "github.com/Oudwins/zog"
	"go.lumeweb.com/portal/config"
)

var _ config.Defaults = (*ProtocolConfig)(nil)

var BootstrapPeers = []string{
	"s1.lbry.network",
}

type ProtocolConfig struct {
	Peers         []string `config:"peers"`
	PeerPort      uint     `config:"peer_port"`
	ReflectorPort uint     `config:"reflector_port"`
	DHTPort       uint     `config:"dht_port"`
}

func (c ProtocolConfig) Defaults() map[string]any {
	return map[string]any{
		"PeerPort":      5567,
		"ReflectorPort": 5666,
		"DHTPort":       4444,
	}
}

func (c ProtocolConfig) Schema() z.ZogSchema {
	return z.Struct(z.Shape{
		"Peers":         z.Slice(z.String()),
		"PeerPort":      z.Uint(),
		"ReflectorPort": z.Uint(),
		"DHTPort":       z.Uint(),
	})
}
