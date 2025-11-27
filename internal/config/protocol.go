package config

import (
	z "github.com/Oudwins/zog"
	"go.lumeweb.com/lbry-dht/bits"
	"go.lumeweb.com/liblbry/protocol"
	"go.lumeweb.com/portal/config"
)

var _ config.Defaults = (*ProtocolConfig)(nil)

var BootstrapPeers = protocol.DefaultSeedNodes

type ProtocolConfig struct {
	DHTSeedPeers             []string `config:"dht_seed_peers"`
	FixedPeers               []string `config:"fixed_peers"`
	PeerPort                 uint     `config:"peer_port"`
	ReflectorPort            uint     `config:"reflector_port"`
	DHTPort                  uint     `config:"dht_port"`
	PublicIP                 string   `config:"public_ip"`
	FullDHT                  bool     `config:"full_dht"`
	TransferDHTRetryAttempts int      `config:"transfer_dht_retry_attempts"`
	TransferMaxPeers         int      `config:"transfer_max_peers"`
	DHTNodeID                string   `config:"dht_node_id"`
}

func (c ProtocolConfig) Defaults() map[string]any {
	return map[string]any{
		"PeerPort":                 5567,
		"ReflectorPort":            5666,
		"DHTPort":                  4444,
		"PublicIP":                 "",
		"FullDHT":                  false,
		"TransferDHTRetryAttempts": 5,
		"TransferMaxPeers":         -1, // -1 represents math.MaxInt
		"DHTNodeID":                bits.Rand().Hex(),
	}
}

func (c ProtocolConfig) Schema() z.ZogSchema {
	return z.Struct(z.Shape{
		"DHTSeedPeers":             z.Slice(z.String()),
		"FixedPeers":               z.Slice(z.String()).Optional(),
		"PeerPort":                 z.Uint(),
		"ReflectorPort":            z.Uint(),
		"DHTPort":                  z.Uint(),
		"PublicIP":                 z.String().Optional().IP(),
		"FullDHT":                  z.Bool(),
		"TransferDHTRetryAttempts": z.Int(),
		"TransferMaxPeers":         z.Int(),
		"DHTNodeID":                z.String().Len(96),
	})
}
