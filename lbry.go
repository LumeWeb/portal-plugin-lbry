package lbry

import (
	"go.lumeweb.com/portal-plugin-lbry/build"
	"go.lumeweb.com/portal-plugin-lbry/internal"
	"go.lumeweb.com/portal-plugin-lbry/internal/api"
	"go.lumeweb.com/portal-plugin-lbry/internal/protocol"
	"go.lumeweb.com/portal/core"
)

func init() {
	core.RegisterPlugin(core.PluginInfo{
		ID:       internal.ProtocolName,
		Version:  build.GetInfo(),
		API:      api.NewAPI,
		Protocol: protocol.NewProtocol,
		Services: func() ([]core.ServiceInfo, error) {
			return []core.ServiceInfo{}, nil
		},
	})
}
