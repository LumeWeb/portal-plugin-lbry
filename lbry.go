package lbry

import (
	"go.lumeweb.com/portal-plugin-lbry/internal/info"
	"go.lumeweb.com/portal/core"
)

func init() {
	core.RegisterPlugin(info.GetPluginInfo())
}
