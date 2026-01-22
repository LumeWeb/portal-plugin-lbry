package info

import (
	"go.lumeweb.com/portal-plugin-lbry/build"
	pluginCore "go.lumeweb.com/portal-plugin-lbry/core"
	"go.lumeweb.com/portal-plugin-lbry/internal"
	"go.lumeweb.com/portal-plugin-lbry/internal/api"
	"go.lumeweb.com/portal-plugin-lbry/internal/db/migrations"
	"go.lumeweb.com/portal-plugin-lbry/internal/protocol"
	"go.lumeweb.com/portal-plugin-lbry/internal/service/devices"
	"go.lumeweb.com/portal-plugin-lbry/internal/service/upload"
	"go.lumeweb.com/portal/core"
	portal_plugin_lbry "go.lumeweb.com/web/go/portal-plugin-lbry"
)

func GetPluginInfo() core.PluginInfo {
	return core.PluginInfo{
		ID:       internal.ProtocolName,
		Version:  build.GetInfo(),
		API:      api.NewAPI,
		Protocol: protocol.NewProtocol,
		Services: func() ([]core.ServiceInfo, error) {
			return []core.ServiceInfo{
				{
					ID:      pluginCore.UPLOAD_SERVICE,
					Factory: upload.NewUploadService,
				},
				{
					ID:      pluginCore.DEVICE_SERVICE,
					Factory: devices.NewDeviceService,
				},
			}, nil
		},
		Migrations: core.DBMigration{
			core.DB_TYPE_SQLITE: migrations.GetSQLite(),
			core.DB_TYPE_MYSQL:  migrations.GetMySQL(),
		},
		WebBundles: core.NewWebBundles(core.NewWebBundle(portal_plugin_lbry.GetFS(), core.WithWebBundleTargetApps("dashboard"))),
	}
}
