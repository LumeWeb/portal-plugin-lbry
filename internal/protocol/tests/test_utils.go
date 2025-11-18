package tests

import (
	pluginCore "go.lumeweb.com/portal-plugin-lbry/core"
	"go.lumeweb.com/portal-plugin-lbry/internal"
	"go.lumeweb.com/portal-plugin-lbry/internal/api"
	pluginConfig "go.lumeweb.com/portal-plugin-lbry/internal/config"
	"go.lumeweb.com/portal-plugin-lbry/internal/db/migrations"
	"go.lumeweb.com/portal-plugin-lbry/internal/protocol"
	"go.lumeweb.com/portal-plugin-lbry/internal/service/upload"
	pluginTesting "go.lumeweb.com/portal-plugin-lbry/internal/testing"
	"go.lumeweb.com/portal/core"
	coreTesting "go.lumeweb.com/portal/core/testing"
	"go.lumeweb.com/portal/service"
)

func GetCoreTestOptions() []coreTesting.TestContextBuilderOption {
	return []coreTesting.TestContextBuilderOption{
		coreTesting.WithStatefulMockRenterService(),
		coreTesting.WithServiceFactory(core.UPLOAD_SERVICE, service.NewMetadataService),
		coreTesting.WithServiceFactory(core.PIN_SERVICE, service.NewPinService),
		coreTesting.WithServiceFactory(core.STORAGE_SERVICE, service.NewStorageService),
		coreTesting.WithServiceFactory(core.REQUEST_SERVICE, service.NewRequestService),
		coreTesting.WithServiceFactory(core.WORKFLOW_SERVICE, service.NewWorkflowCoordinator),
		coreTesting.WithServiceFactory(core.USER_SERVICE, service.NewUserService),
	}
}

func GetPluginTestOptions() []coreTesting.TestContextBuilderOption {
	var freePeerPort, _ = pluginTesting.GetFreePort()
	var freeDhtPort, _ = pluginTesting.GetFreePort()
	var freeReflectorPort, _ = pluginTesting.GetFreePort()

	return []coreTesting.TestContextBuilderOption{
		coreTesting.WithServiceFactory(pluginCore.UPLOAD_SERVICE, upload.NewUploadService),
		coreTesting.WithProtocol(internal.ProtocolName, protocol.NewProtocol),
		coreTesting.WithConfig("plugin.lbry.protocol.peer_port", uint(freePeerPort)),
		coreTesting.WithConfig("plugin.lbry.protocol.dht_port", uint(freeDhtPort)),
		coreTesting.WithConfig("plugin.lbry.protocol.reflector_port", uint(freeDhtPort)),
		coreTesting.WithProtocolConfig(internal.ProtocolName, pluginConfig.ProtocolConfig{
			PeerPort:      uint(freePeerPort),
			DHTPort:       uint(freeDhtPort),
			ReflectorPort: uint(freeReflectorPort),
		}),
		coreTesting.WithMockS3(),
	}
}

func GetCommonTestOptions() []coreTesting.TestContextBuilderOption {
	return []coreTesting.TestContextBuilderOption{coreTesting.CombineOptions(GetCoreTestOptions(), GetPluginTestOptions())}
}

func GetDbTestOptions() []coreTesting.TestContextBuilderOption {
	return []coreTesting.TestContextBuilderOption{
		coreTesting.WithSQLitePluginMigrations(
			internal.ProtocolName, migrations.GetSQLite(),
		),
	}
}

func GetTUSUploadTestOptions() []coreTesting.TestContextBuilderOption {
	return []coreTesting.TestContextBuilderOption{
		coreTesting.CombineOptions(GetCommonTestOptions(), GetDbTestOptions(),
			coreTesting.WithServiceFactory(core.TUS_SERVICE, service.NewTUSService),
			coreTesting.WithAPI(internal.ProtocolName, api.NewAPI),
			coreTesting.WithAPIConfig(internal.ProtocolName, &pluginConfig.APIConfig{}),
			coreTesting.WithMockS3()),
	}
}
