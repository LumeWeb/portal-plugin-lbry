package protocol

import (
	"testing"

	pluginCore "go.lumeweb.com/portal-plugin-lbry/core"
	"go.lumeweb.com/portal-plugin-lbry/internal"
	"go.lumeweb.com/portal-plugin-lbry/internal/db/migrations"
	"go.lumeweb.com/portal-plugin-lbry/internal/service/devices"
	"go.lumeweb.com/portal-plugin-lbry/internal/service/upload"
	coreTesting "go.lumeweb.com/portal/core/testing"
)

var (
	cfg        = coreTesting.NewConfigBuilder().Build()
	testConfig = coreTesting.CombineOptions(
		coreTesting.WithMockProtocol(internal.ProtocolName, func(protocol *coreTesting.MockProtocol) {
			protocol.WithConfig(cfg)
		}),
		coreTesting.WithProtocolConfig(internal.ProtocolName, cfg),
		coreTesting.WithSQLitePluginMigrations(internal.ProtocolName, migrations.GetSQLite()),
		coreTesting.WithServiceFactory(pluginCore.UPLOAD_SERVICE, upload.NewUploadService),
		coreTesting.WithServiceFactory(pluginCore.DEVICE_SERVICE, devices.NewDeviceService),
	)
)

// Helper function for common test runner pattern
func runBlobStoreTest(t *testing.T, testFunc func(testing.TB, coreTesting.TestContext)) {
	coreTesting.RunTestCaseWithDB(t, testFunc, testConfig)
}
