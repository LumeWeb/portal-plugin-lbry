package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"mime/multipart"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	pluginCore "go.lumeweb.com/portal-plugin-lbry/core"
	pluginMocks "go.lumeweb.com/portal-plugin-lbry/core/mocks"
	"go.lumeweb.com/portal-plugin-lbry/internal"
	"go.lumeweb.com/portal-plugin-lbry/internal/api/dto"
	pluginConfig "go.lumeweb.com/portal-plugin-lbry/internal/config"
	"go.lumeweb.com/portal-plugin-lbry/internal/db"
	"go.lumeweb.com/portal-plugin-lbry/internal/db/migrations"
	"go.lumeweb.com/portal-plugin-lbry/internal/protocol"
	pluginTesting "go.lumeweb.com/portal-plugin-lbry/internal/testing"
	"go.lumeweb.com/portal/core"
	coreTesting "go.lumeweb.com/portal/core/testing"
	"go.lumeweb.com/portal/service"
	"go.lumeweb.com/queryutil"
	"gorm.io/gorm"
)

const (
	TestEmail    = "test@example.com"
	TestPassword = "example"
	TestIP       = "127.0.0.1"
)

func getTestOptions() coreTesting.TestContextBuilderOption {
	freePeerPort, err := pluginTesting.GetFreePort()
	if err != nil {
		panic(fmt.Sprintf("failed to get free peer port: %v", err))
	}
	freeDhtPort, err := pluginTesting.GetFreePort()
	if err != nil {
		panic(fmt.Sprintf("failed to get free DHT port: %v", err))
	}
	freeReflectorPort, err := pluginTesting.GetFreePort()
	if err != nil {
		panic(fmt.Sprintf("failed to get free reflector port: %v", err))
	}
	return coreTesting.CombineOptions(
		coreTesting.WithServiceFactory(core.CRON_SERVICE, service.NewCronService),
		coreTesting.WithServiceFactory(core.UPLOAD_SERVICE, service.NewMetadataService),
		coreTesting.WithServiceFactory(core.PIN_SERVICE, service.NewPinService),
		coreTesting.WithServiceFactory(core.REQUEST_SERVICE, service.NewRequestService),
		coreTesting.WithServiceFactory(core.WORKFLOW_SERVICE, service.NewWorkflowCoordinator),
		coreTesting.WithServiceFactory(core.USER_SERVICE, service.NewUserService),
		coreTesting.WithServiceFactory(core.AUTH_SERVICE, service.NewAuthService),
		coreTesting.WithServiceFactory(core.STORAGE_SERVICE, service.NewStorageService),
		coreTesting.WithAPI(internal.ProtocolName, NewAPI),
		coreTesting.WithAPIID(internal.ProtocolName),
		coreTesting.WithProtocol(internal.ProtocolName, protocol.NewProtocol),
		coreTesting.WithConfig("plugin.lbry.protocol.peer_port", uint(freePeerPort)),
		coreTesting.WithConfig("plugin.lbry.protocol.dht_port", uint(freeDhtPort)),
		coreTesting.WithConfig("plugin.lbry.protocol.reflector_port", uint(freeReflectorPort)),
		coreTesting.WithConfig("plugin.lbry.protocol.dht_seed_peers", []string{"0.0.0.1"}),
		coreTesting.WithProtocolConfig(internal.ProtocolName, pluginConfig.ProtocolConfig{
			PeerPort:      uint(freePeerPort),
			DHTPort:       uint(freeDhtPort),
			ReflectorPort: uint(freeReflectorPort),
		}),
		coreTesting.WithSQLitePluginMigrations(
			internal.ProtocolName, migrations.GetSQLite(),
		),
		// Include mock service factory for plugin-specific upload service
		coreTesting.WithMockServiceFactory(pluginCore.UPLOAD_SERVICE, pluginMocks.NewMockUploadService),
		coreTesting.WithMockServiceFactory(pluginCore.DEVICE_SERVICE, pluginMocks.NewMockDeviceService),
	)
}

func createTestUserAndLogin(ctx coreTesting.TestContext) (string, uint) {
	userSvc := core.GetService[core.UserService](ctx, core.USER_SERVICE)
	authSvc := core.GetService[core.AuthService](ctx, core.AUTH_SERVICE)

	user, err := userSvc.CreateAccount(context.Background(), TestEmail, TestPassword, false)
	if err != nil {
		ctx.T().Fatalf("failed to create test user: %v", err)
	}

	token, _, err := authSvc.LoginPassword(context.Background(), TestEmail, TestPassword, TestIP, false)
	if err != nil {
		ctx.T().Fatalf("failed to login test user: %v", err)
	}

	return token, user.ID
}

func setAuthHeader(req *http.Request, token string) {
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))
}

func createBaseMultipartRequest(ctx coreTesting.TestContext, t *testing.T, method, url, token string, bodyBuilder func(*multipart.Writer) error) *http.Request {
	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)

	// Call the body builder function to create the specific multipart content
	err := bodyBuilder(writer)
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)

	req := ctx.NewAPIRequest(method, url, body.Bytes())
	req.Header.Set("Content-Type", "multipart/form-data; boundary="+writer.Boundary())

	if token != "" {
		req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))
	}

	return req
}

func createMultipartRequest(ctx coreTesting.TestContext, t *testing.T, method, url, content, filename, token string) *http.Request {
	return createBaseMultipartRequest(ctx, t, method, url, token, func(writer *multipart.Writer) error {
		part, err := writer.CreateFormFile("file", filename)
		require.NoError(t, err)

		_, err = part.Write([]byte(content))
		require.NoError(t, err)

		return nil
	})
}

func createEmptyMultipartRequest(ctx coreTesting.TestContext, t *testing.T, method, url, filename, token string) *http.Request {
	return createBaseMultipartRequest(ctx, t, method, url, token, func(writer *multipart.Writer) error {
		// Create form file without writing content
		_, err := writer.CreateFormFile("file", filename)
		require.NoError(t, err)

		return nil
	})
}

func createMalformedMultipartRequestWithMetadata(ctx coreTesting.TestContext, t *testing.T, method, url, content, filename, token string, streamName, suggestedFileName string) *http.Request {
	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)

	// Create metadata
	metadata := &dto.StreamMetadataRequest{
		StreamName:        streamName,
		SuggestedFileName: suggestedFileName,
	}

	// Marshal metadata to JSON
	metadataJSON, err := json.Marshal(metadata)
	require.NoError(t, err)

	// Add metadata as "meta" form field
	metaPart, err := writer.CreateFormField("meta")
	require.NoError(t, err)
	_, err = metaPart.Write(metadataJSON)
	require.NoError(t, err)

	// Add file content as "file" form field
	filePart, err := writer.CreateFormFile("file", filename)
	require.NoError(t, err)
	_, err = filePart.Write([]byte(content))
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)

	// Corrupt the multipart data by removing the final boundary
	validData := body.Bytes()
	boundary := writer.Boundary()
	corruptedData := validData[:len(validData)-len("--"+boundary+"--\r\n")]

	req := ctx.NewAPIRequest(method, url, corruptedData)
	req.Header.Set("Content-Type", "multipart/form-data; boundary="+boundary)

	if token != "" {
		req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))
	}

	return req
}

// createMultipartRequestWithMetadata creates a multipart request with both file content and stream metadata
func createMultipartRequestWithMetadata(ctx coreTesting.TestContext, t *testing.T, method, url, content, filename, token string, streamName, suggestedFileName string) *http.Request {
	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)

	// Create metadata
	metadata := &dto.StreamMetadataRequest{
		StreamName:        streamName,
		SuggestedFileName: suggestedFileName,
	}

	// Marshal metadata to JSON
	metadataJSON, err := json.Marshal(metadata)
	require.NoError(t, err)

	// Add metadata as "meta" form field
	metaPart, err := writer.CreateFormField("meta")
	require.NoError(t, err)
	_, err = metaPart.Write(metadataJSON)
	require.NoError(t, err)

	// Add file content as "file" form field
	filePart, err := writer.CreateFormFile("file", filename)
	require.NoError(t, err)
	_, err = filePart.Write([]byte(content))
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)

	req := ctx.NewAPIRequest(method, url, body.Bytes())
	req.Header.Set("Content-Type", "multipart/form-data; boundary="+writer.Boundary())

	if token != "" {
		req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))
	}

	return req
}

// generateMockStreams generates mock stream data for testing
func generateMockStreams(count int, startID int) []*db.Stream {
	streams := make([]*db.Stream, count)
	for i := 0; i < count; i++ {
		id := startID + i
		streams[i] = &db.Stream{
			Model:             gorm.Model{ID: uint(id), CreatedAt: time.Now(), UpdatedAt: time.Now()},
			StreamHash:        fmt.Sprintf("hash_%d", id),
			SDHash:            fmt.Sprintf("sd_hash_%d", id),
			StreamName:        fmt.Sprintf("test_stream_%d", id),
			StreamType:        []string{"video", "audio", "image"}[id%3],
			SuggestedFileName: fmt.Sprintf("test_file_%d.mp4", id),
		}
	}
	return streams
}

// computeExpectedItems calculates expected items based on pagination
func computeExpectedItems(totalItems int, pagination *queryutil.Pagination) int {
	if pagination == nil {
		// Default pagination behavior (10 items max)
		if totalItems > 10 {
			return 10
		}
		return totalItems
	}

	// Calculate based on pagination parameters
	start := pagination.Start
	end := pagination.End

	// Handle edge cases
	if start >= totalItems {
		return 0
	}

	if end > totalItems {
		end = totalItems
	}

	return end - start
}
