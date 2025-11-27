package protocol

import (
	"context"
	"fmt"

	"go.lumeweb.com/liblbry/server"
	"go.lumeweb.com/liblbry/stream"
	pluginCore "go.lumeweb.com/portal-plugin-lbry/core"
	"go.lumeweb.com/portal-plugin-lbry/internal"
	"go.lumeweb.com/portal-plugin-lbry/internal/protocol/util"
	"go.lumeweb.com/portal/core"
	"go.lumeweb.com/portal/db/models"
)

// GetProtocolInterface retrieves the raw protocol interface from core
// This function handles the basic protocol lookup
func GetProtocolInterface() (core.Protocol, error) {
	protocolInterface := core.GetProtocol(internal.ProtocolName)
	if protocolInterface == nil {
		return nil, fmt.Errorf("protocol %q not found", internal.ProtocolName)
	}
	return protocolInterface, nil
}

// CastToProtocol casts a protocol interface to *Protocol with type safety
// This function handles the type assertion and validation
func CastToProtocol(proto core.Protocol) (*Protocol, error) {
	if proto == nil {
		return nil, fmt.Errorf("protocol interface cannot be nil")
	}

	protocol, ok := proto.(*Protocol)
	if !ok {
		return nil, fmt.Errorf("protocol %q is not of type *Protocol", internal.ProtocolName)
	}

	return protocol, nil
}

// GetStorageProtocol retrieves and validates the LBRY protocol as core.StorageProtocol
// This function wraps GetProtocolInterface and CastToStorageProtocol for convenience
func GetStorageProtocol() (core.StorageProtocol, error) {
	protocolInterface, err := GetProtocolInterface()
	if err != nil {
		return nil, err
	}

	return util.CastToStorageProtocol(protocolInterface)
}

// GetProtocol retrieves and validates the LBRY protocol with type safety
func GetProtocol() (*Protocol, error) {
	protocolInterface, err := GetProtocolInterface()
	if err != nil {
		return nil, err
	}

	return CastToProtocol(protocolInterface)
}

// GetProtocolNode retrieves and validates the LBRY protocol node
func GetProtocolNode(protocol *Protocol) (server.Server, error) {
	if protocol == nil {
		return nil, fmt.Errorf("protocol cannot be nil")
	}

	// Get the node with safety check
	proto := protocol.Node()
	if proto == nil {
		return nil, fmt.Errorf("protocol node is not initialized")
	}

	return proto, nil
}

// GetProtocolWithValidation retrieves and validates the LBRY protocol with type safety
func GetProtocolWithValidation() (*Protocol, server.Server, error) {
	protocol, err := GetProtocol()
	if err != nil {
		return nil, nil, err
	}

	proto, err := GetProtocolNode(protocol)
	if err != nil {
		return nil, nil, err
	}

	return protocol, proto, nil
}

// ValidateRequest validates common request fields used across operation handlers
// This function centralizes request validation logic for hash and user ID
func ValidateRequest(req *models.Request) error {
	// Validate hash
	if len(req.Hash) == 0 {
		return fmt.Errorf("hash is required")
	}

	// Validate user ID
	if req.UserID == nil {
		return fmt.Errorf("user ID is required")
	}
	if *req.UserID == 0 {
		return fmt.Errorf("user ID cannot be zero")
	}

	return nil
}

// ValidateRequestWithHash validates request fields including hash nil check
// This function is for operations that require hash to be non-nil (like post upload)
func ValidateRequestWithHash(req *models.Request) error {
	// Validate hash is not nil (for operations that require upload hash)
	if req.Hash == nil {
		return fmt.Errorf("upload hash is required")
	}

	// Use shared validation for the rest
	return ValidateRequest(req)
}

// GetStorageService retrieves and validates the storage service
func GetStorageService(coreCtx core.Context) (core.StorageService, error) {
	storageSvc := core.GetService[core.StorageService](coreCtx, core.STORAGE_SERVICE)
	if storageSvc == nil {
		return nil, fmt.Errorf("storage service not available")
	}
	return storageSvc, nil
}

// ProcessStreamResult handles the common stream processing logic for both upload and retrieve operations
func ProcessStreamResult(ctx context.Context, coreCtx core.Context, streamResult *stream.StreamResult, userID uint) error {
	lbryHash := streamResult.SDBlob.HashHex()
	// Get upload service
	uploadSvc := core.GetService[pluginCore.UploadService](coreCtx, pluginCore.UPLOAD_SERVICE)
	if uploadSvc == nil {
		return fmt.Errorf("upload service not available")
	}

	// Process upload to create upload and pin records
	err := uploadSvc.ProcessUpload(ctx, streamResult, userID)
	if err != nil {
		return fmt.Errorf("failed to process upload for stream %q: %w", lbryHash, err)
	}

	// Create stream pin record
	sdCid, err := internal.LBRYHashToCID(lbryHash)
	if err != nil {
		return fmt.Errorf("failed to convert SD hash to CID: %w", err)
	}

	_, err = uploadSvc.CreateStreamPin(ctx, userID, sdCid)
	if err != nil {
		return fmt.Errorf("failed to create stream pin for %q: %w", lbryHash, err)
	}

	return nil
}
