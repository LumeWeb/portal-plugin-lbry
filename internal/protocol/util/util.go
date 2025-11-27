package util

import (
	"fmt"

	"go.lumeweb.com/portal-plugin-lbry/internal"
	"go.lumeweb.com/portal/core"
)

// CastToStorageProtocol casts a protocol interface to core.StorageProtocol with type safety
// This function handles the type assertion and validation for storage operations
func CastToStorageProtocol(proto core.Protocol) (core.StorageProtocol, error) {
	if proto == nil {
		return nil, fmt.Errorf("protocol interface cannot be nil")
	}

	storageProtocol, ok := proto.(core.StorageProtocol)
	if !ok {
		return nil, fmt.Errorf("protocol %q does not implement core.StorageProtocol", internal.ProtocolName)
	}

	return storageProtocol, nil
}
