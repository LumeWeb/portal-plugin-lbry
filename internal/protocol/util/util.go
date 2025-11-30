package util

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"

	"go.lumeweb.com/liblbry/blob"
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

// GenerateTerminatingBlobHash generates a unique hash for terminating blobs using RNG
// This ensures database uniqueness constraints are satisfied while maintaining
// the semantic meaning of "empty" for terminating blobs in blob lists
func GenerateTerminatingBlobHash() (string, error) {
	// Generate random bytes of correct length for LBRY blob hash
	randomBytes := make([]byte, blob.BlobHashSize) // Use proper LBRY blob hash size
	_, err := rand.Read(randomBytes)
	if err != nil {
		return "", fmt.Errorf("failed to generate random bytes: %w", err)
	}

	// Hash the random bytes using liblbry blob ComputeBlobHashBytes to ensure proper LBRY hash format
	hash, err := blob.ComputeBlobHashBytes(randomBytes)
	if err != nil {
		return "", fmt.Errorf("failed to hash random bytes: %w", err)
	}

	// Convert to hex string and add prefix to identify as terminating blob hash tring(hash)
	return hex.EncodeToString(hash), nil
}
