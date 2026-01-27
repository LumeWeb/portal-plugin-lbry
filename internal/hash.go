package internal

import (
	"encoding/hex"
	"fmt"

	"crypto/subtle"

	"github.com/ipfs/go-cid"
	"go.lumeweb.com/liblbry/stream"
	"go.lumeweb.com/portal/core"
)

// lbryHashToCid is a helper function that converts an LBRY hash to a CID
func lbryHashToCid(hash string) (cid.Cid, error) {
	multihash, err := stream.ToMultihash(hash)
	if err != nil {
		return cid.Cid{}, fmt.Errorf("failed to convert hash %q: %w", hash, err)
	}

	c, err := cid.Decode(multihash)
	if err != nil {
		return cid.Cid{}, fmt.Errorf("failed to decode CID from multihash: %w", err)
	}

	return c, nil
}

// LBRYHashToStorageHash converts an LBRY hash to a StorageHash
func LBRYHashToStorageHash(hash string) (core.StorageHash, error) {
	c, err := lbryHashToCid(hash)
	if err != nil {
		return nil, err
	}

	return core.NewStorageHashFromMultihash(c.Hash(), c.Type(), nil), nil
}

// LBRYHashToCID converts an LBRY hash to a CID
func LBRYHashToCID(hash string) (cid.Cid, error) {
	return lbryHashToCid(hash)
}

// cidToLbryHash is a helper function that converts a CID to an LBRY hash
func cidToLbryHash(c cid.Cid) (string, error) {
	lbryHash, err := stream.FromMultihash(c.String())
	if err != nil {
		return "", fmt.Errorf("failed to convert CID %q to LBRY hash: %w", c.String(), err)
	}

	return lbryHash, nil
}

// CIDToLBRYHash converts a CID to an LBRY hash
func CIDToLBRYHash(c cid.Cid) (string, error) {
	return cidToLbryHash(c)
}

// SetSDBlobProfileByHash attempts to set the correct profile for an SD blob
// by testing both profiles and using the one that produces a matching SD hash
func SetSDBlobProfileByHash(sdBlob *stream.SDBlob, sdHash string) error {
	// Decode the expected hash bytes
	expectedHashBytes, err := hex.DecodeString(sdHash)
	if err != nil {
		return fmt.Errorf("failed to decode expected SD hash %q: %w", sdHash, err)
	}

	// Try ProfileNewSort first (default)
	sdBlob.SetProfile(stream.ProfileNewSort)
	_, err = sdBlob.ToBlob()
	if err != nil {
		return fmt.Errorf("failed to serialize SD blob with new sort profile: %w", err)
	}

	// Check if the SD hash matches the expected hash using constant-time comparison
	computedHash := sdBlob.Hash()
	if subtle.ConstantTimeCompare(computedHash, expectedHashBytes) == 1 {
		return nil
	}

	// Try ProfileOldSort if new sort didn't match
	sdBlob.SetProfile(stream.ProfileOldSort)
	_, err = sdBlob.ToBlob()
	if err != nil {
		return fmt.Errorf("failed to serialize SD blob with old sort profile: %w", err)
	}

	// Check if the SD hash matches with old sort using constant-time comparison
	computedHash = sdBlob.Hash()
	if subtle.ConstantTimeCompare(computedHash, expectedHashBytes) == 1 {
		return nil
	}

	// Neither profile produced a matching SD hash
	return fmt.Errorf("SD blob %q hash mismatch: expected %s, got %s",
		sdHash, hex.EncodeToString(computedHash))
}
