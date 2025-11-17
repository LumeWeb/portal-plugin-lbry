package internal

import (
	"fmt"

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
