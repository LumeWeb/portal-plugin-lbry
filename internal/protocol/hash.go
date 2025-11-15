package protocol

import (
	"fmt"

	"github.com/ipfs/go-cid"
	"go.lumeweb.com/liblbry/stream"
	"go.lumeweb.com/portal/core"
)

func LBRYHashToHash(hash string) (core.StorageHash, error) {
	multihash, err := stream.ToMultihash(hash)
	if err != nil {
		return nil, fmt.Errorf("failed to convert hash %q: %w", hash, err)
	}

	c, err := cid.Decode(multihash)
	if err != nil {
		return nil, fmt.Errorf("failed to decode CID from multihash: %w", err)
	}

	return core.NewStorageHashFromMultihash(c.Hash(), c.Type(), nil), nil
}
