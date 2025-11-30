package internal

import (
	"bytes"
	"encoding/hex"
	"io"

	"go.lumeweb.com/liblbry/blob"
)

const ProtocolName = "lbry"
const ProtocolDisplayName = "LBRY"

// TerminatingBlobHash is the deterministic sentinel value used for terminating blobs
const TerminatingBlobHash = "TERMINATING"

// readSeekCloser is a helper that implements io.ReadSeekCloser
type readSeekCloser struct {
	*bytes.Reader
}

func (rsc *readSeekCloser) Close() error {
	return nil
}

// NewReadSeekCloser creates a new io.ReadSeekCloser from byte data
func NewReadSeekCloser(data []byte) io.ReadSeekCloser {
	return &readSeekCloser{Reader: bytes.NewReader(data)}
}

// GetTerminatingBlobHash generates a deterministic terminating blob hash
func GetTerminatingBlobHash() string {
	hash, err := blob.ComputeBlobHashBytes([]byte(TerminatingBlobHash))
	if err != nil {
		// This should never happen, but if it does, return empty string as fallback
		return ""
	}
	return hex.EncodeToString(hash)
}
