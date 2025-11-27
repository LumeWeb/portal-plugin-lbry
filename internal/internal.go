package internal

import (
	"bytes"
	"io"
)

const ProtocolName = "lbry"
const ProtocolDisplayName = "LBRY"

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
