package testing

import (
	"bytes"
	"io"
	"net"
)

// readSeekCloser is a test helper that implements io.ReadSeekCloser
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

// GetFreePort asks the kernel for a free open port that is ready to use.
func GetFreePort() (int, error) {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		return 0, err
	}

	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}
