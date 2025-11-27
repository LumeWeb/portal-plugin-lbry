package testing

import (
	"fmt"
	"net"
	"sync"
)

const defaultMaxAttempts = 100

// portTracker manages the allocation and tracking of ports to prevent collisions
type portTracker struct {
	mu          sync.Mutex   // Changed from RWMutex since we never use RLock/RUnlock
	history     map[int]bool // All ports ever allocated (to prevent reuse)
	maxAttempts int          // Maximum attempts to find a unique port
}

var globalTracker = newPortTracker()

// newPortTracker creates a port tracker with sane defaults
func newPortTracker() *portTracker {
	return &portTracker{
		history:     make(map[int]bool),
		maxAttempts: defaultMaxAttempts,
	}
}

// GetFreePort asks the kernel for a free open port that is ready to use.
// It ensures the port has never been handed out by this helper before to prevent test collisions.
// Note: There is still a TOCTOU window where another process could grab the same port
// after the kernel allocates it but before the caller binds to it.
func GetFreePort() (int, error) {
	return globalTracker.getUniquePort()
}

// getUniquePort finds a port that has never been used before
func (pt *portTracker) getUniquePort() (int, error) {
	for attempt := 0; attempt < pt.maxAttempts; attempt++ {
		// Get a free port from the kernel
		port, err := getKernelFreePort()
		if err != nil {
			return 0, err
		}

		pt.mu.Lock()
		// Check if this port was ever used before
		if !pt.history[port] {
			// Mark as used in history
			pt.history[port] = true
			pt.mu.Unlock()
			return port, nil
		}
		pt.mu.Unlock()

		// Port was used before, try again
		// Note: The port is already released by getKernelFreePort's defer l.Close()
	}

	return 0, PortAllocationError{Attempts: pt.maxAttempts}
}

// getKernelFreePort gets a free port from the kernel
func getKernelFreePort() (int, error) {
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

// PortAllocationError is returned when unable to allocate a unique port
type PortAllocationError struct {
	Attempts int
}

func (e PortAllocationError) Error() string {
	return fmt.Sprintf("failed to allocate unique port after %d attempts", e.Attempts)
}
