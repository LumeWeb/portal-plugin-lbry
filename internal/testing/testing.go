package testing

import (
	"fmt"
	"net"
	"sync"
)

// portTracker manages the allocation and tracking of ports to prevent collisions
type portTracker struct {
	mu          sync.RWMutex
	history     map[int]bool // All ports ever allocated (to prevent reuse)
	maxAttempts int          // Maximum attempts to find a unique port
}

var globalTracker = &portTracker{
	history:     make(map[int]bool),
	maxAttempts: 100,
}

// GetFreePort asks the kernel for a free open port that is ready to use.
// It ensures the port has never been used before to prevent test collisions.
func GetFreePort() (int, error) {
	return globalTracker.getUniquePort()
}

// getUniquePort finds a port that has never been used before
func (pt *portTracker) getUniquePort() (int, error) {
	for attempt := 0; attempt < pt.maxAttempts; attempt++ {
		// Get a free port from the kernel
		port, err := pt.getKernelFreePort()
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

	return 0, &PortAllocationError{Attempts: pt.maxAttempts}
}

// getKernelFreePort gets a free port from the kernel
func (pt *portTracker) getKernelFreePort() (int, error) {
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

func (e *PortAllocationError) Error() string {
	return fmt.Sprintf("failed to allocate unique port after %d attempts", e.Attempts)
}
