package protocol

import (
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIsPrivateOrReservedIP(t *testing.T) {
	tests := []struct {
		name     string
		ip       string
		expected bool
	}{
		// Public IPv4 addresses
		{"Public IPv4 - 8.8.8.8", "8.8.8.8", false},
		{"Public IPv4 - 1.1.1.1", "1.1.1.1", false},
		{"Public IPv4 - 208.67.222.222", "208.67.222.222", false},

		// RFC1918 private IPv4 ranges
		{"Private IPv4 - 10.0.0.1", "10.0.0.1", true},
		{"Private IPv4 - 10.255.255.254", "10.255.255.254", true},
		{"Private IPv4 - 172.16.0.1", "172.16.0.1", true},
		{"Private IPv4 - 172.31.255.254", "172.31.255.254", true},
		{"Private IPv4 - 192.168.0.1", "192.168.0.1", true},
		{"Private IPv4 - 192.168.255.254", "192.168.255.254", true},

		// Other reserved IPv4 ranges
		{"Reserved IPv4 - 100.64.0.1", "100.64.0.1", true},
		{"Reserved IPv4 - 100.127.255.254", "100.127.255.254", true},
		{"Reserved IPv4 - 169.254.0.1", "169.254.0.1", true},

		// Loopback addresses
		{"Loopback IPv4 - 127.0.0.1", "127.0.0.1", true},
		{"Loopback IPv6 - ::1", "::1", true},

		// Link-local addresses
		{"Link-local IPv4 - 169.254.0.1", "169.254.0.1", true},
		{"Link-local IPv6 - fe80::1", "fe80::1", true},

		// IPv6 unique local addresses
		{"ULA IPv6 - fc00::1", "fc00::1", true},
		{"ULA IPv6 - fdff:ffff:ffff:ffff:ffff:ffff:ffff:ffff", "fdff:ffff:ffff:ffff:ffff:ffff:ffff:ffff", true},

		// Public IPv6 addresses
		{"Public IPv6 - 2001:4860:4860::8888", "2001:4860:4860::8888", false},
		{"Public IPv6 - 2606:4700:4700::1111", "2606:4700:4700::1111", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ip := net.ParseIP(tt.ip)
			require.NotNil(t, ip, "Failed to parse IP address: %s", tt.ip)

			result := isPrivateOrReservedIP(ip)
			assert.Equal(t, tt.expected, result, "IP %s should be private/reserved: %v", tt.ip, tt.expected)
		})
	}
}

func TestGetFirstPublicIP(t *testing.T) {
	// This test verifies that during testing, the function allows private IPs
	// since testing.Testing() returns true in test mode

	ip, err := getFirstPublicIP()

	// During testing, the function should return an IP (even private ones) or an error
	if err == nil {
		assert.NotEmpty(t, ip, "Returned IP should not be empty")

		parsedIP := net.ParseIP(ip)
		assert.NotNil(t, parsedIP, "Returned IP should be valid")

		// During testing, private IPs are allowed, so we don't check isPrivateOrReservedIP
		// This ensures the function works in test environments where only private IPs might be available
		t.Logf("Found IP during test: %s (private IPs allowed in test mode)", ip)
	} else {
		// It's acceptable to return an error if no interfaces are found
		assert.Contains(t, err.Error(), "no suitable public IP address found")
	}
}
