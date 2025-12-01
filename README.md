# LBRY Portal Plugin

A Go-based LBRY protocol plugin for the Lume Portal framework that provides seamless integration with the LBRY network for stream uploads, device management, and content operations.

## Overview

This plugin extends the Lume Portal framework with LBRY protocol capabilities, enabling users to upload, manage, and interact with LBRY streams through a comprehensive REST API. It follows a modular architecture with clear separation of concerns and implements best practices for Go development.

## Features

- **Stream Upload Management**: Complete upload workflow with TUS (Resumable Upload Protocol) support
- **Device Whitelist Management**: IP-based device access control
- **LBRY Protocol Integration**: Full integration with LBRY DHT and reflector services
- **REST API**: Echo-based API with OpenAPI/Swagger documentation
- **Database Support**: SQLite and MySQL with GORM ORM
- **Blob Storage**: Efficient blob handling and stream assembly
- **Stream Pinning**: User-specific stream pinning functionality

## Architecture

### Core Components

- **API Layer** (`internal/api/`): REST endpoints for stream operations, device management, and uploads
- **Protocol Layer** (`internal/protocol/`): Core LBRY protocol implementation and network operations
- **Services** (`internal/service/`): Business logic for uploads and device management
- **Database** (`internal/db/`): GORM models and migrations for data persistence
- **Configuration** (`internal/config/`): Protocol and API configuration management

### Database Schema

The plugin manages several key entities:
- **Streams**: LBRY streams with metadata (hash, name, type, encryption)
- **Blobs**: Individual data blobs with size and IV information
- **Devices**: Whitelisted devices with IP addresses for access control
- **Pending Operations**: Temporary records for upload workflows

## Installation

### Prerequisites

- Go 1.19 or later
- Database (SQLite or MySQL)

### Installation

Install the xportal CLI tool:

```bash
go install go.lumeweb.com/xportal/cmd/xportal@v0.2.14
```

Build the portal with the LBRY plugin:

```bash
PORTAL_VERSION="9d285282f553c8c614e6db6e344dc03772f714a6" xportal build --with go.lumeweb.com/portal-plugin-lbry@c2eb2728cac7a54f448f2d93e15ae40a57efb2c6
```

Note that the commit hash is provided to ensure compatibility.

## Usage

### API Endpoints

The plugin provides a comprehensive REST API for LBRY stream management and device access control:

#### Stream Management
- **List Streams**: `GET /api/streams` - Paginated listing of user's streams with metadata
- **Upload Stream**: `POST /api/streams/upload` - Direct file upload with multipart form data
- **Delete Stream**: `DELETE /api/streams/{sd_hash}` - Remove streams by SD hash

#### TUS Resumable Upload Protocol
- **TUS Endpoint**: `/api/streams/upload/tus` - Full TUS protocol implementation for resumable uploads
  - `POST` - Create new upload
  - `PATCH` - Continue upload with data chunks
  - `HEAD` - Check upload status and offset
  - `DELETE` - Terminate upload
  - `OPTIONS` - Get server capabilities

#### Stream Pinning
- **Pin Stream**: `POST /api/streams/pin` - Pin streams to keep them available on LBRY network

#### Device Whitelist Management
- **List Devices**: `GET /api/devices` - Paginated device listing
- **Create Device**: `POST /api/devices` - Add device to whitelist (name, IP address)
- **Get Device**: `GET /api/devices/{id}` - Retrieve specific device
- **Update Device**: `PUT /api/devices/{id}` - Update device information
- **Delete Device**: `DELETE /api/devices/{id}` - Remove device from whitelist

#### Authentication & Security
- Most endpoints require authentication
- User-scoped operations for streams and devices
- Standard HTTP error responses with detailed JSON error messages
- Support for pagination, filtering, and sorting on list endpoints

### Configuration

Configure the plugin through the configuration files in `internal/config/`:
- Protocol settings (DHT, peers, ports)
- API configuration
- Database connection settings

## Development

### Building and Testing

```bash
# Run all tests
go test ./...

# Run tests with verbose output for internal packages
go test -v ./internal/...

# Run integration tests specifically
go test ./internal/protocol/tests/...

# Build all packages
go build ./...

# Clean up module dependencies
go mod tidy

# Format all Go code
go fmt ./...
```

### Project Structure

```
├── lbry.go                 # Main plugin entry point
├── core/                   # Core service interfaces and mocks
├── internal/               # Internal implementation details
│   ├── api/               # REST API endpoints and DTOs
│   ├── config/            # Configuration management
│   ├── db/                # Database models and migrations
│   ├── protocol/          # LBRY protocol implementation
│   ├── service/           # Business logic services
│   └── testing/           # Test utilities
└── build/                 # Build-time information
```

### Key Dependencies

- `go.lumeweb.com/portal` - Core portal framework
- `go.lumeweb.com/liblbry` - LBRY protocol library
- `github.com/tus/tusd/v2` - TUS upload protocol
- `gorm.io/gorm` - ORM for database operations
- `github.com/labstack/echo/v4` - HTTP framework

## Testing Strategy

The project includes comprehensive testing:

- **Unit Tests**: Individual component testing
- **Integration Tests**: Protocol operation testing
- **Test Utilities**: Helper functions and test data
- **Mock Implementations**: Service interface mocks

Tests are located in:
- `internal/testing/` - General test utilities
- `internal/protocol/tests/` - Protocol integration tests
- `core/mocks/` - Service interface mocks

## Contributing

Contributions are welcome! Please open an issue before submitting pull requests if you are planning on large changes.

1. Follow Go best practices and coding standards
2. Ensure all tests pass before submitting changes
3. Add appropriate tests for new functionality
4. Update documentation as needed
5. Use `go fmt ./...` to format code

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Support

For issues and questions related to this plugin, please open an issue or reach out through [lumeweb.com](https://lumeweb.com) contact channels.