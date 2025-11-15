package protocol

import (
	"fmt"

	"github.com/knadh/koanf/v2"
	"go.lumeweb.com/liblbry"
	"go.lumeweb.com/liblbry/storage"
	"go.lumeweb.com/portal/core"
)

// StoreFactory implements the storage.StoreFactory interface for creating BlobStore instances.
//
// It creates BlobStore objects and requires a core.Context to be provided via the WithContext option.
type StoreFactory struct {
	ctx    core.Context
	logger *core.Logger
}

// ContextOption implements the StoreFactoryOption interface for passing core.Context.
//
// This option allows setting the core.Context for the factory.
type ContextOption struct {
	ctx core.Context
}

// CreateStore creates a new BlobStore instance from the provided context.
//
// Parameters:
//   - config: A koanf configuration object (not used in this implementation)
//
// Returns:
//   - storage.BlobStore: A new BlobStore instance
//   - error: Any error encountered during store creation, or an error if the context is invalid
func (f StoreFactory) CreateStore(_ *koanf.Koanf) (storage.BlobStore, error) {
	if f.ctx == nil {
		return nil, fmt.Errorf("invalid context: context cannot be nil")
	}

	// Create and return a new BlobStore instance
	store, err := NewLBRYBlobStore(f.ctx)
	if err != nil {
		return nil, err
	}

	return store, nil
}

// Name returns the name of the factory.
//
// This method is part of the StoreFactory interface.
func (f StoreFactory) Name() string {
	return BLOBSTORE_NAME
}

// WithContext creates a new ContextOption with the provided core.Context.
//
// Parameters:
//   - ctx: The core.Context to use for the factory
//
// Returns:
//   - StoreFactoryOption: A new ContextOption instance
func WithContext(ctx core.Context) liblbry.StoreFactoryOption {
	return ContextOption{ctx: ctx}
}

// Apply applies the context option to the factory.
//
// This method implements the StoreFactoryOption interface.
func (o ContextOption) Apply(factory any) error {
	if lbryFactory, ok := factory.(*StoreFactory); ok {
		lbryFactory.ctx = o.ctx
		lbryFactory.logger = o.ctx.Logger()
		return nil
	}
	return fmt.Errorf("invalid factory type: expected *StoreFactory")
}
