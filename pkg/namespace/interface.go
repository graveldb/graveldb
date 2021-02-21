package namespace

import (
	"context"

	"argc.in/graveldb/pkg/tock"
)

// Service defines the primary Namespace Interface.
type Service interface {
	// IsActive checks if the Namespace exists and it is active.
	IsActive(name string) (bool, error)
	// CreateNamespace creates the Namespace in the Database. This function is
	// not idempotent and is expected to return ErrNamespaceAlreadyExists on
	// subsequent calls. This function may fail for recently deleted Namespaces
	// with ErrNamespaceDeleting. This is until the Namespace keys are garbage
	// collected to prevent leakage of keys.
	CreateNamespace(name string) error
	// DeleteNamespace deletes the Namespace from the Database. This function is
	// expected to return in bounded time. Namespace won't be accessible after
	// this function succeeds.
	DeleteNamespace(name string) error

	// All the Datastore operations must validate the existence of Namespace.

	// GetKey is the primitive Datastore operation. It returns the "key" from
	// "ns" Namespace. This operation will return ErrKeyNotFound if the key does
	// not exist.
	GetKey(ns, key string) ([]byte, error)
	// SetKey is the primitive Datastore operation. It sets the value of "key"
	// in "ns" Namespace. This function will overwrite the value or create new
	// key.
	SetKey(ns, key string, value []byte) error
	// DeleteKey is the primitive Datastore operation. It deletes the "key" from
	// the "ns" Namespace. This function is idempotent and all subsequent calls
	// with be Noop.
	DeleteKey(ns, key string) error

	// GarbageCollection must be called inside a Go-routine. It will respect the
	// Ticker and trigger the garbage collection logic. If the Context is
	// cancelled, the Garbage Collection will gracefully stop and return.
	GarbageCollection(ctx context.Context, t tock.Ticker)
}
