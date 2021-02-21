package namespace

import "github.com/pkg/errors"

var (
	ErrNamespaceAlreadyExists = errors.New("namespace already exists")
	ErrNamespaceNotFound      = errors.New("namespace not found")
	ErrNamespaceDeleting      = errors.New("namespace is being deleted")
	ErrKeyNotFound            = errors.New("key not found")
)
