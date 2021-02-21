package namespace

import (
	"bytes"
	"context"

	"github.com/go-logr/logr"
	"github.com/pkg/errors"

	"argc.in/graveldb/pkg/kv"
	"argc.in/graveldb/pkg/tock"
)

// nolint:gochecknoglobals
var (
	NamespaceExistsValue   = []byte("EXISTS")
	NamespaceDeletingValue = []byte("DELETING")
)

// service implements the namespace.Service interface. Check the interface for
// more details.
type service struct {
	db  kv.Service
	log logr.Logger
}

var _ Service = (*service)(nil)

// NewService creates a instance of Service backed by the Key-Value Datastore.
func NewService(db kv.Service, opts ...Option) Service {
	svc := &service{
		db:  db,
		log: DefaultLogger,
	}

	for _, opt := range opts {
		opt(svc)
	}

	return svc
}

func (svc *service) IsActive(name string) (bool, error) {
	key := generateMetaKey(name)

	value, err := svc.db.Get(key)
	if err != nil {
		if errors.Is(err, kv.ErrKeyNotFound) {
			return false, nil
		}

		return false, err
	}

	return bytes.Equal(value, NamespaceExistsValue), nil
}

func (svc *service) CreateNamespace(name string) error {
	key := generateMetaKey(name)

	value, err := svc.db.Get(key)
	if err != nil && !errors.Is(err, kv.ErrKeyNotFound) {
		return err
	}

	if bytes.Equal(value, NamespaceExistsValue) {
		return ErrNamespaceAlreadyExists
	}

	if bytes.Equal(value, NamespaceDeletingValue) {
		return ErrNamespaceDeleting
	}

	if err := svc.db.Set(key, NamespaceExistsValue); err != nil {
		return err
	}

	return nil
}

func (svc *service) DeleteNamespace(name string) error {
	key := generateMetaKey(name)

	value, err := svc.db.Get(key)
	if err != nil {
		if errors.Is(err, kv.ErrKeyNotFound) {
			return ErrNamespaceNotFound
		}

		return err
	}

	if bytes.Equal(value, NamespaceDeletingValue) {
		return ErrNamespaceDeleting
	}

	if err := svc.db.Set(key, NamespaceDeletingValue); err != nil {
		return err
	}

	return nil
}

func (svc *service) GetKey(ns, key string) ([]byte, error) {
	active, err := svc.IsActive(ns)
	if err != nil {
		return nil, err
	}

	if !active {
		return nil, ErrNamespaceNotFound
	}

	byteKey := generateNamespaceKey(ns, key)

	value, err := svc.db.Get(byteKey)
	if err != nil {
		return nil, err
	}

	return value, nil
}

func (svc *service) SetKey(ns, key string, value []byte) error {
	active, err := svc.IsActive(ns)
	if err != nil {
		return err
	}

	if !active {
		return ErrNamespaceNotFound
	}

	byteKey := generateNamespaceKey(ns, key)

	err = svc.db.Set(byteKey, value)
	if err != nil {
		return err
	}

	return nil
}

func (svc *service) DeleteKey(ns, key string) error {
	active, err := svc.IsActive(ns)
	if err != nil {
		return err
	}

	if !active {
		return ErrNamespaceNotFound
	}

	byteKey := generateNamespaceKey(ns, key)

	err = svc.db.Delete(byteKey)
	if err != nil {
		return err
	}

	return nil
}

func (svc *service) GarbageCollection(ctx context.Context, t tock.Ticker) {
	svc.log.Info("starting garbage collection in background")

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.Chan():
			// TODO(ankit): Do I reset the Ticker?
			svc.log.Info("finding deleted namespaces")

			names := deletedNamespaces(svc.db)
			if len(names) == 0 {
				svc.log.Info("no namespace for garbage collection!")
				continue
			}

			if err := svc.garbageCollectNamespaces(names); err != nil {
				svc.log.Error(err, "garbage collecting")
			}
		}
	}
}

func (svc *service) garbageCollectNamespaces(names []string) (err error) {
	for _, name := range names {
		name := name
		svc.log.Info("garbage collecting", "namespace", name)

		if err = svc.db.RWBatch(func(rw kv.ReadWriter) error {
			return wipeNamespace(rw, name)
		}); err != nil {
			return errors.WithMessagef(err, "namespace: %s", name)
		}
	}

	return nil
}
