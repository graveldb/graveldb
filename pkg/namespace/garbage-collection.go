package namespace

import (
	"bytes"
	"strings"

	"argc.in/graveldb/pkg/kv"
)

// deletedNamespaces looks in the Datastore for Namespaces which are marked for
// deletion.
func deletedNamespaces(db kv.Reader) []string {
	prefix := generateMetaKey("")

	iter := db.NewIterator(prefix /* lower */, nil /* upper */)
	defer iter.Close()

	// TODO(ankit): Maybe initialize the slice with some decent capacity to
	// avoid allocations.
	var names []string

	for iter.First(); iter.Valid(); iter.Next() {
		if !bytes.Equal(iter.Value(), NamespaceDeletingValue) {
			continue
		}

		key := iter.Key()
		names = append(names, strings.TrimPrefix(string(key), MetaPrefix+"."))
	}

	return names
}

// wipeNamespace removes the Namespace and all the related metadata from
// Datastore. It expects a Batched ReadWriter.
func wipeNamespace(rw kv.ReadWriter, name string) error {
	key := generateNamespaceKey(name, "")

	iter := rw.NewIterator(key /* lower */, nil /* upper */)
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		err := rw.Delete(iter.Key())
		if err != nil {
			return err
		}
	}

	key = generateMetaKey(name)

	if err := rw.Delete(key); err != nil {
		return err
	}

	return nil
}
