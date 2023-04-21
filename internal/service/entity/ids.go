package entity

import (
	"encoding/binary"
	"fmt"
	"strings"

	"github.com/dgraph-io/badger/v3"
	"github.com/mimiro-io/datahub/internal/service/store"
	"github.com/mimiro-io/datahub/internal/service/types"
	"github.com/pkg/errors"
)

func (l Lookup) asCURIE(id string) (types.CURIE, error) {
	if prefix, ok := l.namespaces.ExtractPrefix(id); ok && !strings.HasPrefix(id, "http") {
		if _, err := l.namespaces.ExpandPrefix(prefix); err == nil {
			return types.CURIE(id), nil
		} else {
			return "", err
		}
	}
	if nsURI, localValue, ok := l.namespaces.ExtractNamespaceURI(id); ok {
		if prefix, err := l.namespaces.GetNamespacePrefix(nsURI); err == nil {
			return types.CURIE(fmt.Sprintf("%v:%v", prefix, localValue)), nil
		} else {
			return "", err
		}
	}
	return "", fmt.Errorf("input %v is neither in CURIE format (prefix:value) nor a URI", id)
}

func (l Lookup) internalIDForCURIE(txn *badger.Txn, curie types.CURIE) (types.InternalID, error) {
	var rid uint64

	// check if it exists already uri => id
	item, err := txn.Get(store.GetCurieKey(curie))
	if err != nil {
		return 0, errors.WithMessagef(err, "could not load internal id for curie: %v", curie)
	} else {
		err := item.Value(func(val []byte) error {
			rid = binary.BigEndian.Uint64(val)
			return nil
		})
		if err != nil {
			return 0, errors.WithMessagef(err, "could not process found interalId for curie: %v", curie)
		}
	}

	return types.InternalID(rid), nil
}
