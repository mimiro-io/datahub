// Copyright 2023 MIMIRO AS
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package entity

import (
	"encoding/binary"
	"fmt"
	"strings"

	"github.com/dgraph-io/badger/v4"
	"github.com/pkg/errors"

	"github.com/mimiro-io/datahub/internal/service/store"
	"github.com/mimiro-io/datahub/internal/service/types"
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

func (l Lookup) InternalIDForCURIE(txn *badger.Txn, curie types.CURIE) (types.InternalID, error) {
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
