// Copyright 2022 MIMIRO AS
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

package server

import (
	"fmt"

	"github.com/dgraph-io/badger/v4"

	"github.com/mimiro-io/datahub/internal/service/types"
)

// BadgerAccess implements service/store.BadgerStore and bridges badger access
// without cyclic dependencies
type BadgerAccess struct {
	b   *badger.DB
	dsm *DsManager
}

func (b BadgerAccess) LookupDatasetName(internalDatasetID types.InternalDatasetID) (string, bool) {
	result := ""
	b.dsm.store.datasets.Range(func(k interface{}, val interface{}) bool {
		name := k.(string)
		ds := val.(*Dataset)
		if ds.InternalID == uint32(internalDatasetID) {
			result = name
			return false
		}
		return true
	})
	return result, result != ""
}

func (b BadgerAccess) IsDatasetDeleted(datasetID types.InternalDatasetID) bool {
	return b.dsm.store.deletedDatasets[uint32(datasetID)]
}

func (b BadgerAccess) LookupExpansionPrefix(namespaceURI types.URI) (types.Prefix, error) {
	b.dsm.store.NamespaceManager.lock.Lock()
	defer b.dsm.store.NamespaceManager.lock.Unlock()
	if prefix, found := b.dsm.store.NamespaceManager.expansionToPrefixMapping[string(namespaceURI)]; found {
		return types.Prefix(prefix), nil
	}
	return "", fmt.Errorf("prefix mapping for namespace %v not found", namespaceURI)
}

func (b BadgerAccess) LookupNamespaceExpansion(prefix types.Prefix) (types.URI, error) {
	b.dsm.store.NamespaceManager.lock.Lock()
	defer b.dsm.store.NamespaceManager.lock.Unlock()
	if expansion, found := b.dsm.store.NamespaceManager.prefixToExpansionMapping[string(prefix)]; found {
		return types.URI(expansion), nil
	}
	return "", fmt.Errorf("expansion for prefix %v not found", prefix)
}

func (b BadgerAccess) GetAllNamespacePrefixes() map[string]string {
	return b.dsm.store.NamespaceManager.GetPrefixToExpansionMap()
}

func (b BadgerAccess) DeleteNamespacePrefix(prefix string) error {
	return b.dsm.store.NamespaceManager.DeleteNamespacePrefix(prefix)
}

func (b BadgerAccess) LookupDatasetIDs(datasetNames []string) []types.InternalDatasetID {
	var scopeArray []types.InternalDatasetID
	if len(datasetNames) > 0 {
		for _, ds := range datasetNames {
			dataset, found := b.dsm.store.datasets.Load(ds)
			if found {
				scopeArray = append(scopeArray, types.InternalDatasetID(dataset.(*Dataset).InternalID))
			}
		}
	}
	return scopeArray
}

func (b BadgerAccess) GetDB() *badger.DB {
	return b.b
}

func (b BadgerAccess) LookupDatasetID(datasetName string) (types.InternalDatasetID, bool) {
	ds := b.dsm.GetDataset(datasetName)
	if ds == nil {
		return 0, false
	}
	return types.InternalDatasetID(ds.InternalID), true
}

func NewBadgerAccess(s *Store, dsm *DsManager) BadgerAccess {
	return BadgerAccess{s.database, dsm}
}