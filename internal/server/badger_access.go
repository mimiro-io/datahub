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

import "github.com/dgraph-io/badger/v3"

// BadgerAccess implements service/store.BadgerStore and bridges badger access
// without cyclic dependencies
type BadgerAccess struct {
	b   *badger.DB
	dsm *DsManager
}

func (b BadgerAccess) GetDB() *badger.DB {
	return b.b
}

func (b BadgerAccess) LookupDatasetID(datasetName string) (uint32, bool) {
	ds := b.dsm.GetDataset(datasetName)
	if ds == nil {
		return 0, false
	}
	return ds.InternalID, true
}

func NewBadgerAccess(s *Store, dsm *DsManager) BadgerAccess {
	return BadgerAccess{s.database, dsm}
}
