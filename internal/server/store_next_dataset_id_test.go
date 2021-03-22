// Copyright 2021 MIMIRO AS
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
	"encoding/binary"
	"os"
	"sync"
	"testing"

	"github.com/DataDog/datadog-go/statsd"
	"go.uber.org/zap"
)

func TestMigration(t *testing.T) {

	storeLocation := "./test_migrate_store_next_dataset_id"
	_ = os.RemoveAll(storeLocation)
	defer os.RemoveAll(storeLocation)

	// create store
	store := &Store{
		datasets:        &sync.Map{},
		deletedDatasets: make(map[uint32]bool),
		storeLocation:   storeLocation,
		logger:          zap.NewNop().Sugar().Named("store"),
		statsdClient:    &statsd.NoOpClient{},
	}
	store.NamespaceManager = NewNamespaceManager(store)
	_ = store.Open()
	dsm := &DsManager{
		store:  store,
		logger: zap.NewNop().Sugar().Named("ds-manager"),
		eb:     NoOpBus(),
	}
	dsm.CreateDataset(datasetCore)
	dsm.CreateDataset("foo")
	dsm.CreateDataset("bar")

	value := store.readValue(STORE_NEXT_DATASET_ID_BYTES)
	store.storeValue([]byte("STORE_NEXT_DATASET_ID"), value)
	store.deleteValue(STORE_NEXT_DATASET_ID_BYTES)

	readValue := store.readValue([]byte("STORE_NEXT_DATASET_ID"))
	if binary.BigEndian.Uint32(readValue) != uint32(4) {
		t.Error("did not have correct next-id in old key")
	}
	if store.readValue(STORE_NEXT_DATASET_ID_BYTES) != nil {
		t.Error("new key should not be set yet")
	}

	//restart
	store.Close()
	store.Open()

	if store.readValue([]byte("STORE_NEXT_DATASET_ID")) != nil {
		t.Error("old key should  be gone")
	}
	if binary.BigEndian.Uint32(store.readValue(STORE_NEXT_DATASET_ID_BYTES)) != uint32(4) {
		t.Error("new key should be 4")
	}
	if dsm.store.nextDatasetID != uint32(4) {
		t.Error("internal next id is wrong")
	}
}
