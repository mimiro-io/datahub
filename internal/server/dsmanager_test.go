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
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/DataDog/datadog-go/statsd"
	"github.com/mimiro-io/datahub/internal/conf"
	"go.uber.org/fx/fxtest"
	"go.uber.org/zap"

	"github.com/franela/goblin"
)

func TestDatasetManager(t *testing.T) {
	g := goblin.Goblin(t)
	g.Describe("The dataset manager", func() {
		testCnt := 0
		var dsm *DsManager
		var store *Store
		var storeLocation string
		g.BeforeEach(func() {
			testCnt += 1
			storeLocation = fmt.Sprintf("./test_dataset_manager_%v", testCnt)
			err := os.RemoveAll(storeLocation)
			g.Assert(err).IsNil("should be allowed to clean testfiles in " + storeLocation)
			e := &conf.Env{
				Logger:        zap.NewNop().Sugar(),
				StoreLocation: storeLocation,
			}

			devNull, _ := os.Open("/dev/null")
			oldErr := os.Stderr
			os.Stderr = devNull
			lc := fxtest.NewLifecycle(t)
			store = NewStore(lc, e, &statsd.NoOpClient{})
			dsm = NewDsManager(lc, e, store, NoOpBus())

			err = lc.Start(context.Background())
			g.Assert(err).IsNil()
			os.Stderr = oldErr
		})
		g.AfterEach(func() {
			_ = store.Close()
			_ = os.RemoveAll(storeLocation)
		})

		g.It("Should give correct dataset details after storage", func() {
			ds, err := dsm.CreateDataset("people")
			g.Assert(err).IsNil()
			g.Assert(ds).IsNotZero()

			details, found, err := dsm.GetDatasetDetails("people")
			g.Assert(err).IsNil()
			g.Assert(found).IsTrue()
			g.Assert(details).IsNotZero()
			g.Assert(details.ID).Eql("ns0:people")
			g.Assert(details.Properties).Eql(map[string]interface{}{"ns0:items": 0, "ns0:name": "people"})

			ds, err = dsm.CreateDataset("more.people")
			g.Assert(err).IsNil()
			g.Assert(ds).IsNotZero()

			err = ds.StoreEntities([]*Entity{{ID: "hei"}})
			g.Assert(err).IsNil()

			details, found, err = dsm.GetDatasetDetails("people")
			g.Assert(details.Properties).Eql(map[string]interface{}{"ns0:items": 0, "ns0:name": "people"},
				"people dataset was not unchanged")

			details, found, err = dsm.GetDatasetDetails("more.people")
			g.Assert(details.Properties).Eql(map[string]interface{}{"ns0:items": 1, "ns0:name": "more.people"})
		})

		g.It("Should persist internal IDs of deleted datasets, so that they are not given out again", func() {
			ds, err := dsm.CreateDataset("people")
			g.Assert(err).IsNil()
			g.Assert(ds).IsNotZero()

			err = dsm.DeleteDataset("people")
			g.Assert(err).IsNil()

			err = store.Close()
			g.Assert(err).IsNil()

			err = store.Open()
			g.Assert(err).IsNil()

			g.Assert(len(store.deletedDatasets)).Eql(1, "Deleted datasets should have surviced restart of store")
			_, ok := store.deletedDatasets[ds.InternalID]
			g.Assert(ok).IsTrue("our ds should still be deleted")

		})

		g.It("Should assign a new internal id to re-created datasets", func() {
			ds, _ := dsm.CreateDataset("people")
			g.Assert(ds).IsNotZero()

			_ = dsm.DeleteDataset("people")

			ds1, _ := dsm.CreateDataset("people")
			g.Assert(ds1).IsNotZero()

			g.Assert(ds1.InternalID == ds.InternalID).IsFalse("re-creating the same dataset after deletin should result in new internal id")
			g.Assert(ds1.ID).Eql(ds.ID, "the re-created dataset's ID should be equal to previously deleted ID")
		})

		g.It("Should persist internal IDs correctly across restarts", func() {
			ds, _ := dsm.CreateDataset("people")
			g.Assert(ds).IsNotZero()
			g.Assert(ds.InternalID).Eql(uint32(2))

			_ = store.Close()
			_ = store.Open()

			ds = dsm.GetDataset("people")
			g.Assert(ds).IsNotZero()
			g.Assert(ds.InternalID).Eql(uint32(2))

			ds2, _ := dsm.CreateDataset("animals")
			g.Assert(ds2).IsNotZero()
			g.Assert(ds2.InternalID).Eql(uint32(3))

		})
	})
}
