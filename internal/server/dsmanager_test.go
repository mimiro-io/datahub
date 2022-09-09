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
	"encoding/binary"
	"fmt"
	"github.com/mimiro-io/datahub/internal"
	"math"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/DataDog/datadog-go/v5/statsd"
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
		var gc *GarbageCollector
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
			lc := fxtest.NewLifecycle(internal.FxTestLog(t, false))
			store = NewStore(lc, e, &statsd.NoOpClient{})
			dsm = NewDsManager(lc, e, store, NoOpBus())
			gc = NewGarbageCollector(lc, store, e)

			err = lc.Start(context.Background())
			g.Assert(err).IsNil()
		})
		g.AfterEach(func() {
			_ = store.Close()
			_ = os.RemoveAll(storeLocation)
		})

		g.It("Should give correct dataset details after storage", func() {
			ds, err := dsm.CreateDataset("people", nil)
			g.Assert(err).IsNil()
			g.Assert(ds).IsNotZero()

			details, found, err := dsm.GetDatasetDetails("people")
			g.Assert(err).IsNil()
			g.Assert(found).IsTrue()
			g.Assert(details).IsNotZero()
			g.Assert(details.ID).Eql("ns0:people")
			g.Assert(details.Properties).Eql(map[string]interface{}{"ns0:items": 0, "ns0:name": "people"})

			ds, err = dsm.CreateDataset("more.people", nil)
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
			ds, err := dsm.CreateDataset("people", nil)
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
			ds, _ := dsm.CreateDataset("people", nil)
			g.Assert(ds).IsNotZero()

			_ = dsm.DeleteDataset("people")

			ds1, _ := dsm.CreateDataset("people", nil)
			g.Assert(ds1).IsNotZero()

			g.Assert(ds1.InternalID == ds.InternalID).IsFalse("re-creating the same dataset after deletin should result in new internal id")
			g.Assert(ds1.ID).Eql(ds.ID, "the re-created dataset's ID should be equal to previously deleted ID")
		})

		g.It("Should persist internal IDs correctly across restarts", func() {
			ds, _ := dsm.CreateDataset("people", nil)
			g.Assert(ds).IsNotZero()
			g.Assert(ds.InternalID).Eql(uint32(2))

			_ = store.Close()
			_ = store.Open()

			ds = dsm.GetDataset("people")
			g.Assert(ds).IsNotZero()
			g.Assert(ds.InternalID).Eql(uint32(2))

			ds2, _ := dsm.CreateDataset("animals", nil)
			g.Assert(ds2).IsNotZero()
			g.Assert(ds2.InternalID).Eql(uint32(3))

		})

		g.It("Should store and overwrite and restore datasets at same pace", func() {
			// create datasets
			ds, _ := dsm.CreateDataset("people0", nil)

			batchSize := 100
			iterationSize := 10
			totalcnt := 0
			uniq := 0
			var avgs []int64
			// we write the same batches to a new dataset 3 times:
			//  - first iteration to an empty store
			//  - second iteration after a dataset delete + dataset create, followed by GC
			//  - third iteration after a dataset delete + dataset create, no GC
			// all three write processes should be in the same performance range
			for deletes := 0; deletes < 3; deletes++ {
				//t.Log("restored dataset times: ", deletes)
				var times []time.Duration
				for rewrites := 0; rewrites < 2; rewrites++ {
					prefix := "http://data.mimiro.io/people/p1-"
					idcounter := uint64(0)
					//write 5 batches
					for j := 0; j < iterationSize; j++ {
						entities := make([]*Entity, batchSize)
						// of 10000 entities each
						for i := 0; i < batchSize; i++ {
							uniq++
							entity := NewEntity(fmt.Sprint(idcounter), idcounter)
							entity.Properties[prefix+":Name"] = "homer"
							entity.Properties[prefix+":uniqueness"] = fmt.Sprint(uniq)
							entity.References[prefix+":type"] = prefix + "/Person"
							entity.References[prefix+":f1"] = prefix + "/Person-1"
							entity.References[prefix+":f2"] = prefix + "/Person-2"
							entity.References[prefix+":f3"] = prefix + "/Person-3"
							entity.References[prefix+":f4"] = prefix + "/Person-4"
							entity.References[prefix+":f5"] = prefix + "/Person-5"
							entity.References[prefix+":f6"] = prefix + "/Person-6"
							entity.References[prefix+":f7"] = prefix + "/Person-7"
							entity.References[prefix+":f8"] = prefix + "/Person-8"
							entity.References[prefix+":f9"] = prefix + "/Person-9"

							entities[i] = entity
							idcounter++
						}
						ts := time.Now()
						totalcnt += len(entities)
						_ = ds.StoreEntities(entities)
						//t.Log("StoreEntities of batch took ", time.Now().Sub(ts))
						times = append(times, time.Now().Sub(ts))
					}
				}
				totalDur := time.Duration(0)
				for _, d := range times {
					totalDur = d + totalDur
				}
				avgs = append(avgs, totalDur.Nanoseconds()/int64(len(times)))
				_ = dsm.DeleteDataset("people0")
				ds, _ = dsm.CreateDataset("people0", nil)
				if deletes == 0 {
					_ = gc.Cleandeleted()
					_ = gc.GC()
				}
			}
			g.Assert(math.Abs(float64(avgs[1])-float64(avgs[0])) < float64(avgs[0])).IsTrue(
				"Average batch time after first delete with gc should not exceed twice average of first batch-loop")
			g.Assert(math.Abs(float64(avgs[2])-float64(avgs[0])) < float64(avgs[0])).IsTrue(
				"Average batch time after 2nd delete should not exceed twice average of first batch-loop")
		})

		g.Describe("rename dataset", func() {
			g.It("should fail but not panic for non existing src", func() {
				_, err := dsm.UpdateDataset("people0", &UpdateDatasetConfig{ID: "people1"})
				g.Assert(err).IsNotZero()
			})
			g.It("should fail but not panic for existing rename target", func() {
				_, _ = dsm.CreateDataset("people0", nil)
				_, _ = dsm.CreateDataset("people1", nil)
				_, err := dsm.UpdateDataset("people0", &UpdateDatasetConfig{ID: "people1"})
				g.Assert(err).IsNotZero()
			})
			g.It("should replace entry in system collection", func() {
				_, _ = dsm.CreateDataset("people0", nil)
				prefix := make([]byte, 2)
				binary.BigEndian.PutUint16(prefix, SYS_DATASETS_ID)
				storedDatasets := map[string]*Dataset{}
				dsm.store.iterateObjects(prefix, reflect.TypeOf(Dataset{}), func(i interface{}) error {
					ds := i.(*Dataset)
					storedDatasets[ds.ID] = ds
					return nil
				})
				g.Assert(len(storedDatasets)).Eql(2)
				g.Assert(storedDatasets["people0"]).IsNotNil()
				g.Assert(storedDatasets["people0"].InternalID).Eql(uint32(2))
				g.Assert(storedDatasets["people0"].SubjectIdentifier).Eql("http://data.mimiro.io/datasets/people0")

				// do the rename
				_, err := dsm.UpdateDataset("people0", &UpdateDatasetConfig{ID: "people1"})
				g.Assert(err).IsNil()

				storedDatasets = map[string]*Dataset{}
				dsm.store.iterateObjects(prefix, reflect.TypeOf(Dataset{}), func(i interface{}) error {
					ds := i.(*Dataset)
					storedDatasets[ds.ID] = ds
					return nil
				})
				g.Assert(len(storedDatasets)).Eql(2)
				g.Assert(storedDatasets["people1"]).IsNotNil()
				g.Assert(storedDatasets["people1"].InternalID).Eql(uint32(2))
				g.Assert(storedDatasets["people1"].SubjectIdentifier).Eql("http://data.mimiro.io/datasets/people1")
			})
			g.It("should replace entry in cached dataset list", func() {

				_, _ = dsm.CreateDataset("people0", nil)
				g.Assert(len(dsm.GetDatasetNames())).Eql(2)
				seen := false
				for _, n := range dsm.GetDatasetNames() {
					if n.Name == "people0" {
						seen = true
					}
				}
				g.Assert(seen).IsTrue()
				g.Assert(dsm.GetDataset("people0").InternalID).Eql(uint32(2))

				// do the rename
				_, err := dsm.UpdateDataset("people0", &UpdateDatasetConfig{ID: "people1"})
				g.Assert(err).IsNil()

				g.Assert(len(dsm.GetDatasetNames())).Eql(2)
				seen = false
				for _, n := range dsm.GetDatasetNames() {
					if n.Name == "people1" {
						seen = true
					}
				}
				g.Assert(seen).IsTrue()
				g.Assert(dsm.GetDataset("people1").InternalID).Eql(uint32(2))
				g.Assert(dsm.GetDataset("people0")).IsNil()
			})

			g.It("should replace entry in core.Dataset, and dataset content", func() {
				coreDs := dsm.GetDataset("core.Dataset")
				ds, _ := dsm.CreateDataset("people0", nil)
				_ = ds.StoreEntities([]*Entity{NewEntity("item0", 0), NewEntity("item1", 0), NewEntity("item2", 0)})
				dsContent, err := ds.GetChanges(0, 100, false)
				g.Assert(err).IsNil()
				coreChanges, _ := coreDs.GetChanges(0, 100, true)
				g.Assert(coreChanges).IsNotNil()
				g.Assert(len(coreChanges.Entities)).Eql(2)
				seen := false
				for _, e := range coreChanges.Entities {
					if e.ID == "ns0:people0" {
						seen = true
						g.Assert(e.IsDeleted).IsFalse()
						g.Assert(e.Properties["ns0:name"]).Eql("people0")
						g.Assert(e.Properties["ns0:items"]).Eql(3.0)
					}
				}
				g.Assert(seen).IsTrue()

				// do the rename
				_, err = dsm.UpdateDataset("people0", &UpdateDatasetConfig{ID: "people1"})
				g.Assert(err).IsNil()
				coreChanges, _ = coreDs.GetChanges(0, 100, true)
				g.Assert(coreChanges).IsNotNil()
				g.Assert(len(coreChanges.Entities)).Eql(3)
				seen0 := false
				seen1 := false
				for _, e := range coreChanges.Entities {
					if e.ID == "ns0:people0" {
						seen0 = true
						g.Assert(e.IsDeleted).IsTrue()
					}
					if e.ID == "ns0:people1" {
						seen1 = true
						g.Assert(e.IsDeleted).IsFalse()
						g.Assert(e.Properties["ns0:name"]).Eql("people1")
						g.Assert(e.Properties["ns0:items"]).Eql(3.0)
					}
				}
				g.Assert(seen0).IsTrue()
				g.Assert(seen1).IsTrue()
				dsContentNew, err := ds.GetChanges(0, 100, false)
				g.Assert(err).IsNil()
				g.Assert(dsContentNew).Eql(dsContent)
			})
		})
	})
}
