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
	"math"
	"os"
	"reflect"
	"time"

	"github.com/DataDog/datadog-go/v5/statsd"
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/fx/fxtest"
	"go.uber.org/zap"

	"github.com/mimiro-io/datahub/internal"
	"github.com/mimiro-io/datahub/internal/conf"
)

var _ = ginkgo.Describe("The dataset manager", func() {
	testCnt := 0
	var dsm *DsManager
	var store *Store
	var gc *GarbageCollector
	var storeLocation string
	ginkgo.BeforeEach(func() {
		testCnt += 1
		storeLocation = fmt.Sprintf("./test_dataset_manager_%v", testCnt)
		err := os.RemoveAll(storeLocation)
		Expect(err).To(BeNil(), "should be allowed to clean testfiles in "+storeLocation)
		e := &conf.Env{
			Logger:        zap.NewNop().Sugar(),
			StoreLocation: storeLocation,
		}
		lc := fxtest.NewLifecycle(internal.FxTestLog(ginkgo.GinkgoT(), false))
		store = NewStore(lc, e, &statsd.NoOpClient{})
		dsm = NewDsManager(lc, e, store, NoOpBus())
		gc = NewGarbageCollector(lc, store, e)

		err = lc.Start(context.Background())
		Expect(err).To(BeNil())
	})
	ginkgo.AfterEach(func() {
		_ = store.Close()
		_ = os.RemoveAll(storeLocation)
	})

	ginkgo.It("Should give correct dataset details after storage", func() {
		ds, err := dsm.CreateDataset("people", nil)
		Expect(err).To(BeNil())
		Expect(ds).NotTo(BeZero())

		details, found, err := dsm.GetDatasetDetails("people")
		Expect(err).To(BeNil())
		Expect(found).To(BeTrue())
		Expect(details).NotTo(BeZero())
		Expect(details.ID).To(Equal("ns0:people"))
		Expect(details.Properties).To(Equal(map[string]interface{}{"ns0:items": float64(0), "ns0:name": "people"}))

		ds, err = dsm.CreateDataset("more.people", nil)
		Expect(err).To(BeNil())
		Expect(ds).NotTo(BeZero())

		err = ds.StoreEntities([]*Entity{{ID: "hei"}})
		Expect(err).To(BeNil())

		details, _, _ = dsm.GetDatasetDetails("people")
		Expect(details.Properties).To(Equal(map[string]interface{}{"ns0:items": float64(0), "ns0:name": "people"}),
			"people dataset was not unchanged")

		details, _, _ = dsm.GetDatasetDetails("more.people")
		Expect(details.Properties).To(Equal(map[string]interface{}{"ns0:items": float64(1), "ns0:name": "more.people"}))
	})

	ginkgo.It("Should persist internal IDs of deleted datasets, so that they are not given out again", func() {
		ds, err := dsm.CreateDataset("people", nil)
		Expect(err).To(BeNil())
		Expect(ds).NotTo(BeZero())

		err = dsm.DeleteDataset("people")
		Expect(err).To(BeNil())

		err = store.Close()
		Expect(err).To(BeNil())

		err = store.Open()
		Expect(err).To(BeNil())

		Expect(len(store.deletedDatasets)).To(Equal(1), "Deleted datasets should have surviced restart of store")
		_, ok := store.deletedDatasets[ds.InternalID]
		Expect(ok).To(BeTrue(), "our ds should still be deleted")
	})

	ginkgo.It("Should assign a new internal id to re-created datasets", func() {
		ds, _ := dsm.CreateDataset("people", nil)
		Expect(ds).NotTo(BeZero())

		_ = dsm.DeleteDataset("people")

		ds1, _ := dsm.CreateDataset("people", nil)
		Expect(ds1).NotTo(BeZero())

		Expect(ds1.InternalID == ds.InternalID).
			To(BeFalse(), "re-creating the same dataset after deletin should result in new internal id")
		Expect(ds1.ID).To(Equal(ds.ID), "the re-created dataset's ID should be equal to previously deleted ID")
	})

	ginkgo.It("Should persist internal IDs correctly across restarts", func() {
		ds, _ := dsm.CreateDataset("people", nil)
		Expect(ds).NotTo(BeZero())
		Expect(ds.InternalID).To(Equal(uint32(2)))

		_ = store.Close()
		_ = store.Open()

		ds = dsm.GetDataset("people")
		Expect(ds).NotTo(BeZero())
		Expect(ds.InternalID).To(Equal(uint32(2)))

		ds2, _ := dsm.CreateDataset("animals", nil)
		Expect(ds2).NotTo(BeZero())
		Expect(ds2.InternalID).To(Equal(uint32(3)))
	})

	ginkgo.It("Should store and overwrite and restore datasets at same pace", func() {
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
			// t.Log("restored dataset times: ", deletes)
			var times []time.Duration
			for rewrites := 0; rewrites < 2; rewrites++ {
				prefix := "http://data.mimiro.io/people/p1-"
				idcounter := uint64(0)
				// write 5 batches
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
					// t.Log("StoreEntities of batch took ", time.Now().Sub(ts))
					times = append(times, time.Since(ts))
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
		Expect(math.Abs(float64(avgs[1])-float64(avgs[0])) < float64(avgs[0])).To(BeTrue(),
			"Average batch time after first delete with gc should not exceed twice average of first batch-loop")
		Expect(math.Abs(float64(avgs[2])-float64(avgs[0])) < float64(avgs[0])).To(BeTrue(),
			"Average batch time after 2nd delete should not exceed twice average of first batch-loop")
	})

	ginkgo.Describe("rename dataset", func() {
		ginkgo.It("should fail but not panic for non existing src", func() {
			_, err := dsm.UpdateDataset("people0", &UpdateDatasetConfig{ID: "people1"})
			Expect(err).NotTo(BeZero())
		})
		ginkgo.It("should fail but not panic for existing rename target", func() {
			_, _ = dsm.CreateDataset("people0", nil)
			_, _ = dsm.CreateDataset("people1", nil)
			_, err := dsm.UpdateDataset("people0", &UpdateDatasetConfig{ID: "people1"})
			Expect(err).NotTo(BeZero())
		})
		ginkgo.It("should replace entry in system collection", func() {
			_, _ = dsm.CreateDataset("people0", nil)
			prefix := make([]byte, 2)
			binary.BigEndian.PutUint16(prefix, SysDatasetsID)
			storedDatasets := map[string]*Dataset{}
			dsm.store.iterateObjects(prefix, reflect.TypeOf(Dataset{}), func(i interface{}) error {
				ds := i.(*Dataset)
				storedDatasets[ds.ID] = ds
				return nil
			})
			Expect(len(storedDatasets)).To(Equal(2))
			Expect(storedDatasets["people0"]).NotTo(BeNil())
			Expect(storedDatasets["people0"].InternalID).To(Equal(uint32(2)))
			Expect(storedDatasets["people0"].SubjectIdentifier).To(Equal("http://data.mimiro.io/datasets/people0"))

			// do the rename
			_, err := dsm.UpdateDataset("people0", &UpdateDatasetConfig{ID: "people1"})
			Expect(err).To(BeNil())

			storedDatasets = map[string]*Dataset{}
			dsm.store.iterateObjects(prefix, reflect.TypeOf(Dataset{}), func(i interface{}) error {
				ds := i.(*Dataset)
				storedDatasets[ds.ID] = ds
				return nil
			})
			Expect(len(storedDatasets)).To(Equal(2))
			Expect(storedDatasets["people1"]).NotTo(BeNil())
			Expect(storedDatasets["people1"].InternalID).To(Equal(uint32(2)))
			Expect(storedDatasets["people1"].SubjectIdentifier).To(Equal("http://data.mimiro.io/datasets/people1"))
		})
		ginkgo.It("should replace entry in cached dataset list", func() {
			_, _ = dsm.CreateDataset("people0", nil)
			Expect(len(dsm.GetDatasetNames())).To(Equal(2))
			seen := false
			for _, n := range dsm.GetDatasetNames() {
				if n.Name == "people0" {
					seen = true
				}
			}
			Expect(seen).To(BeTrue())
			Expect(dsm.GetDataset("people0").InternalID).To(Equal(uint32(2)))

			// do the rename
			_, err := dsm.UpdateDataset("people0", &UpdateDatasetConfig{ID: "people1"})
			Expect(err).To(BeNil())

			Expect(len(dsm.GetDatasetNames())).To(Equal(2))
			seen = false
			for _, n := range dsm.GetDatasetNames() {
				if n.Name == "people1" {
					seen = true
				}
			}
			Expect(seen).To(BeTrue())
			Expect(dsm.GetDataset("people1").InternalID).To(Equal(uint32(2)))
			Expect(dsm.GetDataset("people0")).To(BeNil())
		})

		ginkgo.It("should replace entry in core.Dataset, and dataset content", func() {
			coreDs := dsm.GetDataset("core.Dataset")
			ds, _ := dsm.CreateDataset("people0", nil)
			_ = ds.StoreEntities([]*Entity{NewEntity("item0", 0), NewEntity("item1", 0), NewEntity("item2", 0)})
			dsContent, err := ds.GetChanges(0, 100, false)
			Expect(err).To(BeNil())
			coreChanges, _ := coreDs.GetChanges(0, 100, true)
			Expect(coreChanges).NotTo(BeNil())
			Expect(len(coreChanges.Entities)).To(Equal(2))
			seen := false
			for _, e := range coreChanges.Entities {
				if e.ID == "ns0:people0" {
					seen = true
					Expect(e.IsDeleted).To(BeFalse())
					Expect(e.Properties["ns0:name"]).To(Equal("people0"))
					Expect(e.Properties["ns0:items"]).To(Equal(3.0))
				}
			}
			Expect(seen).To(BeTrue())

			// do the rename
			_, err = dsm.UpdateDataset("people0", &UpdateDatasetConfig{ID: "people1"})
			Expect(err).To(BeNil())
			coreChanges, _ = coreDs.GetChanges(0, 100, true)
			Expect(coreChanges).NotTo(BeNil())
			Expect(len(coreChanges.Entities)).To(Equal(3))
			seen0 := false
			seen1 := false
			for _, e := range coreChanges.Entities {
				if e.ID == "ns0:people0" {
					seen0 = true
					Expect(e.IsDeleted).To(BeTrue())
				}
				if e.ID == "ns0:people1" {
					seen1 = true
					Expect(e.IsDeleted).To(BeFalse())
					Expect(e.Properties["ns0:name"]).To(Equal("people1"))
					Expect(e.Properties["ns0:items"]).To(Equal(3.0))
				}
			}
			Expect(seen0).To(BeTrue())
			Expect(seen1).To(BeTrue())
			dsContentNew, err := ds.GetChanges(0, 100, false)
			Expect(err).To(BeNil())
			Expect(dsContentNew).To(Equal(dsContent))
		})
	})
})
