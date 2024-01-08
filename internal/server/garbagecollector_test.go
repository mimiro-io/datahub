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
	"fmt"
	"os"

	"github.com/DataDog/datadog-go/v5/statsd"
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/zap"

	"github.com/mimiro-io/datahub/internal/conf"
)

var _ = ginkgo.Describe("The GarbageCollector", func() {
	testCnt := 0
	var dsm *DsManager
	var store *Store
	var storeLocation string
	var gc *GarbageCollector

	// var lc *fxtest.Lifecycle
	ginkgo.BeforeEach(func() {
		testCnt += 1
		storeLocation = fmt.Sprintf("./test_gc_%v", testCnt)
		err := os.RemoveAll(storeLocation)
		Expect(err).To(BeNil(), "should be allowed to clean testfiles in "+storeLocation)
		e := &conf.Config{
			Logger:        zap.NewNop().Sugar(),
			StoreLocation: storeLocation,
		}

		devNull, _ := os.Open("/dev/null")
		oldErr := os.Stderr
		os.Stderr = devNull
		// lc = fxtest.NewLifecycle(internal.FxTestLog(ginkgo.GinkgoT(), false))
		store = NewStore(e, &statsd.NoOpClient{})
		dsm = NewDsManager(e, store, NoOpBus())
		gc = NewGarbageCollector(store, e)
		Expect(err).To(BeNil())

		// err = lc.Start(context.Background())
		// Expect(err).To(BeNil())
		os.Stderr = oldErr
	})
	ginkgo.AfterEach(func() {
		_ = store.Close()
		_ = os.RemoveAll(storeLocation)
	})
	ginkgo.It("Should not delete used data", func() {
		b := gc.store.database
		Expect(count(b)).To(Equal(16))

		ds, _ := dsm.CreateDataset("test", nil)
		Expect(count(b)).To(Equal(24))

		_ = ds.StoreEntities([]*Entity{NewEntity("hei", 0)})
		Expect(count(b)).To(Equal(34))

		gc.GC()
		Expect(count(b)).To(Equal(34))

		gc.Cleandeleted()
		Expect(count(b)).To(Equal(34))
	})

	ginkgo.It("Should remove entities and indexes from deleted datasets", func() {
		b := store.database
		Expect(count(b)).To(Equal(16))

		ds, _ := dsm.CreateDataset("test", nil)
		Expect(count(b)).To(Equal(24))

		ds2, _ := dsm.CreateDataset("delete.me", nil)
		Expect(count(b)).To(Equal(32))

		_ = ds.StoreEntities([]*Entity{NewEntity("p1", 0)})
		Expect(count(b)).To(Equal(42))

		_ = ds2.StoreEntities([]*Entity{NewEntity("p1", 0)})
		Expect(count(b)).To(Equal(50))

		_ = dsm.DeleteDataset("delete.me")
		Expect(count(b)).To(Equal(54), "before cleanup, 4 new keys are expected (deleted dataset state)")

		err := gc.Cleandeleted()
		Expect(err).To(BeNil())
		Expect(count(b)).To(Equal(51), "3 keys should be gone now for the one referenced entity in "+
			"'delete.me': main index, latest and changes")
	})

	ginkgo.It("Should delete the correct incoming and outgoing references", func() {
		b := store.database
		peopleNamespacePrefix, _ := store.NamespaceManager.AssertPrefixMappingForExpansion(
			"http://data.mimiro.io/people/",
		)
		workPrefix, _ := store.NamespaceManager.AssertPrefixMappingForExpansion("http://data.mimiro.io/work/")
		Expect(count(b)).To(Equal(16))

		friendsDS, _ := dsm.CreateDataset("friends", nil)
		Expect(count(b)).To(Equal(24))

		workDS, _ := dsm.CreateDataset("work", nil)
		Expect(count(b)).To(Equal(32))

		_ = friendsDS.StoreEntities([]*Entity{
			NewEntityFromMap(map[string]interface{}{
				"id":    peopleNamespacePrefix + ":person-1",
				"props": map[string]interface{}{peopleNamespacePrefix + ":Name": "Lisa"},
				"refs": map[string]interface{}{
					peopleNamespacePrefix + ":Friend": peopleNamespacePrefix + ":person-3",
				},
			}),
			NewEntityFromMap(map[string]interface{}{
				"id":    peopleNamespacePrefix + ":person-2",
				"props": map[string]interface{}{peopleNamespacePrefix + ":Name": "Bob"},
				"refs": map[string]interface{}{
					peopleNamespacePrefix + ":Friend": peopleNamespacePrefix + ":person-1",
				},
			}),
		})
		Expect(count(b)).To(Equal(55))

		// check that we can query outgoing
		result, err := store.GetManyRelatedEntities(
			[]string{"http://data.mimiro.io/people/person-1"},
			peopleNamespacePrefix+":Friend",
			false,
			nil, true,
		)
		Expect(err).To(BeNil())
		Expect(len(result)).To(Equal(1))
		Expect(result[0][1]).To(Equal(peopleNamespacePrefix + ":Friend"))
		Expect(result[0][2].(*Entity).ID).To(Equal(peopleNamespacePrefix + ":person-3"))

		// check that we can query incoming
		result, err = store.GetManyRelatedEntities(
			[]string{"http://data.mimiro.io/people/person-1"},
			peopleNamespacePrefix+":Friend",
			true,
			nil, true,
		)
		Expect(err).To(BeNil())
		Expect(len(result)).To(Equal(1))
		Expect(result[0][1]).To(Equal(peopleNamespacePrefix + ":Friend"))
		Expect(result[0][2].(*Entity).ID).To(Equal(peopleNamespacePrefix + ":person-2"))

		_ = workDS.StoreEntities([]*Entity{
			NewEntityFromMap(map[string]interface{}{
				"id":    peopleNamespacePrefix + ":person-1",
				"props": map[string]interface{}{workPrefix + ":Wage": "110"},
				"refs":  map[string]interface{}{workPrefix + ":Coworker": peopleNamespacePrefix + ":person-2"},
			}),
			NewEntityFromMap(map[string]interface{}{
				"id":    peopleNamespacePrefix + ":person-2",
				"props": map[string]interface{}{workPrefix + ":Wage": "100"},
				"refs":  map[string]interface{}{workPrefix + ":Coworker": peopleNamespacePrefix + ":person-3"},
			}),
		})
		Expect(count(b)).To(Equal(72))

		// check that we can still query outgoing
		result, err = store.GetManyRelatedEntities(
			[]string{"http://data.mimiro.io/people/person-1"},
			peopleNamespacePrefix+":Friend",
			false,
			nil, true,
		)
		Expect(err).To(BeNil())
		Expect(len(result)).To(Equal(1), "Expected still to find person-3 as a friend")
		Expect(result[0][2].(*Entity).ID).To(Equal(peopleNamespacePrefix + ":person-3"))

		// and incoming
		result, err = store.GetManyRelatedEntities(
			[]string{"http://data.mimiro.io/people/person-1"},
			peopleNamespacePrefix+":Friend",
			true,
			nil, true,
		)
		Expect(err).To(BeNil())
		Expect(len(result)).To(Equal(1), "Expected still to find person-2 as reverse friend")
		Expect(result[0][2].(*Entity).ID).To(Equal(peopleNamespacePrefix + ":person-2"))

		_ = dsm.DeleteDataset("work")
		Expect(count(b)).To(Equal(76), "before cleanup, 4 new keys are expected (deleted dataset state)")

		err = gc.Cleandeleted()
		Expect(err).To(BeNil())
		Expect(count(b)).To(Equal(66), "two entities with 5 keys each should be removed now")

		// make sure we still can query
		result, err = store.GetManyRelatedEntities(
			[]string{"http://data.mimiro.io/people/person-1"},
			"*",
			false,
			nil, true,
		)
		Expect(err).To(BeNil())
		Expect(len(result)).To(Equal(1))
		Expect(result[0][1]).To(Equal(peopleNamespacePrefix + ":Friend"))
		Expect(result[0][2].(*Entity).ID).To(Equal(peopleNamespacePrefix + ":person-3"))

		result, err = store.GetManyRelatedEntities(
			[]string{"http://data.mimiro.io/people/person-2"},
			"*",
			false,
			nil, true,
		)
		Expect(err).To(BeNil())
		Expect(len(result)).To(Equal(1))
		Expect(result[0][1]).To(Equal(peopleNamespacePrefix + ":Friend"))
		Expect(result[0][2].(*Entity).ID).To(Equal(peopleNamespacePrefix + ":person-1"))
	})

	ginkgo.It("Should stop when asked to", func() {
		go func() { gc.quit <- true }()
		err := gc.Cleandeleted()
		Expect(err).NotTo(BeZero())
		Expect(err.Error()).To(Equal("gc cancelled"))
	})
})
