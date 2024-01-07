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
	"reflect"
	"strconv"
	"sync"
	"time"

	"github.com/DataDog/datadog-go/v5/statsd"
	"github.com/dgraph-io/badger/v4"
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/zap"

	"github.com/mimiro-io/datahub/internal/conf"
)

var _ = ginkgo.Describe("The dataset storage", func() {
	testCnt := 0
	var dsm *DsManager
	var store *Store
	var storeLocation string
	ginkgo.BeforeEach(func() {
		testCnt += 1
		storeLocation = fmt.Sprintf("./test_store_relations_%v", testCnt)
		err := os.RemoveAll(storeLocation)
		Expect(err).To(BeNil(), "should be allowed to clean testfiles in "+storeLocation)
		e := &conf.Config{
			Logger:        zap.NewNop().Sugar(),
			StoreLocation: storeLocation,
		}
		// lc := fxtest.NewLifecycle(internal.FxTestLog(ginkgo.GinkgoT(), false))
		store = NewStore(e, &statsd.NoOpClient{})
		dsm = NewDsManager(e, store, NoOpBus())

		// err = lc.Start(context.Background())
		// Expect(err).To(BeNil())
	})
	ginkgo.AfterEach(func() {
		// closing is cleaner, but may trigger compaction and other stuff causing the tests to hang.
		// since we are not reopening the same store, we can just skip closing
		//	_ = store.Close()
		_ = os.RemoveAll(storeLocation)
	})

	ginkgo.It("Should delete the correct old outgoing references in array after entity modified", func() {
		peopleNamespacePrefix, _ := store.NamespaceManager.AssertPrefixMappingForExpansion(
			"http://data.mimiro.io/people/",
		)
		friendsDS, _ := dsm.CreateDataset("friends", nil)

		_ = friendsDS.StoreEntities([]*Entity{
			NewEntityFromMap(map[string]interface{}{
				"id":    peopleNamespacePrefix + ":person-1",
				"props": map[string]interface{}{peopleNamespacePrefix + ":Name": "Lisa"},
				"refs": map[string]interface{}{
					peopleNamespacePrefix + ":Friend": []string{
						peopleNamespacePrefix + ":person-3",
						peopleNamespacePrefix + ":person-2",
					},
				},
			}),
		})

		// check that we can query outgoing
		result, err := store.GetManyRelatedEntities(
			[]string{"http://data.mimiro.io/people/person-1"},
			peopleNamespacePrefix+":Friend",
			false,
			nil, true,
		)
		Expect(err).To(BeNil())
		Expect(len(result)).To(Equal(2))

		// check that we can query incoming
		result, err = store.GetManyRelatedEntities(
			[]string{"http://data.mimiro.io/people/person-2"},
			peopleNamespacePrefix+":Friend",
			true,
			nil, true,
		)
		Expect(err).To(BeNil())
		Expect(len(result)).To(Equal(1))

		// update lisa
		e := NewEntityFromMap(map[string]interface{}{
			"id":    peopleNamespacePrefix + ":person-1",
			"props": map[string]interface{}{peopleNamespacePrefix + ":Name": "Lisa"},
			"refs":  map[string]interface{}{peopleNamespacePrefix + ":Friend": peopleNamespacePrefix + ":person-3"},
		})

		_ = friendsDS.StoreEntities([]*Entity{
			e,
		})
		// check that outgoing related entities is 0
		result, err = store.GetManyRelatedEntities(
			[]string{"http://data.mimiro.io/people/person-1"},
			peopleNamespacePrefix+":Friend",
			false,
			nil, true,
		)
		Expect(err).To(BeNil())
		Expect(len(result)).To(Equal(1))

		result, err = store.GetManyRelatedEntities(
			[]string{"http://data.mimiro.io/people/person-3"},
			peopleNamespacePrefix+":Friend",
			true,
			nil, true,
		)
		Expect(err).To(BeNil())
		Expect(len(result)).To(Equal(1))

		result, err = store.GetManyRelatedEntities(
			[]string{"http://data.mimiro.io/people/person-2"},
			peopleNamespacePrefix+":Friend",
			true,
			nil, true,
		)
		Expect(err).To(BeNil())
		Expect(len(result)).To(Equal(0))
	})

	ginkgo.It("Should delete the correct old outgoing references after entity modified", func() {
		peopleNamespacePrefix, _ := store.NamespaceManager.AssertPrefixMappingForExpansion(
			"http://data.mimiro.io/people/",
		)
		friendsDS, _ := dsm.CreateDataset("friends", nil)

		_ = friendsDS.StoreEntities([]*Entity{
			NewEntityFromMap(map[string]interface{}{
				"id":    peopleNamespacePrefix + ":person-1",
				"props": map[string]interface{}{peopleNamespacePrefix + ":Name": "Lisa"},
				"refs": map[string]interface{}{
					peopleNamespacePrefix + ":Friend": peopleNamespacePrefix + ":person-3",
				},
			}),
		})

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

		// delete lisa
		e := NewEntityFromMap(map[string]interface{}{
			"id":    peopleNamespacePrefix + ":person-1",
			"props": map[string]interface{}{peopleNamespacePrefix + ":Name": "Lisa"},
			"refs":  map[string]interface{}{},
		})

		_ = friendsDS.StoreEntities([]*Entity{
			e,
		})

		// check that outgoing related entities is 0
		result, err = store.GetManyRelatedEntities(
			[]string{"http://data.mimiro.io/people/person-1"},
			peopleNamespacePrefix+":Friend",
			false,
			nil, true,
		)
		Expect(err).To(BeNil())
		Expect(len(result)).To(Equal(0))
	})

	ginkgo.It("Should delete the correct incoming and outgoing references after entity deleted", func() {
		peopleNamespacePrefix, _ := store.NamespaceManager.AssertPrefixMappingForExpansion(
			"http://data.mimiro.io/people/",
		)
		friendsDS, _ := dsm.CreateDataset("friends", nil)

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
				"props": map[string]interface{}{peopleNamespacePrefix + ":Name": "Homer"},
				"refs": map[string]interface{}{
					peopleNamespacePrefix + ":Friend": peopleNamespacePrefix + ":person-1",
				},
			}),
		})

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

		// delete lisa
		e := NewEntityFromMap(map[string]interface{}{
			"id":    peopleNamespacePrefix + ":person-1",
			"props": map[string]interface{}{peopleNamespacePrefix + ":Name": "Lisa"},
			"refs":  map[string]interface{}{peopleNamespacePrefix + ":Friend": peopleNamespacePrefix + ":person-3"},
		})
		e.IsDeleted = true

		_ = friendsDS.StoreEntities([]*Entity{
			e,
		})

		// check that outgoing related entities is 0
		result, err = store.GetManyRelatedEntities(
			[]string{"http://data.mimiro.io/people/person-1"},
			peopleNamespacePrefix+":Friend",
			false,
			nil, true,
		)
		Expect(err).To(BeNil())
		Expect(len(result)).To(Equal(0))

		// check that we can query incoming count to person-3 is 0
		result, err = store.GetManyRelatedEntities(
			[]string{"http://data.mimiro.io/people/person-3"},
			peopleNamespacePrefix+":Friend",
			true,
			nil, true,
		)
		Expect(err).To(BeNil())
		Expect(len(result)).To(Equal(0))

		// check that we can query incoming count to person-1 is still 1
		result, err = store.GetManyRelatedEntities(
			[]string{"http://data.mimiro.io/people/person-1"},
			peopleNamespacePrefix+":Friend",
			true,
			nil, true,
		)
		Expect(err).To(BeNil())
		Expect(len(result)).To(Equal(1))
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
				"props": map[string]interface{}{peopleNamespacePrefix + ":Name": "Lisa"},
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
		// result, err = store.GetManyRelatedEntities( []string{"http://data.mimiro.io/people/person-1"}, "*", false, nil)
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
	})

	ginkgo.It("Should store references of new deleted entities as deleted", func() {
		peopleNamespacePrefix, _ := store.NamespaceManager.AssertPrefixMappingForExpansion(
			"http://data.mimiro.io/people/",
		)
		friendsDS, _ := dsm.CreateDataset("friends", nil)

		p1 := NewEntityFromMap(map[string]interface{}{
			"id":    peopleNamespacePrefix + ":person-1",
			"props": map[string]interface{}{peopleNamespacePrefix + ":Name": "Lisa"},
			"refs":  map[string]interface{}{peopleNamespacePrefix + ":Friend": peopleNamespacePrefix + ":person-3"},
		})
		p1.IsDeleted = true
		_ = friendsDS.StoreEntities([]*Entity{
			p1,
			NewEntityFromMap(map[string]interface{}{
				"id":    peopleNamespacePrefix + ":person-2",
				"props": map[string]interface{}{peopleNamespacePrefix + ":Name": "Homer"},
				"refs": map[string]interface{}{
					peopleNamespacePrefix + ":Friend": peopleNamespacePrefix + ":person-1",
				},
			}),
		})
		// check that we can not query outgoing from deleted entity
		result, err := store.GetManyRelatedEntities(
			[]string{"http://data.mimiro.io/people/person-1"},
			peopleNamespacePrefix+":Friend",
			false,
			nil, true,
		)
		Expect(err).To(BeNil())
		Expect(len(result)).To(Equal(0))

		// check that we can query incoming. this relation is owned by Homer
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

		// inverse query from person-3 should not return anything because person-1 (which owns the relation) is deleted
		result, err = store.GetManyRelatedEntities(
			[]string{"http://data.mimiro.io/people/person-3"},
			peopleNamespacePrefix+":Friend",
			true,
			nil, true,
		)
		Expect(err).To(BeNil())
		Expect(len(result)).To(Equal(0))
	})

	ginkgo.It("Should build query results", func() {
		// create dataset
		ds, _ := dsm.CreateDataset("people", nil)

		batchSize := 5
		entities := make([]*Entity, batchSize)

		peopleNamespacePrefix, _ := store.NamespaceManager.AssertPrefixMappingForExpansion(
			"http://data.mimiro.io/people/",
		)
		modelNamespacePrefix, _ := store.NamespaceManager.AssertPrefixMappingForExpansion(
			"http://data.mimiro.io/model/",
		)
		rdfNamespacePrefix, _ := store.NamespaceManager.AssertPrefixMappingForExpansion(RdfNamespaceExpansion)

		for i := 0; i < batchSize; i++ {
			entity := NewEntity(peopleNamespacePrefix+":p-"+strconv.Itoa(i), 0)
			entity.Properties[modelNamespacePrefix+":Name"] = "homer"
			entity.References[rdfNamespacePrefix+":type"] = modelNamespacePrefix + ":Person"
			entity.References[modelNamespacePrefix+":f1"] = peopleNamespacePrefix + ":Person-1"
			entity.References[modelNamespacePrefix+":f2"] = peopleNamespacePrefix + ":Person-2"
			entity.References[modelNamespacePrefix+":f3"] = peopleNamespacePrefix + ":Person-3"
			entities[i] = entity
		}

		err := ds.StoreEntities(entities)
		Expect(err).To(BeNil())

		results, err := store.getRelated("http://data.mimiro.io/people/p-1", RdfTypeURI, false, []string{}, true)
		Expect(err).To(BeNil())
		Expect(len(results)).To(Equal(1))

		invresults, err := store.getRelated("http://data.mimiro.io/model/Person", RdfTypeURI, true, []string{}, true)
		Expect(err).To(BeNil())
		Expect(len(invresults)).To(Equal(5))

		invresults, err = store.getRelated("http://data.mimiro.io/model/Person", "*", true, []string{}, true)
		Expect(err).To(BeNil())
		Expect(len(invresults)).To(Equal(5))

		for i := 0; i < batchSize; i++ {
			entity := NewEntity(peopleNamespacePrefix+":p-"+strconv.Itoa(i), 0)
			entity.Properties[modelNamespacePrefix+":Name"] = "homer"
			entity.References[modelNamespacePrefix+":f1"] = peopleNamespacePrefix + ":Person-1"
			entity.References[modelNamespacePrefix+":f2"] = peopleNamespacePrefix + ":Person-2"
			entity.References[modelNamespacePrefix+":f3"] = peopleNamespacePrefix + ":Person-3"
			entities[i] = entity
		}

		err = ds.StoreEntities(entities)
		Expect(err).To(BeNil())

		time0 := time.Now().UnixNano()

		invresults, err = store.getRelated("http://data.mimiro.io/model/Person", RdfTypeURI, true, []string{}, true)
		Expect(err).To(BeNil())
		Expect(len(invresults)).To(Equal(0))

		results, err = store.getRelated("http://data.mimiro.io/people/p-1", RdfTypeURI, false, []string{}, true)
		Expect(err).To(BeNil())
		Expect(len(results)).To(Equal(0))

		for i := 0; i < batchSize; i++ {
			entity := NewEntity(peopleNamespacePrefix+":p-"+strconv.Itoa(i), 0)
			entity.Properties[modelNamespacePrefix+":Name"] = "homer"
			entity.References[rdfNamespacePrefix+":type"] = modelNamespacePrefix + ":Person"
			entity.References[modelNamespacePrefix+":f1"] = peopleNamespacePrefix + ":Person-1"
			entity.References[modelNamespacePrefix+":f2"] = peopleNamespacePrefix + ":Person-2"
			entity.References[modelNamespacePrefix+":f3"] = peopleNamespacePrefix + ":Person-3"
			entities[i] = entity
		}

		err = ds.StoreEntities(entities)
		Expect(err).To(BeNil())

		invresults, err = store.getRelated("http://data.mimiro.io/model/Person", RdfTypeURI, true, []string{}, true)
		Expect(err).To(BeNil())
		Expect(len(invresults)).To(Equal(5))

		results, err = store.getRelated("http://data.mimiro.io/people/p-1", RdfTypeURI, false, []string{}, true)
		Expect(err).To(BeNil())
		Expect(len(results)).To(Equal(1))

		// test at point in time
		from, _ := store.ToRelatedFrom(
			[]string{"http://data.mimiro.io/people/p-1"},
			RdfTypeURI,
			false,
			nil,
			time0,
		)
		results2, _, err := store.GetRelatedAtTime(from[0], 0)
		Expect(err).To(BeNil())
		Expect(len(results2)).To(Equal(0))

		from, _ = store.ToRelatedFrom(
			[]string{"http://data.mimiro.io/model/Person"},
			RdfTypeURI,
			true,
			nil,
			time0,
		)
		invresults2, _, err := store.GetRelatedAtTime(from[0], 0)
		Expect(err).To(BeNil())
		Expect(len(invresults2)).To(Equal(0))
	})
})

var _ = ginkgo.Describe("The dataset storage", func() {
	testCnt := 0
	var dsm *DsManager
	var store *Store
	var storeLocation string
	ginkgo.BeforeEach(func() {
		testCnt += 1
		storeLocation = fmt.Sprintf("./test_store_%v", testCnt)
		err := os.RemoveAll(storeLocation)
		Expect(err).To(BeNil(), "should be allowed to clean testfiles in "+storeLocation)
		e := &conf.Config{
			Logger:        zap.NewNop().Sugar(),
			StoreLocation: storeLocation,
		}
		// lc := fxtest.NewLifecycle(internal.FxTestLog(ginkgo.GinkgoT(), false))
		store = NewStore(e, &statsd.NoOpClient{})
		dsm = NewDsManager(e, store, NoOpBus())

		// err = lc.Start(context.Background())
		// Expect(err).To(BeNil())
	})
	ginkgo.AfterEach(func() {
		_ = store.Close()
		_ = os.RemoveAll(storeLocation)
	})

	ginkgo.It("Should store the curi id mapping", func() {
		cache := make(map[string]uint64)
		id, _, _ := dsm.store.assertIDForURI("ns0:gra", cache)
		err := dsm.store.commitIDTxn()
		Expect(err).To(BeNil())
		curi, _ := dsm.store.getURIForID(id)
		Expect(curi).To(Equal("ns0:gra"))
	})

	ginkgo.It("Should store entity batches without error", func() {
		// create dataset
		ds, _ := dsm.CreateDataset("people", nil)

		entities := make([]*Entity, 1)
		entity := NewEntity("http://data.mimiro.io/people/homer", 0)
		entity.Properties["Name"] = "homer"
		entity.References["type"] = "http://data.mimiro.io/model/Person"
		entity.References["typed"] = "http://data.mimiro.io/model/Person"
		entities[0] = entity

		err := ds.StoreEntities(entities)
		Expect(err).To(BeNil(), "Expected no error when storing entity")
	})

	ginkgo.It("Should replace entity namespace prefixes", func() {
		// define store context
		_, _ = store.NamespaceManager.AssertPrefixMappingForExpansion(RdfNamespaceExpansion)
		prefix, _ := store.NamespaceManager.AssertPrefixMappingForExpansion("http://data.mimiro.io/model/Person/")

		entity := NewEntity(prefix+":homer", 0)
		entity.Properties[prefix+":Name"] = "homer"
		entity.References[prefix+":type"] = "ns1:Person"
		entity.References[prefix+":company"] = "ns1:Person"

		Expect(entity.ID).To(Equal(prefix + ":homer"))
		err := entity.ExpandIdentifiers(store)
		Expect(err).To(BeNil())
		Expect(entity.ID).To(Equal("http://data.mimiro.io/model/Person/homer"))
	})

	ginkgo.It("Should make entities retrievable", func() {
		// create dataset
		ds, _ := dsm.CreateDataset("people", nil)

		entities := make([]*Entity, 1)
		entity := NewEntity("http://data.mimiro.io/people/homer", 0)
		entity.Properties["Name"] = "homer"
		entity.References["type"] = "http://data.mimiro.io/model/Person"
		entity.References["typed"] = "http://data.mimiro.io/model/Person"
		entities[0] = entity

		err := ds.StoreEntities(entities)
		Expect(err).To(BeNil())

		allEntities := make([]*Entity, 0)
		_, err = ds.MapEntities("", 0, func(e *Entity) error {
			allEntities = append(allEntities, e)
			return nil
		})
		Expect(err).To(BeNil())
		Expect(len(allEntities)).To(Equal(1), "Expected to retrieve same number of entities as was written")
	})

	ginkgo.It("Should only update entities if they are different (batch)", func() {
		// create dataset
		ds, _ := dsm.CreateDataset("people", nil)

		entities := make([]*Entity, 1)
		entity := NewEntity("http://data.mimiro.io/people/homer", 0)
		entity.Properties["Name"] = "homer"
		entity.References["type"] = "http://data.mimiro.io/model/Person"
		entity.References["typed"] = "http://data.mimiro.io/model/Person"
		entities[0] = entity

		// 1. store entity
		err := ds.StoreEntities(entities)
		Expect(err).To(BeNil())
		// and get first versions of object
		res, err := ds.GetEntities("", 1)
		Expect(err).To(BeNil())
		firstVersionTS := res.Entities[0].Recorded

		// 2. store again
		err = ds.StoreEntities(entities)
		Expect(err).To(BeNil())
		// and get version again
		res, _ = ds.GetEntities("", 1)
		Expect(res.Entities[0].Recorded).To(Equal(firstVersionTS), "Should be unchanged version")

		// 3. set same name and store again
		entity.Properties["Name"] = "homer"
		err = ds.StoreEntities(entities)
		Expect(err).To(BeNil())
		// and get version again
		res, _ = ds.GetEntities("", 1)
		Expect(res.Entities[0].Recorded).To(Equal(firstVersionTS), "Should be unchanged version")

		// 4. now set a new name and store
		entity.Properties["Name"] = "homer simpson"
		err = ds.StoreEntities(entities)
		Expect(err).To(BeNil())
		// version should change
		res, _ = ds.GetEntities("", 1)
		Expect(res.Entities[0].Recorded != firstVersionTS).To(BeTrue(), "Should be new version")
	})

	ginkgo.It("Should track changes", func() {
		// create dataset
		_, _ = dsm.CreateDataset("companies", nil)
		_, _ = dsm.CreateDataset("animals", nil)
		ds, err := dsm.CreateDataset("people", nil)
		Expect(err).To(BeNil())

		entities := make([]*Entity, 3)
		entity1 := NewEntity("http://data.mimiro.io/people/homer", 0)
		entities[0] = entity1
		entity2 := NewEntity("http://data.mimiro.io/people/bob", 0)
		entities[1] = entity2
		entity3 := NewEntity("http://data.mimiro.io/people/jim", 0)
		entities[2] = entity3

		_ = ds.StoreEntities(entities)

		_ = dsm.DeleteDataset("people")
		ds, _ = dsm.CreateDataset("people", nil)

		entities = make([]*Entity, 3)
		entity1 = NewEntity("http://data.mimiro.io/people/homer", 0)
		entities[0] = entity1
		entity2 = NewEntity("http://data.mimiro.io/people/bob", 0)
		entities[1] = entity2
		entity3 = NewEntity("http://data.mimiro.io/people/jim", 0)
		entities[2] = entity3

		err = ds.StoreEntities(entities)
		Expect(err).To(BeNil())

		// get changes
		changes, err := ds.GetChanges(0, 10, false)
		Expect(err).To(BeNil())
		Expect(len(changes.Entities)).To(Equal(3))
		Expect(changes.NextToken).To(Equal(uint64(3)))

		changes, err = ds.GetChanges(1, 1, false)
		Expect(err).To(BeNil())
		Expect(len(changes.Entities)).To(Equal(1))
		Expect(changes.NextToken).To(Equal(uint64(2)))

		changes, err = ds.GetChanges(2, 1, false)
		Expect(err).To(BeNil())
		Expect(len(changes.Entities)).To(Equal(1))
		Expect(changes.NextToken).To(Equal(uint64(3)))

		changes, _ = ds.GetChanges(3, 10, false)
		Expect(len(changes.Entities)).To(Equal(0))
		Expect(changes.NextToken).To(Equal(uint64(3)))

		// store modified entities again and then get changes latest true
		entity1.Properties = map[string]interface{}{"http://test.org/name": "homer"}
		ds.StoreEntities(entities)

		changes, _ = ds.GetChanges(0, 0, true)
		Expect(len(changes.Entities)).To(Equal(3))
		Expect(changes.NextToken).To(Equal(uint64(4)))
	})

	ginkgo.It("Should paginate entities with continuation tokens", func() {
		// create dataset
		c, _ := dsm.CreateDataset("companies", nil)
		_, _ = dsm.CreateDataset("animals", nil)
		dsm.CreateDataset("people", nil)

		_ = dsm.DeleteDataset("people")
		ds, _ := dsm.CreateDataset("people", nil)

		entities := make([]*Entity, 3)
		entity1 := NewEntity("http://data.mimiro.io/people/homer", 0)
		entities[0] = entity1
		entity2 := NewEntity("http://data.mimiro.io/people/bob", 0)
		entities[1] = entity2
		entity3 := NewEntity("http://data.mimiro.io/people/jim", 0)
		entities[2] = entity3

		err := ds.StoreEntities(entities)
		Expect(err).To(BeNil())
		// store same entitites in two datasets. should give merged results
		err = c.StoreEntities(entities)
		Expect(err).To(BeNil())

		result, err := ds.GetEntities("", 50)
		Expect(err).To(BeNil())
		Expect(len(result.Entities)).To(Equal(3), "Expected all 3 entitites back")

		result1, err := ds.GetEntities(result.ContinuationToken, 1)
		Expect(err).To(BeNil())
		Expect(len(result1.Entities)).To(Equal(0), "first page should have returned all entities, "+
			"therefore this page should be empty")
		Expect(result1.ContinuationToken).To(Equal(result.ContinuationToken))

		// test c token
		result, err = ds.GetEntities("", 1)
		Expect(err).To(BeNil())
		Expect(len(result.Entities)).To(Equal(1), "we only asked for 1 entitiy this page")

		result, err = ds.GetEntities(result.ContinuationToken, 5)
		Expect(err).To(BeNil())
		Expect(len(result.Entities)).To(Equal(2), "second page should have remaining 2 enitities")

		Expect(result.Entities[0].ID).To(Equal("http://data.mimiro.io/people/bob"), "first result 2nd page is not bob")
	})

	ginkgo.It("Should not struggle with many batch writes", func(_ ginkgo.SpecContext) {
		// create dataset
		ds1, err := dsm.CreateDataset("people0", nil)
		ds2, err := dsm.CreateDataset("people1", nil)
		ds3, err := dsm.CreateDataset("people2", nil)
		ds4, err := dsm.CreateDataset("people3", nil)
		ds5, err := dsm.CreateDataset("people4", nil)

		batchSize := 1000
		iterationSize := 100

		f := func(ds *Dataset, prefix string, wg *sync.WaitGroup) {
			idcounter := uint64(0)
			for j := 0; j < iterationSize; j++ {
				entities := make([]*Entity, batchSize)
				for i := 0; i < batchSize; i++ {
					entity := NewEntity(prefix+strconv.Itoa(i), idcounter)
					entity.Properties[prefix+":Name"] = "homer"
					entity.References[prefix+":type"] = prefix + "/Person"
					entity.References[prefix+":f1"] = prefix + "/Person-1"
					entity.References[prefix+":f2"] = prefix + "/Person-2"
					entity.References[prefix+":f3"] = prefix + "/Person-3"

					entities[i] = entity
					idcounter++
				}
				err = ds.StoreEntities(entities)
				Expect(err).To(BeNil(), "Storing should never fail under load")
			}
			wg.Done()
		}

		var wg sync.WaitGroup
		wg.Add(5)

		go f(ds1, "http://data.mimiro.io/people/p1-", &wg)
		go f(ds2, "http://data.mimiro.io/people/p1-", &wg)
		go f(ds3, "http://data.mimiro.io/people/p1-", &wg)
		go f(ds4, "http://data.mimiro.io/people/p1-", &wg)
		go f(ds5, "http://data.mimiro.io/people/p1-", &wg)

		wg.Wait()
	}, ginkgo.SpecTimeout(5*time.Minute))

	ginkgo.It("Should do deletion detection when running in fullsync mode", func() {
		// create dataset
		ds, err := dsm.CreateDataset("people", nil)
		Expect(err).To(BeNil())
		batchSize := 5

		err = ds.StartFullSync()
		Expect(err).To(BeNil())

		entities := make([]*Entity, batchSize)
		for i := 0; i < batchSize; i++ {
			entity := NewEntity("http://data.mimiro.io/people/p-"+strconv.Itoa(i), 0)
			entity.Properties["Name"] = "homer"
			entities[i] = entity
		}
		err = ds.StoreEntities(entities)
		Expect(err).To(BeNil())

		err = ds.CompleteFullSync(context.Background())
		Expect(err).To(BeNil())

		// start 2nd fullsync
		err = ds.StartFullSync()
		Expect(err).To(BeNil())

		entities = make([]*Entity, 1)
		entity := NewEntity("http://data.mimiro.io/people/p-0", 0)
		entity.Properties["Name"] = "homer"
		entities[0] = entity
		// entities[0] = res.Entities[0]
		err = ds.StoreEntities(entities)
		Expect(err).To(BeNil())

		err = ds.CompleteFullSync(context.Background())
		Expect(err).To(BeNil())

		allEntities := make([]*Entity, 0)
		deletedEntities := make([]*Entity, 0)
		_, err = ds.MapEntities("", 0, func(e *Entity) error {
			if !e.IsDeleted {
				allEntities = append(allEntities, e)
			} else {
				deletedEntities = append(deletedEntities, e)
			}
			return nil
		})
		Expect(err).To(BeNil(), "MapEntities should have worked")
		Expect(len(allEntities)).To(Equal(1), "There should be one undeleted entity left")
		Expect(len(deletedEntities)).To(Equal(batchSize-1), "all other entities should be deleted")
	})

	ginkgo.It("Should enable iterating over a dataset (MapEntitites)", func() {
		// create dataset
		ds, err := dsm.CreateDataset("people", nil)
		Expect(err).To(BeNil())
		batchSize := 5

		entities := make([]*Entity, batchSize)
		for i := 0; i < batchSize; i++ {
			entity := NewEntity("http://data.mimiro.io/people/p-"+strconv.Itoa(i), 0)
			entity.Properties["Name"] = "homer"
			entities[i] = entity
		}
		err = ds.StoreEntities(entities)
		Expect(err).To(BeNil())

		var entity *Entity
		ctoken, _ := ds.MapEntities("", 1, func(e *Entity) error {
			entity = e
			return nil
		})
		Expect(entities).NotTo(BeZero(), "MapEntities should have processed 1 entity")
		Expect(ctoken).NotTo(BeZero(), "MapEntities should have given us a token")

		var nextEntity *Entity
		newtoken, _ := ds.MapEntities(ctoken, 1, func(e *Entity) error {
			nextEntity = e
			return nil
		})
		Expect(nextEntity).NotTo(BeZero(), "MapEntities should have processed 1 entity again")
		Expect(newtoken).NotTo(BeZero(), "MapEntities should have given us a new token")
		Expect(nextEntity.ID != entity.ID).To(BeTrue(), "We should have a different entity")
		Expect(ctoken != newtoken).To(BeTrue(), "We should have a different token")
	})

	ginkgo.Describe(
		"When retrieving data, it can combine multiple entities from different datasets with the same ID",
		func() {
			ginkgo.It("Should merge entity data from different entities with same id (partials)", func() {
				e1 := NewEntity("x:e1", 0)
				e2 := NewEntity("x:e1", 0)
				partials := make([]*Entity, 2)
				partials[0] = e1
				partials[1] = e2
				result := store.mergePartials(partials)
				Expect(result).NotTo(BeZero())
				Expect(result.ID).To(Equal("x:e1"))
				Expect(result.Properties["x:name"]).To(BeNil())

				e2.Properties["x:name"] = "homer"
				result = store.mergePartials(partials)
				Expect(result).NotTo(BeZero())
				Expect(result.ID).To(Equal("x:e1"))
				Expect(result.Properties["x:name"]).To(Equal("homer"))
			})

			ginkgo.It("Should merge partials without error if a source is empty", func() {
				e1 := NewEntity("x:e1", 0)
				e2 := NewEntity("x:e1", 0)
				partials := make([]*Entity, 2)
				partials[0] = e1
				partials[1] = e2

				e1.Properties["x:name"] = "homer"

				result := store.mergePartials(partials)
				Expect(result).NotTo(BeZero())
				Expect(result.ID).To(Equal("x:e1"))
				Expect(result.Properties["x:name"]).To(Equal("homer"))
			})

			ginkgo.It("Should merge partials if target entity is empty", func() {
				e1 := NewEntity("x:e1", 0)
				e2 := NewEntity("x:e1", 0)
				partials := make([]*Entity, 2)
				partials[0] = e1
				partials[1] = e2

				e2.Properties["x:name"] = "homer"

				result := store.mergePartials(partials)
				Expect(result).NotTo(BeZero())
				Expect(result.ID).To(Equal("x:e1"))
				Expect(result.Properties["x:name"]).To(Equal("homer"))
			})

			ginkgo.It("Should merge single values from 2 partials to a list value", func() {
				e1 := NewEntity("x:e1", 0)
				e2 := NewEntity("x:e1", 0)
				partials := make([]*Entity, 2)
				partials[0] = e1
				partials[1] = e2

				e1.Properties["x:name"] = "homer"
				e2.Properties["x:name"] = "bob"

				result := store.mergePartials(partials)
				Expect(result).NotTo(BeZero())
				Expect(result.ID).To(Equal("x:e1"))
				Expect(result.Properties["x:name"]).To(Equal([]interface{}{"homer", "bob"}))
			})

			ginkgo.It("Should add single values to existing list when merging partials", func() {
				e1 := NewEntity("x:e1", 0)
				e2 := NewEntity("x:e1", 0)
				partials := make([]*Entity, 2)
				partials[0] = e1
				partials[1] = e2

				names := make([]interface{}, 0)
				names = append(names, "homer")
				names = append(names, "brian")
				e1.Properties["x:name"] = names
				e2.Properties["x:name"] = "bob"

				result := store.mergePartials(partials)
				Expect(result).NotTo(BeZero())
				Expect(result.ID).To(Equal("x:e1"))
				Expect(result.Properties["x:name"]).To(Equal([]interface{}{"homer", "brian", "bob"}))
			})

			ginkgo.It("Should add list to existing single value when merging partials", func() {
				e1 := NewEntity("x:e1", 0)
				e2 := NewEntity("x:e1", 0)
				partials := make([]*Entity, 2)
				partials[0] = e1
				partials[1] = e2

				names := make([]interface{}, 0)
				names = append(names, "homer")
				names = append(names, "brian")
				e1.Properties["x:name"] = "bob"
				e2.Properties["x:name"] = names

				result := store.mergePartials(partials)
				Expect(result).NotTo(BeZero())
				Expect(result.ID).To(Equal("x:e1"))
				Expect(result.Properties["x:name"]).To(Equal([]interface{}{"bob", "homer", "brian"}))
			})

			ginkgo.It("Should combine two value lists when merging partials", func() {
				e1 := NewEntity("x:e1", 0)
				e2 := NewEntity("x:e1", 0)
				partials := make([]*Entity, 2)
				partials[0] = e1
				partials[1] = e2

				names := make([]interface{}, 0)
				names = append(names, "homer")
				names = append(names, "brian")
				e1.Properties["x:name"] = names

				names1 := make([]interface{}, 0)
				names1 = append(names1, "bob")
				names1 = append(names1, "james")

				e2.Properties["x:name"] = names1

				result := store.mergePartials(partials)
				Expect(result).NotTo(BeZero())
				Expect(result.ID).To(Equal("x:e1"))
				Expect(result.Properties["x:name"]).To(Equal([]interface{}{"homer", "brian", "bob", "james"}))
			})

			ginkgo.It("Should merge 3 partials", func() {
				e1 := NewEntity("x:e1", 0)
				e2 := NewEntity("x:e1", 0)
				e3 := NewEntity("x:e1", 0)
				partials := make([]*Entity, 3)
				partials[0] = e1
				partials[1] = e2
				partials[2] = e3

				e1.Properties["x:name"] = "bob"

				e2.Properties["x:name"] = "brian"
				e2.Properties["x:dob"] = "01-01-2001"

				e3.Properties["x:name"] = "james"

				partials = make([]*Entity, 3)
				partials[0] = e1
				partials[1] = e2
				partials[2] = e3

				result := store.mergePartials(partials)
				Expect(result).NotTo(BeZero())
				Expect(result.ID).To(Equal("x:e1"))
				Expect(result.Properties["x:name"]).To(Equal([]interface{}{"bob", "brian", "james"}))
				Expect(result.Properties["x:dob"]).To(Equal("01-01-2001"))
			})
		},
	)

	ginkgo.It("Should build query results", func() {
		// create dataset
		ds, _ := dsm.CreateDataset("people", nil)

		batchSize := 5
		entities := make([]*Entity, batchSize)

		peopleNamespacePrefix, _ := store.NamespaceManager.AssertPrefixMappingForExpansion(
			"http://data.mimiro.io/people/",
		)
		modelNamespacePrefix, _ := store.NamespaceManager.AssertPrefixMappingForExpansion(
			"http://data.mimiro.io/model/",
		)
		rdfNamespacePrefix, _ := store.NamespaceManager.AssertPrefixMappingForExpansion(RdfNamespaceExpansion)

		for i := 0; i < batchSize; i++ {
			entity := NewEntity(peopleNamespacePrefix+":p-"+strconv.Itoa(i), 0)
			entity.Properties[modelNamespacePrefix+":Name"] = "homer"
			entity.References[rdfNamespacePrefix+":type"] = modelNamespacePrefix + ":Person"
			entity.References[modelNamespacePrefix+":f1"] = peopleNamespacePrefix + ":Person-1"
			entity.References[modelNamespacePrefix+":f2"] = peopleNamespacePrefix + ":Person-2"
			entity.References[modelNamespacePrefix+":f3"] = peopleNamespacePrefix + ":Person-3"
			entities[i] = entity
		}

		err := ds.StoreEntities(entities)
		Expect(err).To(BeNil())

		results, err := store.getRelated("http://data.mimiro.io/people/p-1", RdfTypeURI, false, []string{}, true)
		Expect(err).To(BeNil())
		Expect(len(results)).To(Equal(1))

		invresults, err := store.getRelated("http://data.mimiro.io/model/Person", RdfTypeURI, true, []string{}, true)
		Expect(err).To(BeNil())
		Expect(len(invresults)).To(Equal(5))

		invresults, err = store.getRelated("http://data.mimiro.io/model/Person", "*", true, []string{}, true)
		Expect(err).To(BeNil())
		Expect(len(invresults)).To(Equal(5))

		for i := 0; i < batchSize; i++ {
			entity := NewEntity(peopleNamespacePrefix+":p-"+strconv.Itoa(i), 0)
			entity.Properties[modelNamespacePrefix+":Name"] = "homer"
			entity.References[modelNamespacePrefix+":f1"] = peopleNamespacePrefix + ":Person-1"
			entity.References[modelNamespacePrefix+":f2"] = peopleNamespacePrefix + ":Person-2"
			entity.References[modelNamespacePrefix+":f3"] = peopleNamespacePrefix + ":Person-3"
			entities[i] = entity
		}

		err = ds.StoreEntities(entities)
		Expect(err).To(BeNil())

		invresults, err = store.getRelated("http://data.mimiro.io/model/Person", RdfTypeURI, true, []string{}, true)
		Expect(err).To(BeNil())
		Expect(len(invresults)).To(Equal(0))

		results, err = store.getRelated("http://data.mimiro.io/people/p-1", RdfTypeURI, false, []string{}, true)
		Expect(err).To(BeNil())
		Expect(len(results)).To(Equal(0))

		for i := 0; i < batchSize; i++ {
			entity := NewEntity(peopleNamespacePrefix+":p-"+strconv.Itoa(i), 0)
			entity.Properties[modelNamespacePrefix+":Name"] = "homer"
			entity.References[rdfNamespacePrefix+":type"] = modelNamespacePrefix + ":Person"
			entity.References[modelNamespacePrefix+":f1"] = peopleNamespacePrefix + ":Person-1"
			entity.References[modelNamespacePrefix+":f2"] = peopleNamespacePrefix + ":Person-2"
			entity.References[modelNamespacePrefix+":f3"] = peopleNamespacePrefix + ":Person-3"
			entities[i] = entity
		}

		err = ds.StoreEntities(entities)
		Expect(err).To(BeNil())

		invresults, err = store.getRelated("http://data.mimiro.io/model/Person", RdfTypeURI, true, []string{}, true)
		Expect(err).To(BeNil())
		Expect(len(invresults)).To(Equal(5))

		results, err = store.getRelated("http://data.mimiro.io/people/p-1", RdfTypeURI, false, []string{}, true)
		Expect(err).To(BeNil())
		Expect(len(results)).To(Equal(1))
	})

	ginkgo.It("Should perform batch updates without error with concurrent requests", func() {
		// create dataset
		ds, err := dsm.CreateDataset("people", nil)
		Expect(err).To(BeNil())

		batchSize := 5000
		entities := make([]*Entity, batchSize)

		for i := 0; i < batchSize; i++ {
			entity := NewEntity("http://data.mimiro.io/people/p-"+strconv.Itoa(i), 0)
			entity.Properties["Name"] = "homer"
			entity.References["rdf:type"] = "http://data.mimiro.io/model/Person"
			entity.References["rdf:f1"] = "http://data.mimiro.io/people/Person-1"
			entity.References["rdf:f2"] = "http://data.mimiro.io/people/Person-2"
			entity.References["rdf:f3"] = "http://data.mimiro.io/people/Person-3"
			entities[i] = entity
		}

		var wg sync.WaitGroup
		for i := 1; i <= 10; i++ {
			wg.Add(1)
			go func() {
				err := ds.StoreEntities(entities)
				Expect(err).To(BeNil(), "StoreEntities failed unexpectedly")
				defer wg.Done()
			}()
		}
		wg.Wait()
	})

	ginkgo.It("Should store and replace large batches without error", func() {
		// create dataset
		ds, _ := dsm.CreateDataset("people", nil)

		batchSize := 5000
		entities := make([]*Entity, batchSize)

		for i := 0; i < batchSize; i++ {
			entity := NewEntity("http://data.mimiro.io/people/p-"+strconv.Itoa(i), 0)
			entity.Properties["Name"] = "homer"

			entity.References["rdf:type"] = "http://data.mimiro.io/model/Person"
			entity.References["rdf:f1"] = "http://data.mimiro.io/people/Person-1"
			entity.References["rdf:f2"] = "http://data.mimiro.io/people/Person-2"
			entity.References["rdf:f3"] = "http://data.mimiro.io/people/Person-3"

			entities[i] = entity
		}
		err := ds.StoreEntities(entities)
		Expect(err).To(BeNil())

		// replace
		for i := 0; i < batchSize; i++ {
			entity := NewEntity("http://data.mimiro.io/people/p-"+strconv.Itoa(i), 0)
			entity.Properties["Name"] = "homer"

			entity.References["rdf:type"] = "http://data.mimiro.io/model/Person"
			entity.References["rdf:f1"] = "http://data.mimiro.io/people/Person-1"
			//		refs["rdf:f2"] = "http://data.mimiro.io/people/Person-2"
			entity.References["rdf:f3"] = "http://data.mimiro.io/people/Person-3"

			entities[i] = entity
		}

		err = ds.StoreEntities(entities)
		Expect(err).To(BeNil())
		/*allEntities := make([]*Entity, 0)
		ds.GetEntities(func(e *Entity) {
			allEntities = append(allEntities, e)
		})

		if len(allEntities) != batchSize {
			t.Fail()
		} */
		// replace again
		/* for i := 0; i < batchSize; i++ {
		   	entity := NewEntity("http://data.mimiro.io/people/p-" + strconv.Itoa(i), 0)
		   	entity.Properties["Name"] = "homer"

		   	entity.References["rdf:type"] = "http://data.mimiro.io/model/Person"
		   	entity.References["rdf:f1"] = "http://data.mimiro.io/people/Person-1"
		   	entity.References["rdf:f2"] = "http://data.mimiro.io/people/Person-6"
		   	entity.References["rdf:f3"] = "http://data.mimiro.io/people/Person-3"

		   	entities[i] = entity
		   }

		   ds.StoreEntities(entities) */
	})

	ginkgo.It("Should build correct query results with large number of entities", func() {
		// namespaces
		peopleNamespacePrefix, _ := store.NamespaceManager.AssertPrefixMappingForExpansion(
			"http://data.mimiro.io/people/",
		)
		companyNamespacePrefix, _ := store.NamespaceManager.AssertPrefixMappingForExpansion(
			"http://data.mimiro.io/company/",
		)
		modelNamespacePrefix, _ := store.NamespaceManager.AssertPrefixMappingForExpansion(
			"http://data.mimiro.io/model/",
		)
		rdfNamespacePrefix, _ := store.NamespaceManager.AssertPrefixMappingForExpansion(RdfNamespaceExpansion)

		// create dataset
		peopleDataset, _ := dsm.CreateDataset("people", nil)
		companiesDataset, _ := dsm.CreateDataset("companies", nil)

		numOfCompanies := 10
		numOfEmployees := 1000
		companies := make([]*Entity, 0)
		people := make([]*Entity, 0)
		queryIds := make([]string, 0)
		empID := 0

		for i := 0; i < numOfCompanies; i++ {

			c := NewEntity(companyNamespacePrefix+":company-"+strconv.Itoa(i), 0)
			c.References[rdfNamespacePrefix+":type"] = modelNamespacePrefix + ":Company"

			for j := 0; j < numOfEmployees; j++ {
				person := NewEntity(peopleNamespacePrefix+":person-"+strconv.Itoa(empID), 0)
				person.Properties[peopleNamespacePrefix+":Name"] = "person " + strconv.Itoa(i)
				person.References[rdfNamespacePrefix+":type"] = modelNamespacePrefix + ":Person"
				person.References[peopleNamespacePrefix+":worksfor"] = companyNamespacePrefix + ":company-" + strconv.Itoa(
					i,
				)
				people = append(people, person)

				if empID < 100 {
					queryIds = append(queryIds, "http://data.mimiro.io/people/person-"+strconv.Itoa(empID))
				}

				empID++
			}

			companies = append(companies, c)
		}
		err := companiesDataset.StoreEntities(companies)
		Expect(err).To(BeNil())

		err = peopleDataset.StoreEntities(people)
		Expect(err).To(BeNil())
		// query
		result, err := store.GetManyRelatedEntities(
			queryIds,
			"http://data.mimiro.io/people/worksfor",
			false,
			[]string{}, true,
		)
		Expect(err).To(BeNil())
		Expect(len(result)).To(Equal(100))

		// check inverse
		queryIds = make([]string, 0)
		queryIds = append(queryIds, "http://data.mimiro.io/company/company-1")
		queryIds = append(queryIds, "http://data.mimiro.io/company/company-2")
		result, err = store.GetManyRelatedEntities(
			queryIds,
			"http://data.mimiro.io/people/worksfor",
			true,
			[]string{}, true,
		)
		Expect(err).To(BeNil())
		Expect(len(result)).To(Equal(numOfEmployees * 2))
	})

	ginkgo.It("Should return entity placeholders (links) in queries", func() {
		// namespaces
		peopleNamespacePrefix, _ := store.NamespaceManager.AssertPrefixMappingForExpansion(
			"http://data.mimiro.io/people/",
		)
		companyNamespacePrefix, _ := store.NamespaceManager.AssertPrefixMappingForExpansion(
			"http://data.mimiro.io/company/",
		)
		modelNamespacePrefix, _ := store.NamespaceManager.AssertPrefixMappingForExpansion(
			"http://data.mimiro.io/model/",
		)
		rdfNamespacePrefix, _ := store.NamespaceManager.AssertPrefixMappingForExpansion(RdfNamespaceExpansion)

		// create dataset
		peopleDataset, _ := dsm.CreateDataset("people", nil)
		// 	companiesDataset, err := dsm.CreateDataset("companies")

		people := make([]*Entity, 0)
		queryIds := make([]string, 0)

		person := NewEntity(peopleNamespacePrefix+":person-"+strconv.Itoa(1), 0)
		person.Properties[peopleNamespacePrefix+":Name"] = "person " + strconv.Itoa(2)
		person.References[rdfNamespacePrefix+":type"] = modelNamespacePrefix + ":Person"
		person.References[peopleNamespacePrefix+":worksfor"] = companyNamespacePrefix + ":company-3"
		people = append(people, person)

		err := peopleDataset.StoreEntities(people)
		Expect(err).To(BeNil())

		queryIds = append(queryIds, "http://data.mimiro.io/people/person-"+strconv.Itoa(1))
		// query
		result, err := store.GetManyRelatedEntities(
			queryIds,
			"http://data.mimiro.io/people/worksfor",
			false,
			[]string{}, true,
		)
		Expect(err).To(BeNil())
		Expect(len(result)).To(Equal(1))
	})
})

var _ = ginkgo.Describe("Scoped storage functions", func() {
	testCnt := 0
	var dsm *DsManager
	var store *Store
	var storeLocation string
	var peopleNamespacePrefix string
	var companyNamespacePrefix string
	const PEOPLE = "people"
	const COMPANIES = "companies"
	const HISTORY = "history"
	ginkgo.BeforeEach(func() {
		testCnt += 1
		storeLocation = fmt.Sprintf("./test_store_dsscope_%v", testCnt)
		err := os.RemoveAll(storeLocation)
		Expect(err).To(BeNil(), "should be allowed to clean testfiles in "+storeLocation)
		e := &conf.Config{
			Logger:        zap.NewNop().Sugar(),
			StoreLocation: storeLocation,
		}
		// lc := fxtest.NewLifecycle(internal.FxTestLog(ginkgo.GinkgoT(), false))
		store = NewStore(e, &statsd.NoOpClient{})
		dsm = NewDsManager(e, store, NoOpBus())

		// err = lc.Start(context.Background())
		// Expect(err).To(BeNil())

		// namespaces
		peopleNamespacePrefix, _ = store.NamespaceManager.AssertPrefixMappingForExpansion(
			"http://data.mimiro.io/people/",
		)
		companyNamespacePrefix, _ = store.NamespaceManager.AssertPrefixMappingForExpansion(
			"http://data.mimiro.io/company/",
		)
		modelNamespacePrefix, _ := store.NamespaceManager.AssertPrefixMappingForExpansion(
			"http://data.mimiro.io/model/",
		)
		rdfNamespacePrefix, _ := store.NamespaceManager.AssertPrefixMappingForExpansion(RdfNamespaceExpansion)
		// create dataset
		peopleDataset, _ := dsm.CreateDataset(PEOPLE, nil)
		companiesDataset, _ := dsm.CreateDataset(COMPANIES, nil)
		employmentHistoryDataset, _ := dsm.CreateDataset(HISTORY, nil)

		/* Install the following setup
		{person: 1, workhistory: []int{}, currentwork: 1},
		{person: 2, workhistory: []int{1}, currentwork: 2},
		{person: 3, workhistory: []int{2}, currentwork: 1},
		{person: 4, workhistory: []int{1, 2}, currentwork: 3},
		{person: 5, workhistory: []int{3}, currentwork: 1},
		*/
		//insert companies
		for _, companyID := range []int{1, 2, 3} {
			cid := companyNamespacePrefix + ":company-" + strconv.Itoa(companyID)
			_ = companiesDataset.StoreEntities([]*Entity{NewEntityFromMap(map[string]interface{}{
				"id": cid,
				"props": map[string]interface{}{
					companyNamespacePrefix + ":Name": "Company " + strconv.Itoa(companyID),
				},
				"refs": map[string]interface{}{rdfNamespacePrefix + ":type": modelNamespacePrefix + ":Company"},
			})})
		}

		// insert people with worksfor relations
		currentWork := []int{1, 2, 1, 3, 1}
		for personID, workID := range currentWork {
			pid := peopleNamespacePrefix + ":person-" + strconv.Itoa(personID+1)
			_ = peopleDataset.StoreEntities([]*Entity{NewEntityFromMap(map[string]interface{}{
				"id": pid,
				"props": map[string]interface{}{
					peopleNamespacePrefix + ":Name": "Person " + strconv.Itoa(personID+1),
				},
				"refs": map[string]interface{}{
					rdfNamespacePrefix + ":type": modelNamespacePrefix + ":Person",
					peopleNamespacePrefix + ":worksfor": companyNamespacePrefix + ":company-" + strconv.Itoa(
						workID,
					),
				},
			})})
		}

		// insert workHistory
		for personID, workHistIds := range [][]int{{}, {1}, {2}, {1, 2}, {3}} {
			var workHistory []string
			for _, histID := range workHistIds {
				workHistory = append(workHistory, companyNamespacePrefix+":company-"+strconv.Itoa(histID))
			}
			pid := peopleNamespacePrefix + ":person-" + strconv.Itoa(personID+1)
			entity := NewEntityFromMap(map[string]interface{}{
				"id":    pid,
				"props": map[string]interface{}{},
				"refs": map[string]interface{}{
					rdfNamespacePrefix + ":type":        modelNamespacePrefix + ":Employment",
					peopleNamespacePrefix + ":Employee": strconv.Itoa(personID + 1),
					peopleNamespacePrefix + ":worksfor": companyNamespacePrefix + ":company-" + strconv.Itoa(
						currentWork[personID],
					),
					peopleNamespacePrefix + ":workedfor": workHistory,
				},
			})
			_ = employmentHistoryDataset.StoreEntities([]*Entity{entity})
		}
	})
	ginkgo.AfterEach(func() {
		dsm.DeleteDataset(PEOPLE)
		dsm.DeleteDataset(COMPANIES)
		dsm.DeleteDataset(HISTORY)
		_ = store.Close()
		_ = os.RemoveAll(storeLocation)
	})

	ginkgo.It("Should return all of an entity's relations when no dataset constraints are applied", func() {
		result, _ := store.GetManyRelatedEntities(
			[]string{peopleNamespacePrefix + ":person-1"},
			"http://data.mimiro.io/people/worksfor",
			false,
			[]string{}, true,
		)
		Expect(len(result)).
			To(Equal(1), "expected 1 relation to be found. both the people and workhistory datasets point to the same company.")

		entity := findEntity(result, "ns4:company-1")
		Expect(entity).NotTo(BeNil())
		Expect(entity.ID).To(Equal("ns4:company-1"), "first relation ID should be found")
		Expect(entity.Properties["ns4:Name"]).To(Equal("Company 1"), "first relation property should be resolved")
	})

	ginkgo.It("Should return only relation ID if access to relation dataset is not given", func() {
		// constraint on "people" only. we should find the relation to a company in "people",
		// but resolving the company should be omitted because we lack access to the "companies" dataset
		result, _ := store.GetManyRelatedEntities(
			[]string{peopleNamespacePrefix + ":person-1"},
			"http://data.mimiro.io/people/worksfor",
			false,
			[]string{PEOPLE}, true,
		)
		Expect(len(result)).To(Equal(1), "expected 1 relation to be found in people dataset (not in workHistory)")

		entity := findEntity(result, "ns4:company-1")
		Expect(entity).NotTo(BeNil())
		Expect(entity.ID).To(Equal("ns4:company-1"), "first relation ID should be found")
		Expect(entity.Properties["ns4:Name"]).To(BeNil(), "first relation property should NOT be resolved")
	})

	ginkgo.It("Should not find outgoing relations if the datasets owning these relations are disallowed", func() {
		// constraint on "companies". we cannot access outgoing refs in the "people" or "history" datasets, therefore we should not find anything
		result, _ := store.GetManyRelatedEntities(
			[]string{peopleNamespacePrefix + ":person-1"},
			"http://data.mimiro.io/people/worksfor",
			false,
			[]string{COMPANIES}, true,
		)
		Expect(len(result)).To(Equal(0), "expected 0 relation to be found (people and history are excluded)")
	})

	ginkgo.It("Should treat a list of (all) unknown datasets like an empty scope list", func() {
		// constraint on bogus dataset. due to implementation, this dataset filter will be ignored, query behaves as unrestricted
		// TODO: this can be discussed. Producing an error would make Queries less unpredictable. But ignoring invalid input makes the API more approachable
		result, _ := store.GetManyRelatedEntities(
			[]string{peopleNamespacePrefix + ":person-1"},
			"http://data.mimiro.io/people/worksfor",
			false,
			[]string{"bogus"}, true,
		)
		Expect(len(result)).To(Equal(1), "expected 1 relation (company-1)")

		entity := findEntity(result, "ns4:company-1")
		Expect(entity).NotTo(BeNil())
		Expect(entity.ID).To(Equal("ns4:company-1"), "first relation ID should be found")
		Expect(entity.Properties["ns4:Name"]).To(Equal("Company 1"), "first relation property should be resolved")
	})

	ginkgo.It("Should find relations in secondary datasets if main dataset is disallowed", func() {
		// constraint on history dataset. we should find an outgoing relation from history to a company,
		// but company resolving should be disabled since we lack access to the companies dataset
		result, _ := store.GetManyRelatedEntities(
			[]string{peopleNamespacePrefix + ":person-1"},
			"http://data.mimiro.io/people/worksfor",
			false,
			[]string{HISTORY}, true,
		)
		Expect(len(result)).To(Equal(1), "expected 1 relation to be found in people dataset (not in workHistory)")

		entity := findEntity(result, "ns4:company-1")
		Expect(entity).NotTo(BeNil())
		Expect(entity.ID).To(Equal("ns4:company-1"), "first relation ID should be found")
		Expect(entity.Properties["ns4:Name"]).To(BeNil(), "first relation property should NOT be resolved")
	})

	ginkgo.It("Should return all incoming relations of an entity in inverse query, when no scope is given", func() {
		// inverse query for "company-1", no datasets restriction. should find 3 people with resolved entities
		result, _ := store.GetManyRelatedEntities(
			[]string{companyNamespacePrefix + ":company-1"},
			"http://data.mimiro.io/people/worksfor",
			true,
			[]string{}, true,
		)
		Expect(len(result)).
			To(Equal(3), "there are 6 relations, 3 in dataset people and 3 owned by history. all relations come from total of 3 people")
		entity := findEntity(result, "ns3:person-5")
		Expect(entity).NotTo(BeNil())
		Expect(entity.Properties["ns3:Name"]).To(Equal("Person 5"), "first relation property should be resolved")
		Expect(len(entity.References)).To(Equal(4))
		// verify that history entity has been merged in by checking for "workedfor"
		Expect(entity.References["ns3:workedfor"].([]interface{})[0]).To(Equal("ns4:company-3"),
			"inverse query should find company-3 in history following 'worksfor' to person-5's employment enity")
		// verify that worksfor is duplicated - since the relation is merged together from people and history
		Expect(entity.References["ns3:worksfor"]).To(Equal([]interface{}{"ns4:company-1", "ns4:company-1"}))
	})

	ginkgo.It("Should omit disallowed datasets when resolving found entities", func() {
		// inverse query for "company-1" with only "people" accessible.
		// should still find 3 people (incoming relation is owned by people dataset, which we have access to)
		// people should be resolved
		// but resolved relations in people should only be "people" data with no "history" refs merged in
		result, _ := store.GetManyRelatedEntities(
			[]string{companyNamespacePrefix + ":company-1"},
			"http://data.mimiro.io/people/worksfor",
			true,
			[]string{PEOPLE}, true,
		)
		Expect(len(result)).To(Equal(3), "expected 3 relations to be found in people dataset (not in workHistory)")

		entity := findEntity(result, "ns3:person-5")
		Expect(entity).NotTo(BeNil())
		Expect(entity.ID).To(Equal("ns3:person-5"), "first relation ID should be found")
		Expect(entity.Properties["ns3:Name"]).To(Equal("Person 5"), "first relation property should be resolved")
		// make sure we only have two refs returned (worksfor + type), confirming that history refs have not been accessed
		Expect(len(entity.References)).To(Equal(2))
	})

	ginkgo.It("Should not return results if the relation-owning datasets are disallowed", func() {
		// inverse query for "company-1" with restriction to "companies" dataset.
		// all incoming relations are stored in either history or people dataset.
		// with access limited to companies dataset, we should not find incoming relations
		result, _ := store.GetManyRelatedEntities(
			[]string{companyNamespacePrefix + ":company-1"},
			"http://data.mimiro.io/people/worksfor",
			true,
			[]string{COMPANIES}, true,
		)
		Expect(len(result)).To(Equal(0))
	})

	ginkgo.It("Should resolve all elements in relation arrays, when no scope restriction is given", func() {
		// find person-4 and follow its workedfor relations without restriction
		// should find 2 companies in "history" dataset, and fully resolve the company entities
		result, _ := store.GetManyRelatedEntities(
			[]string{peopleNamespacePrefix + ":person-4"},
			"http://data.mimiro.io/people/workedfor",
			false,
			[]string{}, true,
		)
		Expect(len(result)).To(Equal(2), "expected 2 relations to be found in history dataset")

		entity := findEntity(result, "ns4:company-2")
		Expect(entity).NotTo(BeNil())
		Expect(entity.ID).To(Equal("ns4:company-2"), "first relation ID should be found")
		Expect(entity.Properties["ns4:Name"]).To(Equal("Company 2"), "first relation property should be resolved")
	})

	ginkgo.It("Should find, but not resolve all relations in an array if relation dataset is disallowed", func() {
		// find person-4 and follow its workedfor relations in dataset "history"
		// should find 2 companies in "history" dataset,
		// but not fully resolve the company entities because we lack access to "company"
		result, _ := store.GetManyRelatedEntities(
			[]string{peopleNamespacePrefix + ":person-4"},
			"http://data.mimiro.io/people/workedfor",
			false,
			[]string{HISTORY}, true,
		)
		Expect(len(result)).To(Equal(2), "expected 2 relations to be found in history dataset")

		entity := findEntity(result, "ns4:company-2")
		Expect(entity).NotTo(BeNil())
		Expect(entity.ID).To(Equal("ns4:company-2"), "first relation ID should be found")
		Expect(entity.Properties["ns4:Name"]).
			To(BeNil(), "companies dataset is not accessible, therefor name should be nil")
	})

	ginkgo.It("Should inversely find all relations in an array", func() {
		// find company-2 and inversely follow workedfor relations without restriction
		// should find person 3 and 4 in "history" dataset, and fully resolve the person entities
		result, _ := store.GetManyRelatedEntities(
			[]string{companyNamespacePrefix + ":company-2"},
			"http://data.mimiro.io/people/workedfor",
			true,
			[]string{}, true,
		)
		Expect(len(result)).To(Equal(2), "expected 2 people to be found from history dataset")

		entity := findEntity(result, "ns3:person-4")
		Expect(entity).NotTo(BeNil())
		Expect(entity.ID).To(Equal("ns3:person-4"), "first relation ID should be found")
		Expect(entity.Properties["ns3:Name"]).To(Equal("Person 4"), "first relation property should be resolved")
		// There should be 4 references both from history and people
		Expect(len(entity.References)).
			To(Equal(4), "Expected 4 refs in inversly found person from people and history datasets")
		// check that reference values are merged from all datasets. type should be list of type refs from both people and history
		if reflect.DeepEqual(entity.References["ns2:type"], []string{"ns5:Person", "ns5:Employment"}) {
			ginkgo.Fail(
				fmt.Sprintf(
					"Expected Person+Employment as type ref values, but found %v",
					entity.References["ns2:type"],
				),
			)
		}
	})

	ginkgo.It(
		"Should inversely find all relations in array, but not resolve the relation-intities if no access",
		func() {
			// find company-2 and inversely follow workedfor relations with filter on history
			// should find person 3 and 4 in "history" dataset, but not fully resolve the person entities
			result, _ := store.GetManyRelatedEntities(
				[]string{companyNamespacePrefix + ":company-2"},
				"http://data.mimiro.io/people/workedfor",
				true,
				[]string{HISTORY}, true,
			)
			Expect(len(result)).To(Equal(2), "expected 2 people to be found from history dataset")

			entity := findEntity(result, "ns3:person-4")
			Expect(entity).NotTo(BeNil())
			Expect(entity.ID).To(Equal("ns3:person-4"), "first relation ID should be found")
			Expect(
				entity.Properties["ns3:Name"],
			).To(BeNil(), "Person 4 should not be resolved since access to people is missing")
			// There should be 4 references both from history and people
			Expect(len(entity.References)).To(Equal(4), "Expected 4 refs in inversly found person from history dataset")
			// check that reference values are merged from all datasets. type should be list of type refs from both people and history
			Expect(entity.References["ns2:type"]).
				To(Equal("ns5:Employment"), "Expected only Employment type, not Person type")
		},
	)

	ginkgo.It("Should not inversly find array relations if relation-owning dataset is disallowed", func() {
		// find company-2 and inversely follow workedfor relations with filter on people and companies
		// should not find any relations since history access is not allowed, and workedfor is only stored there
		result, _ := store.GetManyRelatedEntities(
			[]string{companyNamespacePrefix + ":company-2"},
			"http://data.mimiro.io/people/workedfor",
			true,
			[]string{PEOPLE, COMPANIES}, true,
		)
		Expect(len(result)).To(Equal(0))
	})

	ginkgo.It("Should respect dataset scope in GetEntity (single lookup query)", func() {
		// namespaces
		employeeNamespacePrefix, _ := store.NamespaceManager.AssertPrefixMappingForExpansion(
			"http://data.mimiro.io/employee/",
		)
		modelNamespacePrefix, _ := store.NamespaceManager.AssertPrefixMappingForExpansion(
			"http://data.mimiro.io/model/",
		)
		rdfNamespacePrefix, _ := store.NamespaceManager.AssertPrefixMappingForExpansion(RdfNamespaceExpansion)

		// create dataset
		const PEOPLE = "people"
		peopleDataset, _ := dsm.CreateDataset(PEOPLE, nil)
		const EMPLOYEES = "employees"
		employeeDataset, _ := dsm.CreateDataset(EMPLOYEES, nil)

		_ = peopleDataset.StoreEntities([]*Entity{
			NewEntityFromMap(map[string]interface{}{
				"id":    peopleNamespacePrefix + ":person-1",
				"props": map[string]interface{}{peopleNamespacePrefix + ":Name": "Lisa"},
				"refs": map[string]interface{}{
					rdfNamespacePrefix + ":type":     modelNamespacePrefix + ":Person",
					peopleNamespacePrefix + ":knows": peopleNamespacePrefix + "person-2",
				},
			}),
			NewEntityFromMap(map[string]interface{}{
				"id":    peopleNamespacePrefix + ":person-2",
				"props": map[string]interface{}{peopleNamespacePrefix + ":Name": "Bob"},
				"refs": map[string]interface{}{
					rdfNamespacePrefix + ":type":     modelNamespacePrefix + ":Person",
					peopleNamespacePrefix + ":knows": peopleNamespacePrefix + "person-1",
				},
			}),
		})

		_ = employeeDataset.StoreEntities([]*Entity{
			NewEntityFromMap(map[string]interface{}{
				"id":    peopleNamespacePrefix + ":person-1",
				"props": map[string]interface{}{employeeNamespacePrefix + ":Title": "President"},
				"refs": map[string]interface{}{
					rdfNamespacePrefix + ":type": modelNamespacePrefix + ":Employee",
				},
			}),
			NewEntityFromMap(map[string]interface{}{
				"id":    peopleNamespacePrefix + ":person-2",
				"props": map[string]interface{}{employeeNamespacePrefix + ":Title": "Vice President"},
				"refs": map[string]interface{}{
					rdfNamespacePrefix + ":type":            modelNamespacePrefix + ":Employee",
					employeeNamespacePrefix + ":Supervisor": peopleNamespacePrefix + ":person-1",
				},
			}),
		})

		// first no datasets restriction, check that props from both employee and people dataset are merged
		result, err := store.GetEntity(peopleNamespacePrefix+":person-1", []string{}, true)
		Expect(err).To(BeNil())
		Expect(result.Properties[employeeNamespacePrefix+":Title"]).To(Equal("President"),
			"expected to merge property employees:Title into result with value 'President'")
		Expect(result.Properties[peopleNamespacePrefix+":Name"]).To(Equal("Lisa"))

		// now, restrict on dataset "people"
		result, _ = store.GetEntity(peopleNamespacePrefix+":person-2", []string{PEOPLE}, true)
		// we should not find the Title from the employees dataset
		Expect(result.Properties[employeeNamespacePrefix+":Title"]).To(BeNil())
		// people:Name should be found though
		Expect(result.Properties[peopleNamespacePrefix+":Name"]).To(Equal("Bob"))

		// now, restrict on dataset "employees". now Name should be gone but title should be found
		result, _ = store.GetEntity(peopleNamespacePrefix+":person-2", []string{EMPLOYEES}, true)
		Expect(result.Properties[employeeNamespacePrefix+":Title"]).To(Equal("Vice President"),
			"expected to merge property employees:Title into result with value 'Vice President'")
		Expect(result.Properties[peopleNamespacePrefix+":Name"]).To(BeNil())
	})

	ginkgo.It("Should perform txn updates without error", func() {
		// create dataset
		_, err := dsm.CreateDataset("people", nil)
		Expect(err).To(BeNil())

		_, err = dsm.CreateDataset("places", nil)
		Expect(err).To(BeNil())

		entities := make([]*Entity, 1)
		entity := NewEntity("http://data.mimiro.io/people/person1", 0)
		entity.Properties["Name"] = "homer"
		entity.References["rdf:type"] = "http://data.mimiro.io/model/Person"
		entity.References["rdf:place"] = "http://data.mimiro.io/places/place1"
		entities[0] = entity

		entities1 := make([]*Entity, 1)
		entity1 := NewEntity("http://data.mimiro.io/places/place1", 0)
		entity1.Properties["Name"] = "home"
		entity1.References["rdf:type"] = "http://data.mimiro.io/model/Place"
		entities1[0] = entity1

		txn := &Transaction{}
		txn.DatasetEntities = make(map[string][]*Entity)
		txn.DatasetEntities["people"] = entities
		txn.DatasetEntities["places"] = entities1

		// get counts before txn
		dsInfo, err := store.NamespaceManager.GetDatasetNamespaceInfo()

		// check counts after txn
		datasets := []string{"core.Dataset"}
		dsEntity, err := store.GetEntity(fmt.Sprintf("%s:%s", dsInfo.DatasetPrefix, "people"), datasets, true)
		Expect(err).To(BeNil())

		if dsEntity != nil {
			items, ok := dsEntity.Properties[dsInfo.ItemsKey]
			Expect(ok).To(BeTrue())
			Expect(items).To(Equal(float64(5)))
		}

		// check that the item counts have been updated	for places
		dsEntity, err = store.GetEntity(fmt.Sprintf("%s:%s", dsInfo.DatasetPrefix, "places"), datasets, true)
		Expect(err).To(BeNil())

		if dsEntity != nil {
			items, ok := dsEntity.Properties[dsInfo.ItemsKey]
			Expect(ok).To(BeTrue())
			Expect(items).To(Equal(float64(0)))
		}

		err = store.ExecuteTransaction(txn)
		Expect(err).To(BeNil())

		people := dsm.GetDataset("people")
		peopleEntities, _ := people.GetEntities("", 100)
		Expect(len(peopleEntities.Entities)).To(Equal(6))

		// check counts after txn
		datasets = []string{"core.Dataset"}
		dsEntity, err = store.GetEntity(fmt.Sprintf("%s:%s", dsInfo.DatasetPrefix, people.ID), datasets, true)
		Expect(err).To(BeNil())

		if dsEntity != nil {
			items, ok := dsEntity.Properties[dsInfo.ItemsKey]
			Expect(ok).To(BeTrue())
			Expect(items).To(Equal(float64(6)))
		}

		// check that the item counts have been updated	for places
		dsEntity, err = store.GetEntity(fmt.Sprintf("%s:%s", dsInfo.DatasetPrefix, "places"), datasets, true)
		Expect(err).To(BeNil())

		if dsEntity != nil {
			items, ok := dsEntity.Properties[dsInfo.ItemsKey]
			Expect(ok).To(BeTrue())
			Expect(items).To(Equal(float64(1)))
		}
	})

	ginkgo.It("should find deleted version of entity with lookup", func() {
		peopleNamespacePrefix, _ := store.NamespaceManager.AssertPrefixMappingForExpansion(
			"http://data.mimiro.io/people/",
		)

		// create dataset
		ds, err := dsm.CreateDataset("people", nil)
		Expect(err).To(BeNil())

		entities := make([]*Entity, 1)
		entity := NewEntity(peopleNamespacePrefix+":p1", 0)
		entity.Properties["Name"] = "homer"
		entity.References["rdf:type"] = "http://data.mimiro.io/model/Person"
		entity.References["rdf:f1"] = "http://data.mimiro.io/people/Person-1"
		entity.References["rdf:f2"] = "http://data.mimiro.io/people/Person-2"
		entity.References["rdf:f3"] = "http://data.mimiro.io/people/Person-3"
		entities[0] = entity

		err = ds.StoreEntities(entities)
		Expect(err).To(BeNil(), "StoreEntities failed unexpectedly")

		// get entity
		e, err := store.GetEntity(peopleNamespacePrefix+":p1", nil, true)
		Expect(err).To(BeNil(), "GetEntity failed unexpectedly")
		Expect(e.IsDeleted).To(BeFalse())

		entities = make([]*Entity, 1)
		entity = NewEntity(peopleNamespacePrefix+":p1", 0)
		entity.IsDeleted = true
		entities[0] = entity

		err = ds.StoreEntities(entities)
		Expect(err).To(BeNil(), "StoreEntities failed unexpectedly")

		e, err = store.GetEntity("http://data.mimiro.io/people/p1", nil, true)
		Expect(err).To(BeNil(), "GetEntity failed unexpectedly")
		Expect(e.IsDeleted).To(BeTrue())
	})
})

func findEntity(result [][]interface{}, id string) *Entity {
	var entity *Entity
	for _, r := range result {
		currentEntity := r[2].(*Entity)
		if currentEntity.ID == id {
			entity = currentEntity
			break
		}
	}
	return entity
}

func count(b *badger.DB) int {
	items := 0
	_ = b.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)

		for it.Rewind(); it.Valid(); it.Next() {
			items++
			// os.Stdout.Write([]byte(fmt.Sprintf("%v\n",it.Item())))
		}

		it.Close()
		return nil
	})
	// os.Stdout.Write([]byte(fmt.Sprintf("%v\n\n\n",items)))
	return items
}
