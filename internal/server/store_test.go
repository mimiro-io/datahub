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
	"testing"
	"time"

	"github.com/dgraph-io/badger/v3"

	"github.com/franela/goblin"

	"github.com/DataDog/datadog-go/v5/statsd"
	"go.uber.org/fx/fxtest"

	"github.com/mimiro-io/datahub/internal/conf"

	"go.uber.org/zap"
)

func TestStoreRelations(test *testing.T) {
	g := goblin.Goblin(test)
	g.Describe("The dataset storage", func() {
		testCnt := 0
		var dsm *DsManager
		var store *Store
		var storeLocation string
		g.BeforeEach(func() {
			testCnt += 1
			storeLocation = fmt.Sprintf("./test_store_relations_%v", testCnt)
			err := os.RemoveAll(storeLocation)
			g.Assert(err).IsNil("should be allowed to clean testfiles in " + storeLocation)
			e := &conf.Env{
				Logger:        zap.NewNop().Sugar(),
				StoreLocation: storeLocation,
			}

			devNull, _ := os.Open("/dev/null")
			oldErr := os.Stderr
			os.Stderr = devNull
			lc := fxtest.NewLifecycle(test)
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

		g.It("Should delete the correct old outgoing references in array after entity modified", func() {
			peopleNamespacePrefix, _ := store.NamespaceManager.AssertPrefixMappingForExpansion("http://data.mimiro.io/people/")
			friendsDS, _ := dsm.CreateDataset("friends", nil)

			_ = friendsDS.StoreEntities([]*Entity{
				NewEntityFromMap(map[string]interface{}{
					"id":    peopleNamespacePrefix + ":person-1",
					"props": map[string]interface{}{peopleNamespacePrefix + ":Name": "Lisa"},
					"refs":  map[string]interface{}{peopleNamespacePrefix + ":Friend": []string{peopleNamespacePrefix + ":person-3", peopleNamespacePrefix + ":person-2"}}}),
			})

			// check that we can query outgoing
			result, err := store.GetManyRelatedEntities(
				[]string{"http://data.mimiro.io/people/person-1"}, peopleNamespacePrefix+":Friend", false, nil)
			g.Assert(err).IsNil()
			g.Assert(len(result)).Eql(2)

			// check that we can query incoming
			result, err = store.GetManyRelatedEntities(
				[]string{"http://data.mimiro.io/people/person-2"}, peopleNamespacePrefix+":Friend", true, nil)
			g.Assert(err).IsNil()
			g.Assert(len(result)).Eql(1)

			// update lisa
			e := NewEntityFromMap(map[string]interface{}{
				"id":    peopleNamespacePrefix + ":person-1",
				"props": map[string]interface{}{peopleNamespacePrefix + ":Name": "Lisa"},
				"refs":  map[string]interface{}{peopleNamespacePrefix + ":Friend": peopleNamespacePrefix + ":person-3"}})

			_ = friendsDS.StoreEntities([]*Entity{
				e,
			})

			// check that outgoing related entities is 0
			result, err = store.GetManyRelatedEntities(
				[]string{"http://data.mimiro.io/people/person-1"}, peopleNamespacePrefix+":Friend", false, nil)
			g.Assert(err).IsNil()
			g.Assert(len(result)).Eql(1)

			result, err = store.GetManyRelatedEntities(
				[]string{"http://data.mimiro.io/people/person-3"}, peopleNamespacePrefix+":Friend", true, nil)
			g.Assert(err).IsNil()
			g.Assert(len(result)).Eql(1)

			result, err = store.GetManyRelatedEntities(
				[]string{"http://data.mimiro.io/people/person-2"}, peopleNamespacePrefix+":Friend", true, nil)
			g.Assert(err).IsNil()
			g.Assert(len(result)).Eql(0)

		})

		g.It("Should delete the correct old outgoing references after entity modified", func() {
			peopleNamespacePrefix, _ := store.NamespaceManager.AssertPrefixMappingForExpansion("http://data.mimiro.io/people/")
			friendsDS, _ := dsm.CreateDataset("friends", nil)

			_ = friendsDS.StoreEntities([]*Entity{
				NewEntityFromMap(map[string]interface{}{
					"id":    peopleNamespacePrefix + ":person-1",
					"props": map[string]interface{}{peopleNamespacePrefix + ":Name": "Lisa"},
					"refs":  map[string]interface{}{peopleNamespacePrefix + ":Friend": peopleNamespacePrefix + ":person-3"}}),
			})

			// check that we can query outgoing
			result, err := store.GetManyRelatedEntities(
				[]string{"http://data.mimiro.io/people/person-1"}, peopleNamespacePrefix+":Friend", false, nil)
			g.Assert(err).IsNil()
			g.Assert(len(result)).Eql(1)
			g.Assert(result[0][1]).Eql(peopleNamespacePrefix + ":Friend")
			g.Assert(result[0][2].(*Entity).ID).Eql(peopleNamespacePrefix + ":person-3")

			// delete lisa
			e := NewEntityFromMap(map[string]interface{}{
				"id":    peopleNamespacePrefix + ":person-1",
				"props": map[string]interface{}{peopleNamespacePrefix + ":Name": "Lisa"},
				"refs":  map[string]interface{}{}})

			_ = friendsDS.StoreEntities([]*Entity{
				e,
			})

			// check that outgoing related entities is 0
			result, err = store.GetManyRelatedEntities(
				[]string{"http://data.mimiro.io/people/person-1"}, peopleNamespacePrefix+":Friend", false, nil)
			g.Assert(err).IsNil()
			g.Assert(len(result)).Eql(0)
		})

		g.It("Should delete the correct incoming and outgoing references after entity deleted", func() {
			peopleNamespacePrefix, _ := store.NamespaceManager.AssertPrefixMappingForExpansion("http://data.mimiro.io/people/")
			friendsDS, _ := dsm.CreateDataset("friends", nil)

			_ = friendsDS.StoreEntities([]*Entity{
				NewEntityFromMap(map[string]interface{}{
					"id":    peopleNamespacePrefix + ":person-1",
					"props": map[string]interface{}{peopleNamespacePrefix + ":Name": "Lisa"},
					"refs":  map[string]interface{}{peopleNamespacePrefix + ":Friend": peopleNamespacePrefix + ":person-3"}}),
				NewEntityFromMap(map[string]interface{}{
					"id":    peopleNamespacePrefix + ":person-2",
					"props": map[string]interface{}{peopleNamespacePrefix + ":Name": "Homer"},
					"refs":  map[string]interface{}{peopleNamespacePrefix + ":Friend": peopleNamespacePrefix + ":person-1"}}),
			})

			// check that we can query outgoing
			result, err := store.GetManyRelatedEntities(
				[]string{"http://data.mimiro.io/people/person-1"}, peopleNamespacePrefix+":Friend", false, nil)
			g.Assert(err).IsNil()
			g.Assert(len(result)).Eql(1)
			g.Assert(result[0][1]).Eql(peopleNamespacePrefix + ":Friend")
			g.Assert(result[0][2].(*Entity).ID).Eql(peopleNamespacePrefix + ":person-3")

			// check that we can query incoming
			result, err = store.GetManyRelatedEntities(
				[]string{"http://data.mimiro.io/people/person-1"}, peopleNamespacePrefix+":Friend", true, nil)
			g.Assert(err).IsNil()
			g.Assert(len(result)).Eql(1)
			g.Assert(result[0][1]).Eql(peopleNamespacePrefix + ":Friend")
			g.Assert(result[0][2].(*Entity).ID).Eql(peopleNamespacePrefix + ":person-2")

			// delete lisa
			e := NewEntityFromMap(map[string]interface{}{
				"id":    peopleNamespacePrefix + ":person-1",
				"props": map[string]interface{}{peopleNamespacePrefix + ":Name": "Lisa"},
				"refs":  map[string]interface{}{peopleNamespacePrefix + ":Friend": peopleNamespacePrefix + ":person-3"}})
			e.IsDeleted = true

			_ = friendsDS.StoreEntities([]*Entity{
				e,
			})

			// check that outgoing related entities is 0
			result, err = store.GetManyRelatedEntities(
				[]string{"http://data.mimiro.io/people/person-1"}, peopleNamespacePrefix+":Friend", false, nil)
			g.Assert(err).IsNil()
			g.Assert(len(result)).Eql(0)

			// check that we can query incoming count to person-3 is 0
			result, err = store.GetManyRelatedEntities(
				[]string{"http://data.mimiro.io/people/person-3"}, peopleNamespacePrefix+":Friend", true, nil)
			g.Assert(err).IsNil()
			g.Assert(len(result)).Eql(0)

			// check that we can query incoming count to person-1 is still 1
			result, err = store.GetManyRelatedEntities(
				[]string{"http://data.mimiro.io/people/person-1"}, peopleNamespacePrefix+":Friend", true, nil)
			g.Assert(err).IsNil()
			g.Assert(len(result)).Eql(1)

		})

		g.It("Should delete the correct incoming and outgoing references", func() {
			b := store.database
			peopleNamespacePrefix, _ := store.NamespaceManager.AssertPrefixMappingForExpansion("http://data.mimiro.io/people/")
			workPrefix, _ := store.NamespaceManager.AssertPrefixMappingForExpansion("http://data.mimiro.io/work/")
			g.Assert(count(b)).Eql(16)

			friendsDS, _ := dsm.CreateDataset("friends", nil)
			g.Assert(count(b)).Eql(24)

			workDS, _ := dsm.CreateDataset("work", nil)
			g.Assert(count(b)).Eql(32)

			_ = friendsDS.StoreEntities([]*Entity{
				NewEntityFromMap(map[string]interface{}{
					"id":    peopleNamespacePrefix + ":person-1",
					"props": map[string]interface{}{peopleNamespacePrefix + ":Name": "Lisa"},
					"refs":  map[string]interface{}{peopleNamespacePrefix + ":Friend": peopleNamespacePrefix + ":person-3"}}),
				NewEntityFromMap(map[string]interface{}{
					"id":    peopleNamespacePrefix + ":person-2",
					"props": map[string]interface{}{peopleNamespacePrefix + ":Name": "Lisa"},
					"refs":  map[string]interface{}{peopleNamespacePrefix + ":Friend": peopleNamespacePrefix + ":person-1"}}),
			})
			g.Assert(count(b)).Eql(55)

			// check that we can query outgoing
			result, err := store.GetManyRelatedEntities(
				[]string{"http://data.mimiro.io/people/person-1"}, peopleNamespacePrefix+":Friend", false, nil)
			g.Assert(err).IsNil()
			g.Assert(len(result)).Eql(1)
			g.Assert(result[0][1]).Eql(peopleNamespacePrefix + ":Friend")
			g.Assert(result[0][2].(*Entity).ID).Eql(peopleNamespacePrefix + ":person-3")

			// check that we can query incoming
			result, err = store.GetManyRelatedEntities(
				[]string{"http://data.mimiro.io/people/person-1"}, peopleNamespacePrefix+":Friend", true, nil)
			g.Assert(err).IsNil()
			g.Assert(len(result)).Eql(1)
			g.Assert(result[0][1]).Eql(peopleNamespacePrefix + ":Friend")
			g.Assert(result[0][2].(*Entity).ID).Eql(peopleNamespacePrefix + ":person-2")

			_ = workDS.StoreEntities([]*Entity{
				NewEntityFromMap(map[string]interface{}{
					"id":    peopleNamespacePrefix + ":person-1",
					"props": map[string]interface{}{workPrefix + ":Wage": "110"},
					"refs":  map[string]interface{}{workPrefix + ":Coworker": peopleNamespacePrefix + ":person-2"}}),
				NewEntityFromMap(map[string]interface{}{
					"id":    peopleNamespacePrefix + ":person-2",
					"props": map[string]interface{}{workPrefix + ":Wage": "100"},
					"refs":  map[string]interface{}{workPrefix + ":Coworker": peopleNamespacePrefix + ":person-3"}}),
			})
			g.Assert(count(b)).Eql(72)

			// check that we can still query outgoing
			result, err = store.GetManyRelatedEntities(
				[]string{"http://data.mimiro.io/people/person-1"}, peopleNamespacePrefix+":Friend", false, nil)
			// result, err = store.GetManyRelatedEntities( []string{"http://data.mimiro.io/people/person-1"}, "*", false, nil)
			g.Assert(err).IsNil()
			g.Assert(len(result)).Eql(1, "Expected still to find person-3 as a friend")
			g.Assert(result[0][2].(*Entity).ID).Eql(peopleNamespacePrefix + ":person-3")

			// and incoming
			result, err = store.GetManyRelatedEntities(
				[]string{"http://data.mimiro.io/people/person-1"}, peopleNamespacePrefix+":Friend", true, nil)
			g.Assert(err).IsNil()
			g.Assert(len(result)).Eql(1, "Expected still to find person-2 as reverse friend")
			g.Assert(result[0][2].(*Entity).ID).Eql(peopleNamespacePrefix + ":person-2")
		})

		g.It("Should store references of new deleted entities as deleted", func() {
			peopleNamespacePrefix, _ := store.NamespaceManager.AssertPrefixMappingForExpansion("http://data.mimiro.io/people/")
			friendsDS, _ := dsm.CreateDataset("friends", nil)

			p1 := NewEntityFromMap(map[string]interface{}{
				"id":    peopleNamespacePrefix + ":person-1",
				"props": map[string]interface{}{peopleNamespacePrefix + ":Name": "Lisa"},
				"refs":  map[string]interface{}{peopleNamespacePrefix + ":Friend": peopleNamespacePrefix + ":person-3"}})
			p1.IsDeleted = true
			_ = friendsDS.StoreEntities([]*Entity{
				p1,
				NewEntityFromMap(map[string]interface{}{
					"id":    peopleNamespacePrefix + ":person-2",
					"props": map[string]interface{}{peopleNamespacePrefix + ":Name": "Homer"},
					"refs":  map[string]interface{}{peopleNamespacePrefix + ":Friend": peopleNamespacePrefix + ":person-1"}}),
			})
			// check that we can not query outgoing from deleted entity
			result, err := store.GetManyRelatedEntities(
				[]string{"http://data.mimiro.io/people/person-1"}, peopleNamespacePrefix+":Friend", false, nil)
			g.Assert(err).IsNil()
			g.Assert(len(result)).Eql(0)

			// check that we can query incoming. this relation is owned by Homer
			result, err = store.GetManyRelatedEntities(
				[]string{"http://data.mimiro.io/people/person-1"}, peopleNamespacePrefix+":Friend", true, nil)
			g.Assert(err).IsNil()
			g.Assert(len(result)).Eql(1)
			g.Assert(result[0][1]).Eql(peopleNamespacePrefix + ":Friend")
			g.Assert(result[0][2].(*Entity).ID).Eql(peopleNamespacePrefix + ":person-2")

		})

		g.It("Should build query results", func() {
			// create dataset
			ds, err := dsm.CreateDataset("people", nil)

			batchSize := 5
			entities := make([]*Entity, batchSize)

			peopleNamespacePrefix, err := store.NamespaceManager.AssertPrefixMappingForExpansion("http://data.mimiro.io/people/")
			modelNamespacePrefix, err := store.NamespaceManager.AssertPrefixMappingForExpansion("http://data.mimiro.io/model/")
			rdfNamespacePrefix, err := store.NamespaceManager.AssertPrefixMappingForExpansion(RdfNamespaceExpansion)

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
			g.Assert(err).IsNil()

			results, err := store.GetRelated("http://data.mimiro.io/people/p-1", RdfTypeUri, false, []string{})
			g.Assert(err).IsNil()
			g.Assert(len(results)).Eql(1)

			invresults, err := store.GetRelated("http://data.mimiro.io/model/Person", RdfTypeUri, true, []string{})
			g.Assert(err).IsNil()
			g.Assert(len(invresults)).Eql(5)

			invresults, err = store.GetRelated("http://data.mimiro.io/model/Person", "*", true, []string{})
			g.Assert(err).IsNil()
			g.Assert(len(invresults)).Eql(5)

			for i := 0; i < batchSize; i++ {
				entity := NewEntity(peopleNamespacePrefix+":p-"+strconv.Itoa(i), 0)
				entity.Properties[modelNamespacePrefix+":Name"] = "homer"
				entity.References[modelNamespacePrefix+":f1"] = peopleNamespacePrefix + ":Person-1"
				entity.References[modelNamespacePrefix+":f2"] = peopleNamespacePrefix + ":Person-2"
				entity.References[modelNamespacePrefix+":f3"] = peopleNamespacePrefix + ":Person-3"
				entities[i] = entity
			}

			err = ds.StoreEntities(entities)
			g.Assert(err).IsNil()

			time0 := time.Now().UnixNano()

			invresults, err = store.GetRelated("http://data.mimiro.io/model/Person", RdfTypeUri, true, []string{})
			g.Assert(err).IsNil()
			g.Assert(len(invresults)).Eql(0)

			results, err = store.GetRelated("http://data.mimiro.io/people/p-1", RdfTypeUri, false, []string{})
			g.Assert(err).IsNil()
			g.Assert(len(results)).Eql(0)

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
			g.Assert(err).IsNil()

			invresults, err = store.GetRelated("http://data.mimiro.io/model/Person", RdfTypeUri, true, []string{})
			g.Assert(err).IsNil()
			g.Assert(len(invresults)).Eql(5)

			results, err = store.GetRelated("http://data.mimiro.io/people/p-1", RdfTypeUri, false, []string{})
			g.Assert(err).IsNil()
			g.Assert(len(results)).Eql(1)

			// test at point in time
			results, err = store.GetRelatedAtTime("http://data.mimiro.io/people/p-1", RdfTypeUri, false, []uint32{}, time0)
			g.Assert(err).IsNil()
			g.Assert(len(results)).Eql(0)

			invresults, err = store.GetRelatedAtTime("http://data.mimiro.io/model/Person", RdfTypeUri, true, []uint32{}, time0)
			g.Assert(err).IsNil()
			g.Assert(len(invresults)).Eql(0)
		})
	})
}

func TestStore(test *testing.T) {
	g := goblin.Goblin(test)
	g.Describe("The dataset storage", func() {
		testCnt := 0
		var dsm *DsManager
		var store *Store
		var storeLocation string
		g.BeforeEach(func() {
			testCnt += 1
			storeLocation = fmt.Sprintf("./test_store_%v", testCnt)
			err := os.RemoveAll(storeLocation)
			g.Assert(err).IsNil("should be allowed to clean testfiles in " + storeLocation)
			e := &conf.Env{
				Logger:        zap.NewNop().Sugar(),
				StoreLocation: storeLocation,
			}

			devNull, _ := os.Open("/dev/null")
			oldErr := os.Stderr
			os.Stderr = devNull
			lc := fxtest.NewLifecycle(test)
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

		g.It("Should store the curi id mapping", func() {
			cache := make(map[string]uint64)
			id, _, _ := dsm.store.assertIDForURI("ns0:gra", cache)
			err := dsm.store.commitIdTxn()
			g.Assert(err).IsNil()
			curi, _ := dsm.store.getURIForID(id)
			g.Assert(curi).Eql("ns0:gra")
		})

		g.It("Should store entity batches without error", func() {
			// create dataset
			ds, _ := dsm.CreateDataset("people", nil)

			entities := make([]*Entity, 1)
			entity := NewEntity("http://data.mimiro.io/people/homer", 0)
			entity.Properties["Name"] = "homer"
			entity.References["type"] = "http://data.mimiro.io/model/Person"
			entity.References["typed"] = "http://data.mimiro.io/model/Person"
			entities[0] = entity

			err := ds.StoreEntities(entities)
			g.Assert(err).IsNil("Expected no error when storing entity")
		})

		g.It("Should replace entity namespace prefixes", func() {
			// define store context
			_, _ = store.NamespaceManager.AssertPrefixMappingForExpansion(RdfNamespaceExpansion)
			prefix, _ := store.NamespaceManager.AssertPrefixMappingForExpansion("http://data.mimiro.io/model/Person/")

			entity := NewEntity(prefix+":homer", 0)
			entity.Properties[prefix+":Name"] = "homer"
			entity.References[prefix+":type"] = "ns1:Person"
			entity.References[prefix+":company"] = "ns1:Person"

			g.Assert(entity.ID).Eql(prefix + ":homer")
			err := entity.ExpandIdentifiers(store)
			g.Assert(err).IsNil()
			g.Assert(entity.ID).Eql("http://data.mimiro.io/model/Person/homer")
		})

		g.It("Should make entities retrievable", func() {
			// create dataset
			ds, _ := dsm.CreateDataset("people", nil)

			entities := make([]*Entity, 1)
			entity := NewEntity("http://data.mimiro.io/people/homer", 0)
			entity.Properties["Name"] = "homer"
			entity.References["type"] = "http://data.mimiro.io/model/Person"
			entity.References["typed"] = "http://data.mimiro.io/model/Person"
			entities[0] = entity

			err := ds.StoreEntities(entities)
			g.Assert(err).IsNil()

			allEntities := make([]*Entity, 0)
			_, err = ds.MapEntities("", 0, func(e *Entity) error {
				allEntities = append(allEntities, e)
				return nil
			})
			g.Assert(err).IsNil()
			g.Assert(len(allEntities)).Eql(1, "Expected to retrieve same number of entities as was written")
		})

		g.It("Should only update entities if they are different (batch)", func() {
			// create dataset
			ds, _ := dsm.CreateDataset("people", nil)

			entities := make([]*Entity, 1)
			entity := NewEntity("http://data.mimiro.io/people/homer", 0)
			entity.Properties["Name"] = "homer"
			entity.References["type"] = "http://data.mimiro.io/model/Person"
			entity.References["typed"] = "http://data.mimiro.io/model/Person"
			entities[0] = entity

			//1. store entity
			err := ds.StoreEntities(entities)
			g.Assert(err).IsNil()
			// and get first versions of object
			res, err := ds.GetEntities("", 1)
			g.Assert(err).IsNil()
			firstVersionTs := res.Entities[0].Recorded

			//2. store again
			err = ds.StoreEntities(entities)
			g.Assert(err).IsNil()
			// and get version again
			res, _ = ds.GetEntities("", 1)
			g.Assert(res.Entities[0].Recorded).Eql(firstVersionTs, "Should be unchanged version")

			//3. set same name and store again
			entity.Properties["Name"] = "homer"
			err = ds.StoreEntities(entities)
			g.Assert(err).IsNil()
			// and get version again
			res, _ = ds.GetEntities("", 1)
			g.Assert(res.Entities[0].Recorded).Eql(firstVersionTs, "Should be unchanged version")

			// 4. now set a new name and store
			entity.Properties["Name"] = "homer simpson"
			err = ds.StoreEntities(entities)
			g.Assert(err).IsNil()
			// version should change
			res, _ = ds.GetEntities("", 1)
			g.Assert(res.Entities[0].Recorded != firstVersionTs).IsTrue("Should be new version")
		})

		g.It("Should track changes", func() {
			// create dataset
			_, _ = dsm.CreateDataset("companies", nil)
			_, _ = dsm.CreateDataset("animals", nil)
			ds, err := dsm.CreateDataset("people", nil)
			g.Assert(err).IsNil()

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
			g.Assert(err).IsNil()

			// get changes
			changes, err := ds.GetChanges(0, 10, false)
			g.Assert(err).IsNil()
			g.Assert(len(changes.Entities)).Eql(3)
			g.Assert(changes.NextToken).Eql(uint64(3))

			changes, err = ds.GetChanges(1, 1, false)
			g.Assert(err).IsNil()
			g.Assert(len(changes.Entities)).Eql(1)
			g.Assert(changes.NextToken).Eql(uint64(2))

			changes, err = ds.GetChanges(2, 1, false)
			g.Assert(err).IsNil()
			g.Assert(len(changes.Entities)).Eql(1)
			g.Assert(changes.NextToken).Eql(uint64(3))

			changes, _ = ds.GetChanges(3, 10, false)
			g.Assert(len(changes.Entities)).Eql(0)
			g.Assert(changes.NextToken).Eql(uint64(3))
		})

		g.It("Should paginate entities with continuation tokens", func() {
			// create dataset
			c, _ := dsm.CreateDataset("companies", nil)
			_, _ = dsm.CreateDataset("animals", nil)
			ds, _ := dsm.CreateDataset("people", nil)

			_ = dsm.DeleteDataset("people")
			ds, _ = dsm.CreateDataset("people", nil)

			entities := make([]*Entity, 3)
			entity1 := NewEntity("http://data.mimiro.io/people/homer", 0)
			entities[0] = entity1
			entity2 := NewEntity("http://data.mimiro.io/people/bob", 0)
			entities[1] = entity2
			entity3 := NewEntity("http://data.mimiro.io/people/jim", 0)
			entities[2] = entity3

			err := ds.StoreEntities(entities)
			g.Assert(err).IsNil()
			//store same entitites in two datasets. should give merged results
			err = c.StoreEntities(entities)
			g.Assert(err).IsNil()

			result, err := ds.GetEntities("", 50)
			g.Assert(err).IsNil()
			g.Assert(len(result.Entities)).Eql(3, "Expected all 3 entitites back")

			result1, err := ds.GetEntities(result.ContinuationToken, 1)
			g.Assert(err).IsNil()
			g.Assert(len(result1.Entities)).Eql(0, "first page should have returned all entities, "+
				"therefore this page should be empty")
			g.Assert(result1.ContinuationToken).Eql(result.ContinuationToken)

			// test c token
			result, err = ds.GetEntities("", 1)
			g.Assert(err).IsNil()
			g.Assert(len(result.Entities)).Eql(1, "we only asked for 1 entitiy this page")

			result, err = ds.GetEntities(result.ContinuationToken, 5)
			g.Assert(err).IsNil()
			g.Assert(len(result.Entities)).Eql(2, "second page should have remaining 2 enitities")

			g.Assert(result.Entities[0].ID).Eql("http://data.mimiro.io/people/bob", "first result 2nd page is not bob")
		})

		g.It("Should not struggle with many batch writes", func() {
			g.Timeout(5 * time.Minute)
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
					g.Assert(err).IsNil("Storing should never fail under load")
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
		})

		g.It("Should do deletion detection when running in fullsync mode", func() {
			// create dataset
			ds, err := dsm.CreateDataset("people", nil)
			g.Assert(err).IsNil()
			batchSize := 5

			err = ds.StartFullSync()
			g.Assert(err).IsNil()

			entities := make([]*Entity, batchSize)
			for i := 0; i < batchSize; i++ {
				entity := NewEntity("http://data.mimiro.io/people/p-"+strconv.Itoa(i), 0)
				entity.Properties["Name"] = "homer"
				entities[i] = entity
			}
			err = ds.StoreEntities(entities)
			g.Assert(err).IsNil()

			err = ds.CompleteFullSync()
			g.Assert(err).IsNil()

			//start 2nd fullsync
			err = ds.StartFullSync()
			g.Assert(err).IsNil()

			entities = make([]*Entity, 1)
			entity := NewEntity("http://data.mimiro.io/people/p-0", 0)
			entity.Properties["Name"] = "homer"
			entities[0] = entity
			//entities[0] = res.Entities[0]
			err = ds.StoreEntities(entities)
			g.Assert(err).IsNil()

			err = ds.CompleteFullSync()
			g.Assert(err).IsNil()

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
			g.Assert(err).IsNil("MapEntities should have worked")
			g.Assert(len(allEntities)).Eql(1, "There should be one undeleted entity left")
			g.Assert(len(deletedEntities)).Eql(batchSize-1, "all other entities should be deleted")

		})

		g.It("Should enable iterating over a dataset (MapEntitites)", func() {
			// create dataset
			ds, err := dsm.CreateDataset("people", nil)
			g.Assert(err).IsNil()
			batchSize := 5

			entities := make([]*Entity, batchSize)
			for i := 0; i < batchSize; i++ {
				entity := NewEntity("http://data.mimiro.io/people/p-"+strconv.Itoa(i), 0)
				entity.Properties["Name"] = "homer"
				entities[i] = entity
			}
			err = ds.StoreEntities(entities)
			g.Assert(err).IsNil()

			var entity *Entity
			ctoken, err := ds.MapEntities("", 1, func(e *Entity) error {
				entity = e
				return nil
			})
			g.Assert(entities).IsNotZero("MapEntities should have processed 1 entity")
			g.Assert(ctoken).IsNotZero("MapEntities should have given us a token")

			var nextEntity *Entity
			newtoken, err := ds.MapEntities(ctoken, 1, func(e *Entity) error {
				nextEntity = e
				return nil
			})
			g.Assert(nextEntity).IsNotZero("MapEntities should have processed 1 entity again")
			g.Assert(newtoken).IsNotZero("MapEntities should have given us a new token")
			g.Assert(nextEntity.ID != entity.ID).IsTrue("We should have a different entity")
			g.Assert(ctoken != newtoken).IsTrue("We should have a different token")
		})

		g.Describe("When retrieving data, it can combine multiple entities from different datasets with the same ID", func() {

			g.It("Should merge entity data from different entities with same id (partials)", func() {
				e1 := NewEntity("x:e1", 0)
				e2 := NewEntity("x:e1", 0)
				partials := make([]*Entity, 2)
				partials[0] = e1
				partials[1] = e2
				result := store.MergePartials(partials)
				g.Assert(result).IsNotZero()
				g.Assert(result.ID).Eql("x:e1")
				g.Assert(result.Properties["x:name"]).IsNil()

				e2.Properties["x:name"] = "homer"
				result = store.MergePartials(partials)
				g.Assert(result).IsNotZero()
				g.Assert(result.ID).Eql("x:e1")
				g.Assert(result.Properties["x:name"]).Eql("homer")
			})

			g.It("Should merge partials without error if a source is empty", func() {
				e1 := NewEntity("x:e1", 0)
				e2 := NewEntity("x:e1", 0)
				partials := make([]*Entity, 2)
				partials[0] = e1
				partials[1] = e2

				e1.Properties["x:name"] = "homer"

				result := store.MergePartials(partials)
				g.Assert(result).IsNotZero()
				g.Assert(result.ID).Eql("x:e1")
				g.Assert(result.Properties["x:name"]).Eql("homer")
			})

			g.It("Should merge partials if target entity is empty", func() {
				e1 := NewEntity("x:e1", 0)
				e2 := NewEntity("x:e1", 0)
				partials := make([]*Entity, 2)
				partials[0] = e1
				partials[1] = e2

				e2.Properties["x:name"] = "homer"

				result := store.MergePartials(partials)
				g.Assert(result).IsNotZero()
				g.Assert(result.ID).Eql("x:e1")
				g.Assert(result.Properties["x:name"]).Eql("homer")
			})

			g.It("Should merge single values from 2 partials to a list value", func() {
				e1 := NewEntity("x:e1", 0)
				e2 := NewEntity("x:e1", 0)
				partials := make([]*Entity, 2)
				partials[0] = e1
				partials[1] = e2

				e1.Properties["x:name"] = "homer"
				e2.Properties["x:name"] = "bob"

				result := store.MergePartials(partials)
				g.Assert(result).IsNotZero()
				g.Assert(result.ID).Eql("x:e1")
				g.Assert(result.Properties["x:name"]).Eql([]interface{}{"homer", "bob"})
			})

			g.It("Should add single values to existing list when merging partials", func() {
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

				result := store.MergePartials(partials)
				g.Assert(result).IsNotZero()
				g.Assert(result.ID).Eql("x:e1")
				g.Assert(result.Properties["x:name"]).Eql([]interface{}{"homer", "brian", "bob"})
			})

			g.It("Should add list to existing single value when merging partials", func() {
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

				result := store.MergePartials(partials)
				g.Assert(result).IsNotZero()
				g.Assert(result.ID).Eql("x:e1")
				g.Assert(result.Properties["x:name"]).Eql([]interface{}{"bob", "homer", "brian"})
			})

			g.It("Should combine two value lists when merging partials", func() {
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

				result := store.MergePartials(partials)
				g.Assert(result).IsNotZero()
				g.Assert(result.ID).Eql("x:e1")
				g.Assert(result.Properties["x:name"]).Eql([]interface{}{"homer", "brian", "bob", "james"})
			})

			g.It("Should merge 3 partials", func() {
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

				result := store.MergePartials(partials)
				g.Assert(result).IsNotZero()
				g.Assert(result.ID).Eql("x:e1")
				g.Assert(result.Properties["x:name"]).Eql([]interface{}{"bob", "brian", "james"})
				g.Assert(result.Properties["x:dob"]).Eql("01-01-2001")
			})
		})

		g.It("Should build query results", func() {
			// create dataset
			ds, err := dsm.CreateDataset("people", nil)

			batchSize := 5
			entities := make([]*Entity, batchSize)

			peopleNamespacePrefix, err := store.NamespaceManager.AssertPrefixMappingForExpansion("http://data.mimiro.io/people/")
			modelNamespacePrefix, err := store.NamespaceManager.AssertPrefixMappingForExpansion("http://data.mimiro.io/model/")
			rdfNamespacePrefix, err := store.NamespaceManager.AssertPrefixMappingForExpansion(RdfNamespaceExpansion)

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
			g.Assert(err).IsNil()

			results, err := store.GetRelated("http://data.mimiro.io/people/p-1", RdfTypeUri, false, []string{})
			g.Assert(err).IsNil()
			g.Assert(len(results)).Eql(1)

			invresults, err := store.GetRelated("http://data.mimiro.io/model/Person", RdfTypeUri, true, []string{})
			g.Assert(err).IsNil()
			g.Assert(len(invresults)).Eql(5)

			invresults, err = store.GetRelated("http://data.mimiro.io/model/Person", "*", true, []string{})
			g.Assert(err).IsNil()
			g.Assert(len(invresults)).Eql(5)

			for i := 0; i < batchSize; i++ {
				entity := NewEntity(peopleNamespacePrefix+":p-"+strconv.Itoa(i), 0)
				entity.Properties[modelNamespacePrefix+":Name"] = "homer"
				entity.References[modelNamespacePrefix+":f1"] = peopleNamespacePrefix + ":Person-1"
				entity.References[modelNamespacePrefix+":f2"] = peopleNamespacePrefix + ":Person-2"
				entity.References[modelNamespacePrefix+":f3"] = peopleNamespacePrefix + ":Person-3"
				entities[i] = entity
			}

			err = ds.StoreEntities(entities)
			g.Assert(err).IsNil()

			invresults, err = store.GetRelated("http://data.mimiro.io/model/Person", RdfTypeUri, true, []string{})
			g.Assert(err).IsNil()
			g.Assert(len(invresults)).Eql(0)

			results, err = store.GetRelated("http://data.mimiro.io/people/p-1", RdfTypeUri, false, []string{})
			g.Assert(err).IsNil()
			g.Assert(len(results)).Eql(0)

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
			g.Assert(err).IsNil()

			invresults, err = store.GetRelated("http://data.mimiro.io/model/Person", RdfTypeUri, true, []string{})
			g.Assert(err).IsNil()
			g.Assert(len(invresults)).Eql(5)

			results, err = store.GetRelated("http://data.mimiro.io/people/p-1", RdfTypeUri, false, []string{})
			g.Assert(err).IsNil()
			g.Assert(len(results)).Eql(1)

		})

		g.It("Should perform batch updates without error with concurrent requests", func() {
			// create dataset
			ds, err := dsm.CreateDataset("people", nil)
			g.Assert(err).IsNil()

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
					g.Assert(err).IsNil("StoreEntities failed unexpectedly")
					defer wg.Done()
				}()
			}
			wg.Wait()
		})

		g.It("Should store and replace large batches without error", func() {

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
			g.Assert(err).IsNil()

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
			g.Assert(err).IsNil()
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

		g.It("Should build correct query results with large number of entities", func() {
			// namespaces
			peopleNamespacePrefix, err := store.NamespaceManager.AssertPrefixMappingForExpansion("http://data.mimiro.io/people/")
			companyNamespacePrefix, err := store.NamespaceManager.AssertPrefixMappingForExpansion("http://data.mimiro.io/company/")
			modelNamespacePrefix, err := store.NamespaceManager.AssertPrefixMappingForExpansion("http://data.mimiro.io/model/")
			rdfNamespacePrefix, err := store.NamespaceManager.AssertPrefixMappingForExpansion(RdfNamespaceExpansion)

			// create dataset
			peopleDataset, _ := dsm.CreateDataset("people", nil)
			companiesDataset, _ := dsm.CreateDataset("companies", nil)

			numOfCompanies := 10
			numOfEmployees := 1000
			companies := make([]*Entity, 0)
			people := make([]*Entity, 0)
			queryIds := make([]string, 0)
			empId := 0

			for i := 0; i < numOfCompanies; i++ {

				c := NewEntity(companyNamespacePrefix+":company-"+strconv.Itoa(i), 0)
				c.References[rdfNamespacePrefix+":type"] = modelNamespacePrefix + ":Company"

				for j := 0; j < numOfEmployees; j++ {
					person := NewEntity(peopleNamespacePrefix+":person-"+strconv.Itoa(empId), 0)
					person.Properties[peopleNamespacePrefix+":Name"] = "person " + strconv.Itoa(i)
					person.References[rdfNamespacePrefix+":type"] = modelNamespacePrefix + ":Person"
					person.References[peopleNamespacePrefix+":worksfor"] = companyNamespacePrefix + ":company-" + strconv.Itoa(i)
					people = append(people, person)

					if empId < 100 {
						queryIds = append(queryIds, "http://data.mimiro.io/people/person-"+strconv.Itoa(empId))
					}

					empId++
				}

				companies = append(companies, c)
			}
			err = companiesDataset.StoreEntities(companies)
			g.Assert(err).IsNil()

			err = peopleDataset.StoreEntities(people)
			g.Assert(err).IsNil()

			// query
			result, err := store.GetManyRelatedEntities(queryIds, "http://data.mimiro.io/people/worksfor", false, []string{})
			g.Assert(len(result)).Eql(100)

			// check inverse
			queryIds = make([]string, 0)
			queryIds = append(queryIds, "http://data.mimiro.io/company/company-1")
			queryIds = append(queryIds, "http://data.mimiro.io/company/company-2")
			result, err = store.GetManyRelatedEntities(queryIds, "http://data.mimiro.io/people/worksfor", true, []string{})
			g.Assert(len(result)).Eql(numOfEmployees * 2)
		})

		g.It("Should return entity placeholders (links) in queries", func() {
			// namespaces
			peopleNamespacePrefix, err := store.NamespaceManager.AssertPrefixMappingForExpansion("http://data.mimiro.io/people/")
			companyNamespacePrefix, err := store.NamespaceManager.AssertPrefixMappingForExpansion("http://data.mimiro.io/company/")
			modelNamespacePrefix, err := store.NamespaceManager.AssertPrefixMappingForExpansion("http://data.mimiro.io/model/")
			rdfNamespacePrefix, err := store.NamespaceManager.AssertPrefixMappingForExpansion(RdfNamespaceExpansion)

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

			err = peopleDataset.StoreEntities(people)
			g.Assert(err).IsNil()

			queryIds = append(queryIds, "http://data.mimiro.io/people/person-"+strconv.Itoa(1))
			// query
			result, err := store.GetManyRelatedEntities(queryIds, "http://data.mimiro.io/people/worksfor", false, []string{})
			g.Assert(len(result)).Eql(1)
		})
	})
}

func TestDatasetScope(test *testing.T) {
	g := goblin.Goblin(test)
	g.Describe("Scoped storage functions", func() {
		testCnt := 0
		var dsm *DsManager
		var store *Store
		var storeLocation string
		var peopleNamespacePrefix string
		var companyNamespacePrefix string
		const PEOPLE = "people"
		const COMPANIES = "companies"
		const HISTORY = "history"
		g.BeforeEach(func() {
			testCnt += 1
			storeLocation = fmt.Sprintf("./test_store_dsscope_%v", testCnt)
			err := os.RemoveAll(storeLocation)
			g.Assert(err).IsNil("should be allowed to clean testfiles in " + storeLocation)
			e := &conf.Env{
				Logger:        zap.NewNop().Sugar(),
				StoreLocation: storeLocation,
			}

			devNull, _ := os.Open("/dev/null")
			oldErr := os.Stderr
			os.Stderr = devNull
			lc := fxtest.NewLifecycle(test)
			store = NewStore(lc, e, &statsd.NoOpClient{})
			dsm = NewDsManager(lc, e, store, NoOpBus())

			err = lc.Start(context.Background())
			g.Assert(err).IsNil()
			os.Stderr = oldErr

			// namespaces
			peopleNamespacePrefix, _ = store.NamespaceManager.AssertPrefixMappingForExpansion("http://data.mimiro.io/people/")
			companyNamespacePrefix, _ = store.NamespaceManager.AssertPrefixMappingForExpansion("http://data.mimiro.io/company/")
			modelNamespacePrefix, _ := store.NamespaceManager.AssertPrefixMappingForExpansion("http://data.mimiro.io/model/")
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
			for _, companyId := range []int{1, 2, 3} {
				cid := companyNamespacePrefix + ":company-" + strconv.Itoa(companyId)
				_ = companiesDataset.StoreEntities([]*Entity{NewEntityFromMap(map[string]interface{}{
					"id":    cid,
					"props": map[string]interface{}{companyNamespacePrefix + ":Name": "Company " + strconv.Itoa(companyId)},
					"refs":  map[string]interface{}{rdfNamespacePrefix + ":type": modelNamespacePrefix + ":Company"}})})
			}

			// insert people with worksfor relations
			currentWork := []int{1, 2, 1, 3, 1}
			for personId, workId := range currentWork {
				pid := peopleNamespacePrefix + ":person-" + strconv.Itoa(personId+1)
				_ = peopleDataset.StoreEntities([]*Entity{NewEntityFromMap(map[string]interface{}{
					"id":    pid,
					"props": map[string]interface{}{peopleNamespacePrefix + ":Name": "Person " + strconv.Itoa(personId+1)},
					"refs": map[string]interface{}{
						rdfNamespacePrefix + ":type":        modelNamespacePrefix + ":Person",
						peopleNamespacePrefix + ":worksfor": companyNamespacePrefix + ":company-" + strconv.Itoa(workId),
					}})})
			}

			//insert workHistory
			for personId, workHistIds := range [][]int{{}, {1}, {2}, {1, 2}, {3}} {
				var workHistory []string
				for _, histId := range workHistIds {
					workHistory = append(workHistory, companyNamespacePrefix+":company-"+strconv.Itoa(histId))
				}
				pid := peopleNamespacePrefix + ":person-" + strconv.Itoa(personId+1)
				entity := NewEntityFromMap(map[string]interface{}{
					"id":    pid,
					"props": map[string]interface{}{},
					"refs": map[string]interface{}{
						rdfNamespacePrefix + ":type":         modelNamespacePrefix + ":Employment",
						peopleNamespacePrefix + ":Employee":  strconv.Itoa(personId + 1),
						peopleNamespacePrefix + ":worksfor":  companyNamespacePrefix + ":company-" + strconv.Itoa(currentWork[personId]),
						peopleNamespacePrefix + ":workedfor": workHistory,
					}})
				_ = employmentHistoryDataset.StoreEntities([]*Entity{entity})
			}
		})
		g.AfterEach(func() {
			dsm.DeleteDataset(PEOPLE)
			dsm.DeleteDataset(COMPANIES)
			dsm.DeleteDataset(HISTORY)
			_ = store.Close()
			_ = os.RemoveAll(storeLocation)
		})

		g.It("Should return all of an entity's relations when no dataset constraints are applied", func() {
			result, _ := store.GetManyRelatedEntities([]string{peopleNamespacePrefix + ":person-1"},
				"http://data.mimiro.io/people/worksfor", false, []string{})
			g.Assert(len(result)).Eql(2,
				"expected 2 relations to be found. one from people, one from workhistory")

			entity := findEntity(result, "ns4:company-1")
			g.Assert(entity).IsNotNil()
			g.Assert(entity.ID).Eql("ns4:company-1", "first relation ID should be found")
			g.Assert(entity.Properties["ns4:Name"]).Eql("Company 1", "first relation property should be resolved")
		})

		g.It("Should return only relation ID if access to relation dataset is not given", func() {
			//constraint on "people" only. we should find the relation to a company in "people",
			//but resolving the company should be omitted because we lack access to the "companies" dataset
			result, _ := store.GetManyRelatedEntities([]string{peopleNamespacePrefix + ":person-1"},
				"http://data.mimiro.io/people/worksfor", false, []string{PEOPLE})
			g.Assert(len(result)).Eql(1, "expected 1 relation to be found in people dataset (not in workHistory)")

			entity := findEntity(result, "ns4:company-1")
			g.Assert(entity).IsNotNil()
			g.Assert(entity.ID).Eql("ns4:company-1", "first relation ID should be found")
			g.Assert(entity.Properties["ns4:Name"]).IsNil("first relation property should NOT be resolved")
		})

		g.It("Should not find outgoing relations if the datasets owning these relations are disallowed", func() {
			//constraint on "companies". we cannot access outgoing refs in the "people" or "history" datasets, therefore we should not find anything
			result, _ := store.GetManyRelatedEntities([]string{peopleNamespacePrefix + ":person-1"},
				"http://data.mimiro.io/people/worksfor", false, []string{COMPANIES})
			g.Assert(len(result)).Eql(0, "expected 0 relation to be found (people and history are excluded)")
		})

		g.It("Should treat a list of (all) unknown datasets like an empty scope list", func() {
			//constraint on bogus dataset. due to implementation, this dataset filter will be ignored, query behaves as unrestricted
			//TODO: this can be discussed. Producing an error would make Queries less unpredictable. But ignoring invalid input makes the API more approachable
			result, _ := store.GetManyRelatedEntities([]string{peopleNamespacePrefix + ":person-1"},
				"http://data.mimiro.io/people/worksfor", false, []string{"bogus"})
			g.Assert(len(result)).Eql(2, "expected 2 relations")

			entity := findEntity(result, "ns4:company-1")
			g.Assert(entity).IsNotNil()
			g.Assert(entity.ID).Eql("ns4:company-1", "first relation ID should be found")
			g.Assert(entity.Properties["ns4:Name"]).Eql("Company 1", "first relation property should be resolved")
		})

		g.It("Should find relations in secondary datasets if main dataset is disallowed", func() {
			//constraint on history dataset. we should find an outgoing relation from history to a company,
			//but company resolving should be disabled since we lack access to the companies dataset
			result, _ := store.GetManyRelatedEntities([]string{peopleNamespacePrefix + ":person-1"},
				"http://data.mimiro.io/people/worksfor", false, []string{HISTORY})
			g.Assert(len(result)).Eql(1, "expected 1 relation to be found in people dataset (not in workHistory)")

			entity := findEntity(result, "ns4:company-1")
			g.Assert(entity).IsNotNil()
			g.Assert(entity.ID).Eql("ns4:company-1", "first relation ID should be found")
			g.Assert(entity.Properties["ns4:Name"]).IsNil("first relation property should NOT be resolved")

		})

		g.It("Should return all incoming relations of an entity in inverse query, when no scope is given", func() {
			//inverse query for "company-1", no datasets restriction. should find 3 people with resolved entities
			result, _ := store.GetManyRelatedEntities([]string{companyNamespacePrefix + ":company-1"},
				"http://data.mimiro.io/people/worksfor", true, []string{})
			g.Assert(len(result)).Eql(6, "expected 6 relation to be found 3 owned by people and 3 owned by history")
			entity := findEntity(result, "ns3:person-5")
			g.Assert(entity).IsNotNil()
			g.Assert(entity.Properties["ns3:Name"]).Eql("Person 5", "first relation property should be resolved")
			g.Assert(len(entity.References)).Eql(4)
			//verify that history entity has been merged in by checking for "workedfor"
			g.Assert(entity.References["ns3:workedfor"].([]interface{})[0]).Eql("ns4:company-3",
				"inverse query should find company-3 in history following 'worksfor' to person-5's employment enity")
			//verify that worksfor is duplicated - since the relation is merged together from people and history
			g.Assert(entity.References["ns3:worksfor"]).Eql([]interface{}{"ns4:company-1", "ns4:company-1"})
		})

		g.It("Should omit disallowed datasets when resolving found entities", func() {
			//inverse query for "company-1" with only "people" accessible.
			//should still find 3 people (incoming relation is owned by people dataset, which we have access to)
			//people should be resolved
			//but resolved relations in people should only be "people" data with no "history" refs merged in
			result, _ := store.GetManyRelatedEntities([]string{companyNamespacePrefix + ":company-1"},
				"http://data.mimiro.io/people/worksfor", true, []string{PEOPLE})
			g.Assert(len(result)).Eql(3, "expected 3 relations to be found in people dataset (not in workHistory)")

			entity := findEntity(result, "ns3:person-5")
			g.Assert(entity).IsNotNil()
			g.Assert(entity.ID).Eql("ns3:person-5", "first relation ID should be found")
			g.Assert(entity.Properties["ns3:Name"]).Eql("Person 5", "first relation property should be resolved")
			//make sure we only have two refs returned (worksfor + type), confirming that history refs have not been accessed
			g.Assert(len(entity.References)).Eql(2)
		})

		g.It("Should not return results if the relation-owning datasets are disallowed", func() {
			//inverse query for "company-1" with restriction to "companies" dataset.
			//all incoming relations are stored in either history or people dataset.
			//with access limited to companies dataset, we should not find incoming relations
			result, _ := store.GetManyRelatedEntities([]string{companyNamespacePrefix + ":company-1"},
				"http://data.mimiro.io/people/worksfor", true, []string{COMPANIES})
			g.Assert(len(result)).Eql(0)
		})

		g.It("Should resolve all elements in relation arrays, when no scope restriction is given", func() {
			//find person-4 and follow its workedfor relations without restriction
			//should find 2 companies in "history" dataset, and fully resolve the company entities
			result, _ := store.GetManyRelatedEntities([]string{peopleNamespacePrefix + ":person-4"},
				"http://data.mimiro.io/people/workedfor", false, []string{})
			g.Assert(len(result)).Eql(2, "expected 2 relations to be found in history dataset")

			entity := findEntity(result, "ns4:company-2")
			g.Assert(entity).IsNotNil()
			g.Assert(entity.ID).Eql("ns4:company-2", "first relation ID should be found")
			g.Assert(entity.Properties["ns4:Name"]).Eql("Company 2", "first relation property should be resolved")
		})

		g.It("Should find, but not resolve all relations in an array if relation dataset is disallowed", func() {
			//find person-4 and follow its workedfor relations in dataset "history"
			//should find 2 companies in "history" dataset,
			//but not fully resolve the company entities because we lack access to "company"
			result, _ := store.GetManyRelatedEntities([]string{peopleNamespacePrefix + ":person-4"},
				"http://data.mimiro.io/people/workedfor", false, []string{HISTORY})
			g.Assert(len(result)).Eql(2, "expected 2 relations to be found in history dataset")

			entity := findEntity(result, "ns4:company-2")
			g.Assert(entity).IsNotNil()
			g.Assert(entity.ID).Eql("ns4:company-2", "first relation ID should be found")
			g.Assert(entity.Properties["ns4:Name"]).IsNil("companies dataset is not accessible, therefor name should be nil")
		})

		g.It("Should inversely find all relations in an array", func() {
			//find company-2 and inversely follow workedfor relations without restriction
			//should find person 3 and 4 in "history" dataset, and fully resolve the person entities
			result, _ := store.GetManyRelatedEntities([]string{companyNamespacePrefix + ":company-2"},
				"http://data.mimiro.io/people/workedfor", true, []string{})
			g.Assert(len(result)).Eql(2, "expected 2 people to be found from history dataset")

			entity := findEntity(result, "ns3:person-4")
			g.Assert(entity).IsNotNil()
			g.Assert(entity.ID).Eql("ns3:person-4", "first relation ID should be found")
			g.Assert(entity.Properties["ns3:Name"]).Eql("Person 4", "first relation property should be resolved")
			//There should be 4 references both from history and people
			g.Assert(len(entity.References)).Eql(4, "Expected 4 refs in inversly found person from people and history datasets")
			//check that reference values are merged from all datasets. type should be list of type refs from both people and history
			if reflect.DeepEqual(entity.References["ns2:type"], []string{"ns5:Person", "ns5:Employment"}) {
				g.Failf("Expected Person+Employment as type ref values, but found %v", entity.References["ns2:type"])
			}
		})

		g.It("Should inversely find all relations in array, but not resolve the relation-intities if no access", func() {
			//find company-2 and inversely follow workedfor relations with filter on history
			//should find person 3 and 4 in "history" dataset, but not fully resolve the person entities
			result, _ := store.GetManyRelatedEntities([]string{companyNamespacePrefix + ":company-2"},
				"http://data.mimiro.io/people/workedfor", true, []string{HISTORY})
			g.Assert(len(result)).Eql(2, "expected 2 people to be found from history dataset")

			entity := findEntity(result, "ns3:person-4")
			g.Assert(entity).IsNotNil()
			g.Assert(entity.ID).Eql("ns3:person-4", "first relation ID should be found")
			g.Assert(entity.Properties["ns3:Name"]).IsNil("Person 4 should not be resolved since access to people is missing")
			//There should be 4 references both from history and people
			g.Assert(len(entity.References)).Eql(4, "Expected 4 refs in inversly found person from history dataset")
			//check that reference values are merged from all datasets. type should be list of type refs from both people and history
			g.Assert(entity.References["ns2:type"]).Eql("ns5:Employment", "Expected only Employment type, not Person type")
		})

		g.It("Should not inversly find array relations if relation-owning dataset is disallowed", func() {
			//find company-2 and inversely follow workedfor relations with filter on people and companies
			//should not find any relations since history access is not allowed, and workedfor is only stored there
			result, _ := store.GetManyRelatedEntities([]string{companyNamespacePrefix + ":company-2"},
				"http://data.mimiro.io/people/workedfor", true, []string{PEOPLE, COMPANIES})
			g.Assert(len(result)).Eql(0)
		})

		g.It("Should respect dataset scope in GetEntity (single lookup query)", func() {
			// namespaces
			employeeNamespacePrefix, _ := store.NamespaceManager.AssertPrefixMappingForExpansion("http://data.mimiro.io/employee/")
			modelNamespacePrefix, _ := store.NamespaceManager.AssertPrefixMappingForExpansion("http://data.mimiro.io/model/")
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
					}}),
				NewEntityFromMap(map[string]interface{}{
					"id":    peopleNamespacePrefix + ":person-2",
					"props": map[string]interface{}{peopleNamespacePrefix + ":Name": "Bob"},
					"refs": map[string]interface{}{
						rdfNamespacePrefix + ":type":     modelNamespacePrefix + ":Person",
						peopleNamespacePrefix + ":knows": peopleNamespacePrefix + "person-1",
					}}),
			})

			_ = employeeDataset.StoreEntities([]*Entity{
				NewEntityFromMap(map[string]interface{}{
					"id":    peopleNamespacePrefix + ":person-1",
					"props": map[string]interface{}{employeeNamespacePrefix + ":Title": "President"},
					"refs": map[string]interface{}{
						rdfNamespacePrefix + ":type": modelNamespacePrefix + ":Employee",
					}}),
				NewEntityFromMap(map[string]interface{}{
					"id":    peopleNamespacePrefix + ":person-2",
					"props": map[string]interface{}{employeeNamespacePrefix + ":Title": "Vice President"},
					"refs": map[string]interface{}{
						rdfNamespacePrefix + ":type":            modelNamespacePrefix + ":Employee",
						employeeNamespacePrefix + ":Supervisor": peopleNamespacePrefix + ":person-1",
					}})})

			//first no datasets restriction, check that props from both employee and people dataset are merged
			result, err := store.GetEntity(peopleNamespacePrefix+":person-1", []string{})
			g.Assert(err).IsNil()
			g.Assert(result.Properties[employeeNamespacePrefix+":Title"]).Eql("President",
				"expected to merge property employees:Title into result with value 'President'")
			g.Assert(result.Properties[peopleNamespacePrefix+":Name"]).Eql("Lisa")

			//now, restrict on dataset "people"
			result, _ = store.GetEntity(peopleNamespacePrefix+":person-2", []string{PEOPLE})
			//we should not find the Title from the employees dataset
			g.Assert(result.Properties[employeeNamespacePrefix+":Title"]).IsNil()
			//people:Name should be found though
			g.Assert(result.Properties[peopleNamespacePrefix+":Name"]).Eql("Bob")

			//now, restrict on dataset "employees". now Name should be gone but title should be found
			result, _ = store.GetEntity(peopleNamespacePrefix+":person-2", []string{EMPLOYEES})
			g.Assert(result.Properties[employeeNamespacePrefix+":Title"]).Eql("Vice President",
				"expected to merge property employees:Title into result with value 'Vice President'")
			g.Assert(result.Properties[peopleNamespacePrefix+":Name"]).IsNil()
		})

		g.It("Should perform txn updates without error", func() {
			// create dataset
			_, err := dsm.CreateDataset("people", nil)
			g.Assert(err).IsNil()

			_, err = dsm.CreateDataset("places", nil)
			g.Assert(err).IsNil()

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

			err = store.ExecuteTransaction(txn)
			g.Assert(err).IsNil()

			people := dsm.GetDataset("people")
			peopleEntities, _ := people.GetEntities("", 100)
			g.Assert(len(peopleEntities.Entities)).Eql(6)

		})

		g.It("should find deleted version of entity with lookup", func() {
			peopleNamespacePrefix, err := store.NamespaceManager.AssertPrefixMappingForExpansion("http://data.mimiro.io/people/")

			// create dataset
			ds, err := dsm.CreateDataset("people", nil)
			g.Assert(err).IsNil()

			entities := make([]*Entity, 1)
			entity := NewEntity(peopleNamespacePrefix+":p1", 0)
			entity.Properties["Name"] = "homer"
			entity.References["rdf:type"] = "http://data.mimiro.io/model/Person"
			entity.References["rdf:f1"] = "http://data.mimiro.io/people/Person-1"
			entity.References["rdf:f2"] = "http://data.mimiro.io/people/Person-2"
			entity.References["rdf:f3"] = "http://data.mimiro.io/people/Person-3"
			entities[0] = entity

			err = ds.StoreEntities(entities)
			g.Assert(err).IsNil("StoreEntities failed unexpectedly")

			// get entity
			e, err := store.GetEntity(peopleNamespacePrefix+":p1", nil)
			g.Assert(err).IsNil("GetEntity failed unexpectedly")
			g.Assert(e.IsDeleted).IsFalse()

			entities = make([]*Entity, 1)
			entity = NewEntity(peopleNamespacePrefix+":p1", 0)
			entity.IsDeleted = true
			entities[0] = entity

			err = ds.StoreEntities(entities)
			g.Assert(err).IsNil("StoreEntities failed unexpectedly")

			e, _ = store.GetEntity("http://data.mimiro.io/people/p1", nil)
			g.Assert(e.IsDeleted).IsTrue()
		})
	})
}

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
			//os.Stdout.Write([]byte(fmt.Sprintf("%v\n",it.Item())))
		}

		it.Close()
		return nil
	})
	//os.Stdout.Write([]byte(fmt.Sprintf("%v\n\n\n",items)))
	return items
}
