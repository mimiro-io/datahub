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
	"github.com/dgraph-io/badger/v2"
	"os"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/franela/goblin"

	"github.com/DataDog/datadog-go/statsd"
	"github.com/mimiro-io/datahub/internal/conf"
	"go.uber.org/fx/fxtest"

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
		g.It("Should delete the correct incoming and outgoing references", func() {
			g.Timeout(time.Hour)
			b := store.database
			peopleNamespacePrefix, _ := store.NamespaceManager.AssertPrefixMappingForExpansion("http://data.mimiro.io/people/")
			workPrefix, _ := store.NamespaceManager.AssertPrefixMappingForExpansion("http://data.mimiro.io/work/")
			g.Assert(count(b)).Eql(16)

			friendsDS, _ := dsm.CreateDataset("friends")
			g.Assert(count(b)).Eql(24)

			workDS, _ := dsm.CreateDataset("work")
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

		g.It("Should build query results", func() {
			g.Timeout(time.Hour)

			// create dataset
			ds, err := dsm.CreateDataset("people")

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
		g.It("Should respect dataset scope in queries", func() {
			g.Timeout(time.Hour)

			// namespaces
			peopleNamespacePrefix, _ := store.NamespaceManager.AssertPrefixMappingForExpansion("http://data.mimiro.io/people/")
			companyNamespacePrefix, _ := store.NamespaceManager.AssertPrefixMappingForExpansion("http://data.mimiro.io/company/")
			modelNamespacePrefix, _ := store.NamespaceManager.AssertPrefixMappingForExpansion("http://data.mimiro.io/model/")
			rdfNamespacePrefix, _ := store.NamespaceManager.AssertPrefixMappingForExpansion(RdfNamespaceExpansion)

			type TestModel struct {
				person      int   //id of a person
				workhistory []int //list of company-ids
				currentwork int   //id of a company
			}

			testdata := []TestModel{
				{person: 1, workhistory: []int{}, currentwork: 1},
				{person: 2, workhistory: []int{1}, currentwork: 2},
				{person: 3, workhistory: []int{2}, currentwork: 1},
				{person: 4, workhistory: []int{1, 2}, currentwork: 3},
				{person: 5, workhistory: []int{3}, currentwork: 1},
			}

			type TestCase struct {
				CaseID                     int
				startURI                   string
				predicate                  string
				inverse                    bool
				datasets                   []string
				expectedRelationCount      int
				firstRelationID            string
				firstRelationPropertyName  string
				firstRelationPropertyValue interface{}
				firstRelationPropertyCheck interface{} // nil, true or false
				extraCheck                 func(e *Entity, g *goblin.G)
			}

			const PEOPLE = "people"
			const COMPANIES = "companies"
			const HISTORY = "history"
			testMatrix := []TestCase{
				//no constraints, resolving company should work
				{CaseID: 1, startURI: peopleNamespacePrefix + ":person-1", predicate: "http://data.mimiro.io/people/worksfor",
					inverse: false, datasets: []string{},
					expectedRelationCount: 2, firstRelationID: "ns4:company-1", firstRelationPropertyName: "ns4:Name", firstRelationPropertyValue: "Company 1"},

				//constraint on "people" only. we should find the relation to a company in "people",
				//but resolving the company should be omitted because we lack access to the "companies" dataset
				{CaseID: 2, startURI: peopleNamespacePrefix + ":person-1", predicate: "http://data.mimiro.io/people/worksfor",
					inverse: false, datasets: []string{PEOPLE},
					expectedRelationCount: 1, firstRelationID: "ns4:company-1", firstRelationPropertyName: "ns4:Name", firstRelationPropertyValue: nil},

				//constraint on "companies". we cannot access outgoing refs in the "people" or "history" datasets, therefore we should not find anything
				{CaseID: 3, startURI: peopleNamespacePrefix + ":person-1", predicate: "http://data.mimiro.io/people/worksfor",
					inverse: false, datasets: []string{COMPANIES},
					expectedRelationCount: 0},

				//constraint on bogus dataset. due to implementation, this dataset filter will be ignored, query behaves as unrestricted
				//TODO: this can be discussed. Producing an error would make Queries less unpredictable. But ignoring invalid input makes the API more approachable
				{CaseID: 4, startURI: peopleNamespacePrefix + ":person-1", predicate: "http://data.mimiro.io/people/worksfor",
					inverse: false, datasets: []string{"bogus"},
					expectedRelationCount: 2, firstRelationID: "ns4:company-1", firstRelationPropertyName: "ns4:Name", firstRelationPropertyValue: "Company 1"},

				//constraint on history dataset. we should find an outgoing relatoin from history to a company,
				//but company resolving should be disabled since we lack access to the companies dataset
				{CaseID: 5, startURI: peopleNamespacePrefix + ":person-1", predicate: "http://data.mimiro.io/people/worksfor",
					inverse: false, datasets: []string{HISTORY},
					expectedRelationCount: 1, firstRelationID: "ns4:company-1", firstRelationPropertyName: "ns4:Name", firstRelationPropertyValue: nil},

				//inverse query for "company-1", no datasets restriction. should find 3 people with resolved entities
				{CaseID: 6, startURI: companyNamespacePrefix + ":company-1", predicate: "http://data.mimiro.io/people/worksfor",
					inverse: true, datasets: []string{},
					expectedRelationCount: 6, firstRelationID: "ns3:person-5", firstRelationPropertyName: "ns3:Name", firstRelationPropertyValue: "Person 5",
					extraCheck: func(e *Entity, g *goblin.G) {
						//we should find 4 refs for the first resolved person, from both history and people datasets
						g.Assert(len(e.References)).Eql(4)
						//verify that history entity has been merged in by checking for "workedfor"
						g.Assert(e.References["ns3:workedfor"].([]interface{})[0]).Eql("ns4:company-3", "inverse query should find company-3 in history following 'worksfor' to person-5's employment enity")
					},
				},

				//inverse query for "company-1" with only "people" accessible.
				//should still find 3 people (incoming relation is owned by people dataset, which we have access to)
				//people should be resolved
				//but resolved relations in people should only be "people" data with no "history" refs merged in
				{CaseID: 7, startURI: companyNamespacePrefix + ":company-1", predicate: "http://data.mimiro.io/people/worksfor",
					inverse: true, datasets: []string{PEOPLE},
					expectedRelationCount: 3, firstRelationID: "ns3:person-5", firstRelationPropertyName: "ns3:Name", firstRelationPropertyCheck: true,
					extraCheck: func(e *Entity, g *goblin.G) {
						//make sure we only have two refs returned (worksfor + type), confirming that history refs have not been accessed
						g.Assert(len(e.References)).Eql(2)
					},
				},

				//inverse query for "company-1" with restriction to "companies" dataset.
				//all incoming relations are stored in either history or people dataset.
				//with access limited to companies dataset, we should not find incoming relations
				{CaseID: 8, startURI: companyNamespacePrefix + ":company-1", predicate: "http://data.mimiro.io/people/worksfor",
					inverse: true, datasets: []string{COMPANIES}, expectedRelationCount: 0,
				},

				//find person-4 and follow its workedfor relations without restriction
				//should find 2 companies in "history" dataset, and fully resolve the company entities
				{CaseID: 9, startURI: peopleNamespacePrefix + ":person-4", predicate: "http://data.mimiro.io/people/workedfor",
					inverse: false, datasets: []string{}, expectedRelationCount: 2, firstRelationID: "ns4:company-2",
					firstRelationPropertyName: "ns4:Name", firstRelationPropertyValue: "Company 2",
				},

				//find person-4 and follow its workedfor relations in dataset "history"
				//should find 2 companies in "history" dataset,
				//but not fully resolve the company entities because we lack access to "company"
				{CaseID: 10, startURI: peopleNamespacePrefix + ":person-4", predicate: "http://data.mimiro.io/people/workedfor",
					inverse: false, datasets: []string{HISTORY}, expectedRelationCount: 2, firstRelationID: "ns4:company-2",
					firstRelationPropertyName: "ns4:Name", firstRelationPropertyValue: nil,
				},

				//find company-2 and inversely follow workedfor relations without restriction
				//should find person 3 and 4 in "history" dataset, and fully resolve the person entities
				{CaseID: 11, startURI: companyNamespacePrefix + ":company-2", predicate: "http://data.mimiro.io/people/workedfor",
					inverse: true, datasets: []string{}, expectedRelationCount: 2, firstRelationID: "ns3:person-4",
					firstRelationPropertyName: "ns3:Name", firstRelationPropertyValue: "Person 4",
					extraCheck: func(e *Entity, g *goblin.G) {
						//There should be 4 references both from history and people
						if len(e.References) != 4 {
							g.Failf("Expected %v refs in inversely found person from both people and history datasets, actual: %v", 2, len(e.References))
						}
						//check that reference values are merged from all datasets. type should be list of type refs from both people and history
						if reflect.DeepEqual(e.References["ns2:type"], []string{"ns5:Person", "ns5:Employment"}) {
							g.Failf("Expected Person+Employment as type ref values, but found %v", e.References["ns2:type"])
						}
					},
				},

				//find company-2 and inversely follow workedfor relations with filter on history
				//should find person 3 and 4 in "history" dataset, but not fully resolve the person entities
				{CaseID: 12, startURI: companyNamespacePrefix + ":company-2", predicate: "http://data.mimiro.io/people/workedfor",
					inverse: true, datasets: []string{HISTORY}, expectedRelationCount: 2, firstRelationID: "ns3:person-4",
					firstRelationPropertyName: "ns3:Name", firstRelationPropertyValue: nil,
					extraCheck: func(e *Entity, g *goblin.G) {
						//there should be 4 refs from the history entity
						if len(e.References) != 4 {
							g.Failf("Case 12 :: Expected %v refs in inversely found person from history dataset only, actual: %v", 4, len(e.References))
						}
						//but we should only find type=Employment now. not Person
						if e.References["ns2:type"] != "ns5:Employment" {
							g.Failf("Expected ns5:Employment as type ref value, but found %v", e.References["ns2:type"])
						}
					},
				},

				//find company-2 and inversely follow workedfor relations with filter on people and companies
				//should not find any relations since history access is not allowed, and workedfor is only stored there
				{CaseID: 13, startURI: companyNamespacePrefix + ":company-2", predicate: "http://data.mimiro.io/people/workedfor",
					inverse: true, datasets: []string{PEOPLE, COMPANIES}, expectedRelationCount: 0,
				},
			}

			// create dataset
			peopleDataset, _ := dsm.CreateDataset(PEOPLE)
			companiesDataset, _ := dsm.CreateDataset(COMPANIES)
			employmentHistoryDataset, _ := dsm.CreateDataset(HISTORY)

			for _, td := range testdata {
				cid := companyNamespacePrefix + ":company-" + strconv.Itoa(td.currentwork)
				_ = companiesDataset.StoreEntities([]*Entity{NewEntityFromMap(map[string]interface{}{
					"id":    cid,
					"props": map[string]interface{}{companyNamespacePrefix + ":Name": "Company " + strconv.Itoa(td.currentwork)},
					"refs":  map[string]interface{}{rdfNamespacePrefix + ":type": modelNamespacePrefix + ":Company"}})})

				pid := peopleNamespacePrefix + ":person-" + strconv.Itoa(td.person)
				_ = peopleDataset.StoreEntities([]*Entity{NewEntityFromMap(map[string]interface{}{
					"id":    pid,
					"props": map[string]interface{}{peopleNamespacePrefix + ":Name": "Person " + strconv.Itoa(td.person)},
					"refs": map[string]interface{}{
						rdfNamespacePrefix + ":type":        modelNamespacePrefix + ":Person",
						peopleNamespacePrefix + ":worksfor": companyNamespacePrefix + ":company-" + strconv.Itoa(td.currentwork),
					}})})

				var workHistory []string
				for _, histId := range td.workhistory {
					workHistory = append(workHistory, companyNamespacePrefix+":company-"+strconv.Itoa(histId))
				}
				_ = employmentHistoryDataset.StoreEntities([]*Entity{NewEntityFromMap(map[string]interface{}{
					"id":    pid,
					"props": map[string]interface{}{},
					"refs": map[string]interface{}{
						rdfNamespacePrefix + ":type":         modelNamespacePrefix + ":Employment",
						peopleNamespacePrefix + ":Employee":  pid,
						peopleNamespacePrefix + ":worksfor":  companyNamespacePrefix + ":company-" + strconv.Itoa(td.currentwork),
						peopleNamespacePrefix + ":workedfor": workHistory,
					}})})
			}

			for _, tc := range testMatrix {
				result, _ := store.GetManyRelatedEntities([]string{tc.startURI}, tc.predicate, tc.inverse, tc.datasets)
				g.Assert(len(result)).Eql(tc.expectedRelationCount, "Case "+strconv.Itoa(tc.CaseID))
				if tc.expectedRelationCount == 0 {
					continue
				}
				/*
					for _, r := range result {
						e := r[2].(*Entity)
						t.Logf("%v",e)
						t.Logf("%v :: %v, props: %v, refs: %v", tc.CaseID, e.ID, e.Properties, e.References)
					}
				*/
				entity := result[0][2].(*Entity)
				g.Assert(entity.ID).Eql(tc.firstRelationID, fmt.Sprintf("Case %v :: startURI was %v; dataset constraints were %v", tc.CaseID, tc.startURI, tc.datasets))
				observedPropVal := entity.Properties[tc.firstRelationPropertyName]
				if tc.firstRelationPropertyCheck == nil {
					g.Assert(observedPropVal).Eql(tc.firstRelationPropertyValue,
						fmt.Sprintf("Case %v :: should resolve Relation property %v as %v, actually observed value: %v",
							tc.CaseID, tc.firstRelationPropertyName, tc.firstRelationPropertyValue, observedPropVal))
				}
				if tc.firstRelationPropertyCheck != nil && (observedPropVal == nil) == tc.firstRelationPropertyCheck {
					g.Failf("Case %v :: should resolve Relation property %v : %v, actually observed value: %v",
						tc.CaseID, tc.firstRelationPropertyName, tc.firstRelationPropertyCheck, observedPropVal)
				}
				if tc.extraCheck != nil {
					tc.extraCheck(entity, g)
				}
			}
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
			ds, _ := dsm.CreateDataset("people")

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
			ds, _ := dsm.CreateDataset("people")

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
			ds, _ := dsm.CreateDataset("people")

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
			_, _ = dsm.CreateDataset("companies")
			_, _ = dsm.CreateDataset("animals")
			ds, err := dsm.CreateDataset("people")
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
			ds, _ = dsm.CreateDataset("people")

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
			changes, err := ds.GetChanges(0, 10)
			g.Assert(err).IsNil()
			g.Assert(len(changes.Entities)).Eql(3)
			g.Assert(changes.NextToken).Eql(uint64(3))

			changes, err = ds.GetChanges(1, 1)
			g.Assert(err).IsNil()
			g.Assert(len(changes.Entities)).Eql(1)
			g.Assert(changes.NextToken).Eql(uint64(2))

			changes, err = ds.GetChanges(2, 1)
			g.Assert(err).IsNil()
			g.Assert(len(changes.Entities)).Eql(1)
			g.Assert(changes.NextToken).Eql(uint64(3))

			changes, _ = ds.GetChanges(3, 10)
			g.Assert(len(changes.Entities)).Eql(0)
			g.Assert(changes.NextToken).Eql(uint64(3))
		})

		g.It("Should paginate entities with continuation tokens", func() {
			// create dataset
			c, _ := dsm.CreateDataset("companies")
			_, _ = dsm.CreateDataset("animals")
			ds, _ := dsm.CreateDataset("people")

			_ = dsm.DeleteDataset("people")
			ds, _ = dsm.CreateDataset("people")

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
			ds1, err := dsm.CreateDataset("people0")
			ds2, err := dsm.CreateDataset("people1")
			ds3, err := dsm.CreateDataset("people2")
			ds4, err := dsm.CreateDataset("people3")
			ds5, err := dsm.CreateDataset("people4")

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
			ds, err := dsm.CreateDataset("people")
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
			ds, err := dsm.CreateDataset("people")
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
			ds, err := dsm.CreateDataset("people")

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
			ds, err := dsm.CreateDataset("people")
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
			ds, _ := dsm.CreateDataset("people")

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
			peopleDataset, _ := dsm.CreateDataset("people")
			companiesDataset, _ := dsm.CreateDataset("companies")

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
			peopleDataset, _ := dsm.CreateDataset("people")
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

		g.It("Should respect dataset scope in queries", func() {
			// namespaces
			peopleNamespacePrefix, _ := store.NamespaceManager.AssertPrefixMappingForExpansion("http://data.mimiro.io/people/")
			companyNamespacePrefix, _ := store.NamespaceManager.AssertPrefixMappingForExpansion("http://data.mimiro.io/company/")
			modelNamespacePrefix, _ := store.NamespaceManager.AssertPrefixMappingForExpansion("http://data.mimiro.io/model/")
			rdfNamespacePrefix, _ := store.NamespaceManager.AssertPrefixMappingForExpansion(RdfNamespaceExpansion)

			type TestModel struct {
				person      int   //id of a person
				workhistory []int //list of company-ids
				currentwork int   //id of a company
			}

			testdata := []TestModel{
				{person: 1, workhistory: []int{}, currentwork: 1},
				{person: 2, workhistory: []int{1}, currentwork: 2},
				{person: 3, workhistory: []int{2}, currentwork: 1},
				{person: 4, workhistory: []int{1, 2}, currentwork: 3},
				{person: 5, workhistory: []int{3}, currentwork: 1},
			}

			type TestCase struct {
				CaseID                     int
				startURI                   string
				predicate                  string
				inverse                    bool
				datasets                   []string
				expectedRelationCount      int
				firstRelationID            string
				firstRelationPropertyName  string
				firstRelationPropertyValue interface{}
				firstRelationPropertyCheck interface{} // nil, true or false
				extraCheck                 func(e *Entity, g *goblin.G)
			}

			const PEOPLE = "people"
			const COMPANIES = "companies"
			const HISTORY = "history"
			testMatrix := []TestCase{
				//no constraints, resolving company should work
				{CaseID: 1, startURI: peopleNamespacePrefix + ":person-1", predicate: "http://data.mimiro.io/people/worksfor",
					inverse: false, datasets: []string{},
					expectedRelationCount: 2, firstRelationID: "ns4:company-1", firstRelationPropertyName: "ns4:Name", firstRelationPropertyValue: "Company 1"},

				//constraint on "people" only. we should find the relation to a company in "people",
				//but resolving the company should be omitted because we lack access to the "companies" dataset
				{CaseID: 2, startURI: peopleNamespacePrefix + ":person-1", predicate: "http://data.mimiro.io/people/worksfor",
					inverse: false, datasets: []string{PEOPLE},
					expectedRelationCount: 1, firstRelationID: "ns4:company-1", firstRelationPropertyName: "ns4:Name", firstRelationPropertyValue: nil},

				//constraint on "companies". we cannot access outgoing refs in the "people" or "history" datasets, therefore we should not find anything
				{CaseID: 3, startURI: peopleNamespacePrefix + ":person-1", predicate: "http://data.mimiro.io/people/worksfor",
					inverse: false, datasets: []string{COMPANIES},
					expectedRelationCount: 0},

				//constraint on bogus dataset. due to implementation, this dataset filter will be ignored, query behaves as unrestricted
				//TODO: this can be discussed. Producing an error would make Queries less unpredictable. But ignoring invalid input makes the API more approachable
				{CaseID: 4, startURI: peopleNamespacePrefix + ":person-1", predicate: "http://data.mimiro.io/people/worksfor",
					inverse: false, datasets: []string{"bogus"},
					expectedRelationCount: 2, firstRelationID: "ns4:company-1", firstRelationPropertyName: "ns4:Name", firstRelationPropertyValue: "Company 1"},

				//constraint on history dataset. we should find an outgoing relatoin from history to a company,
				//but company resolving should be disabled since we lack access to the companies dataset
				{CaseID: 5, startURI: peopleNamespacePrefix + ":person-1", predicate: "http://data.mimiro.io/people/worksfor",
					inverse: false, datasets: []string{HISTORY},
					expectedRelationCount: 1, firstRelationID: "ns4:company-1", firstRelationPropertyName: "ns4:Name", firstRelationPropertyValue: nil},

				//inverse query for "company-1", no datasets restriction. should find 3 people with resolved entities
				{CaseID: 6, startURI: companyNamespacePrefix + ":company-1", predicate: "http://data.mimiro.io/people/worksfor",
					inverse: true, datasets: []string{},
					expectedRelationCount: 6, firstRelationID: "ns3:person-5", firstRelationPropertyName: "ns3:Name", firstRelationPropertyValue: "Person 5",
					extraCheck: func(e *Entity, g *goblin.G) {
						//we should find 4 refs for the first resolved person, from both history and people datasets
						g.Assert(len(e.References)).Eql(4)
						//verify that history entity has been merged in by checking for "workedfor"
						g.Assert(e.References["ns3:workedfor"].([]interface{})[0]).Eql("ns4:company-3", "inverse query should find company-3 in history following 'worksfor' to person-5's employment enity")
					},
				},

				//inverse query for "company-1" with only "people" accessible.
				//should still find 3 people (incoming relation is owned by people dataset, which we have access to)
				//people should be resolved
				//but resolved relations in people should only be "people" data with no "history" refs merged in
				{CaseID: 7, startURI: companyNamespacePrefix + ":company-1", predicate: "http://data.mimiro.io/people/worksfor",
					inverse: true, datasets: []string{PEOPLE},
					expectedRelationCount: 3, firstRelationID: "ns3:person-5", firstRelationPropertyName: "ns3:Name", firstRelationPropertyCheck: true,
					extraCheck: func(e *Entity, g *goblin.G) {
						//make sure we only have two refs returned (worksfor + type), confirming that history refs have not been accessed
						g.Assert(len(e.References)).Eql(2)
					},
				},

				//inverse query for "company-1" with restriction to "companies" dataset.
				//all incoming relations are stored in either history or people dataset.
				//with access limited to companies dataset, we should not find incoming relations
				{CaseID: 8, startURI: companyNamespacePrefix + ":company-1", predicate: "http://data.mimiro.io/people/worksfor",
					inverse: true, datasets: []string{COMPANIES}, expectedRelationCount: 0,
				},

				//find person-4 and follow its workedfor relations without restriction
				//should find 2 companies in "history" dataset, and fully resolve the company entities
				{CaseID: 9, startURI: peopleNamespacePrefix + ":person-4", predicate: "http://data.mimiro.io/people/workedfor",
					inverse: false, datasets: []string{}, expectedRelationCount: 2, firstRelationID: "ns4:company-2",
					firstRelationPropertyName: "ns4:Name", firstRelationPropertyValue: "Company 2",
				},

				//find person-4 and follow its workedfor relations in dataset "history"
				//should find 2 companies in "history" dataset,
				//but not fully resolve the company entities because we lack access to "company"
				{CaseID: 10, startURI: peopleNamespacePrefix + ":person-4", predicate: "http://data.mimiro.io/people/workedfor",
					inverse: false, datasets: []string{HISTORY}, expectedRelationCount: 2, firstRelationID: "ns4:company-2",
					firstRelationPropertyName: "ns4:Name", firstRelationPropertyValue: nil,
				},

				//find company-2 and inversely follow workedfor relations without restriction
				//should find person 3 and 4 in "history" dataset, and fully resolve the person entities
				{CaseID: 11, startURI: companyNamespacePrefix + ":company-2", predicate: "http://data.mimiro.io/people/workedfor",
					inverse: true, datasets: []string{}, expectedRelationCount: 2, firstRelationID: "ns3:person-4",
					firstRelationPropertyName: "ns3:Name", firstRelationPropertyValue: "Person 4",
					extraCheck: func(e *Entity, g *goblin.G) {
						//There should be 4 references both from history and people
						if len(e.References) != 4 {
							g.Failf("Expected %v refs in inversely found person from both people and history datasets, actual: %v", 2, len(e.References))
						}
						//check that reference values are merged from all datasets. type should be list of type refs from both people and history
						if reflect.DeepEqual(e.References["ns2:type"], []string{"ns5:Person", "ns5:Employment"}) {
							g.Failf("Expected Person+Employment as type ref values, but found %v", e.References["ns2:type"])
						}
					},
				},

				//find company-2 and inversely follow workedfor relations with filter on history
				//should find person 3 and 4 in "history" dataset, but not fully resolve the person entities
				{CaseID: 12, startURI: companyNamespacePrefix + ":company-2", predicate: "http://data.mimiro.io/people/workedfor",
					inverse: true, datasets: []string{HISTORY}, expectedRelationCount: 2, firstRelationID: "ns3:person-4",
					firstRelationPropertyName: "ns3:Name", firstRelationPropertyValue: nil,
					extraCheck: func(e *Entity, g *goblin.G) {
						//there should be 4 refs from the history entity
						if len(e.References) != 4 {
							g.Failf("Case 12 :: Expected %v refs in inversely found person from history dataset only, actual: %v", 4, len(e.References))
						}
						//but we should only find type=Employment now. not Person
						if e.References["ns2:type"] != "ns5:Employment" {
							g.Failf("Expected ns5:Employment as type ref value, but found %v", e.References["ns2:type"])
						}
					},
				},

				//find company-2 and inversely follow workedfor relations with filter on people and companies
				//should not find any relations since history access is not allowed, and workedfor is only stored there
				{CaseID: 13, startURI: companyNamespacePrefix + ":company-2", predicate: "http://data.mimiro.io/people/workedfor",
					inverse: true, datasets: []string{PEOPLE, COMPANIES}, expectedRelationCount: 0,
				},
			}

			// create dataset
			peopleDataset, _ := dsm.CreateDataset(PEOPLE)
			companiesDataset, _ := dsm.CreateDataset(COMPANIES)
			employmentHistoryDataset, _ := dsm.CreateDataset(HISTORY)

			for _, td := range testdata {
				cid := companyNamespacePrefix + ":company-" + strconv.Itoa(td.currentwork)
				_ = companiesDataset.StoreEntities([]*Entity{NewEntityFromMap(map[string]interface{}{
					"id":    cid,
					"props": map[string]interface{}{companyNamespacePrefix + ":Name": "Company " + strconv.Itoa(td.currentwork)},
					"refs":  map[string]interface{}{rdfNamespacePrefix + ":type": modelNamespacePrefix + ":Company"}})})

				pid := peopleNamespacePrefix + ":person-" + strconv.Itoa(td.person)
				_ = peopleDataset.StoreEntities([]*Entity{NewEntityFromMap(map[string]interface{}{
					"id":    pid,
					"props": map[string]interface{}{peopleNamespacePrefix + ":Name": "Person " + strconv.Itoa(td.person)},
					"refs": map[string]interface{}{
						rdfNamespacePrefix + ":type":        modelNamespacePrefix + ":Person",
						peopleNamespacePrefix + ":worksfor": companyNamespacePrefix + ":company-" + strconv.Itoa(td.currentwork),
					}})})

				var workHistory []string
				for _, histId := range td.workhistory {
					workHistory = append(workHistory, companyNamespacePrefix+":company-"+strconv.Itoa(histId))
				}
				_ = employmentHistoryDataset.StoreEntities([]*Entity{NewEntityFromMap(map[string]interface{}{
					"id":    pid,
					"props": map[string]interface{}{},
					"refs": map[string]interface{}{
						rdfNamespacePrefix + ":type":         modelNamespacePrefix + ":Employment",
						peopleNamespacePrefix + ":Employee":  pid,
						peopleNamespacePrefix + ":worksfor":  companyNamespacePrefix + ":company-" + strconv.Itoa(td.currentwork),
						peopleNamespacePrefix + ":workedfor": workHistory,
					}})})
			}

			for _, tc := range testMatrix {
				result, _ := store.GetManyRelatedEntities([]string{tc.startURI}, tc.predicate, tc.inverse, tc.datasets)
				g.Assert(len(result)).Eql(tc.expectedRelationCount, "Case "+strconv.Itoa(tc.CaseID))
				if tc.expectedRelationCount == 0 {
					continue
				}
				/*
					for _, r := range result {
						e := r[2].(*Entity)
						t.Logf("%v",e)
						t.Logf("%v :: %v, props: %v, refs: %v", tc.CaseID, e.ID, e.Properties, e.References)
					}
				*/
				entity := result[0][2].(*Entity)
				g.Assert(entity.ID).Eql(tc.firstRelationID, fmt.Sprintf("Case %v :: startURI was %v; dataset constraints were %v", tc.CaseID, tc.startURI, tc.datasets))
				observedPropVal := entity.Properties[tc.firstRelationPropertyName]
				if tc.firstRelationPropertyCheck == nil {
					g.Assert(observedPropVal).Eql(tc.firstRelationPropertyValue,
						fmt.Sprintf("Case %v :: should resolve Relation property %v as %v, actually observed value: %v",
							tc.CaseID, tc.firstRelationPropertyName, tc.firstRelationPropertyValue, observedPropVal))
				}
				if tc.firstRelationPropertyCheck != nil && (observedPropVal == nil) == tc.firstRelationPropertyCheck {
					g.Failf("Case %v :: should resolve Relation property %v : %v, actually observed value: %v",
						tc.CaseID, tc.firstRelationPropertyName, tc.firstRelationPropertyCheck, observedPropVal)
				}
				if tc.extraCheck != nil {
					tc.extraCheck(entity, g)
				}
			}
		})

		g.It("Should respect dataset scope in GetEntity (single lookup query)", func() {
			// namespaces
			peopleNamespacePrefix, err := store.NamespaceManager.AssertPrefixMappingForExpansion("http://data.mimiro.io/people/")
			employeeNamespacePrefix, err := store.NamespaceManager.AssertPrefixMappingForExpansion("http://data.mimiro.io/employee/")
			modelNamespacePrefix, err := store.NamespaceManager.AssertPrefixMappingForExpansion("http://data.mimiro.io/model/")
			rdfNamespacePrefix, err := store.NamespaceManager.AssertPrefixMappingForExpansion(RdfNamespaceExpansion)

			// create dataset
			const PEOPLE = "people"
			peopleDataset, _ := dsm.CreateDataset(PEOPLE)
			const EMPLOYEES = "employees"
			employeeDataset, _ := dsm.CreateDataset(EMPLOYEES)

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
			result, _ := store.GetEntity(peopleNamespacePrefix+":person-1", []string{})
			if result.Properties["ns4:Title"] != "President" {
				g.Errorf("expected to merge property employees:Title into result with value 'President',"+
					" but found %v", result.Properties["ns4:Title"])
			}
			if result.Properties["ns3:Name"] != "Lisa" {
				g.Errorf("expected to find property people:Name in result with value 'Lisa',"+
					" but found %v", result.Properties["ns3:Name"])
			}

			//now, restrict on dataset "people"
			result, _ = store.GetEntity(peopleNamespacePrefix+":person-2", []string{PEOPLE})
			//we should not find the Title from the employees dataset
			if result.Properties["ns4:Title"] != nil {
				g.Errorf("did not expect employees:Title to be set, but found %v", result.Properties["ns4:Title"])
			}
			//people:Name should be found though
			if result.Properties["ns3:Name"] != "Bob" {
				g.Errorf("expected to find property people:Name in result with value 'Bob',"+
					" but found %v", result.Properties["ns3:Name"])
			}

			//now, restrict on dataset "employees". now Name should be gone but title should be found
			result, _ = store.GetEntity(peopleNamespacePrefix+":person-2", []string{EMPLOYEES})
			if result.Properties["ns4:Title"] != "Vice President" {
				g.Errorf("expected to merge property employees:Title into result with value 'Vice President',"+
					" but found %v", result.Properties["ns4:Title"])
			}
			if result.Properties["ns3:Name"] != nil {
				g.Errorf("did not expect people:Name in result, but found %v", result.Properties["ns3:Name"])
			}

			g.Assert(err).IsNil()
		})

		g.It("Should delete the correct incoming and outgoing references", func() {
			g.Timeout(time.Hour)
			b := store.database
			peopleNamespacePrefix, _ := store.NamespaceManager.AssertPrefixMappingForExpansion("http://data.mimiro.io/people/")
			workPrefix, _ := store.NamespaceManager.AssertPrefixMappingForExpansion("http://data.mimiro.io/work/")
			g.Assert(count(b)).Eql(16)

			friendsDS, _ := dsm.CreateDataset("friends")
			g.Assert(count(b)).Eql(24)

			workDS, _ := dsm.CreateDataset("work")
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
	})
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
