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

package source_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/DataDog/datadog-go/v5/statsd"
	"github.com/franela/goblin"
	"go.uber.org/fx/fxtest"
	"go.uber.org/zap"

	"github.com/mimiro-io/datahub/internal"
	"github.com/mimiro-io/datahub/internal/conf"
	"github.com/mimiro-io/datahub/internal/jobs/source"
	"github.com/mimiro-io/datahub/internal/server"
)

func TestMultiSource(t *testing.T) {
	g := goblin.Goblin(t)
	g.Describe("dependency tracking", func() {
		testCnt := 0
		var dsm *server.DsManager
		var store *server.Store
		var storeLocation string
		g.BeforeEach(func() {
			testCnt += 1
			storeLocation = fmt.Sprintf("./test_multi_source_%v", testCnt)
			err := os.RemoveAll(storeLocation)
			g.Assert(err).IsNil("should be allowed to clean testfiles in " + storeLocation)

			e := &conf.Env{
				Logger:        zap.NewNop().Sugar(),
				StoreLocation: storeLocation,
			}
			lc := fxtest.NewLifecycle(internal.FxTestLog(t, false))

			store = server.NewStore(lc, e, &statsd.NoOpClient{})
			dsm = server.NewDsManager(lc, e, store, server.NoOpBus())

			err = lc.Start(context.Background())
			if err != nil {
				fmt.Println(err.Error())
				t.FailNow()
			}
		})
		g.AfterEach(func() {
			_ = store.Close()
			_ = os.RemoveAll(storeLocation)
		})

		g.It("should emit changes in main dataset", func() {
			people, peoplePrefix := createTestDataset("people", []string{"Bob", "Alice"}, nil, dsm, g, store)
			// add one change to each entity -> 4 changes
			addChanges("people", []string{"Bob", "Alice"}, dsm, store)

			testSource := source.MultiSource{DatasetName: "people", Store: store, DatasetManager: dsm}
			var tokens []source.DatasetContinuation
			var recordedEntities []server.Entity
			token := &source.MultiDatasetContinuation{}
			testSource.StartFullSync()
			err := testSource.ReadEntities(
				token,
				1000,
				func(entities []*server.Entity, token source.DatasetContinuation) error {
					tokens = append(tokens, token)
					for _, e := range entities {
						recordedEntities = append(recordedEntities, *e)
					}
					return nil
				},
			)
			g.Assert(err).IsNil()
			testSource.EndFullSync()
			g.Assert(len(recordedEntities)).Eql(4, "two entities with 2 changes each expected")

			// now, modify alice and verify that we get alice emitted in next read
			err = people.StoreEntities([]*server.Entity{
				server.NewEntityFromMap(map[string]interface{}{
					"id":    peoplePrefix + ":Alice",
					"props": map[string]interface{}{"name": "Alice-changed"},
					"refs":  map[string]interface{}{},
				}),
			})
			g.Assert(err).IsNil()
			since := tokens[len(tokens)-1]
			tokens = []source.DatasetContinuation{}
			recordedEntities = []server.Entity{}
			err = testSource.ReadEntities(
				since,
				1000,
				func(entities []*server.Entity, token source.DatasetContinuation) error {
					tokens = append(tokens, token)
					for _, e := range entities {
						recordedEntities = append(recordedEntities, *e)
					}
					return nil
				},
			)
			g.Assert(err).IsNil()
			g.Assert(len(recordedEntities)).Eql(1)
			g.Assert(recordedEntities[0].Properties["name"]).Eql("Alice-changed")
		})

		g.It("should emit only latest changes in main dataset", func() {
			// prime dataset with 2 entities
			people, peoplePrefix := createTestDataset("people", []string{"Bob", "Alice"}, nil, dsm, g, store)
			// add one change to each entity -> 4 changes
			addChanges("people", []string{"Bob", "Alice"}, dsm, store)

			testSource := source.MultiSource{
				DatasetName: "people", Store: store, DatasetManager: dsm,
				LatestOnly: true,
			}
			var tokens []source.DatasetContinuation
			var recordedEntities []server.Entity
			token := &source.MultiDatasetContinuation{}
			testSource.StartFullSync()
			err := testSource.ReadEntities(
				token,
				1000,
				func(entities []*server.Entity, token source.DatasetContinuation) error {
					tokens = append(tokens, token)
					for _, e := range entities {
						recordedEntities = append(recordedEntities, *e)
					}
					return nil
				},
			)
			g.Assert(err).IsNil()
			testSource.EndFullSync()
			g.Assert(len(recordedEntities)).
				Eql(2, "There are 4 changes present, we expect only 2 (latest) changes emitted")

			// now, modify alice and verify that we get alice emitted in next read
			err = people.StoreEntities([]*server.Entity{
				server.NewEntityFromMap(map[string]interface{}{
					"id":    peoplePrefix + ":Alice",
					"props": map[string]interface{}{"name": "Alice-changed"},
					"refs":  map[string]interface{}{},
				}),
			})
			g.Assert(err).IsNil()
			since := tokens[len(tokens)-1]
			tokens = []source.DatasetContinuation{}
			recordedEntities = []server.Entity{}
			err = testSource.ReadEntities(
				since,
				1000,
				func(entities []*server.Entity, token source.DatasetContinuation) error {
					tokens = append(tokens, token)
					for _, e := range entities {
						recordedEntities = append(recordedEntities, *e)
					}
					return nil
				},
			)
			g.Assert(err).IsNil()
			g.Assert(len(recordedEntities)).Eql(1)
			g.Assert(recordedEntities[0].Properties["name"]).Eql("Alice-changed")
		})

		g.It("should capture watermarks during initial fullsync", func() {
			_, addressPrefix := createTestDataset("address", []string{"Mainstreet", "Sidealley"}, nil, dsm, g, store)
			peoplePrefix, _ := store.NamespaceManager.AssertPrefixMappingForExpansion("http://people/")
			createTestDataset("people", []string{"Bob", "Alice"}, map[string]map[string]interface{}{
				"Bob":   {peoplePrefix + ":address": addressPrefix + ":Mainstreet"},
				"Alice": {peoplePrefix + ":address": addressPrefix + ":Sidealley"},
			}, dsm, g, store)

			testSource := source.MultiSource{DatasetName: "people", Store: store, DatasetManager: dsm}
			srcJSON := `{ "Type" : "MultiSource", "Name" : "people", "Dependencies": [ {
							"dataset": "address",
							"joins": [ { "dataset": "people", "predicate": "http://people/address", "inverse": true } ] } ] }`

			srcConfig := map[string]interface{}{}
			err := json.Unmarshal([]byte(srcJSON), &srcConfig)
			g.Assert(err).IsNil()
			err = testSource.ParseDependencies(srcConfig["Dependencies"])
			g.Assert(err).IsNil()

			var recordedEntities []server.Entity
			token := &source.MultiDatasetContinuation{}
			var lastToken source.DatasetContinuation
			testSource.StartFullSync()
			err = testSource.ReadEntities(
				token,
				1000,
				func(entities []*server.Entity, token source.DatasetContinuation) error {
					lastToken = token
					for _, e := range entities {
						recordedEntities = append(recordedEntities, *e)
					}
					return nil
				},
			)
			g.Assert(err).IsNil()
			testSource.EndFullSync()

			// test that we do not get anything emitted without further changes. verifying that watermarks are stored in lastToken
			recordedEntities = []server.Entity{}
			err = testSource.ReadEntities(
				lastToken,
				1000,
				func(entities []*server.Entity, token source.DatasetContinuation) error {
					lastToken = token
					for _, e := range entities {
						recordedEntities = append(recordedEntities, *e)
					}
					return nil
				},
			)
			g.Assert(err).IsNil()
			g.Assert(len(recordedEntities)).Eql(0)
		})

		g.It("should emit main entity if direct dependency was changed after fullsync", func() {
			addresses, addressPrefix := createTestDataset(
				"address",
				[]string{"Mainstreet", "Sidealley"},
				nil,
				dsm,
				g,
				store,
			)
			peoplePrefix, _ := store.NamespaceManager.AssertPrefixMappingForExpansion("http://people/")
			createTestDataset("people", []string{"Bob", "Alice"}, map[string]map[string]interface{}{
				"Bob":   {peoplePrefix + ":address": addressPrefix + ":Mainstreet"},
				"Alice": {peoplePrefix + ":address": addressPrefix + ":Sidealley"},
			}, dsm, g, store)

			testSource := source.MultiSource{DatasetName: "people", Store: store, DatasetManager: dsm}
			srcJSON := `{ "Type" : "MultiSource", "Name" : "people", "Dependencies": [ {
							"dataset": "address",
							"joins": [ { "dataset": "people", "predicate": "http://people/address", "inverse": true } ] } ] }`

			srcConfig := map[string]interface{}{}
			err := json.Unmarshal([]byte(srcJSON), &srcConfig)
			g.Assert(err).IsNil()
			err = testSource.ParseDependencies(srcConfig["Dependencies"])
			g.Assert(err).IsNil()

			// fullsync
			var recordedEntities []server.Entity
			token := &source.MultiDatasetContinuation{}
			var lastToken source.DatasetContinuation
			testSource.StartFullSync()
			err = testSource.ReadEntities(
				token,
				1000,
				func(entities []*server.Entity, token source.DatasetContinuation) error {
					lastToken = token
					for _, e := range entities {
						recordedEntities = append(recordedEntities, *e)
					}
					return nil
				},
			)
			g.Assert(err).IsNil()
			testSource.EndFullSync()

			// now, modify Mainstreet address and verify that we get Bob emitted in next read (mainstreet is direct dependency to bob)
			err = addresses.StoreEntities([]*server.Entity{
				server.NewEntityFromMap(map[string]interface{}{
					"id":    addressPrefix + ":Mainstreet",
					"props": map[string]interface{}{"name": "Mainstreet-changed"},
					"refs":  map[string]interface{}{},
				}),
			})
			g.Assert(err).IsNil()
			recordedEntities = []server.Entity{}
			err = testSource.ReadEntities(
				lastToken,
				1000,
				func(entities []*server.Entity, token source.DatasetContinuation) error {
					lastToken = token
					for _, e := range entities {
						recordedEntities = append(recordedEntities, *e)
					}
					return nil
				},
			)
			g.Assert(err).IsNil()
			g.Assert(len(recordedEntities)).Eql(1)
			// Bob was emitted enchanged. up to transform to do something with bob and dependency that triggered bob's emission
			g.Assert(recordedEntities[0].Properties["name"]).Eql("Bob")
		})

		// initial incremental will be much slower, as it traverses all main entities and processes all dependency changes without watermarks
		g.It("should emit main entity if direct dependency was changed after initial incremental run", func() {
			addresses, addressPrefix := createTestDataset(
				"address",
				[]string{"Mainstreet", "Sidealley"},
				nil,
				dsm,
				g,
				store,
			)
			peoplePrefix, _ := store.NamespaceManager.AssertPrefixMappingForExpansion("http://people/")
			createTestDataset("people", []string{"Bob", "Alice"}, map[string]map[string]interface{}{
				"Bob":   {peoplePrefix + ":address": addressPrefix + ":Mainstreet"},
				"Alice": {peoplePrefix + ":address": addressPrefix + ":Sidealley"},
			}, dsm, g, store)

			testSource := source.MultiSource{DatasetName: "people", Store: store, DatasetManager: dsm}
			srcJSON := `{ "Type" : "MultiSource", "Name" : "people", "Dependencies": [ {
							"dataset": "address",
							"joins": [ { "dataset": "people", "predicate": "http://people/address", "inverse": true } ] } ] }`

			srcConfig := map[string]interface{}{}
			err := json.Unmarshal([]byte(srcJSON), &srcConfig)
			g.Assert(err).IsNil()
			err = testSource.ParseDependencies(srcConfig["Dependencies"])
			g.Assert(err).IsNil()

			// initial incremental run.
			// Since the fullsync flag is not set, the run will traverse all dependencies in addition to all main entities
			var recordedEntities []server.Entity
			token := &source.MultiDatasetContinuation{}
			var lastToken source.DatasetContinuation
			err = testSource.ReadEntities(
				token,
				1000,
				func(entities []*server.Entity, token source.DatasetContinuation) error {
					lastToken = token
					for _, e := range entities {
						recordedEntities = append(recordedEntities, *e)
					}
					return nil
				},
			)
			g.Assert(err).IsNil()

			// now, modify Mainstreet address and verify that we get Bob emitted in next read (mainstreet is direct dependency to bob)
			err = addresses.StoreEntities([]*server.Entity{
				server.NewEntityFromMap(map[string]interface{}{
					"id":    addressPrefix + ":Mainstreet",
					"props": map[string]interface{}{"name": "Mainstreet-changed"},
					"refs":  map[string]interface{}{},
				}),
			})
			g.Assert(err).IsNil()
			recordedEntities = []server.Entity{}
			err = testSource.ReadEntities(
				lastToken,
				1000,
				func(entities []*server.Entity, token source.DatasetContinuation) error {
					lastToken = token
					for _, e := range entities {
						recordedEntities = append(recordedEntities, *e)
					}
					return nil
				},
			)
			g.Assert(err).IsNil()
			g.Assert(len(recordedEntities)).Eql(1)
			// Bob was emitted enchanged. up to transform to do something with bob and dependency that triggered bob's emission
			g.Assert(recordedEntities[0].Properties["name"]).Eql("Bob")
		})

		g.It("should emit main entity if inverse dependency was changed after fullsync", func() {
			_, peoplePrefix := createTestDataset("people", []string{"Bob", "Alice"}, nil, dsm, g, store)
			employments, employmentPrefix := createTestDataset("employment",
				[]string{"MediumCorp", "LittleSweatshop", "YardSale"}, map[string]map[string]interface{}{
					"MediumCorp":      {peoplePrefix + ":employment": peoplePrefix + ":Bob"},
					"LittleSweatshop": {peoplePrefix + ":employment": peoplePrefix + ":Alice"},
					"YardSale": {
						peoplePrefix + ":employment": []string{peoplePrefix + ":Bob", peoplePrefix + ":Alice"},
					},
				}, dsm, g, store)

			testSource := source.MultiSource{DatasetName: "people", Store: store, DatasetManager: dsm}
			srcJSON := `{ "Type" : "MultiSource", "Name" : "people", "Dependencies": [ {
							"dataset": "employment",
							"joins": [ { "dataset": "people", "predicate": "http://people/employment", "inverse": false } ] } ] }`

			srcConfig := map[string]interface{}{}
			_ = json.Unmarshal([]byte(srcJSON), &srcConfig)
			_ = testSource.ParseDependencies(srcConfig["Dependencies"])

			// fullsync
			var recordedEntities []server.Entity
			token := &source.MultiDatasetContinuation{}
			var lastToken source.DatasetContinuation
			testSource.StartFullSync()
			err := testSource.ReadEntities(
				token,
				1000,
				func(entities []*server.Entity, token source.DatasetContinuation) error {
					lastToken = token
					for _, e := range entities {
						recordedEntities = append(recordedEntities, *e)
					}
					return nil
				},
			)
			g.Assert(err).IsNil()
			testSource.EndFullSync()

			// now, modify MediumCorp employment and verify that we get Bob emitted in next read (MediumCorp is inverse dependency to bob)
			err = employments.StoreEntities([]*server.Entity{
				server.NewEntityFromMap(map[string]interface{}{
					"id":    employmentPrefix + ":MediumCorp",
					"props": map[string]interface{}{"name": "MediumCorp-changed"},
					"refs":  map[string]interface{}{peoplePrefix + ":employment": peoplePrefix + ":Bob"},
				}),
			})
			g.Assert(err).IsNil()

			recordedEntities = []server.Entity{}
			err = testSource.ReadEntities(
				lastToken,
				1000,
				func(entities []*server.Entity, token source.DatasetContinuation) error {
					lastToken = token
					for _, e := range entities {
						recordedEntities = append(recordedEntities, *e)
					}
					return nil
				},
			)
			g.Assert(err).IsNil()
			g.Assert(len(recordedEntities)).Eql(1)
			// Bob was emitted enchanged. up to transform to do something with bob and dependency that triggered bob's emission
			g.Assert(recordedEntities[0].Properties["name"]).Eql("Bob")

			// also, modify YardSale employment and verify that both Bob and Alice emitted in next read
			err = employments.StoreEntities([]*server.Entity{
				server.NewEntityFromMap(map[string]interface{}{
					"id":    employmentPrefix + ":YardSale",
					"props": map[string]interface{}{"name": "YardSale-changed"},
					"refs": map[string]interface{}{
						peoplePrefix + ":employment": []string{peoplePrefix + ":Bob", peoplePrefix + ":Alice"},
					},
				}),
			})
			g.Assert(err).IsNil()

			recordedEntities = []server.Entity{}
			err = testSource.ReadEntities(
				lastToken,
				1000,
				func(entities []*server.Entity, token source.DatasetContinuation) error {
					lastToken = token
					for _, e := range entities {
						recordedEntities = append(recordedEntities, *e)
					}
					return nil
				},
			)
			g.Assert(err).IsNil()
			// expect both referred-to people to be emitted
			g.Assert(len(recordedEntities)).Eql(2)
		})

		g.It("should emit main entity if multi hop dependency was changed after fullsync", func() {
			cities, cityPrefix := createTestDataset("city", []string{"Oslo", "Bergen"}, nil, dsm, g, store)
			addressPrefix, _ := store.NamespaceManager.AssertPrefixMappingForExpansion("http://address/")
			addresses, _ := createTestDataset("address", []string{"Mainstreet", "Sidealley"},
				map[string]map[string]interface{}{
					"Mainstreet": {},
					"Sidealley":  {addressPrefix + ":city": cityPrefix + ":Bergen"},
				}, dsm, g, store)
			peoplePrefix, _ := store.NamespaceManager.AssertPrefixMappingForExpansion("http://people/")
			createTestDataset("people", []string{"Bob", "Alice"}, map[string]map[string]interface{}{
				"Bob":   {peoplePrefix + ":address": addressPrefix + ":Mainstreet"},
				"Alice": {peoplePrefix + ":address": addressPrefix + ":Sidealley"},
			}, dsm, g, store)

			testSource := source.MultiSource{DatasetName: "people", Store: store, DatasetManager: dsm}
			srcJSON := `{ "Type" : "MultiSource", "Name" : "people", "Dependencies": [ {
							"dataset": "city",
							"joins": [
                              { "dataset": "address", "predicate": "http://address/city", "inverse": true },
                              { "dataset": "people", "predicate": "http://people/address", "inverse": true }
                            ] } ] }`

			srcConfig := map[string]interface{}{}
			err := json.Unmarshal([]byte(srcJSON), &srcConfig)
			g.Assert(err).IsNil()
			err = testSource.ParseDependencies(srcConfig["Dependencies"])
			g.Assert(err).IsNil()

			// fullsync
			var recordedEntities []server.Entity
			token := &source.MultiDatasetContinuation{}
			var lastToken source.DatasetContinuation
			testSource.StartFullSync()
			err = testSource.ReadEntities(
				token,
				1000,
				func(entities []*server.Entity, token source.DatasetContinuation) error {
					lastToken = token
					for _, e := range entities {
						recordedEntities = append(recordedEntities, *e)
					}
					return nil
				},
			)
			g.Assert(err).IsNil()
			testSource.EndFullSync()

			// modify Mainstreet address and verify that nothing changed (address is not a dependency, just a join)
			err = addresses.StoreEntities([]*server.Entity{
				server.NewEntityFromMap(map[string]interface{}{
					"id":    addressPrefix + ":Mainstreet",
					"props": map[string]interface{}{"name": "Mainstreet-changed"},
					"refs":  map[string]interface{}{addressPrefix + ":city": cityPrefix + ":Oslo"},
				}),
			})
			g.Assert(err).IsNil()
			recordedEntities = []server.Entity{}
			err = testSource.ReadEntities(
				lastToken,
				1000,
				func(entities []*server.Entity, token source.DatasetContinuation) error {
					lastToken = token
					for _, e := range entities {
						recordedEntities = append(recordedEntities, *e)
					}
					return nil
				},
			)
			g.Assert(err).IsNil()
			g.Assert(len(recordedEntities)).Eql(0)

			// now, modify Oslo city and verify that bob is found via Mainstreet address
			err = cities.StoreEntities([]*server.Entity{
				server.NewEntityFromMap(map[string]interface{}{
					"id":    cityPrefix + ":Oslo",
					"props": map[string]interface{}{"name": "Oslo-changed"},
					"refs":  map[string]interface{}{},
				}),
			})
			g.Assert(err).IsNil()
			recordedEntities = []server.Entity{}
			err = testSource.ReadEntities(
				lastToken,
				1000,
				func(entities []*server.Entity, token source.DatasetContinuation) error {
					lastToken = token
					for _, e := range entities {
						recordedEntities = append(recordedEntities, *e)
					}
					return nil
				},
			)
			g.Assert(err).IsNil()
			g.Assert(len(recordedEntities)).Eql(1)
		})

		g.It("Should emit main entity if inverse multi hop dependency is changed", func() {
			// people <- employment <- salary
			_, peoplePrefix := createTestDataset("people", []string{"Bob", "Alice"}, nil, dsm, g, store)
			_, employmentPrefix := createTestDataset("employment",
				[]string{"MediumCorp", "LittleSweatshop", "YardSale"}, map[string]map[string]interface{}{
					"MediumCorp":      {peoplePrefix + ":employment": peoplePrefix + ":Bob"},
					"LittleSweatshop": {peoplePrefix + ":employment": peoplePrefix + ":Alice"},
					"YardSale": {
						peoplePrefix + ":employment": []string{peoplePrefix + ":Bob", peoplePrefix + ":Alice"},
					},
				}, dsm, g, store)
			incomeRanges, incomeRangePrefix := createTestDataset("incomeRange",
				[]string{"High", "Medium", "Low"}, map[string]map[string]interface{}{
					"High": {employmentPrefix + ":employment": employmentPrefix + ":MediumCorp"},
					"Low": {
						employmentPrefix + ":employment": []string{
							employmentPrefix + ":MediumCorp",
							employmentPrefix + "YardSale",
						},
					},
				}, dsm, g, store)

			testSource := source.MultiSource{DatasetName: "people", Store: store, DatasetManager: dsm}
			srcJSON := `{ "Type" : "MultiSource", "Name" : "people", "Dependencies": [ {
							"dataset": "incomeRange",
							"joins": [ { "dataset": "employment", "predicate": "http://employment/employment", "inverse": false },
									   { "dataset": "people", "predicate": "http://people/employment", "inverse": false }
                                     ] } ] }`

			srcConfig := map[string]interface{}{}
			_ = json.Unmarshal([]byte(srcJSON), &srcConfig)
			_ = testSource.ParseDependencies(srcConfig["Dependencies"])

			// fullsync
			var recordedEntities []server.Entity
			token := &source.MultiDatasetContinuation{}
			var lastToken source.DatasetContinuation
			testSource.StartFullSync()
			err := testSource.ReadEntities(
				token,
				1000,
				func(entities []*server.Entity, token source.DatasetContinuation) error {
					lastToken = token
					for _, e := range entities {
						recordedEntities = append(recordedEntities, *e)
					}
					return nil
				},
			)
			g.Assert(err).IsNil()
			testSource.EndFullSync()

			// now, modify High incomeRange and verify that we get Bob emitted in next read (MediumCorp is inverse dependency to bob via employment MediumCorp)
			err = incomeRanges.StoreEntities([]*server.Entity{
				server.NewEntityFromMap(map[string]interface{}{
					"id":    incomeRangePrefix + ":High",
					"props": map[string]interface{}{"name": "High-changed"},
					"refs":  map[string]interface{}{employmentPrefix + ":employment": employmentPrefix + ":MediumCorp"},
				}),
			})
			g.Assert(err).IsNil()

			recordedEntities = []server.Entity{}
			err = testSource.ReadEntities(
				lastToken,
				1000,
				func(entities []*server.Entity, token source.DatasetContinuation) error {
					lastToken = token
					for _, e := range entities {
						recordedEntities = append(recordedEntities, *e)
					}
					return nil
				},
			)
			g.Assert(err).IsNil()
			g.Assert(len(recordedEntities)).Eql(1)
			// Bob was emitted enchanged. up to transform to do something with bob and dependency that triggered bob's emission
			g.Assert(recordedEntities[0].Properties["name"]).Eql("Bob")
		})

		g.It("should emit main entity if inverse dependency ref is removed", func() {
			_, peoplePrefix := createTestDataset("people", []string{"Bob", "Alice"}, nil, dsm, g, store)
			employments, employmentPrefix := createTestDataset("employment",
				[]string{"MediumCorp", "LittleSweatshop", "YardSale"}, map[string]map[string]interface{}{
					"MediumCorp":      {peoplePrefix + ":employment": peoplePrefix + ":Bob"},
					"LittleSweatshop": {peoplePrefix + ":employment": peoplePrefix + ":Alice"},
					"YardSale": {
						peoplePrefix + ":employment": []string{peoplePrefix + ":Bob", peoplePrefix + ":Alice"},
					},
				}, dsm, g, store)

			testSource := source.MultiSource{DatasetName: "people", Store: store, DatasetManager: dsm}
			srcJSON := `{ "Type" : "MultiSource", "Name" : "people", "Dependencies": [ {
							"dataset": "employment",
							"joins": [ { "dataset": "people", "predicate": "http://people/employment", "inverse": false } ] } ] }`

			srcConfig := map[string]interface{}{}
			_ = json.Unmarshal([]byte(srcJSON), &srcConfig)
			_ = testSource.ParseDependencies(srcConfig["Dependencies"])

			// fullsync
			var recordedEntities []server.Entity
			token := &source.MultiDatasetContinuation{}
			var lastToken source.DatasetContinuation
			testSource.StartFullSync()
			err := testSource.ReadEntities(
				token,
				1000,
				func(entities []*server.Entity, token source.DatasetContinuation) error {
					lastToken = token
					for _, e := range entities {
						recordedEntities = append(recordedEntities, *e)
					}
					return nil
				},
			)
			g.Assert(err).IsNil()
			testSource.EndFullSync()

			// now, remove all refs  from MediumCorp.  this should emit bob because the dependency was changed
			err = employments.StoreEntities([]*server.Entity{
				server.NewEntityFromMap(map[string]interface{}{
					"id":    employmentPrefix + ":MediumCorp",
					"props": map[string]interface{}{"name": "MediumCorp-changed"},
					"refs":  map[string]interface{}{},
				}),
			})
			g.Assert(err).IsNil()

			recordedEntities = []server.Entity{}
			err = testSource.ReadEntities(
				lastToken,
				1000,
				func(entities []*server.Entity, token source.DatasetContinuation) error {
					lastToken = token
					for _, e := range entities {
						recordedEntities = append(recordedEntities, *e)
					}
					return nil
				},
			)
			g.Assert(err).IsNil()
			g.Assert(len(recordedEntities)).Eql(1)
			// Bob was emitted enchanged. up to transform to do something with bob and dependency that triggered bob's emission
			g.Assert(recordedEntities[0].Properties["name"]).Eql("Bob")
		})
		g.It("should support empty dependency dastasets", func() {
			_, _ = createTestDataset("people", []string{"Bob", "Alice"}, nil, dsm, g, store)
			_, _ = createTestDataset("employment", nil, nil, dsm, g, store)

			testSource := source.MultiSource{DatasetName: "people", Store: store, DatasetManager: dsm}
			srcJSON := `{ "Type" : "MultiSource", "Name" : "people", "Dependencies": [ {
							"dataset": "employment",
							"joins": [ { "dataset": "people", "predicate": "http://people/employment", "inverse": false } ] } ] }`

			srcConfig := map[string]interface{}{}
			_ = json.Unmarshal([]byte(srcJSON), &srcConfig)
			_ = testSource.ParseDependencies(srcConfig["Dependencies"])

			// fullsync
			var recordedEntities []server.Entity
			token := &source.MultiDatasetContinuation{}
			var lastToken source.DatasetContinuation
			testSource.StartFullSync()
			err := testSource.ReadEntities(
				token,
				1000,
				func(entities []*server.Entity, token source.DatasetContinuation) error {
					lastToken = token
					for _, e := range entities {
						recordedEntities = append(recordedEntities, *e)
					}
					return nil
				},
			)
			g.Assert(err).IsNil()
			testSource.EndFullSync()

			g.Assert(len(recordedEntities)).Eql(2)

			// run inc
			recordedEntities = []server.Entity{}
			err = testSource.ReadEntities(
				lastToken,
				1000,
				func(entities []*server.Entity, token source.DatasetContinuation) error {
					lastToken = token
					for _, e := range entities {
						recordedEntities = append(recordedEntities, *e)
					}
					return nil
				},
			)
			g.Assert(err).IsNil()
			g.Assert(len(recordedEntities)).Eql(0)
		})

		g.It("should emit main enitity if inverse multi hop dependency is removed", func() {
			// people <- employment <- salary
			_, peoplePrefix := createTestDataset("people", []string{"Bob", "Alice", "Hank"}, nil, dsm, g, store)
			_, employmentPrefix := createTestDataset("employment",
				[]string{"MediumCorp", "LittleSweatshop", "YardSale", "BigCorp"}, map[string]map[string]interface{}{
					"MediumCorp":      {peoplePrefix + ":employment": peoplePrefix + ":Bob"},
					"BigCorp":         {peoplePrefix + ":employment": peoplePrefix + ":Hank"},
					"LittleSweatshop": {peoplePrefix + ":employment": peoplePrefix + ":Alice"},
					"YardSale": {
						peoplePrefix + ":employment": []string{peoplePrefix + ":Bob", peoplePrefix + ":Alice"},
					},
				}, dsm, g, store)
			incomeRanges, incomeRangePrefix := createTestDataset("incomeRange",
				[]string{"High", "Medium", "Low"}, map[string]map[string]interface{}{
					"High": {employmentPrefix + ":employment": employmentPrefix + ":MediumCorp"},
					"Low": {
						employmentPrefix + ":employment": []string{
							employmentPrefix + ":MediumCorp",
							employmentPrefix + "YardSale",
						},
					},
				}, dsm, g, store)

			testSource := source.MultiSource{DatasetName: "people", Store: store, DatasetManager: dsm}
			srcJSON := `{ "Type" : "MultiSource", "Name" : "people", "Dependencies": [ {
							"dataset": "incomeRange",
							"joins": [ { "dataset": "employment", "predicate": "http://employment/employment", "inverse": false },
									   { "dataset": "people", "predicate": "http://people/employment", "inverse": false }
                                     ] } ] }`

			srcConfig := map[string]interface{}{}
			_ = json.Unmarshal([]byte(srcJSON), &srcConfig)
			_ = testSource.ParseDependencies(srcConfig["Dependencies"])

			// fullsync
			var recordedEntities []server.Entity
			token := &source.MultiDatasetContinuation{}
			var lastToken source.DatasetContinuation
			testSource.StartFullSync()
			err := testSource.ReadEntities(
				token,
				1000,
				func(entities []*server.Entity, token source.DatasetContinuation) error {
					lastToken = token
					for _, e := range entities {
						recordedEntities = append(recordedEntities, *e)
					}
					return nil
				},
			)
			g.Assert(err).IsNil()
			testSource.EndFullSync()

			// now, point High incomeRange from MediumCorp to BigCorp.
			// Hank and Bob should be emitted (Hank, because he gained High incomeRange. Bob, because he lost it).
			err = incomeRanges.StoreEntities([]*server.Entity{
				server.NewEntityFromMap(map[string]interface{}{
					"id":    incomeRangePrefix + ":High",
					"props": map[string]interface{}{"name": "High"},
					"refs":  map[string]interface{}{employmentPrefix + ":employment": employmentPrefix + ":BigCorp"},
				}),
			})
			g.Assert(err).IsNil()

			recordedEntities = []server.Entity{}
			batchCnt := 0
			err = testSource.ReadEntities(
				lastToken,
				1,
				func(entities []*server.Entity, token source.DatasetContinuation) error {
					lastToken = token
					batchCnt++
					for _, e := range entities {
						recordedEntities = append(recordedEntities, *e)
					}
					return nil
				},
			)
			g.Assert(batchCnt).Eql(3, "2 batches of 1, and final main dataset batch is empty")
			g.Assert(err).IsNil()
			g.Assert(len(recordedEntities)).Eql(2)
			var seenBob, seenHank bool
			for _, re := range recordedEntities {
				seenBob = seenBob || re.ID == peoplePrefix+":Bob"
				seenHank = seenHank || re.ID == peoplePrefix+":Hank"
			}
			g.Assert(seenBob).IsTrue("expected to find Bob in emitted entities")
			g.Assert(seenHank).IsTrue("expected to find Hank in emitted entities")
		})

		g.It("should support same dataset as dependency multiple times", func() {
			// people <- employment <- salary
			// people <- demographic -> salary
			_, peoplePrefix := createTestDataset("people", []string{"Bob", "Alice", "Hank"}, nil, dsm, g, store)
			_, employmentPrefix := createTestDataset("employment",
				[]string{"MediumCorp", "LittleSweatshop", "YardSale", "BigCorp"}, map[string]map[string]interface{}{
					"MediumCorp":      {peoplePrefix + ":employment": peoplePrefix + ":Bob"},
					"BigCorp":         {peoplePrefix + ":employment": peoplePrefix + ":Hank"},
					"LittleSweatshop": {peoplePrefix + ":employment": peoplePrefix + ":Alice"},
					"YardSale": {
						peoplePrefix + ":employment": []string{peoplePrefix + ":Bob", peoplePrefix + ":Alice"},
					},
				}, dsm, g, store)
			incomeRanges, incomeRangePrefix := createTestDataset("incomeRange",
				[]string{"High", "Medium", "Low"}, map[string]map[string]interface{}{
					"High": {employmentPrefix + ":employment": employmentPrefix + ":MediumCorp"},
					"Low": {
						employmentPrefix + ":employment": []string{
							employmentPrefix + ":MediumCorp",
							employmentPrefix + "YardSale",
						},
					},
				}, dsm, g, store)
			_, _ = createTestDataset("demographic", []string{"young", "middle-aged", "senior"},
				map[string]map[string]interface{}{
					"young": {
						peoplePrefix + ":people":           peoplePrefix + ":Alice",
						incomeRangePrefix + ":incomeRange": incomeRangePrefix + ":High",
					},
				}, dsm, g, store)

			testSource := source.MultiSource{DatasetName: "people", Store: store, DatasetManager: dsm}
			srcJSON := `{ "Type" : "MultiSource", "Name" : "people", "Dependencies": [
                          {
							"dataset": "incomeRange",
							"joins": [ { "dataset": "employment", "predicate": "http://employment/employment", "inverse": false },
									   { "dataset": "people", "predicate": "http://people/employment", "inverse": false } ]
                          }, {
                            "dataset": "incomeRange",
                            "joins": [ { "dataset": "demographic", "predicate": "http://incomeRange/incomeRange", "inverse": true },
                                       { "dataset": "people", "predicate": "http://people/people", "inverse": false }]
                          }
                        ] }`

			srcConfig := map[string]interface{}{}
			_ = json.Unmarshal([]byte(srcJSON), &srcConfig)
			_ = testSource.ParseDependencies(srcConfig["Dependencies"])

			// fullsync
			var recordedEntities []server.Entity
			token := &source.MultiDatasetContinuation{}
			var lastToken source.DatasetContinuation
			testSource.StartFullSync()
			err := testSource.ReadEntities(
				token,
				1000,
				func(entities []*server.Entity, token source.DatasetContinuation) error {
					lastToken = token
					for _, e := range entities {
						recordedEntities = append(recordedEntities, *e)
					}
					return nil
				},
			)
			g.Assert(err).IsNil()
			testSource.EndFullSync()

			// Now, change name of "High" incomeRange. expected Bob emitted through first dependency via employment.
			// Expect also Alice through 2nd dependency via demographic
			err = incomeRanges.StoreEntities([]*server.Entity{
				server.NewEntityFromMap(map[string]interface{}{
					"id":    incomeRangePrefix + ":High",
					"props": map[string]interface{}{"name": "High-changed"},
					"refs":  map[string]interface{}{employmentPrefix + ":employment": employmentPrefix + ":MediumCorp"},
				}),
			})
			g.Assert(err).IsNil()

			recordedEntities = []server.Entity{}
			err = testSource.ReadEntities(
				lastToken,
				1000,
				func(entities []*server.Entity, token source.DatasetContinuation) error {
					lastToken = token
					for _, e := range entities {
						recordedEntities = append(recordedEntities, *e)
					}
					return nil
				},
			)
			g.Assert(err).IsNil()
			g.Assert(len(recordedEntities)).Eql(2)
			var seenBob, seenAlice bool
			for _, re := range recordedEntities {
				seenBob = seenBob || re.ID == peoplePrefix+":Bob"
				seenAlice = seenAlice || re.ID == peoplePrefix+":Alice"
			}
			g.Assert(seenBob).IsTrue("expected to find Bob in emitted entities via 1st dep")
			g.Assert(seenAlice).IsTrue("expected to find Alice in emitted entities via 2nd dep")
		})

		g.It("should support multiple link predicates per join", func() {
			// people <- band -> people
			people, peoplePrefix := createTestDataset(
				"people",
				[]string{"Bob", "Rob", "Mary", "Alice", "Hank", "Lisa"},
				nil,
				dsm,
				g,
				store,
			)

			_, _ = createTestDataset("band", []string{"Rockbuds", "SideshowBand"},
				map[string]map[string]interface{}{
					"Rockbuds": {
						peoplePrefix + ":singer": []string{
							peoplePrefix + ":Rob",
							peoplePrefix + ":Mary",
							peoplePrefix + ":Lisa",
						},
						peoplePrefix + ":drummer": []string{peoplePrefix + ":Alice"},
					},
					"SideshowBand": {
						peoplePrefix + ":singer":  peoplePrefix + ":Bob",
						peoplePrefix + ":drummer": []string{peoplePrefix + ":Hank"},
					},
				}, dsm, g, store)

			testSource := source.MultiSource{DatasetName: "people", Store: store, DatasetManager: dsm}
			srcJSON := `{ "Type" : "MultiSource", "Name" : "people", "Dependencies": [
                          {
							"dataset": "people",
							"joins": [ { "dataset": "band", "predicate": "http://people/drummer", "inverse": true },
									   { "dataset": "people", "predicate": "http://people/singer", "inverse": false } ]
                          },
                          {
							"dataset": "people",
							"joins": [ { "dataset": "band", "predicate": "http://people/singer", "inverse": true },
									   { "dataset": "people", "predicate": "http://people/drummer", "inverse": false } ]
                          },
                          {
							"dataset": "people",
							"joins": [ { "dataset": "band", "predicate": "http://people/singer", "inverse": true },
									   { "dataset": "people", "predicate": "http://people/singer", "inverse": false } ]
                          }
                        ] }`

			srcConfig := map[string]interface{}{}
			_ = json.Unmarshal([]byte(srcJSON), &srcConfig)
			_ = testSource.ParseDependencies(srcConfig["Dependencies"])

			// fullsync
			var recordedEntities []server.Entity
			token := &source.MultiDatasetContinuation{}
			var lastToken source.DatasetContinuation
			testSource.StartFullSync()
			err := testSource.ReadEntities(
				token,
				1000,
				func(entities []*server.Entity, token source.DatasetContinuation) error {
					lastToken = token
					for _, e := range entities {
						recordedEntities = append(recordedEntities, *e)
					}
					return nil
				},
			)
			g.Assert(err).IsNil()
			testSource.EndFullSync()

			// Rename Hank, expect Hank himself (main dataset change) and Bob, who is singer in Hank's band "Sideshow", to be emitted
			err = people.StoreEntities([]*server.Entity{
				server.NewEntityFromMap(map[string]interface{}{
					"id":    peoplePrefix + ":Hank",
					"props": map[string]interface{}{"name": "Hank-changed"},
					"refs":  map[string]interface{}{},
				}),
			})
			g.Assert(err).IsNil()

			recordedEntities = []server.Entity{}
			err = testSource.ReadEntities(
				lastToken,
				1000,
				func(entities []*server.Entity, token source.DatasetContinuation) error {
					lastToken = token
					for _, e := range entities {
						recordedEntities = append(recordedEntities, *e)
					}
					return nil
				},
			)
			g.Assert(err).IsNil()
			g.Assert(len(recordedEntities)).Eql(2)
			var seenBob, seenHank bool
			for _, re := range recordedEntities {
				seenBob = seenBob || re.ID == peoplePrefix+":Bob"
				seenHank = seenHank || re.ID == peoplePrefix+":Hank"
			}
			g.Assert(seenBob).IsTrue("expected to find Bob in emitted entities via 1st dep")
			g.Assert(seenHank).IsTrue("expected to find Hank because Hank was changed in main dataset")

			// Rename Lisa. expect Lisa herself. and all her bandmates (singers Rob+Mary and drummer Alice)
			// Lisa will currently be emitted twice. once from main dataset, and once because lisa is her own singer-singer relation aswell
			err = people.StoreEntities([]*server.Entity{
				server.NewEntityFromMap(map[string]interface{}{
					"id":    peoplePrefix + ":Lisa",
					"props": map[string]interface{}{"name": "Lisa-changed"},
					"refs":  map[string]interface{}{},
				}),
			})
			g.Assert(err).IsNil()

			recordedEntities = []server.Entity{}
			err = testSource.ReadEntities(
				lastToken,
				1000,
				func(entities []*server.Entity, token source.DatasetContinuation) error {
					lastToken = token
					for _, e := range entities {
						recordedEntities = append(recordedEntities, *e)
					}
					return nil
				},
			)
			g.Assert(err).IsNil()
			g.Assert(len(recordedEntities)).Eql(5)
			var lisaSeen, robSeen, marySeen, aliceSeen bool
			lisaCnt := 0
			for _, re := range recordedEntities {
				lisaSeen = lisaSeen || re.ID == peoplePrefix+":Lisa"
				robSeen = robSeen || re.ID == peoplePrefix+":Rob"
				marySeen = marySeen || re.ID == peoplePrefix+":Mary"
				aliceSeen = aliceSeen || re.ID == peoplePrefix+":Alice"
				if re.ID == peoplePrefix+":Lisa" {
					lisaCnt++
				}
			}
			g.Assert(lisaSeen).IsTrue()
			g.Assert(robSeen).IsTrue()
			g.Assert(marySeen).IsTrue()
			g.Assert(aliceSeen).IsTrue()
			g.Assert(lisaCnt).Eql(2)
		})

		g.It("should support deep join paths", func() {
			// people <------ team -----------> people <------------- office -------> address
			//         member      team-lead            contact-person        address
			_, peoplePrefix := createTestDataset("people",
				[]string{"Bob", "Rob", "Mary", "Alice", "Hank", "Lisa"}, nil, dsm, g, store)

			teamPrefix, _ := store.NamespaceManager.AssertPrefixMappingForExpansion("http://team/")
			_, _ = createTestDataset("team", []string{"product", "development"},
				map[string]map[string]interface{}{
					"product": {
						teamPrefix + ":lead": peoplePrefix + ":Bob",
						teamPrefix + ":member": []string{
							peoplePrefix + ":Bob",
							peoplePrefix + ":Rob",
							peoplePrefix + ":Mary",
						},
					},
					"development": {
						teamPrefix + ":lead": peoplePrefix + ":Alice",
						teamPrefix + ":member": []string{
							peoplePrefix + ":Alice",
							peoplePrefix + ":Hank",
							peoplePrefix + ":Lisa",
						},
					},
				}, dsm, g, store)

			addresses, addressPrefix := createTestDataset("address", []string{"BigSquare 1", "Lillevegen 9"},
				nil, dsm, g, store)

			officePrefix, _ := store.NamespaceManager.AssertPrefixMappingForExpansion("http://office/")
			createTestDataset("office", []string{"Brisbane", "Stavanger"}, map[string]map[string]interface{}{
				"Brisbane": {
					officePrefix + ":contact":  peoplePrefix + ":Bob",
					officePrefix + ":location": addressPrefix + ":BigSquare 1",
				},
				"Stavanger": {
					officePrefix + ":contact":  peoplePrefix + ":Alice",
					officePrefix + ":location": addressPrefix + ":Lillevegen 9",
				},
			}, dsm, g, store)

			testSource := source.MultiSource{DatasetName: "people", Store: store, DatasetManager: dsm}
			srcJSON := `{ "Type" : "MultiSource", "Name" : "people", "Dependencies": [
                          {
							"dataset": "address",
							"joins": [
							  { "dataset": "office", "predicate": "http://office/location", "inverse": true },
							  { "dataset": "people", "predicate": "http://office/contact", "inverse": false },
							  { "dataset": "team", "predicate": "http://team/lead", "inverse": true },
							  { "dataset": "people", "predicate": "http://team/member", "inverse": false }
							]
                          }
                        ] }`

			srcConfig := map[string]interface{}{}
			_ = json.Unmarshal([]byte(srcJSON), &srcConfig)
			_ = testSource.ParseDependencies(srcConfig["Dependencies"])

			// fullsync
			var recordedEntities []server.Entity
			token := &source.MultiDatasetContinuation{}
			var lastToken source.DatasetContinuation
			testSource.StartFullSync()
			err := testSource.ReadEntities(
				token,
				1000,
				func(entities []*server.Entity, token source.DatasetContinuation) error {
					lastToken = token
					for _, e := range entities {
						recordedEntities = append(recordedEntities, *e)
					}
					return nil
				},
			)
			g.Assert(err).IsNil()
			testSource.EndFullSync()

			// delete Lillevegen 9, verify that all 3 team members at that address are emitted (Alice, Hank, Lisa)
			fromMap := server.NewEntityFromMap(map[string]interface{}{
				"id":    addressPrefix + ":Lillevegen 9",
				"props": map[string]interface{}{"name": "Lillevegen 9"},
				"refs":  map[string]interface{}{},
			})
			fromMap.References = nil
			fromMap.IsDeleted = true
			err = addresses.StoreEntities([]*server.Entity{fromMap})
			g.Assert(err).IsNil()

			recordedEntities = []server.Entity{}
			err = testSource.ReadEntities(
				lastToken,
				1000,
				func(entities []*server.Entity, token source.DatasetContinuation) error {
					lastToken = token
					for _, e := range entities {
						recordedEntities = append(recordedEntities, *e)
					}
					return nil
				},
			)

			g.Assert(err).IsNil()
			g.Assert(len(recordedEntities)).Eql(3)

			m := make(map[string]bool)
			for _, re := range recordedEntities {
				m["Alice"] = m["Alice"] || re.ID == peoplePrefix+":Alice"
				m["Hank"] = m["Hank"] || re.ID == peoplePrefix+":Hank"
				m["Lisa"] = m["Lisa"] || re.ID == peoplePrefix+":Lisa"
			}
			g.Assert(m["Alice"]).IsTrue()
			g.Assert(m["Hank"]).IsTrue()
			g.Assert(m["Lisa"]).IsTrue()
		})
		g.It("Should only emit main entities that exist in main dataset", func() {
			// people <- employment
			_, peoplePrefix := createTestDataset("people", []string{"Bob", "Alice"}, nil, dsm, g, store)
			copyDs, _ := dsm.CreateDataset("peopleTwo", nil)
			copyDs.StoreEntities([]*server.Entity{
				server.NewEntityFromMap(map[string]interface{}{
					"id":    peoplePrefix + ":Bob",
					"props": map[string]interface{}{"name": "Bob"},
					"refs":  map[string]interface{}{},
				}),
			})
			employmentDs, employmentPrefix := createTestDataset("employment",
				[]string{"MediumCorp", "LittleSweatshop", "YardSale"}, map[string]map[string]interface{}{
					"MediumCorp":      {peoplePrefix + ":employment": peoplePrefix + ":Bob"},
					"LittleSweatshop": {peoplePrefix + ":employment": peoplePrefix + ":Alice"},
				}, dsm, g, store)

			testSource := source.MultiSource{DatasetName: "people", Store: store, DatasetManager: dsm}
			srcJSON := `{ "Type" : "MultiSource", "Name" : "people", "Dependencies": [ {
							"dataset": "employment",
							"joins": [ { "dataset": "people", "predicate": "http://people/employment", "inverse": false } ]
						} ] }`

			srcConfig := map[string]interface{}{}
			_ = json.Unmarshal([]byte(srcJSON), &srcConfig)
			_ = testSource.ParseDependencies(srcConfig["Dependencies"])

			// fullsync
			var recordedEntities []server.Entity
			token := &source.MultiDatasetContinuation{}
			var lastToken source.DatasetContinuation
			testSource.StartFullSync()
			err := testSource.ReadEntities(
				token,
				1000,
				func(entities []*server.Entity, token source.DatasetContinuation) error {
					lastToken = token
					for _, e := range entities {
						recordedEntities = append(recordedEntities, *e)
					}
					return nil
				},
			)
			g.Assert(err).IsNil()
			testSource.EndFullSync()

			// now, add new Employment with refs to Bob (exists) and Franz (non-exists)
			err = employmentDs.StoreEntities([]*server.Entity{
				server.NewEntityFromMap(map[string]interface{}{
					"id":    employmentPrefix + ":YardSale",
					"props": map[string]interface{}{"name": "YardSale"},
					"refs": map[string]interface{}{
						peoplePrefix + ":employment": []string{peoplePrefix + ":Franz", peoplePrefix + ":Bob"},
					},
				}),
			})
			g.Assert(err).IsNil()

			recordedEntities = []server.Entity{}
			err = testSource.ReadEntities(
				lastToken,
				1000,
				func(entities []*server.Entity, token source.DatasetContinuation) error {
					lastToken = token
					for _, e := range entities {
						recordedEntities = append(recordedEntities, *e)
					}
					return nil
				},
			)
			g.Assert(err).IsNil()
			g.Assert(len(recordedEntities)).
				Eql(1, "YardSale change points to non-existing entity 'Franz' in people, therefore only Bob should be emitted")
			g.Assert(recordedEntities[0].ID).Eql(peoplePrefix + ":Bob")
			g.Assert(recordedEntities[0].Properties["name"]).
				Eql("Bob", "Bob exists in two datasets. making sure we dont get a merged result")
		})

		g.Describe("parseDependencies", func() {
			g.It("should translate json to config", func() {
				s := source.MultiSource{DatasetName: "person", Store: store, DatasetManager: dsm}
				srcJSON := `{
				"Type" : "MultiSource",
				"Name" : "person",
				"Dependencies": [
					{
						"dataset": "product",
						"joins": [
							{
								"dataset": "order",
								"predicate": "product-ordered",
								"inverse": true
							},
							{
								"dataset": "person",
								"predicate": "ordering-customer",
								"inverse": false
							}
						]
					},
					{
						"dataset": "order",
						"joins": [
							{
								"dataset": "person",
								"predicate": "ordering-customer",
								"inverse": false
							}
						]
					}
				]
			}`

				srcConfig := map[string]interface{}{}
				err := json.Unmarshal([]byte(srcJSON), &srcConfig)
				g.Assert(err).IsNil()
				err = s.ParseDependencies(srcConfig["Dependencies"])
				g.Assert(err).IsNil()

				g.Assert(s.Dependencies).IsNotZero()
				g.Assert(len(s.Dependencies)).Eql(2)

				dep := s.Dependencies[0]
				g.Assert(dep.Dataset).Eql("product")
				g.Assert(dep.Joins).IsNotZero()
				g.Assert(len(dep.Joins)).Eql(2)
				j := dep.Joins[0]
				g.Assert(j.Dataset).Eql("order")
				g.Assert(j.Predicate).Eql("product-ordered")
				g.Assert(j.Inverse).IsTrue()
				j = dep.Joins[1]
				g.Assert(j.Dataset).Eql("person")
				g.Assert(j.Predicate).Eql("ordering-customer")
				g.Assert(j.Inverse).IsFalse()

				dep = s.Dependencies[1]
				g.Assert(dep.Dataset).Eql("order")
				g.Assert(dep.Joins).IsNotZero()
				g.Assert(len(dep.Joins)).Eql(1)
				j = dep.Joins[0]
				g.Assert(j.Dataset).Eql("person")
				g.Assert(j.Predicate).Eql("ordering-customer")
				g.Assert(j.Inverse).IsFalse()
			})
			g.It("Should fail if main dataset is proxy dataset", func() {
				// create main dataset as proxy dataset
				_, err := dsm.CreateDataset("people", &server.CreateDatasetConfig{
					ProxyDatasetConfig: &server.ProxyDatasetConfig{
						RemoteURL: "http://localhost:7777/datasets/people",
					},
				})
				g.Assert(err).IsNil()

				// now instantiate (simulating job start)
				testSource := source.MultiSource{DatasetName: "people", Store: store, DatasetManager: dsm}
				srcJSON := `{ "Type" : "MultiSource", "Name" : "people", "Dependencies": [
                          {
							"dataset": "address",
							"joins": [
							  { "dataset": "office", "predicate": "http://office/location", "inverse": true },
							  { "dataset": "people", "predicate": "http://office/contact", "inverse": false },
							  { "dataset": "team", "predicate": "http://team/lead", "inverse": true },
							  { "dataset": "people", "predicate": "http://team/member", "inverse": false }
							]
                          }
                        ] }`

				srcConfig := map[string]interface{}{}
				_ = json.Unmarshal([]byte(srcJSON), &srcConfig)
				err = testSource.ParseDependencies(srcConfig["Dependencies"])
				// t.Log(err)
				g.Assert(err).IsNotNil()
			})
			g.It("Should fail if a dependency is a proxy dataset", func() {
				// create dependency dataset as proxy dataset
				_, err := dsm.CreateDataset("address", &server.CreateDatasetConfig{
					ProxyDatasetConfig: &server.ProxyDatasetConfig{
						RemoteURL: "http://localhost:7777/datasets/address",
					},
				})
				g.Assert(err).IsNil()

				// now instantiate (simulating job start)
				testSource := source.MultiSource{DatasetName: "people", Store: store, DatasetManager: dsm}
				srcJSON := `{ "Type" : "MultiSource", "Name" : "people", "Dependencies": [
                          {
							"dataset": "address",
							"joins": [
							  { "dataset": "office", "predicate": "http://office/location", "inverse": true },
							  { "dataset": "people", "predicate": "http://office/contact", "inverse": false },
							  { "dataset": "team", "predicate": "http://team/lead", "inverse": true },
							  { "dataset": "people", "predicate": "http://team/member", "inverse": false }
							]
                          }
                        ] }`

				srcConfig := map[string]interface{}{}
				_ = json.Unmarshal([]byte(srcJSON), &srcConfig)
				err = testSource.ParseDependencies(srcConfig["Dependencies"])
				// t.Log(err)
				g.Assert(err).IsNotNil()
			})
		})
	})
}

func createTestDataset(dsName string, entityNames []string, refMap map[string]map[string]interface{},
	dsm *server.DsManager, g *goblin.G, store *server.Store,
) (*server.Dataset, string) {
	dataset, err := dsm.CreateDataset(dsName, nil)
	g.Assert(err).IsNil()
	peoplePrefix, err := store.NamespaceManager.AssertPrefixMappingForExpansion("http://" + dsName + "/")
	g.Assert(err).IsNil()
	var entities []*server.Entity

	for _, entityName := range entityNames {
		entities = append(entities, server.NewEntityFromMap(map[string]interface{}{
			"id":    peoplePrefix + ":" + entityName,
			"props": map[string]interface{}{"name": entityName},
			"refs":  refMap[entityName],
		}))
	}

	err = dataset.StoreEntities(entities)
	g.Assert(err).IsNil()

	return dataset, peoplePrefix
}

func addChanges(
	dsName string,
	entityNames []string,
	dsm *server.DsManager,
	store *server.Store,
) (*server.Dataset, string) {
	dataset := dsm.GetDataset(dsName)
	peoplePrefix, _ := store.NamespaceManager.AssertPrefixMappingForExpansion("http://" + dsName + "/")
	var entities []*server.Entity

	for _, entityName := range entityNames {
		entities = append(entities, server.NewEntityFromMap(map[string]interface{}{
			"id":    peoplePrefix + ":" + entityName,
			"props": map[string]interface{}{"name": entityName, "changed": "true"},
			"refs":  map[string]interface{}{},
		}))
	}

	_ = dataset.StoreEntities(entities)

	return dataset, peoplePrefix
}
