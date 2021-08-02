package source_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/DataDog/datadog-go/statsd"
	"github.com/franela/goblin"
	"go.uber.org/fx/fxtest"
	"go.uber.org/zap"

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
			// temp redirect of stdout and stderr to swallow some annoying init messages in fx
			devNull, _ := os.Open("/dev/null")
			oldErr := os.Stderr
			oldStd := os.Stdout
			os.Stderr = devNull
			os.Stdout = devNull

			testCnt += 1
			storeLocation = fmt.Sprintf("./test_multi_source_%v", testCnt)
			err := os.RemoveAll(storeLocation)
			g.Assert(err).IsNil("should be allowed to clean testfiles in " + storeLocation)

			e := &conf.Env{
				Logger:        zap.NewNop().Sugar(),
				StoreLocation: storeLocation,
			}
			lc := fxtest.NewLifecycle(t)

			store = server.NewStore(lc, e, &statsd.NoOpClient{})
			dsm = server.NewDsManager(lc, e, store, server.NoOpBus())

			err = lc.Start(context.Background())
			if err != nil {
				fmt.Println(err.Error())
				t.FailNow()
			}

			// undo redirect of stdout and stderr after successful init of fx
			os.Stderr = oldErr
			os.Stdout = oldStd

		})
		g.AfterEach(func() {
			_ = store.Close()
			_ = os.RemoveAll(storeLocation)
		})

		g.It("should emit changes in main dataset", func() {
			people, peoplePrefix := createTestDataset("people", []string{"Bob", "Alice"}, nil, dsm, g, store)

			testSource := source.MultiSource{DatasetName: "people", Store: store, DatasetManager: dsm}
			var tokens []source.DatasetContinuation
			var recordedEntities []server.Entity
			token := &source.MultiDatasetContinuation{}
			testSource.StartFullSync()
			err := testSource.ReadEntities(token, 1000, func(entities []*server.Entity, token source.DatasetContinuation) error {
				tokens = append(tokens, token)
				for _, e := range entities {
					recordedEntities = append(recordedEntities, *e)
				}
				return nil
			})
			g.Assert(err).IsNil()
			testSource.EndFullSync()

			//now, modify alice and verify that we get alice emitted in next read
			err = people.StoreEntities([]*server.Entity{
				server.NewEntityFromMap(map[string]interface{}{
					"id":    peoplePrefix + ":Alice",
					"props": map[string]interface{}{"name": "Alice-changed"},
					"refs":  map[string]interface{}{},
				}),
			})
			since := tokens[len(tokens)-1]
			tokens = []source.DatasetContinuation{}
			recordedEntities = []server.Entity{}
			err = testSource.ReadEntities(since, 1000, func(entities []*server.Entity, token source.DatasetContinuation) error {
				tokens = append(tokens, token)
				for _, e := range entities {
					recordedEntities = append(recordedEntities, *e)
				}
				return nil
			})
			g.Assert(err).IsNil()
			g.Assert(len(recordedEntities)).Eql(1)
			g.Assert(recordedEntities[0].Properties["name"]).Eql("Alice-changed")
		})

		g.It("should emit changes in direct dependencies", func() {
			g.Timeout(1 * time.Hour)
			addresses, addressPrefix := createTestDataset("address", []string{"Mainstreet", "Sidealley"}, nil, dsm, g, store)
			peoplePrefix, _ := store.NamespaceManager.AssertPrefixMappingForExpansion("http://people/")
			createTestDataset("people", []string{"Bob", "Alice"}, map[string]map[string]interface{}{
				"Bob":   {peoplePrefix+":address": addressPrefix + ":Mainstreet"},
				"Alice": {peoplePrefix+":address": addressPrefix + ":Sidealley"},
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

			var tokens []source.DatasetContinuation
			var recordedEntities []server.Entity
			token := &source.MultiDatasetContinuation{}

			testSource.StartFullSync()
			err = testSource.ReadEntities(token, 1000, func(entities []*server.Entity, token source.DatasetContinuation) error {
				tokens = append(tokens, token)
				for _, e := range entities {
					recordedEntities = append(recordedEntities, *e)
				}
				return nil
			})
			g.Assert(err).IsNil()
			testSource.EndFullSync()

			//now, modify Mainstreet address and verify that we get Bob emitted in next read (mainstreet is direct dependency to bob)
			err = addresses.StoreEntities([]*server.Entity{
				server.NewEntityFromMap(map[string]interface{}{
					"id":    addressPrefix + ":Mainstreet",
					"props": map[string]interface{}{"name": "Mainstreet-changed"},
					"refs":  map[string]interface{}{},
				}),
			})
			since := tokens[len(tokens)-1]
			tokens = []source.DatasetContinuation{}
			recordedEntities = []server.Entity{}
			err = testSource.ReadEntities(since, 1000, func(entities []*server.Entity, token source.DatasetContinuation) error {
				tokens = append(tokens, token)
				for _, e := range entities {
					recordedEntities = append(recordedEntities, *e)
				}
				return nil
			})
			t.Log(tokens)
			t.Log(recordedEntities)
			g.Assert(err).IsNil()
			g.Assert(len(recordedEntities)).Eql(1)
			//Bob was emitted enchanged. up to transform to do something with bob and dependency that triggered bob's emission
			g.Assert(recordedEntities[0].Properties["name"]).Eql("Bob")
		})
	})

	g.Describe("parseDependencies", func() {
		g.It("should translate json to config", func() {
			s := source.MultiSource{}
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
	})
}

func createTestDataset(dsName string, entityNames []string, refMap map[string]map[string]interface{},
	dsm *server.DsManager, g *goblin.G, store *server.Store) (*server.Dataset, string) {
	dataset, err := dsm.CreateDataset(dsName)
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
