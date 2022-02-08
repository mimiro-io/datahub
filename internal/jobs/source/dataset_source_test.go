package source_test

import (
	"context"
	"fmt"
	"github.com/DataDog/datadog-go/statsd"
	"github.com/franela/goblin"
	"github.com/mimiro-io/datahub/internal/conf"
	"github.com/mimiro-io/datahub/internal/jobs/source"
	"github.com/mimiro-io/datahub/internal/server"
	"go.uber.org/fx/fxtest"
	"go.uber.org/zap"
	"os"
	"testing"
)

func TestDatasetSource(t *testing.T) {
	g := goblin.Goblin(t)
	g.Describe("a datasetSource", func() {
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
			storeLocation = fmt.Sprintf("./test_dataset_source_%v", testCnt)
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

		g.It("should emit changes if not in fullsync mode", func() {
			people, peoplePrefix := createTestDataset("people", []string{"Bob", "Alice"}, nil, dsm, g, store)
			// add one change to each entity -> 4 changes
			addChanges("people", []string{"Bob", "Alice"}, dsm, store)

			testSource := source.DatasetSource{DatasetName: "people", Store: store, DatasetManager: dsm}
			var tokens []source.DatasetContinuation
			var recordedEntities []server.Entity
			token := &source.StringDatasetContinuation{}
			//testSource.StartFullSync()
			err := testSource.ReadEntities(token, 1000, func(entities []*server.Entity, token source.DatasetContinuation) error {
				tokens = append(tokens, token)
				for _, e := range entities {
					recordedEntities = append(recordedEntities, *e)
				}
				return nil
			})
			g.Assert(err).IsNil()
			testSource.EndFullSync()
			g.Assert(len(recordedEntities)).Eql(4, "two entities with 2 changes each expected")

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

		g.It("should emit entities (latest) if in fullsync mode", func() {
			people, peoplePrefix := createTestDataset("people", []string{"Bob", "Alice"}, nil, dsm, g, store)
			// add one change to each entity -> 4 changes, still 2 entities
			addChanges("people", []string{"Bob", "Alice"}, dsm, store)

			testSource := source.DatasetSource{DatasetName: "people", Store: store, DatasetManager: dsm}
			var tokens []source.DatasetContinuation
			var recordedEntities []server.Entity
			token := &source.StringDatasetContinuation{}
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
			g.Assert(len(recordedEntities)).Eql(2, "two entities")

			// changing an existing entity would not emit anything. but new entities should be emitted
			err = people.StoreEntities([]*server.Entity{
				server.NewEntityFromMap(map[string]interface{}{
					"id":    peoplePrefix + ":Alice",
					"props": map[string]interface{}{"name": "Alice-changed"},
					"refs":  map[string]interface{}{},
				}),
				server.NewEntityFromMap(map[string]interface{}{
					"id":    peoplePrefix + ":Jim",
					"props": map[string]interface{}{"name": "Jim-new"},
					"refs":  map[string]interface{}{},
				}),
			})
			g.Assert(err).IsNil()
			since := tokens[len(tokens)-1]
			tokens = []source.DatasetContinuation{}
			recordedEntities = []server.Entity{}
			testSource.StartFullSync()
			err = testSource.ReadEntities(since, 1000, func(entities []*server.Entity, token source.DatasetContinuation) error {
				tokens = append(tokens, token)
				for _, e := range entities {
					recordedEntities = append(recordedEntities, *e)
				}
				return nil
			})
			g.Assert(err).IsNil()
			testSource.EndFullSync()
			g.Assert(len(recordedEntities)).Eql(1)
			g.Assert(recordedEntities[0].Properties["name"]).Eql("Jim-new")
		})

		g.It("should emit only latest changes if not in fullsync mode", func() {
			// prime dataset with 2 entities
			people, peoplePrefix := createTestDataset("people", []string{"Bob", "Alice"}, nil, dsm, g, store)
			// add one change to each entity -> 4 changes
			addChanges("people", []string{"Bob", "Alice"}, dsm, store)

			testSource := source.DatasetSource{DatasetName: "people", Store: store, DatasetManager: dsm,
				LatestOnly: true}
			var tokens []source.DatasetContinuation
			var recordedEntities []server.Entity
			token := &source.StringDatasetContinuation{}
			err := testSource.ReadEntities(token, 1000, func(entities []*server.Entity, token source.DatasetContinuation) error {
				tokens = append(tokens, token)
				for _, e := range entities {
					recordedEntities = append(recordedEntities, *e)
				}
				return nil
			})
			g.Assert(err).IsNil()
			testSource.EndFullSync()
			g.Assert(len(recordedEntities)).Eql(2, "There are 4 changes present, we expect only 2 (latest) changes emitted")

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

	})
}
