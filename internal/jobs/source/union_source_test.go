// Copyright 2022 MIMIRO AS
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

func TestUnionDatasetSource(t *testing.T) {
	g := goblin.Goblin(t)
	g.Describe("A UnionDatasetSource", func() {
		testCnt := 0
		var dsm *server.DsManager
		var store *server.Store
		var storeLocation string
		g.BeforeEach(func() {
			testCnt += 1
			storeLocation = fmt.Sprintf("./test_dataset_union_source_%v", testCnt)
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

		g.It("should emit changes if not in fullsync mode", func() {
			adults, adultsPrefix := createTestDataset("adults", []string{"Bob", "Alice"}, nil, dsm, store)
			children, childrenPrefix := createTestDataset("children", []string{"Jimmy", "Grace"}, nil, dsm, store)
			// add one change to each entity -> 4 changes
			addChanges("adults", []string{"Bob", "Alice"}, dsm, store)
			addChanges("children", []string{"Jimmy", "Grace"}, dsm, store)

			testSource := source.UnionDatasetSource{
				[]*source.DatasetSource{
					{DatasetName: "adults", Store: store, DatasetManager: dsm},
					{DatasetName: "children", Store: store, DatasetManager: dsm},
				},
			}
			var tokens []source.DatasetContinuation
			var recordedEntities []server.Entity
			token := &source.UnionDatasetContinuation{}
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
			g.Assert(len(recordedEntities)).Eql(8, "two entities per dataset with 2 changes each expected")

			// now, modify alice and verify that we get alice emitted in next read
			err = adults.StoreEntities([]*server.Entity{
				server.NewEntityFromMap(map[string]interface{}{
					"id":    adultsPrefix + ":Alice",
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

			// now, modify Grace and verify that we get Grace emitted in next read
			err = children.StoreEntities([]*server.Entity{
				server.NewEntityFromMap(map[string]interface{}{
					"id":    childrenPrefix + ":Grace",
					"props": map[string]interface{}{"name": "Grace-changed"},
					"refs":  map[string]interface{}{},
				}),
			})
			g.Assert(err).IsNil()
			since = tokens[len(tokens)-1]
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
			g.Assert(recordedEntities[0].Properties["name"]).Eql("Grace-changed")
		})
		g.It("should support fullsync", func() {
			createTestDataset("adults", []string{"Bob", "Alice"}, nil, dsm, store)
			createTestDataset("children", []string{"Jimmy", "Grace"}, nil, dsm, store)
			// add one change to each entity -> 4 changes
			addChanges("adults", []string{"Bob", "Alice"}, dsm, store)
			addChanges("children", []string{"Jimmy", "Grace"}, dsm, store)

			testSource := source.UnionDatasetSource{
				[]*source.DatasetSource{
					{DatasetName: "adults", Store: store, DatasetManager: dsm, LatestOnly: true},
					{DatasetName: "children", Store: store, DatasetManager: dsm},
				},
			}
			var tokens []source.DatasetContinuation
			var recordedEntities []server.Entity
			token := &source.UnionDatasetContinuation{}
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
			g.Assert(len(recordedEntities)).Eql(4, "two entities per dataset in latest state")
		})
		g.It("should respect latestOnly", func() {
			createTestDataset("adults", []string{"Bob", "Alice"}, nil, dsm, store)
			createTestDataset("children", []string{"Jimmy", "Grace"}, nil, dsm, store)
			// add one change to each entity -> 4 changes
			addChanges("adults", []string{"Bob", "Alice"}, dsm, store)
			addChanges("children", []string{"Jimmy", "Grace"}, dsm, store)

			testSource := source.UnionDatasetSource{
				[]*source.DatasetSource{
					{DatasetName: "adults", Store: store, DatasetManager: dsm, LatestOnly: true},
					{DatasetName: "children", Store: store, DatasetManager: dsm},
				},
			}
			var tokens []source.DatasetContinuation
			var recordedEntities []server.Entity
			token := &source.UnionDatasetContinuation{}
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
			g.Assert(len(recordedEntities)).Eql(6, "two entities per dataset, incl 2 changes in children")
		})

		g.It("should respect batch sizes", func() {
			createTestDataset("adults", []string{"Bob", "Alice", "a", "b", "c"}, nil, dsm, store)
			createTestDataset(
				"children",
				[]string{"Jimmy", "Grace", "1", "2", "3", "4", "5", "6", "7", "8"},
				nil,
				dsm,
				store,
			)

			testSource := source.UnionDatasetSource{
				[]*source.DatasetSource{
					{DatasetName: "adults", Store: store, DatasetManager: dsm},
					{DatasetName: "children", Store: store, DatasetManager: dsm},
				},
			}
			var tokens []source.DatasetContinuation
			var recordedEntities []server.Entity
			token := &source.UnionDatasetContinuation{}
			batchCount := 0
			err := testSource.ReadEntities(
				token,
				3,
				func(entities []*server.Entity, token source.DatasetContinuation) error {
					tokens = append(tokens, token)
					for _, e := range entities {
						recordedEntities = append(recordedEntities, *e)
					}
					batchCount = batchCount + 1
					return nil
				},
			)
			g.Assert(err).IsNil()
			g.Assert(len(recordedEntities)).Eql(15)
			g.Assert(batchCount).Eql(7, "2 batches from adults, 4 from children, plus final empty batch.")
		})

		g.It("should recognize it's tokens", func() {
			createTestDataset("adults", []string{"Bob", "Alice"}, nil, dsm, store)
			createTestDataset("children", []string{"Jimmy", "Grace"}, nil, dsm, store)

			testSource := source.UnionDatasetSource{
				[]*source.DatasetSource{
					{DatasetName: "adults", Store: store, DatasetManager: dsm},
					{DatasetName: "children", Store: store, DatasetManager: dsm},
				},
			}
			var tokens []source.DatasetContinuation
			var recordedEntities []server.Entity
			startToken := source.UnionDatasetContinuation{}
			err := testSource.ReadEntities(
				&startToken,
				3,
				func(entities []*server.Entity, token source.DatasetContinuation) error {
					tokens = append(tokens, token)
					for _, e := range entities {
						recordedEntities = append(recordedEntities, *e)
					}
					return nil
				},
			)
			g.Assert(err).IsNil()
			g.Assert(len(recordedEntities)).Eql(4)

			// run with last returned token and no changes
			startToken = *tokens[len(tokens)-1].(*source.UnionDatasetContinuation)
			recordedEntities = make([]server.Entity, 0)
			err = testSource.ReadEntities(
				&startToken,
				3,
				func(entities []*server.Entity, token source.DatasetContinuation) error {
					tokens = append(tokens, token)
					for _, e := range entities {
						recordedEntities = append(recordedEntities, *e)
					}
					return nil
				},
			)
			g.Assert(err).IsNil()
			g.Assert(len(recordedEntities)).Eql(0)
			g.Assert(startToken).Eql(*(tokens[len(tokens)-1].(*source.UnionDatasetContinuation)))

			// add a change to adults
			addChanges("adults", []string{"Bob", "Alice"}, dsm, store)
			startToken = *tokens[len(tokens)-1].(*source.UnionDatasetContinuation)
			recordedEntities = make([]server.Entity, 0)
			err = testSource.ReadEntities(
				&startToken,
				3,
				func(entities []*server.Entity, token source.DatasetContinuation) error {
					tokens = append(tokens, token)
					for _, e := range entities {
						recordedEntities = append(recordedEntities, *e)
					}
					return nil
				},
			)
			g.Assert(err).IsNil()
			g.Assert(len(recordedEntities)).Eql(2)
			g.Assert(recordedEntities[0].Properties["name"]).Eql("Bob")
			g.Assert(recordedEntities[1].Properties["name"]).Eql("Alice")

			// add a change to 2nd ds
			addChanges("children", []string{"Jimmy"}, dsm, store)
			startToken = *tokens[len(tokens)-1].(*source.UnionDatasetContinuation)
			recordedEntities = make([]server.Entity, 0)
			err = testSource.ReadEntities(
				&startToken,
				3,
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

			cont, _ := (tokens[len(tokens)-1].(*source.UnionDatasetContinuation)).Encode()
			g.Assert(cont).
				Eql("{\"Tokens\":[{\"Token\":\"4\"},{\"Token\":\"3\"}],\"DatasetNames\":[\"adults\",\"children\"]}")

			// simulate order change in source config
			startToken = *tokens[len(tokens)-1].(*source.UnionDatasetContinuation)
			startToken.DatasetNames = []string{"children", "adults"}
			recordedEntities = make([]server.Entity, 0)
			err = testSource.ReadEntities(
				&startToken,
				3,
				func(entities []*server.Entity, token source.DatasetContinuation) error {
					tokens = append(tokens, token)
					for _, e := range entities {
						recordedEntities = append(recordedEntities, *e)
					}
					return nil
				},
			)
			g.Assert(err).IsNotZero("should fail due to changed order of datasets")
			g.Assert(len(recordedEntities)).Eql(0)
		})
	})
}
