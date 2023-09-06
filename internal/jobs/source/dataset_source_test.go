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
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/fx/fxtest"
	"go.uber.org/zap"

	"github.com/mimiro-io/datahub/internal"
	"github.com/mimiro-io/datahub/internal/conf"
	"github.com/mimiro-io/datahub/internal/jobs/source"
	"github.com/mimiro-io/datahub/internal/server"
)

func TestDatasetSource(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Source Suite")
}

var _ = Describe("a datasetSource", func() {
	testCnt := 0
	var dsm *server.DsManager
	var store *server.Store
	var storeLocation string
	var ctx context.Context
	BeforeEach(func() {
		testCnt += 1
		storeLocation = fmt.Sprintf("./test_dataset_source_%v", testCnt)
		err := os.RemoveAll(storeLocation)
		Expect(err).To(BeNil(), "should be allowed to clean testfiles in "+storeLocation)

		e := &conf.Env{
			Logger:        zap.NewNop().Sugar(),
			StoreLocation: storeLocation,
		}
		lc := fxtest.NewLifecycle(internal.FxTestLog(GinkgoT(), false))

		store = server.NewStore(lc, e, &statsd.NoOpClient{})
		dsm = server.NewDsManager(lc, e, store, server.NoOpBus())
		ctx = context.Background()
		err = lc.Start(ctx)
		if err != nil {
			fmt.Println(err.Error())
			Fail(err.Error())

		}
	})
	AfterEach(func() {
		_ = store.Close()
		_ = os.RemoveAll(storeLocation)
	})

	It("should emit changes if not in fullsync mode", func() {
		people, peoplePrefix := createTestDataset("people", []string{"Bob", "Alice"}, nil, dsm, store)
		// add one change to each entity -> 4 changes
		addChanges("people", []string{"Bob", "Alice"}, dsm, store)

		testSource := source.DatasetSource{DatasetName: "people", Store: store, DatasetManager: dsm}
		var tokens []source.DatasetContinuation
		var recordedEntities []server.Entity
		token := &source.StringDatasetContinuation{}
		// testSource.StartFullSync()
		err := testSource.ReadEntities(ctx, token, 1000, func(entities []*server.Entity, token source.DatasetContinuation) error {
			tokens = append(tokens, token)
			for _, e := range entities {
				recordedEntities = append(recordedEntities, *e)
			}
			return nil
		})
		Expect(err).To(BeNil())
		testSource.EndFullSync()
		Expect(len(recordedEntities)).To(Equal(4), "two entities with 2 changes each expected")

		// now, modify alice and verify that we get alice emitted in next read
		err = people.StoreEntities([]*server.Entity{
			server.NewEntityFromMap(map[string]interface{}{
				"id":    peoplePrefix + ":Alice",
				"props": map[string]interface{}{"name": "Alice-changed"},
				"refs":  map[string]interface{}{},
			}),
		})
		Expect(err).To(BeNil())
		since := tokens[len(tokens)-1]
		tokens = []source.DatasetContinuation{}
		recordedEntities = []server.Entity{}
		err = testSource.ReadEntities(ctx, since, 1000, func(entities []*server.Entity, token source.DatasetContinuation) error {
			tokens = append(tokens, token)
			for _, e := range entities {
				recordedEntities = append(recordedEntities, *e)
			}
			return nil
		})
		Expect(err).To(BeNil())
		Expect(len(recordedEntities)).To(Equal(1))
		Expect(recordedEntities[0].Properties["name"]).To(Equal("Alice-changed"))
	})

	It("should emit entities (latest) if in fullsync mode", func() {
		people, peoplePrefix := createTestDataset("people", []string{"Bob", "Alice"}, nil, dsm, store)
		// add one change to each entity -> 4 changes, still 2 entities
		addChanges("people", []string{"Bob", "Alice"}, dsm, store)

		testSource := source.DatasetSource{DatasetName: "people", Store: store, DatasetManager: dsm}
		var tokens []source.DatasetContinuation
		var recordedEntities []server.Entity
		token := &source.StringDatasetContinuation{}
		testSource.StartFullSync()
		err := testSource.ReadEntities(ctx, token, 1000, func(entities []*server.Entity, token source.DatasetContinuation) error {
			tokens = append(tokens, token)
			for _, e := range entities {
				recordedEntities = append(recordedEntities, *e)
			}
			return nil
		})
		Expect(err).To(BeNil())
		testSource.EndFullSync()
		Expect(len(recordedEntities)).To(Equal(2), "two entities")

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
		Expect(err).To(BeNil())
		since := tokens[len(tokens)-1]
		tokens = []source.DatasetContinuation{}
		recordedEntities = []server.Entity{}
		testSource.StartFullSync()
		err = testSource.ReadEntities(ctx, since, 1000, func(entities []*server.Entity, token source.DatasetContinuation) error {
			tokens = append(tokens, token)
			for _, e := range entities {
				recordedEntities = append(recordedEntities, *e)
			}
			return nil
		})
		Expect(err).To(BeNil())
		testSource.EndFullSync()
		Expect(len(recordedEntities)).To(Equal(1))
		Expect(recordedEntities[0].Properties["name"]).To(Equal("Jim-new"))
	})

	It("should emit only latest changes if not in fullsync mode", func() {
		// prime dataset with 2 entities
		people, peoplePrefix := createTestDataset("people", []string{"Bob", "Alice"}, nil, dsm, store)
		// add one change to each entity -> 4 changes
		addChanges("people", []string{"Bob", "Alice"}, dsm, store)

		testSource := source.DatasetSource{
			DatasetName: "people", Store: store, DatasetManager: dsm,
			LatestOnly: true,
		}
		var tokens []source.DatasetContinuation
		var recordedEntities []server.Entity
		token := &source.StringDatasetContinuation{}
		err := testSource.ReadEntities(ctx, token, 1000, func(entities []*server.Entity, token source.DatasetContinuation) error {
			tokens = append(tokens, token)
			for _, e := range entities {
				recordedEntities = append(recordedEntities, *e)
			}
			return nil
		})
		Expect(err).To(BeNil())
		testSource.EndFullSync()
		Expect(len(recordedEntities)).
			To(Equal(2), "There are 4 changes present, we expect only 2 (latest) changes emitted")

		// now, modify alice and verify that we get alice emitted in next read
		err = people.StoreEntities([]*server.Entity{
			server.NewEntityFromMap(map[string]interface{}{
				"id":    peoplePrefix + ":Alice",
				"props": map[string]interface{}{"name": "Alice-changed"},
				"refs":  map[string]interface{}{},
			}),
		})
		Expect(err).To(BeNil())
		since := tokens[len(tokens)-1]
		tokens = []source.DatasetContinuation{}
		recordedEntities = []server.Entity{}
		err = testSource.ReadEntities(ctx, since, 1000, func(entities []*server.Entity, token source.DatasetContinuation) error {
			tokens = append(tokens, token)
			for _, e := range entities {
				recordedEntities = append(recordedEntities, *e)
			}
			return nil
		})
		Expect(err).To(BeNil())
		Expect(len(recordedEntities)).To(Equal(1))
		Expect(recordedEntities[0].Properties["name"]).To(Equal("Alice-changed"))
	})
})