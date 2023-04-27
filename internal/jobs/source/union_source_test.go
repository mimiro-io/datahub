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

var _ = Describe("A UnionDatasetSource", func() {
	testCnt := 0
	var dsm *server.DsManager
	var store *server.Store
	var storeLocation string
	BeforeEach(func() {
		testCnt += 1
		storeLocation = fmt.Sprintf("./test_dataset_union_source_%v", testCnt)
		err := os.RemoveAll(storeLocation)
		Expect(err).To(BeNil(), "should be allowed to clean testfiles in "+storeLocation)

		e := &conf.Env{
			Logger:        zap.NewNop().Sugar(),
			StoreLocation: storeLocation,
		}
		lc := fxtest.NewLifecycle(internal.FxTestLog(GinkgoT(), false))

		store = server.NewStore(lc, e, &statsd.NoOpClient{})
		dsm = server.NewDsManager(lc, e, store, server.NoOpBus())

		err = lc.Start(context.Background())
		if err != nil {
			Fail(err.Error())
		}
	})
	AfterEach(func() {
		_ = store.Close()
		_ = os.RemoveAll(storeLocation)
	})

	It("should emit changes if not in fullsync mode", func() {
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
		Expect(err).To(BeNil())
		Expect(len(recordedEntities)).To(Equal(8), "two entities per dataset with 2 changes each expected")

		// now, modify alice and verify that we get alice emitted in next read
		err = adults.StoreEntities([]*server.Entity{
			server.NewEntityFromMap(map[string]interface{}{
				"id":    adultsPrefix + ":Alice",
				"props": map[string]interface{}{"name": "Alice-changed"},
				"refs":  map[string]interface{}{},
			}),
		})
		Expect(err).To(BeNil())
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
		Expect(err).To(BeNil())
		Expect(len(recordedEntities)).To(Equal(1))
		Expect(recordedEntities[0].Properties["name"]).To(Equal("Alice-changed"))

		// now, modify Grace and verify that we get Grace emitted in next read
		err = children.StoreEntities([]*server.Entity{
			server.NewEntityFromMap(map[string]interface{}{
				"id":    childrenPrefix + ":Grace",
				"props": map[string]interface{}{"name": "Grace-changed"},
				"refs":  map[string]interface{}{},
			}),
		})
		Expect(err).To(BeNil())
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
		Expect(err).To(BeNil())
		Expect(len(recordedEntities)).To(Equal(1))
		Expect(recordedEntities[0].Properties["name"]).To(Equal("Grace-changed"))
	})
	It("should support fullsync", func() {
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
		Expect(err).To(BeNil())
		testSource.EndFullSync()
		Expect(len(recordedEntities)).To(Equal(4), "two entities per dataset in latest state")
	})
	It("should respect latestOnly", func() {
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
		Expect(err).To(BeNil())
		Expect(len(recordedEntities)).To(Equal(6), "two entities per dataset, incl 2 changes in children")
	})

	It("should respect batch sizes", func() {
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
		Expect(err).To(BeNil())
		Expect(len(recordedEntities)).To(Equal(15))
		Expect(batchCount).To(Equal(7), "2 batches from adults, 4 from children, plus final empty batch.")
	})

	It("should recognize it's tokens", func() {
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
		Expect(err).To(BeNil())
		Expect(len(recordedEntities)).To(Equal(4))

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
		Expect(err).To(BeNil())
		Expect(len(recordedEntities)).To(Equal(0))
		Expect(startToken).To(Equal(*(tokens[len(tokens)-1].(*source.UnionDatasetContinuation))))

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
		Expect(err).To(BeNil())
		Expect(len(recordedEntities)).To(Equal(2))
		Expect(recordedEntities[0].Properties["name"]).To(Equal("Bob"))
		Expect(recordedEntities[1].Properties["name"]).To(Equal("Alice"))

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
		Expect(err).To(BeNil())
		Expect(len(recordedEntities)).To(Equal(1))

		cont, _ := (tokens[len(tokens)-1].(*source.UnionDatasetContinuation)).Encode()
		Expect(cont).
			To(Equal("{\"Tokens\":[{\"Token\":\"4\"},{\"Token\":\"3\"}],\"DatasetNames\":[\"adults\",\"children\"]}"))

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
		Expect(err).NotTo(BeZero(), "should fail due to changed order of datasets")
		Expect(len(recordedEntities)).To(Equal(0))
	})
})
