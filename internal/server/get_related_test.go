// Copyright 2023 MIMIRO AS
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

	"github.com/DataDog/datadog-go/v5/statsd"
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/fx/fxtest"
	"go.uber.org/zap"

	"github.com/mimiro-io/datahub/internal"
	"github.com/mimiro-io/datahub/internal/conf"
)

var _ = ginkgo.Describe("GetManyRelatedEntitiesBatch", func() {
	testCnt := 0
	var dsm *DsManager
	var store *Store
	var storeLocation string
	ginkgo.BeforeEach(func() {
		testCnt += 1
		storeLocation = fmt.Sprintf("./test_get_relations_%v", testCnt)
		err := os.RemoveAll(storeLocation)
		Expect(err).To(BeNil(), "should be allowed to clean testfiles in "+storeLocation)
		e := &conf.Env{
			Logger:        zap.NewNop().Sugar(),
			StoreLocation: storeLocation,
		}
		lc := fxtest.NewLifecycle(internal.FxTestLog(ginkgo.GinkgoT(), false))
		store = NewStore(lc, e, &statsd.NoOpClient{})
		dsm = NewDsManager(lc, e, store, NoOpBus())

		err = lc.Start(context.Background())
		Expect(err).To(BeNil())
	})
	ginkgo.AfterEach(func() {
		_ = store.Close()
		_ = os.RemoveAll(storeLocation)
	})

	_ = ginkgo.Describe("limiting", func() {
		ginkgo.It("Should respect limit for single start uri", func() {
			peopleNamespacePrefix := persist("friends", store, dsm, buildTestBatch(store, []testPerson{
				{id: 1, friends: []int{0}},
				{id: 2, friends: []int{1, 4}},
				{id: 3, deleted: true, friends: []int{2}},
				{id: 0, friends: []int{1, 2, 3, 4}},
				{id: 3, friends: []int{2}},
				{id: 0, deleted: true, friends: []int{1, 2, 3, 4}},
				{id: 1, friends: []int{3, 2, 4}},
			}))
			start := []string{peopleNamespacePrefix + ":person-1"}
			result, err := store.GetManyRelatedEntitiesBatch(start, "*", false, nil, 0, true)
			Expect(err).To(BeNil())
			Expect(result.Cont).To(BeZero())
			Expect(len(result.Relations)).To(Equal(3), "person-1 points to p2, p3 and (empty) p4")
			Expect(result.Relations[0].RelatedEntity.ID).To(Equal(peopleNamespacePrefix + ":person-3"))
			Expect(result.Relations[0].RelatedEntity.Recorded).NotTo(BeZero(), "make sure person-3 is an real entity")
			Expect(result.Relations[1].RelatedEntity.ID).To(Equal(peopleNamespacePrefix + ":person-4"))
			Expect(result.Relations[1].RelatedEntity.Recorded).To(BeZero(), "make sure person-4 is an open ref")
			Expect(result.Relations[2].RelatedEntity.ID).To(Equal(peopleNamespacePrefix + ":person-2"))
			Expect(result.Relations[2].RelatedEntity.Recorded).NotTo(BeZero(), "make sure person-2 is an real entity")

			result, err = store.GetManyRelatedEntitiesBatch(start, "*", false, nil, 1, true)
			Expect(err).To(BeNil())
			Expect(len(result.Cont)).To(Equal(1))
			Expect(len(result.Relations)).To(Equal(1))

			result, err = store.GetManyRelatedEntitiesBatch(start, "*", false, nil, 2, true)
			Expect(err).To(BeNil())
			Expect(len(result.Cont)).To(Equal(1))
			Expect(len(result.Relations)).To(Equal(2))

			result, err = store.GetManyRelatedEntitiesBatch(start, "*", false, nil, 3, true)
			Expect(err).To(BeNil())
			Expect(len(result.Cont)).To(Equal(0))
			Expect(len(result.Relations)).To(Equal(3))

			result, err = store.GetManyRelatedEntitiesBatch(start, "*", false, nil, 4, true)
			Expect(err).To(BeNil())
			Expect(len(result.Cont)).To(Equal(0))
			Expect(len(result.Relations)).To(Equal(3))
		})
		ginkgo.It("Should respect limit for many start uris", func() {
			peopleNamespacePrefix := persist("friends", store, dsm, buildTestBatch(store, []testPerson{
				{id: 1, friends: []int{0}},
				{id: 2, friends: []int{1, 4}},
				{id: 3, deleted: true, friends: []int{2}},
				{id: 0, friends: []int{1, 2, 3, 4}},
				{id: 3, friends: []int{2}},
				{id: 0, deleted: true, friends: []int{1, 2, 3, 4}},
				{id: 1, friends: []int{3, 2, 4}},
			}))
			// get everything
			start := []string{peopleNamespacePrefix + ":person-1", peopleNamespacePrefix + ":person-2"}
			result, err := store.GetManyRelatedEntitiesBatch(start, "*", false, nil, 0, true)
			Expect(err).To(BeNil())
			Expect(len(result.Cont)).To(BeZero())
			// 3 relations from person-1 and 2 relations from person-2
			Expect(len(result.Relations)).To(Equal(5))

			// limit 1, should return first hit of first startUri
			result, err = store.GetManyRelatedEntitiesBatch(start, "*", false, nil, 1, true)
			Expect(err).To(BeNil())
			// continuations for both startUris expected
			Expect(len(result.Cont)).To(Equal(2))
			Expect(len(result.Relations)).To(Equal(1))

			// limit 3, room for all 3 relations of first startUri
			result, err = store.GetManyRelatedEntitiesBatch(start, "*", false, nil, 3, true)
			Expect(err).To(BeNil())
			Expect(len(result.Relations)).To(Equal(3))
			// only continuation for person-2 remains, since first startUri is exhausted
			Expect(len(result.Cont)).To(Equal(1))

			// limit 4, room for all 3 relations of first startUri plus 1 from other startUri
			result, err = store.GetManyRelatedEntitiesBatch(start, "*", false, nil, 4, true)
			Expect(err).To(BeNil())
			Expect(len(result.Relations)).To(Equal(4))
			// only continuation for person-2 remains, since first startUri is exhausted
			Expect(len(result.Cont)).To(Equal(1))

			// limit 5, room for all 3 relations of first startUri plus all 2 from other startUri
			result, err = store.GetManyRelatedEntitiesBatch(start, "*", false, nil, 5, true)
			Expect(err).To(BeNil())
			Expect(len(result.Relations)).To(Equal(5))
			// only continuation for person-2 remains, since first startUri is exhausted
			Expect(len(result.Cont)).To(Equal(0))

			// limit 6, room for all 3 relations of first startUri plus all 2 from other startUri
			result, err = store.GetManyRelatedEntitiesBatch(start, "*", false, nil, 6, true)
			Expect(err).To(BeNil())
			Expect(len(result.Relations)).To(Equal(5))
			// only continuation for person-2 remains, since first startUri is exhausted
			Expect(len(result.Cont)).To(Equal(0))
		})
		ginkgo.It("Should respect limit in inverse queries", func() {
			peopleNamespacePrefix := persist("friends", store, dsm, buildTestBatch(store, []testPerson{
				{id: 1, friends: []int{0}},
				{id: 2, friends: []int{1, 4}},
				{id: 3, deleted: true, friends: []int{2}},
				{id: 0, friends: []int{1, 2, 3, 4}},
				{id: 3, friends: []int{2}},
				{id: 0, deleted: true, friends: []int{1, 2, 3, 4}},
				{id: 1, friends: []int{3, 2, 4}},
			}))
			// get everything, p2 points to p1; and p1+p3 point to p2
			start := []string{peopleNamespacePrefix + ":person-1", peopleNamespacePrefix + ":person-2"}
			result, err := store.GetManyRelatedEntitiesBatch(start, "*", true, nil, 0, true)
			Expect(err).To(BeNil())
			Expect(result.Cont).To(BeZero())
			Expect(len(result.Relations)).To(Equal(3))
			// limit 1, should return first hit of first startUri
			result, err = store.GetManyRelatedEntitiesBatch(start, "*", true, nil, 1, true)
			Expect(err).To(BeNil())
			// person-1 is exhausted, only expect one cont token
			Expect(len(result.Cont)).To(Equal(1))
			Expect(len(result.Relations)).To(Equal(1))

			// limit 2, room for all 1 relations of first startUri plus 1 from other startUri
			result, err = store.GetManyRelatedEntitiesBatch(start, "*", true, nil, 2, true)
			Expect(err).To(BeNil())
			// one token for rest of person-2 expected
			Expect(len(result.Cont)).To(Equal(1))
			Expect(len(result.Relations)).To(Equal(2))
		})
	})
	ginkgo.It("Should return a working continuation token", func() {
		pref := persist("friends", store, dsm, buildTestBatch(store, []testPerson{
			{id: 1, friends: []int{2, 3, 4, 5, 6, 7, 8, 9, 10}},
			{id: 2, friends: []int{11, 12, 13, 14, 15, 16, 17, 18, 19, 20}},
		}))
		start := []string{pref + ":person-1", pref + ":person-2"}
		queryResult, err := store.GetManyRelatedEntitiesBatch(start, "*", false, nil, 0, true)
		Expect(err).To(BeNil())
		Expect(len(queryResult.Cont)).To(Equal(0))
		Expect(len(queryResult.Relations)).To(Equal(19))

		queryResult, err = store.GetManyRelatedEntitiesBatch(start, "*", false, nil, 2, true)
		Expect(err).To(BeNil())
		Expect(len(queryResult.Cont)).To(Equal(2))
		Expect(len(queryResult.Relations)).To(Equal(2))
		Expect(queryResult.Relations[0].RelatedEntity.ID).To(Equal(pref + ":person-10"))
		Expect(queryResult.Relations[1].RelatedEntity.ID).To(Equal(pref + ":person-9"))
		Expect(queryResult.Cont[0].RelationIndexFromKey).NotTo(BeNil())
		startFromCont := queryResult.Cont
		queryResult, err = store.GetManyRelatedEntitiesAtTime(startFromCont, 3, true)
		Expect(err).To(BeNil())
		Expect(len(queryResult.Cont)).To(Equal(2))
		Expect(len(queryResult.Relations)).To(Equal(3))
		Expect(queryResult.Relations[0].RelatedEntity.ID).To(Equal(pref + ":person-8"))
		Expect(queryResult.Relations[1].RelatedEntity.ID).To(Equal(pref + ":person-7"))
		Expect(queryResult.Relations[2].RelatedEntity.ID).To(Equal(pref + ":person-6"))
		Expect(queryResult.Cont[0].RelationIndexFromKey).NotTo(BeNil())
		Expect(queryResult.Cont[1].RelationIndexFromKey).NotTo(BeNil())
		startFromCont = queryResult.Cont

		// get get next 3, leaving exactly 1 result remaining for 1st startURI
		queryResult, err = store.GetManyRelatedEntitiesAtTime(startFromCont, 3, true)
		Expect(err).To(BeNil())
		Expect(len(queryResult.Cont)).To(Equal(2))
		Expect(len(queryResult.Relations)).To(Equal(3))
		Expect(queryResult.Relations[0].RelatedEntity.ID).To(Equal(pref + ":person-5"))
		Expect(queryResult.Relations[2].RelatedEntity.ID).To(Equal(pref + ":person-3"))
		queryResult, err = store.GetManyRelatedEntitiesAtTime(queryResult.Cont, 0, true)
		Expect(err).To(BeNil())
		Expect(queryResult.Cont).To(BeNil())
		Expect(len(queryResult.Relations)).To(Equal(11))
		Expect(queryResult.Relations[0].RelatedEntity.ID).To(Equal(pref + ":person-2"))
		Expect(queryResult.Relations[10].RelatedEntity.ID).To(Equal(pref + ":person-11"))

		// get all of remaining items from 1st startURI
		queryResult, err = store.GetManyRelatedEntitiesAtTime(startFromCont, 4, true)
		Expect(err).To(BeNil())
		Expect(len(queryResult.Cont)).To(Equal(1), "1st exhausted query, one remaining")
		Expect(len(queryResult.Relations)).To(Equal(4))
		Expect(queryResult.Relations[0].RelatedEntity.ID).To(Equal(pref + ":person-5"))
		Expect(queryResult.Relations[3].RelatedEntity.ID).To(Equal(pref + ":person-2"))
		queryResult, err = store.GetManyRelatedEntitiesAtTime(queryResult.Cont, 0, true)
		Expect(err).To(BeNil())
		Expect(queryResult.Cont).To(BeNil())
		Expect(len(queryResult.Relations)).To(Equal(10))
		Expect(queryResult.Relations[0].RelatedEntity.ID).To(Equal(pref + ":person-20"))
		Expect(queryResult.Relations[9].RelatedEntity.ID).To(Equal(pref + ":person-11"))

		// get all of remaining items from 1st startURI plus 1st item from 2nd startURI
		queryResult, err = store.GetManyRelatedEntitiesAtTime(startFromCont, 5, true)
		Expect(err).To(BeNil())
		Expect(len(queryResult.Cont)).To(Equal(1), "1st exhausted query, one remaining")
		Expect(len(queryResult.Relations)).To(Equal(5))
		Expect(queryResult.Relations[0].RelatedEntity.ID).To(Equal(pref + ":person-5"))
		Expect(queryResult.Relations[4].RelatedEntity.ID).To(Equal(pref + ":person-20"))
		queryResult, err = store.GetManyRelatedEntitiesAtTime(queryResult.Cont, 0, true)
		Expect(err).To(BeNil())
		Expect(queryResult.Cont).To(BeNil())
		Expect(len(queryResult.Relations)).To(Equal(9))
		Expect(queryResult.Relations[0].RelatedEntity.ID).To(Equal(pref + ":person-19"))
		Expect(queryResult.Relations[8].RelatedEntity.ID).To(Equal(pref + ":person-11"))
	})

	ginkgo.It("Should work with star queries across multiple predicates", func() {
		// add friends relations first. they will get a lower internal predicate id
		pref := persist("friends", store, dsm, buildTestBatch(store, []testPerson{{id: 1, friends: []int{2, 3}}}))
		// add family, family gets a higher internal predicate id in the outgoing relations index
		persist("friends", store, dsm,
			buildTestBatch(store, []testPerson{{id: 1, friends: []int{2, 3}, family: []int{4}}}))
		// update both relations so that person 4 is deleted in "family" and added in "friends"
		persist("friends", store, dsm,
			buildTestBatch(store, []testPerson{{id: 1, friends: []int{2, 3, 4}, family: []int{5}}}))

		// make sure we find person 4 as friend now, even though it is deleted from family.
		start := []string{pref + ":person-1"}
		queryResult, err := store.GetManyRelatedEntitiesBatch(start, "*", false, nil, 0, true)
		Expect(err).To(BeNil())
		Expect(queryResult.Relations).To(HaveLen(4))
		Expect(queryResult.Relations[0].PredicateURI).To(Equal("ns3:Family"))
		Expect(queryResult.Relations[0].RelatedEntity.ID).To(Equal("ns3:person-5"))
		Expect(queryResult.Relations[1].PredicateURI).To(Equal("ns3:Friend"))
		Expect(queryResult.Relations[1].RelatedEntity.ID).To(Equal("ns3:person-4"))
		Expect(queryResult.Relations[2].PredicateURI).To(Equal("ns3:Friend"))
		Expect(queryResult.Relations[2].RelatedEntity.ID).To(Equal("ns3:person-3"))
		Expect(queryResult.Relations[3].PredicateURI).To(Equal("ns3:Friend"))
		Expect(queryResult.Relations[3].RelatedEntity.ID).To(Equal("ns3:person-2"))
	})
	ginkgo.Describe("Should respect different deleted states for same entity written in same batch", func() {
		ginkgo.It("deleted first", func() {

			pref := persist("friends", store, dsm, buildTestBatch(store, []testPerson{
				{id: 1, deleted: true, friends: []int{2}},
				{id: 2, friends: []int{}},
				{id: 1, deleted: false, friends: []int{2}},
			}))
			start := []string{pref + ":person-2"}
			queryResult, err := store.GetManyRelatedEntitiesBatch(start, "*", true, []string{"friends"}, 0, true)
			Expect(err).To(BeNil())
			Expect(queryResult.Relations).To(HaveLen(1))
			Expect(queryResult.Relations[0].PredicateURI).To(Equal("ns3:Friend"))
			Expect(queryResult.Relations[0].RelatedEntity.ID).To(Equal("ns3:person-1"))
			start = []string{pref + ":person-1"}
			queryResult, err = store.GetManyRelatedEntitiesBatch(start, "*", false, nil, 0, true)
			Expect(err).To(BeNil())
			Expect(queryResult.Relations).To(HaveLen(1))
			Expect(queryResult.Relations[0].PredicateURI).To(Equal("ns3:Friend"))
			Expect(queryResult.Relations[0].RelatedEntity.ID).To(Equal("ns3:person-2"))
		})
		ginkgo.It("back and forth, non-deleted at end", func() {

			pref := persist("friends", store, dsm, buildTestBatch(store, []testPerson{
				{id: 1, deleted: true, friends: []int{2}},
				{id: 2, friends: []int{}},
				{id: 1, deleted: false, friends: []int{2}},
				{id: 1, deleted: true},
				{id: 1, deleted: false, friends: []int{2}},
			}))
			start := []string{pref + ":person-2"}
			queryResult, err := store.GetManyRelatedEntitiesBatch(start, "*", true, []string{"friends"}, 0, true)
			Expect(err).To(BeNil())
			Expect(queryResult.Relations).To(HaveLen(1))
			Expect(queryResult.Relations[0].PredicateURI).To(Equal("ns3:Friend"))
			Expect(queryResult.Relations[0].RelatedEntity.ID).To(Equal("ns3:person-1"))
			Expect(queryResult.Relations[0].RelatedEntity.IsDeleted).To(BeFalse())
			start = []string{pref + ":person-1"}
			queryResult, err = store.GetManyRelatedEntitiesBatch(start, "*", false, nil, 0, true)
			Expect(err).To(BeNil())
			Expect(queryResult.Relations).To(HaveLen(1))
			Expect(queryResult.Relations[0].PredicateURI).To(Equal("ns3:Friend"))
			Expect(queryResult.Relations[0].RelatedEntity.ID).To(Equal("ns3:person-2"))
		})
		ginkgo.It("deleted last", func() {
			pref := persist("friends", store, dsm, buildTestBatch(store, []testPerson{
				{id: 1, friends: []int{2}},
				{id: 2, friends: []int{}},
				{id: 1, deleted: true, friends: []int{2}},
			}))
			start := []string{pref + ":person-2"}
			queryResult, err := store.GetManyRelatedEntitiesBatch(start, "*", true, []string{"friends"}, 0, true)
			Expect(err).To(BeNil())
			Expect(queryResult.Relations).To(HaveLen(0))
			start = []string{pref + ":person-1"}
			queryResult, err = store.GetManyRelatedEntitiesBatch(start, "*", false, nil, 0, true)
			Expect(err).To(BeNil())
			Expect(queryResult.Relations).To(HaveLen(0))
		})
		ginkgo.It("back and forth, deleted at end", func() {
			pref := persist("friends", store, dsm, buildTestBatch(store, []testPerson{
				{id: 1, deleted: false, friends: []int{2}},
				{id: 1, deleted: true, friends: []int{2}},
				{id: 2, friends: []int{}},
				{id: 1, deleted: false, friends: []int{2}},
				{id: 1, deleted: true},
				{id: 1, deleted: false, friends: []int{2}},
				{id: 1, deleted: true, friends: []int{2}},
			}))
			start := []string{pref + ":person-2"}
			queryResult, err := store.GetManyRelatedEntitiesBatch(start, "*", true, []string{"friends"}, 0, true)
			Expect(err).To(BeNil())
			Expect(queryResult.Relations).To(HaveLen(0))
			start = []string{pref + ":person-1"}
			queryResult, err = store.GetManyRelatedEntitiesBatch(start, "*", false, nil, 0, true)
			Expect(err).To(BeNil())
			Expect(queryResult.Relations).To(HaveLen(0))
		})
	})
	ginkgo.Describe("Should return unified entity with dataset properties", func() {
		ginkgo.It("two datasets, both active", func() {
			pref := persist("friends", store, dsm, buildTestBatch(store, []testPerson{
				{id: 1, friends: []int{2}},
				{id: 2, friends: []int{}},
			}))
			_ = persist("family", store, dsm, buildTestBatch(store, []testPerson{
				{id: 1, friends: []int{2}},
				{id: 2, friends: []int{}},
			}))
			start := []string{pref + ":person-2"}
			queryResult, err := store.GetManyRelatedEntitiesBatch(start, "*", true, nil, 0, false)
			Expect(err).To(BeNil())
			Expect(queryResult.Relations).To(HaveLen(1))
			Expect(queryResult.Relations[0].RelatedEntity.Properties["http://data.mimiro.io/core/partials"]).To(HaveLen(2))
			Expect(queryResult.Relations[0].RelatedEntity.Properties["http://data.mimiro.io/core/partials"].([]any)[0].(*Entity).Properties["http://data.mimiro.io/core/datasetname"]).To(Equal("friends"))
			Expect(queryResult.Relations[0].RelatedEntity.Properties["http://data.mimiro.io/core/partials"].([]any)[1].(*Entity).Properties["http://data.mimiro.io/core/datasetname"]).To(Equal("family"))
		})
	})
	ginkgo.Describe("Multiple datasets and delete states, here:", func() {
		ginkgo.It("two datasets, both active", func() {
			pref := persist("friends", store, dsm, buildTestBatch(store, []testPerson{
				{id: 1, friends: []int{2}},
				{id: 2, friends: []int{}},
			}))
			_ = persist("family", store, dsm, buildTestBatch(store, []testPerson{
				{id: 1, friends: []int{2}},
				{id: 2, friends: []int{}},
			}))
			start := []string{pref + ":person-2"}
			queryResult, err := store.GetManyRelatedEntitiesBatch(start, "*", true, nil, 0, true)
			Expect(err).To(BeNil())
			Expect(queryResult.Relations).To(HaveLen(1))
			Expect(queryResult.Relations[0].RelatedEntity.ID).To(Equal("ns3:person-1"))
			Expect(queryResult.Relations[0].RelatedEntity.Properties[pref+":Name"]).To(Equal([]any{"Person 1", "Person 1"}), "merged properties expected")
			Expect(queryResult.Relations[0].RelatedEntity.IsDeleted).To(BeFalse())
			start = []string{pref + ":person-1"}
			queryResult, err = store.GetManyRelatedEntitiesBatch(start, "*", false, nil, 0, true)
			Expect(err).To(BeNil())
			Expect(queryResult.Relations).To(HaveLen(1))
			Expect(queryResult.Relations[0].RelatedEntity.ID).To(Equal("ns3:person-2"))
			Expect(queryResult.Relations[0].RelatedEntity.Properties[pref+":Name"]).To(Equal([]any{"Person 2", "Person 2"}), "merged properties expected")
		})
		ginkgo.It("two datasets, 2nd deleted", func() {
			pref := persist("friends", store, dsm, buildTestBatch(store, []testPerson{
				{id: 1, friends: []int{2}},
				{id: 2, friends: []int{}},
			}))
			_ = persist("family", store, dsm, buildTestBatch(store, []testPerson{
				{id: 1, deleted: true, friends: []int{2}},
				{id: 2, friends: []int{}},
			}))
			start := []string{pref + ":person-2"}
			queryResult, err := store.GetManyRelatedEntitiesBatch(start, "*", true, nil, 0, true)
			Expect(err).To(BeNil())
			Expect(queryResult.Relations).To(HaveLen(1))
			Expect(queryResult.Relations[0].RelatedEntity.ID).To(Equal("ns3:person-1"))
			Expect(queryResult.Relations[0].RelatedEntity.Properties[pref+":Name"]).To(Equal("Person 1"), "only one partial expected")
			Expect(queryResult.Relations[0].RelatedEntity.IsDeleted).To(BeFalse())
			start = []string{pref + ":person-1"}
			queryResult, err = store.GetManyRelatedEntitiesBatch(start, "*", false, nil, 0, true)
			Expect(err).To(BeNil())
			Expect(queryResult.Relations).To(HaveLen(1))
			Expect(queryResult.Relations[0].RelatedEntity.ID).To(Equal("ns3:person-2"))
			Expect(queryResult.Relations[0].RelatedEntity.Properties[pref+":Name"]).To(Equal([]any{"Person 2", "Person 2"}))
		})
		ginkgo.It("two datasets, 2nd deleted same batch", func() {
			pref := persist("friends", store, dsm, buildTestBatch(store, []testPerson{
				{id: 1, friends: []int{2}},
				{id: 2, friends: []int{}},
			}))
			_ = persist("family", store, dsm, buildTestBatch(store, []testPerson{
				{id: 1, friends: []int{2}},
				{id: 2, friends: []int{}},
				{id: 1, deleted: true, friends: []int{2}},
			}))
			start := []string{pref + ":person-2"}
			queryResult, err := store.GetManyRelatedEntitiesBatch(start, "*", true, nil, 0, true)
			Expect(err).To(BeNil())
			Expect(queryResult.Relations).To(HaveLen(1))
			Expect(queryResult.Relations[0].RelatedEntity.ID).To(Equal("ns3:person-1"))
			Expect(queryResult.Relations[0].RelatedEntity.Properties[pref+":Name"]).To(Equal("Person 1"), "only one partial expected")
			Expect(queryResult.Relations[0].RelatedEntity.IsDeleted).To(BeFalse())
			start = []string{pref + ":person-1"}
			queryResult, err = store.GetManyRelatedEntitiesBatch(start, "*", false, nil, 0, true)
			Expect(err).To(BeNil())
			Expect(queryResult.Relations).To(HaveLen(1))
			Expect(queryResult.Relations[0].RelatedEntity.ID).To(Equal("ns3:person-2"))
			Expect(queryResult.Relations[0].RelatedEntity.Properties[pref+":Name"]).To(Equal([]any{"Person 2", "Person 2"}))
		})
		ginkgo.It("two datasets, 2nd later deleted", func() {
			_ = persist("family", store, dsm, buildTestBatch(store, []testPerson{
				{id: 1, friends: []int{2}},
				{id: 2, friends: []int{}},
			}))
			pref := persist("friends", store, dsm, buildTestBatch(store, []testPerson{
				{id: 1, friends: []int{2}},
				{id: 2, friends: []int{}},
			}))
			_ = persist("family", store, dsm, buildTestBatch(store, []testPerson{
				{id: 1, deleted: true, friends: []int{2}},
				{id: 2, friends: []int{}},
			}))
			start := []string{pref + ":person-2"}
			queryResult, err := store.GetManyRelatedEntitiesBatch(start, "*", true, nil, 0, true)
			Expect(err).To(BeNil())
			Expect(queryResult.Relations).To(HaveLen(1))
			Expect(queryResult.Relations[0].RelatedEntity.ID).To(Equal("ns3:person-1"))
			Expect(queryResult.Relations[0].RelatedEntity.Properties[pref+":Name"]).To(Equal("Person 1"), "only one partial expected")
			Expect(queryResult.Relations[0].RelatedEntity.IsDeleted).To(BeFalse())
			start = []string{pref + ":person-1"}
			queryResult, err = store.GetManyRelatedEntitiesBatch(start, "*", false, nil, 0, true)
			Expect(err).To(BeNil())
			Expect(queryResult.Relations).To(HaveLen(1))
			Expect(queryResult.Relations[0].RelatedEntity.ID).To(Equal("ns3:person-2"))
			Expect(queryResult.Relations[0].RelatedEntity.Properties[pref+":Name"]).To(Equal([]any{"Person 2", "Person 2"}))
		})
		ginkgo.It("two datasets, 1st deleted", func() {
			pref := persist("friends", store, dsm, buildTestBatch(store, []testPerson{
				{id: 1, deleted: true, friends: []int{2}},
				{id: 2, friends: []int{}},
			}))
			_ = persist("family", store, dsm, buildTestBatch(store, []testPerson{
				{id: 1, friends: []int{2}},
				{id: 2, friends: []int{}},
			}))
			start := []string{pref + ":person-2"}
			queryResult, err := store.GetManyRelatedEntitiesBatch(start, "*", true, nil, 0, true)
			Expect(err).To(BeNil())
			Expect(queryResult.Relations).To(HaveLen(1))
			Expect(queryResult.Relations[0].RelatedEntity.ID).To(Equal("ns3:person-1"))
			Expect(queryResult.Relations[0].RelatedEntity.Properties[pref+":Name"]).To(Equal("Person 1"), "only one partial expected")
			Expect(queryResult.Relations[0].RelatedEntity.IsDeleted).To(BeFalse())
			start = []string{pref + ":person-1"}
			queryResult, err = store.GetManyRelatedEntitiesBatch(start, "*", false, nil, 0, true)
			Expect(err).To(BeNil())
			Expect(queryResult.Relations).To(HaveLen(1))
			Expect(queryResult.Relations[0].RelatedEntity.ID).To(Equal("ns3:person-2"))
			Expect(queryResult.Relations[0].RelatedEntity.Properties[pref+":Name"]).To(Equal([]any{"Person 2", "Person 2"}))
		})
		ginkgo.It("two datasets, 1st deleted same batch", func() {
			pref := persist("friends", store, dsm, buildTestBatch(store, []testPerson{
				{id: 1, friends: []int{2}},
				{id: 2, friends: []int{}},
				{id: 1, deleted: true, friends: []int{2}},
			}))
			_ = persist("family", store, dsm, buildTestBatch(store, []testPerson{
				{id: 1, friends: []int{2}},
				{id: 2, friends: []int{}},
			}))
			start := []string{pref + ":person-2"}
			queryResult, err := store.GetManyRelatedEntitiesBatch(start, "*", true, nil, 0, true)
			Expect(err).To(BeNil())
			Expect(queryResult.Relations).To(HaveLen(1))
			Expect(queryResult.Relations[0].RelatedEntity.ID).To(Equal("ns3:person-1"))
			Expect(queryResult.Relations[0].RelatedEntity.Properties[pref+":Name"]).To(Equal("Person 1"), "only one partial expected")
			Expect(queryResult.Relations[0].RelatedEntity.IsDeleted).To(BeFalse())
			start = []string{pref + ":person-1"}
			queryResult, err = store.GetManyRelatedEntitiesBatch(start, "*", false, nil, 0, true)
			Expect(err).To(BeNil())
			Expect(queryResult.Relations).To(HaveLen(1))
			Expect(queryResult.Relations[0].RelatedEntity.ID).To(Equal("ns3:person-2"))
			Expect(queryResult.Relations[0].RelatedEntity.Properties[pref+":Name"]).To(Equal([]any{"Person 2", "Person 2"}))
		})
		ginkgo.It("two datasets, 1st later deleted", func() {
			_ = persist("family", store, dsm, buildTestBatch(store, []testPerson{
				{id: 1, friends: []int{2}},
				{id: 2, friends: []int{}},
			}))
			pref := persist("friends", store, dsm, buildTestBatch(store, []testPerson{
				{id: 1, friends: []int{2}},
				{id: 2, friends: []int{}},
			}))
			_ = persist("friends", store, dsm, buildTestBatch(store, []testPerson{
				{id: 1, deleted: true, friends: []int{2}},
				{id: 2, friends: []int{}},
			}))
			start := []string{pref + ":person-2"}
			queryResult, err := store.GetManyRelatedEntitiesBatch(start, "*", true, nil, 0, true)
			Expect(err).To(BeNil())
			Expect(queryResult.Relations).To(HaveLen(1))
			Expect(queryResult.Relations[0].RelatedEntity.ID).To(Equal("ns3:person-1"))
			Expect(queryResult.Relations[0].RelatedEntity.Properties[pref+":Name"]).To(Equal("Person 1"), "only one partial expected")
			Expect(queryResult.Relations[0].RelatedEntity.IsDeleted).To(BeFalse())
			start = []string{pref + ":person-1"}
			queryResult, err = store.GetManyRelatedEntitiesBatch(start, "*", false, nil, 0, true)
			Expect(err).To(BeNil())
			Expect(queryResult.Relations).To(HaveLen(1))
			Expect(queryResult.Relations[0].RelatedEntity.ID).To(Equal("ns3:person-2"))
			Expect(queryResult.Relations[0].RelatedEntity.Properties[pref+":Name"]).To(Equal([]any{"Person 2", "Person 2"}))
		})
		ginkgo.It("two datasets, both deleted", func() {
			pref := persist("friends", store, dsm, buildTestBatch(store, []testPerson{
				{id: 1, deleted: true, friends: []int{2}},
				{id: 2, friends: []int{}},
			}))
			_ = persist("family", store, dsm, buildTestBatch(store, []testPerson{
				{id: 1, deleted: true, friends: []int{2}},
				{id: 2, friends: []int{}},
			}))
			start := []string{pref + ":person-2"}
			queryResult, err := store.GetManyRelatedEntitiesBatch(start, "*", true, nil, 0, true)
			Expect(err).To(BeNil())
			Expect(queryResult.Relations).To(HaveLen(0))
			start = []string{pref + ":person-1"}
			queryResult, err = store.GetManyRelatedEntitiesBatch(start, "*", false, nil, 0, true)
			Expect(err).To(BeNil())
			Expect(queryResult.Relations).To(HaveLen(0))
		})
		ginkgo.It("two datasets, deleted both, but first deleted later", func() {
			pref := persist("friends", store, dsm, buildTestBatch(store, []testPerson{
				{id: 1, friends: []int{2}},
				{id: 2, friends: []int{}},
			}))
			_ = persist("family", store, dsm, buildTestBatch(store, []testPerson{
				{id: 1, deleted: true, friends: []int{2}},
				{id: 2, friends: []int{}},
			}))
			_ = persist("friends", store, dsm, buildTestBatch(store, []testPerson{
				{id: 1, deleted: true, friends: []int{2}},
				{id: 2, friends: []int{}},
			}))
			start := []string{pref + ":person-2"}
			queryResult, err := store.GetManyRelatedEntitiesBatch(start, "*", true, nil, 0, true)
			Expect(err).To(BeNil())
			Expect(queryResult.Relations).To(HaveLen(0))
			start = []string{pref + ":person-1"}
			queryResult, err = store.GetManyRelatedEntitiesBatch(start, "*", false, nil, 0, true)
			Expect(err).To(BeNil())
			Expect(queryResult.Relations).To(HaveLen(0))
		})
		ginkgo.It("two datasets, deleted both, but first deleted later same batch", func() {
			pref := persist("friends", store, dsm, buildTestBatch(store, []testPerson{
				{id: 1, friends: []int{2}},
				{id: 2, friends: []int{}},
				{id: 1, deleted: true, friends: []int{2}},
			}))
			_ = persist("family", store, dsm, buildTestBatch(store, []testPerson{
				{id: 1, deleted: true, friends: []int{2}},
				{id: 2, friends: []int{}},
			}))
			start := []string{pref + ":person-2"}
			queryResult, err := store.GetManyRelatedEntitiesBatch(start, "*", true, nil, 0, true)
			Expect(err).To(BeNil())
			Expect(queryResult.Relations).To(HaveLen(0))
			start = []string{pref + ":person-1"}
			queryResult, err = store.GetManyRelatedEntitiesBatch(start, "*", false, nil, 0, true)
			Expect(err).To(BeNil())
			Expect(queryResult.Relations).To(HaveLen(0))
		})
		ginkgo.It("two datasets, all deleted but first deleted later ver2", func() {
			pref := persist("friends", store, dsm, buildTestBatch(store, []testPerson{
				{id: 1, friends: []int{2}},
				{id: 2, friends: []int{}},
			}))
			_ = persist("friends", store, dsm, buildTestBatch(store, []testPerson{
				{id: 1, deleted: true, friends: []int{2}},
				{id: 2, friends: []int{}},
			}))
			_ = persist("family", store, dsm, buildTestBatch(store, []testPerson{
				{id: 1, deleted: true, friends: []int{2}},
				{id: 2, friends: []int{}},
			}))
			start := []string{pref + ":person-2"}
			queryResult, err := store.GetManyRelatedEntitiesBatch(start, "*", true, nil, 0, true)
			Expect(err).To(BeNil())
			Expect(queryResult.Relations).To(HaveLen(0))
			start = []string{pref + ":person-1"}
			queryResult, err = store.GetManyRelatedEntitiesBatch(start, "*", false, nil, 0, true)
			Expect(err).To(BeNil())
			Expect(queryResult.Relations).To(HaveLen(0))
		})
		ginkgo.It("two datasets,  all deleted but 2nd deleted later", func() {
			pref := persist("friends", store, dsm, buildTestBatch(store, []testPerson{
				{id: 1, deleted: true, friends: []int{2}},
				{id: 2, friends: []int{}},
			}))
			_ = persist("family", store, dsm, buildTestBatch(store, []testPerson{
				{id: 1, friends: []int{2}},
				{id: 2, friends: []int{}},
			}))
			_ = persist("family", store, dsm, buildTestBatch(store, []testPerson{
				{id: 1, deleted: true, friends: []int{2}},
				{id: 2, friends: []int{}},
			}))
			start := []string{pref + ":person-2"}
			queryResult, err := store.GetManyRelatedEntitiesBatch(start, "*", true, nil, 0, true)
			Expect(err).To(BeNil())
			Expect(queryResult.Relations).To(HaveLen(0))
			start = []string{pref + ":person-1"}
			queryResult, err = store.GetManyRelatedEntitiesBatch(start, "*", false, nil, 0, true)
			Expect(err).To(BeNil())
			Expect(queryResult.Relations).To(HaveLen(0))
		})
		ginkgo.It("two datasets,  all deleted but 2nd deleted later same batch", func() {
			pref := persist("friends", store, dsm, buildTestBatch(store, []testPerson{
				{id: 1, deleted: true, friends: []int{2}},
				{id: 2, friends: []int{}},
			}))
			_ = persist("family", store, dsm, buildTestBatch(store, []testPerson{
				{id: 1, friends: []int{2}},
				{id: 2, friends: []int{}},
				{id: 1, deleted: true, friends: []int{2}},
			}))
			start := []string{pref + ":person-2"}
			queryResult, err := store.GetManyRelatedEntitiesBatch(start, "*", true, nil, 0, true)
			Expect(err).To(BeNil())
			Expect(queryResult.Relations).To(HaveLen(0))
			start = []string{pref + ":person-1"}
			queryResult, err = store.GetManyRelatedEntitiesBatch(start, "*", false, nil, 0, true)
			Expect(err).To(BeNil())
			Expect(queryResult.Relations).To(HaveLen(0))
		})
		ginkgo.It("two datasets, all deleted but 2nd deleted later ver2", func() {
			_ = persist("family", store, dsm, buildTestBatch(store, []testPerson{
				{id: 1, friends: []int{2}},
				{id: 2, friends: []int{}},
			}))
			pref := persist("friends", store, dsm, buildTestBatch(store, []testPerson{
				{id: 1, deleted: true, friends: []int{2}},
				{id: 2, friends: []int{}},
			}))
			_ = persist("family", store, dsm, buildTestBatch(store, []testPerson{
				{id: 1, deleted: true, friends: []int{2}},
				{id: 2, friends: []int{}},
			}))
			start := []string{pref + ":person-2"}
			queryResult, err := store.GetManyRelatedEntitiesBatch(start, "*", true, nil, 0, true)
			Expect(err).To(BeNil())
			Expect(queryResult.Relations).To(HaveLen(0))
			start = []string{pref + ":person-1"}
			queryResult, err = store.GetManyRelatedEntitiesBatch(start, "*", false, nil, 0, true)
			Expect(err).To(BeNil())
			Expect(queryResult.Relations).To(HaveLen(0))
		})
		ginkgo.It("two datasets, all deleted but both deleted later", func() {
			pref := persist("friends", store, dsm, buildTestBatch(store, []testPerson{
				{id: 1, friends: []int{2}},
				{id: 2, friends: []int{}},
			}))
			_ = persist("family", store, dsm, buildTestBatch(store, []testPerson{
				{id: 1, friends: []int{2}},
				{id: 2, friends: []int{}},
			}))
			_ = persist("friends", store, dsm, buildTestBatch(store, []testPerson{
				{id: 1, deleted: true, friends: []int{2}},
				{id: 2, friends: []int{}},
			}))
			_ = persist("family", store, dsm, buildTestBatch(store, []testPerson{
				{id: 1, deleted: true, friends: []int{2}},
				{id: 2, friends: []int{}},
			}))
			start := []string{pref + ":person-2"}
			queryResult, err := store.GetManyRelatedEntitiesBatch(start, "*", true, nil, 0, true)
			Expect(err).To(BeNil())
			Expect(queryResult.Relations).To(HaveLen(0))
			start = []string{pref + ":person-1"}
			queryResult, err = store.GetManyRelatedEntitiesBatch(start, "*", false, nil, 0, true)
			Expect(err).To(BeNil())
			Expect(queryResult.Relations).To(HaveLen(0))
		})
		ginkgo.It("two datasets, all deleted but both deleted later v2", func() {
			pref := persist("friends", store, dsm, buildTestBatch(store, []testPerson{
				{id: 1, friends: []int{2}},
				{id: 2, friends: []int{}},
			}))
			_ = persist("family", store, dsm, buildTestBatch(store, []testPerson{
				{id: 1, friends: []int{2}},
				{id: 2, friends: []int{}},
			}))
			_ = persist("family", store, dsm, buildTestBatch(store, []testPerson{
				{id: 1, deleted: true, friends: []int{2}},
				{id: 2, friends: []int{}},
			}))
			_ = persist("friends", store, dsm, buildTestBatch(store, []testPerson{
				{id: 1, deleted: true, friends: []int{2}},
				{id: 2, friends: []int{}},
			}))
			start := []string{pref + ":person-2"}
			queryResult, err := store.GetManyRelatedEntitiesBatch(start, "*", true, nil, 0, true)
			Expect(err).To(BeNil())
			Expect(queryResult.Relations).To(HaveLen(0))
			start = []string{pref + ":person-1"}
			queryResult, err = store.GetManyRelatedEntitiesBatch(start, "*", false, nil, 0, true)
			Expect(err).To(BeNil())
			Expect(queryResult.Relations).To(HaveLen(0))
		})
		ginkgo.It("many datasets, all but one deleted", func() {
			pref := persist("friends", store, dsm, buildTestBatch(store, []testPerson{
				{id: 1, friends: []int{2}},
				{id: 2, friends: []int{}},
			}))
			_ = persist("family", store, dsm, buildTestBatch(store, []testPerson{
				{id: 1, friends: []int{2}},
				{id: 2, friends: []int{}},
			}))
			_ = persist("other", store, dsm, buildTestBatch(store, []testPerson{
				{id: 1, friends: []int{2}},
				{id: 2, friends: []int{}},
			}))
			_ = persist("family", store, dsm, buildTestBatch(store, []testPerson{
				{id: 1, deleted: true, friends: []int{2}},
				{id: 2, friends: []int{}},
			}))
			_ = persist("other2", store, dsm, buildTestBatch(store, []testPerson{
				{id: 1, friends: []int{2}},
				{id: 2, friends: []int{}},
			}))
			_ = persist("other", store, dsm, buildTestBatch(store, []testPerson{
				{id: 1, deleted: true, friends: []int{2}},
				{id: 2, friends: []int{}},
			}))
			_ = persist("friends", store, dsm, buildTestBatch(store, []testPerson{
				{id: 1, deleted: true, friends: []int{2}},
				{id: 2, friends: []int{}},
			}))
			start := []string{pref + ":person-2"}
			queryResult, err := store.GetManyRelatedEntitiesBatch(start, "*", true, nil, 0, true)
			Expect(err).To(BeNil())
			Expect(queryResult.Relations).To(HaveLen(1))
			Expect(queryResult.Relations[0].RelatedEntity.ID).To(Equal("ns3:person-1"))
			Expect(queryResult.Relations[0].RelatedEntity.Properties[pref+":Name"]).To(Equal("Person 1"), "only one partial expected")
			Expect(queryResult.Relations[0].RelatedEntity.IsDeleted).To(BeFalse())
			start = []string{pref + ":person-1"}
			queryResult, err = store.GetManyRelatedEntitiesBatch(start, "*", false, nil, 0, true)
			Expect(err).To(BeNil())
			Expect(queryResult.Relations).To(HaveLen(1))
			Expect(queryResult.Relations[0].RelatedEntity.ID).To(Equal("ns3:person-2"))
			Expect(queryResult.Relations[0].RelatedEntity.Properties[pref+":Name"]).To(Equal([]any{
				"Person 2", "Person 2", "Person 2", "Person 2"}))
		})
		ginkgo.It("multiple datasets with deletes", func() {
			pref := persist("people", store, dsm, buildTestBatch(store, []testPerson{
				{id: 1, friends: []int{1}},
				{id: 2, friends: []int{1}},
			}))
			_ = persist("people", store, dsm, buildTestBatch(store, []testPerson{
				{id: 1, deleted: true, friends: []int{1}},
			}))
			/*
				_ = persist("other", store, dsm, buildTestBatch(store, []testPerson{
					{id: 1, friends: []int{2}},
					{id: 2, friends: []int{}},
				}))
				_ = persist("family", store, dsm, buildTestBatch(store, []testPerson{
					{id: 1, deleted: true, friends: []int{2}},
					{id: 2, friends: []int{}},
				}))
				_ = persist("other2", store, dsm, buildTestBatch(store, []testPerson{
					{id: 1, friends: []int{2}},
					{id: 2, friends: []int{}},
				}))
				_ = persist("other", store, dsm, buildTestBatch(store, []testPerson{
					{id: 1, deleted: true, family: []int{2}},
					{id: 2, friends: []int{}},
				}))
				_ = persist("friends", store, dsm, buildTestBatch(store, []testPerson{
					{id: 1, deleted: true, friends: []int{2}},
					{id: 2, friends: []int{}},
				})) */
			start := []string{pref + ":person-1"}
			queryResult, err := store.GetManyRelatedEntitiesBatch(start, "*", true, nil, 0, true)
			Expect(err).To(BeNil())
			Expect(queryResult.Relations).To(HaveLen(1))
			Expect(queryResult.Relations[0].RelatedEntity.ID).To(Equal("ns3:person-2"))
			Expect(queryResult.Relations[0].RelatedEntity.IsDeleted).To(BeFalse())
		})
		ginkgo.It("multiple datasets with deletes 2", func() {
			pref := persist("people", store, dsm, buildTestBatch(store, []testPerson{
				{id: 1, friends: []int{1}},
				{id: 2, friends: []int{1}},
			}))
			_ = persist("family", store, dsm, buildTestBatch(store, []testPerson{
				// {id: 1, friends: []int{1}},
				{id: 2, friends: []int{1}},
			}))
			_ = persist("people", store, dsm, buildTestBatch(store, []testPerson{
				{id: 1, deleted: true, friends: []int{1}},
			}))
			/*
				_ = persist("other", store, dsm, buildTestBatch(store, []testPerson{
					{id: 1, friends: []int{2}},
					{id: 2, friends: []int{}},
				}))
				_ = persist("family", store, dsm, buildTestBatch(store, []testPerson{
					{id: 1, deleted: true, friends: []int{2}},
					{id: 2, friends: []int{}},
				}))
				_ = persist("other2", store, dsm, buildTestBatch(store, []testPerson{
					{id: 1, friends: []int{2}},
					{id: 2, friends: []int{}},
				}))
				_ = persist("other", store, dsm, buildTestBatch(store, []testPerson{
					{id: 1, deleted: true, family: []int{2}},
					{id: 2, friends: []int{}},
				}))
				_ = persist("friends", store, dsm, buildTestBatch(store, []testPerson{
					{id: 1, deleted: true, friends: []int{2}},
					{id: 2, friends: []int{}},
				})) */
			start := []string{pref + ":person-1"}
			queryResult, err := store.GetManyRelatedEntitiesBatch(start, "*", true, nil, 0, true)
			Expect(err).To(BeNil())
			Expect(queryResult.Relations).To(HaveLen(1))
			Expect(queryResult.Relations[0].RelatedEntity.ID).To(Equal("ns3:person-2"))
			Expect(queryResult.Relations[0].RelatedEntity.IsDeleted).To(BeFalse())
		})
		ginkgo.It("multiple datasets with deletes 3", func() {
			pref := persist("people", store, dsm, buildTestBatch(store, []testPerson{
				{id: 1, friends: []int{1}},
				// {id: 2, friends: []int{1}},
			}))
			_ = persist("family", store, dsm, buildTestBatch(store, []testPerson{
				{id: 1, friends: []int{1}},
				// {id: 2, friends: []int{1}},
			}))
			_ = persist("people", store, dsm, buildTestBatch(store, []testPerson{
				{id: 1, deleted: true, friends: []int{1}},
			}))
			_ = persist("family", store, dsm, buildTestBatch(store, []testPerson{
				{id: 1, deleted: true, friends: []int{1}},
			}))
			/*
				_ = persist("other", store, dsm, buildTestBatch(store, []testPerson{
					{id: 1, friends: []int{2}},
					{id: 2, friends: []int{}},
				}))
				_ = persist("family", store, dsm, buildTestBatch(store, []testPerson{
					{id: 1, deleted: true, friends: []int{2}},
					{id: 2, friends: []int{}},
				}))
				_ = persist("other2", store, dsm, buildTestBatch(store, []testPerson{
					{id: 1, friends: []int{2}},
					{id: 2, friends: []int{}},
				}))
				_ = persist("other", store, dsm, buildTestBatch(store, []testPerson{
					{id: 1, deleted: true, family: []int{2}},
					{id: 2, friends: []int{}},
				}))
				_ = persist("friends", store, dsm, buildTestBatch(store, []testPerson{
					{id: 1, deleted: true, friends: []int{2}},
					{id: 2, friends: []int{}},
				})) */
			start := []string{pref + ":person-1"}
			queryResult, err := store.GetManyRelatedEntitiesBatch(start, "*", true, nil, 0, true)
			Expect(err).To(BeNil())
			Expect(queryResult.Relations).To(HaveLen(0))
		})
		ginkgo.It("multiple datasets with deletes 4", func() {
			pref := persist("people", store, dsm, buildTestBatch(store, []testPerson{
				{id: 1, friends: []int{1}},
				{id: 2, friends: []int{1}},
			}))
			_ = persist("family", store, dsm, buildTestBatch(store, []testPerson{
				{id: 2, friends: []int{1}},
			}))
			_ = persist("people", store, dsm, buildTestBatch(store, []testPerson{
				{id: 1, friends: []int{}},
			}))
			/*
				_ = persist("other", store, dsm, buildTestBatch(store, []testPerson{
					{id: 1, friends: []int{2}},
					{id: 2, friends: []int{}},
				}))
				_ = persist("family", store, dsm, buildTestBatch(store, []testPerson{
					{id: 1, deleted: true, friends: []int{2}},
					{id: 2, friends: []int{}},
				}))
				_ = persist("other2", store, dsm, buildTestBatch(store, []testPerson{
					{id: 1, friends: []int{2}},
					{id: 2, friends: []int{}},
				}))
				_ = persist("other", store, dsm, buildTestBatch(store, []testPerson{
					{id: 1, deleted: true, family: []int{2}},
					{id: 2, friends: []int{}},
				}))
				_ = persist("friends", store, dsm, buildTestBatch(store, []testPerson{
					{id: 1, deleted: true, friends: []int{2}},
					{id: 2, friends: []int{}},
				})) */
			start := []string{pref + ":person-1"}
			queryResult, err := store.GetManyRelatedEntitiesBatch(start, "*", true, nil, 0, true)
			Expect(err).To(BeNil())
			Expect(queryResult.Relations).To(HaveLen(1))
			Expect(queryResult.Relations[0].RelatedEntity.ID).To(Equal("ns3:person-2"))
			Expect(queryResult.Relations[0].RelatedEntity.IsDeleted).To(BeFalse())
		})
		ginkgo.It("multiple datasets, multiple predicates with deletes 5", func() {
			pref := persist("people", store, dsm, buildTestBatch(store, []testPerson{
				{id: 1, friends: []int{1}},
			}))
			_ = persist("family", store, dsm, buildTestBatch(store, []testPerson{
				{id: 1, friends: []int{1}},
			}))
			_ = persist("people", store, dsm, buildTestBatch(store, []testPerson{
				{id: 1, friends: []int{}, family: []int{1}},
			}))
			/*
				_ = persist("other", store, dsm, buildTestBatch(store, []testPerson{
					{id: 1, friends: []int{2}},
					{id: 2, friends: []int{}},
				}))
				_ = persist("family", store, dsm, buildTestBatch(store, []testPerson{
					{id: 1, deleted: true, friends: []int{2}},
					{id: 2, friends: []int{}},
				}))
				_ = persist("other2", store, dsm, buildTestBatch(store, []testPerson{
					{id: 1, friends: []int{2}},
					{id: 2, friends: []int{}},
				}))
				_ = persist("other", store, dsm, buildTestBatch(store, []testPerson{
					{id: 1, deleted: true, family: []int{2}},
					{id: 2, friends: []int{}},
				}))
				_ = persist("friends", store, dsm, buildTestBatch(store, []testPerson{
					{id: 1, deleted: true, friends: []int{2}},
					{id: 2, friends: []int{}},
				})) */
			start := []string{pref + ":person-1"}
			queryResult, err := store.GetManyRelatedEntitiesBatch(start, "*", true, nil, 0, true)
			Expect(err).To(BeNil())
			Expect(queryResult.Relations).To(HaveLen(2))
			Expect(queryResult.Relations[0].RelatedEntity.ID).To(Equal("ns3:person-1"))
			Expect(queryResult.Relations[0].RelatedEntity.IsDeleted).To(BeFalse())
		})
		ginkgo.It("multiple datasets, multiple predicates with deletes 6", func() {
			pref := persist("people", store, dsm, buildTestBatch(store, []testPerson{
				{id: 1, friends: []int{1}},
			}))
			_ = persist("family", store, dsm, buildTestBatch(store, []testPerson{
				{id: 1, friends: []int{1}},
			}))
			_ = persist("people", store, dsm, buildTestBatch(store, []testPerson{
				{id: 1, friends: []int{}, family: []int{1}},
			}))
			_ = persist("people", store, dsm, buildTestBatch(store, []testPerson{
				{id: 1, friends: []int{}, family: []int{}},
			}))
			_ = persist("family", store, dsm, buildTestBatch(store, []testPerson{
				{id: 1, friends: []int{}, family: []int{}},
			}))
			_ = persist("people", store, dsm, buildTestBatch(store, []testPerson{
				{id: 1, friends: []int{1}},
			}))
			_ = persist("family", store, dsm, buildTestBatch(store, []testPerson{
				{id: 1, friends: []int{1}},
			}))
			/*
				_ = persist("other", store, dsm, buildTestBatch(store, []testPerson{
					{id: 1, friends: []int{2}},
					{id: 2, friends: []int{}},
				}))
				_ = persist("family", store, dsm, buildTestBatch(store, []testPerson{
					{id: 1, deleted: true, friends: []int{2}},
					{id: 2, friends: []int{}},
				}))
				_ = persist("other2", store, dsm, buildTestBatch(store, []testPerson{
					{id: 1, friends: []int{2}},
					{id: 2, friends: []int{}},
				}))
				_ = persist("other", store, dsm, buildTestBatch(store, []testPerson{
					{id: 1, deleted: true, family: []int{2}},
					{id: 2, friends: []int{}},
				}))
				_ = persist("friends", store, dsm, buildTestBatch(store, []testPerson{
					{id: 1, deleted: true, friends: []int{2}},
					{id: 2, friends: []int{}},
				})) */
			start := []string{pref + ":person-1"}
			queryResult, err := store.GetManyRelatedEntitiesBatch(start, "*", true, nil, 0, true)
			Expect(err).To(BeNil())
			Expect(queryResult.Relations).To(HaveLen(2))
		})
		ginkgo.It("multiple datasets, multiple predicates with deletes 7", func() {
			pref := persist("people", store, dsm, buildTestBatch(store, []testPerson{
				{id: 1, friends: []int{1}},
			}))
			_ = persist("family", store, dsm, buildTestBatch(store, []testPerson{
				{id: 1, friends: []int{1}},
			}))
			_ = persist("people", store, dsm, buildTestBatch(store, []testPerson{
				{id: 1, friends: []int{}, family: []int{1}},
			}))
			_ = persist("people", store, dsm, buildTestBatch(store, []testPerson{
				{id: 1, friends: []int{}, family: []int{}},
			}))
			/*
				_ = persist("other", store, dsm, buildTestBatch(store, []testPerson{
					{id: 1, friends: []int{2}},
					{id: 2, friends: []int{}},
				}))
				_ = persist("family", store, dsm, buildTestBatch(store, []testPerson{
					{id: 1, deleted: true, friends: []int{2}},
					{id: 2, friends: []int{}},
				}))
				_ = persist("other2", store, dsm, buildTestBatch(store, []testPerson{
					{id: 1, friends: []int{2}},
					{id: 2, friends: []int{}},
				}))
				_ = persist("other", store, dsm, buildTestBatch(store, []testPerson{
					{id: 1, deleted: true, family: []int{2}},
					{id: 2, friends: []int{}},
				}))
				_ = persist("friends", store, dsm, buildTestBatch(store, []testPerson{
					{id: 1, deleted: true, friends: []int{2}},
					{id: 2, friends: []int{}},
				})) */
			start := []string{pref + ":person-1"}
			queryResult, err := store.GetManyRelatedEntitiesBatch(start, "*", true, []string{"people"}, 0, true)
			Expect(err).To(BeNil())
			Expect(queryResult.Relations).To(HaveLen(0))
			queryResult, err = store.GetManyRelatedEntitiesBatch(start, "*", true, []string{"family"}, 0, true)
			Expect(err).To(BeNil())
			Expect(queryResult.Relations).To(HaveLen(1))
		})

	})
	ginkgo.Describe("Multiple predicates and delete states, here:", func() {
		ginkgo.It("two predicates, single dataset, specific query", func() {
			pref := persist("friends", store, dsm, buildTestBatch(store, []testPerson{
				{id: 1, friends: []int{2}, family: []int{2}},
				{id: 2, friends: []int{}},
			}))
			start := []string{pref + ":person-1"}
			queryResult, err := store.GetManyRelatedEntitiesBatch(start, pref+":Friend", false, nil, 0, true)
			Expect(err).To(BeNil())
			Expect(queryResult.Relations).To(HaveLen(1))
			Expect(queryResult.Relations[0].RelatedEntity.ID).To(Equal("ns3:person-2"))
			start = []string{pref + ":person-2"}
			queryResult, err = store.GetManyRelatedEntitiesBatch(start, pref+":Friend", true, nil, 0, true)
			Expect(err).To(BeNil())
			Expect(queryResult.Relations).To(HaveLen(1))
			Expect(queryResult.Relations[0].RelatedEntity.ID).To(Equal("ns3:person-1"))
		})
		ginkgo.It("two predicates, single dataset, specific query, deleted ref, same batch", func() {
			pref := persist("friends", store, dsm, buildTestBatch(store, []testPerson{
				{id: 1, friends: []int{2}, family: []int{2}},
				{id: 2, friends: []int{}},
				{id: 1, deleted: true, friends: []int{2}, family: []int{2}},
			}))
			start := []string{pref + ":person-1"}
			queryResult, err := store.GetManyRelatedEntitiesBatch(start, pref+":Friend", false, nil, 0, true)
			Expect(err).To(BeNil())
			Expect(queryResult.Relations).To(HaveLen(0))
			start = []string{pref + ":person-2"}
			queryResult, err = store.GetManyRelatedEntitiesBatch(start, pref+":Friend", true, nil, 0, true)
			Expect(err).To(BeNil())
			Expect(queryResult.Relations).To(HaveLen(0))
		})
		ginkgo.It("two predicates, two datasetd, specific query, deleted ref in one ds, same batch", func() {
			pref := persist("friends", store, dsm, buildTestBatch(store, []testPerson{
				{id: 1, friends: []int{2}, family: []int{2}},
				{id: 2, friends: []int{}},
				{id: 1, deleted: true, friends: []int{2}, family: []int{2}},
			}))
			_ = persist("family", store, dsm, buildTestBatch(store, []testPerson{
				{id: 1, friends: []int{2}, family: []int{2}},
				{id: 2, friends: []int{}},
			}))
			start := []string{pref + ":person-1"}
			queryResult, err := store.GetManyRelatedEntitiesBatch(start, pref+":Friend", false, nil, 0, true)
			Expect(err).To(BeNil())

			Expect(queryResult.Relations).To(HaveLen(1))
			Expect(queryResult.Relations[0].RelatedEntity.ID).To(Equal("ns3:person-2"))
			start = []string{pref + ":person-2"}
			queryResult, err = store.GetManyRelatedEntitiesBatch(start, pref+":Friend", true, nil, 0, true)
			Expect(err).To(BeNil())
			Expect(queryResult.Relations).To(HaveLen(1))
			Expect(queryResult.Relations[0].RelatedEntity.ID).To(Equal("ns3:person-1"))
		})
		ginkgo.It("two predicates, two datasets, specific query", func() {
			pref := persist("friends", store, dsm, buildTestBatch(store, []testPerson{
				{id: 1, friends: []int{2}, family: []int{2}},
				{id: 2, friends: []int{}},
			}))
			_ = persist("family", store, dsm, buildTestBatch(store, []testPerson{
				{id: 1, friends: []int{2}, family: []int{2}},
				{id: 2, friends: []int{}},
			}))
			start := []string{pref + ":person-1"}
			queryResult, err := store.GetManyRelatedEntitiesBatch(start, pref+":Friend", false, nil, 0, true)
			Expect(err).To(BeNil())
			Expect(queryResult.Relations).To(HaveLen(1))
			Expect(queryResult.Relations[0].RelatedEntity.ID).To(Equal("ns3:person-2"))
			start = []string{pref + ":person-2"}
			queryResult, err = store.GetManyRelatedEntitiesBatch(start, pref+":Friend", true, nil, 0, true)
			Expect(err).To(BeNil())
			Expect(queryResult.Relations).To(HaveLen(1))
			Expect(queryResult.Relations[0].RelatedEntity.ID).To(Equal("ns3:person-1"))
		})
		ginkgo.It("two predicates, single dataset, star query", func() {
			pref := persist("friends", store, dsm, buildTestBatch(store, []testPerson{
				{id: 1, friends: []int{2}, family: []int{2}},
				{id: 2, friends: []int{}},
			}))
			start := []string{pref + ":person-1"}
			queryResult, err := store.GetManyRelatedEntitiesBatch(start, "*", false, nil, 0, true)
			Expect(err).To(BeNil())
			Expect(queryResult.Relations).To(HaveLen(2))
			Expect(queryResult.Relations[0].RelatedEntity.ID).To(Equal("ns3:person-2"))
			Expect(queryResult.Relations[0].RelatedEntity.Properties[pref+":Name"]).To(Equal("Person 2"))
			Expect(queryResult.Relations[1].RelatedEntity.ID).To(Equal("ns3:person-2"))
			Expect(queryResult.Relations[1].RelatedEntity.Properties[pref+":Name"]).To(Equal("Person 2"))
			start = []string{pref + ":person-2"}
			queryResult, err = store.GetManyRelatedEntitiesBatch(start, "*", true, nil, 0, true)
			Expect(err).To(BeNil())
			Expect(queryResult.Relations).To(HaveLen(2))
			Expect(queryResult.Relations[0].RelatedEntity.ID).To(Equal("ns3:person-1"))
			Expect(queryResult.Relations[0].RelatedEntity.Properties[pref+":Name"]).To(Equal("Person 1"), "only one partial expected")
			Expect(queryResult.Relations[0].RelatedEntity.IsDeleted).To(BeFalse())
			Expect(queryResult.Relations[1].RelatedEntity.ID).To(Equal("ns3:person-1"))
			Expect(queryResult.Relations[1].RelatedEntity.Properties[pref+":Name"]).To(Equal("Person 1"), "only one partial expected")
			Expect(queryResult.Relations[1].RelatedEntity.IsDeleted).To(BeFalse())
		})
	})
})

func persist(dsName string, store *Store, dsm *DsManager, b []*Entity) string {
	friendsDS, _ := dsm.CreateDataset(dsName, nil)
	err := friendsDS.StoreEntities(b)
	if err != nil {
		panic(err)
	}
	peopleNamespacePrefix, _ := store.NamespaceManager.AssertPrefixMappingForExpansion(
		"http://data.mimiro.io/people/",
	)
	return peopleNamespacePrefix
}

type testPerson struct {
	id      int
	friends []int
	family  []int
	deleted bool
}

func buildTestBatch(store *Store, input []testPerson) []*Entity {
	peopleNamespacePrefix, _ := store.NamespaceManager.AssertPrefixMappingForExpansion(
		"http://data.mimiro.io/people/",
	)
	result := make([]*Entity, 0, len(input))
	for _, p := range input {
		e := NewEntity(fmt.Sprintf(peopleNamespacePrefix+":person-%v", p.id), 0)
		e.Properties[peopleNamespacePrefix+":Name"] = fmt.Sprintf("Person %v", p.id)
		var friends []string
		for _, f := range p.friends {
			friends = append(friends, fmt.Sprintf("%v:person-%v", peopleNamespacePrefix, f))
		}
		if len(friends) > 0 {
			e.References[peopleNamespacePrefix+":Friend"] = friends
		}
		var family []string
		for _, f := range p.family {
			family = append(family, fmt.Sprintf("%v:person-%v", peopleNamespacePrefix, f))
		}
		if len(family) > 0 {
			e.References[peopleNamespacePrefix+":Family"] = family
		}
		e.IsDeleted = p.deleted
		result = append(result, e)
	}
	return result
}
