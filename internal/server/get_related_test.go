package server

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
)

func TestGetRelated(test *testing.T) {
	g := goblin.Goblin(test)
	g.Describe("The GetManyRelatedEntitiesBatch functions", func() {
		testCnt := 0
		var dsm *DsManager
		var store *Store
		var storeLocation string
		g.BeforeEach(func() {
			testCnt += 1
			storeLocation = fmt.Sprintf("./test_get_relations_%v", testCnt)
			err := os.RemoveAll(storeLocation)
			g.Assert(err).IsNil("should be allowed to clean testfiles in " + storeLocation)
			e := &conf.Env{
				Logger:        zap.NewNop().Sugar(),
				StoreLocation: storeLocation,
			}
			lc := fxtest.NewLifecycle(internal.FxTestLog(test, false))
			store = NewStore(lc, e, &statsd.NoOpClient{})
			dsm = NewDsManager(lc, e, store, NoOpBus())

			err = lc.Start(context.Background())
			g.Assert(err).IsNil()
		})
		g.AfterEach(func() {
			_ = store.Close()
			_ = os.RemoveAll(storeLocation)
		})

		g.It("Should respect limit for single start uri", func() {
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
			result, err := store.GetManyRelatedEntitiesBatch(start, "*", false, nil, 0)
			g.Assert(err).IsNil()
			g.Assert(len(result)).Eql(1)
			g.Assert(len(result[0].Relations)).Eql(3, "person-1 points to p2, p3 and (empty) p4")
			g.Assert(result[0].Relations[0].RelatedEntity.ID).Eql(peopleNamespacePrefix + ":person-3")
			g.Assert(result[0].Relations[1].RelatedEntity.ID).Eql(peopleNamespacePrefix + ":person-4")
			g.Assert(result[0].Relations[2].RelatedEntity.ID).Eql(peopleNamespacePrefix + ":person-2")

			seenCnt := 0
			for _, r := range result[0].Relations {
				if r.RelatedEntity.ID == peopleNamespacePrefix+":person-2" ||
					r.RelatedEntity.ID == peopleNamespacePrefix+":person-3" {
					seenCnt++
					g.Assert(r.RelatedEntity.Recorded > 0).IsTrue("make sure person-2 is an actual entity")
				}
				if r.RelatedEntity.ID == peopleNamespacePrefix+":person-4" {
					seenCnt++
					g.Assert(r.RelatedEntity.Recorded).Eql(uint64(0), "make sure person-4 is an open ref")
				}
			}
			g.Assert(seenCnt).Eql(3)

			result, err = store.GetManyRelatedEntitiesBatch(start, "*", false, nil, 1)
			g.Assert(err).IsNil()
			g.Assert(len(result)).Eql(1)
			g.Assert(len(result[0].Relations)).Eql(1)

			result, err = store.GetManyRelatedEntitiesBatch(start, "*", false, nil, 2)
			g.Assert(err).IsNil()
			g.Assert(len(result)).Eql(1)
			g.Assert(len(result[0].Relations)).Eql(2)
		})
		g.It("Should respect limit for many start uris", func() {
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
			result, err := store.GetManyRelatedEntitiesBatch(start, "*", false, nil, 0)
			g.Assert(err).IsNil()
			g.Assert(len(result)).Eql(2)
			g.Assert(len(result[0].Relations)).Eql(3)
			g.Assert(len(result[1].Relations)).Eql(2)

			// limit 1, should return first hit of first startUri
			result, err = store.GetManyRelatedEntitiesBatch(start, "*", false, nil, 1)
			g.Assert(err).IsNil()
			g.Assert(len(result)).Eql(1)
			g.Assert(len(result[0].Relations)).Eql(1)

			// limit 4, room for all 3 relations of first startUri plus 1 from other startUri
			result, err = store.GetManyRelatedEntitiesBatch(
				start,
				"*",
				false,
				nil,
				4,
			)
			g.Assert(err).IsNil()
			g.Assert(len(result)).Eql(2)
			g.Assert(len(result[0].Relations)).Eql(3)
			g.Assert(len(result[1].Relations)).Eql(1)
		})
		g.It("Should respect limit in inverse queries", func() {
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
			result, err := store.GetManyRelatedEntitiesBatch(start, "*", true, nil, 0)
			g.Assert(err).IsNil()
			g.Assert(len(result)).Eql(2)
			g.Assert(len(result[0].Relations)).Eql(1)
			g.Assert(len(result[1].Relations)).Eql(2)
			// limit 1, should return first hit of first startUri
			result, err = store.GetManyRelatedEntitiesBatch(
				start,
				"*",
				true,
				nil,
				1,
			)
			g.Assert(err).IsNil()
			g.Assert(len(result)).Eql(1)
			g.Assert(len(result[0].Relations)).Eql(1)

			// limit 2, room for all 1 relations of first startUri plus 1 from other startUri
			result, err = store.GetManyRelatedEntitiesBatch(
				start,
				"*",
				true,
				nil,
				2,
			)
			g.Assert(err).IsNil()
			g.Assert(len(result)).Eql(2)
			g.Assert(len(result[0].Relations)).Eql(1)
			g.Assert(len(result[1].Relations)).Eql(1)
		})
		g.It("Should return a working continuation token", func() {
			pref := persist("friends", store, dsm, buildTestBatch(store, []testPerson{
				{id: 1, friends: []int{2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20}},
			}))
			start := []string{pref + ":person-1"}
			queryResult, err := store.GetManyRelatedEntitiesBatch(start, "*", false, nil, 0)
			g.Assert(err).IsNil()
			g.Assert(len(queryResult)).Eql(1)
			g.Assert(len(queryResult[0].Relations)).Eql(19)

			queryResult, err = store.GetManyRelatedEntitiesBatch(start, "*", false, nil, 2)
			g.Assert(err).IsNil()
			g.Assert(len(queryResult)).Eql(1)
			g.Assert(len(queryResult[0].Relations)).Eql(2)
			g.Assert(queryResult[0].Relations[0].RelatedEntity.ID).Eql(pref + ":person-20")
			g.Assert(queryResult[0].Relations[1].RelatedEntity.ID).Eql(pref + ":person-19")
			g.Assert(queryResult[0].Continuation.RelationIndexFromKey).IsNotNil()
			startFromCont := queryResult.Cont()
			queryResult, err = store.GetManyRelatedEntitiesAtTime(startFromCont, 3)
			g.Assert(err).IsNil()
			g.Assert(len(queryResult)).Eql(1)
			g.Assert(len(queryResult[0].Relations)).Eql(3)
			g.Assert(queryResult[0].Relations[0].RelatedEntity.ID).Eql(pref + ":person-18")
			g.Assert(queryResult[0].Relations[1].RelatedEntity.ID).Eql(pref + ":person-17")
			g.Assert(queryResult[0].Relations[2].RelatedEntity.ID).Eql(pref + ":person-16")
			g.Assert(queryResult[0].Continuation.RelationIndexFromKey).IsNotNil()
			startFromCont = queryResult.Cont()
			queryResult, err = store.GetManyRelatedEntitiesAtTime(startFromCont, 25)
			g.Assert(err).IsNil()
			g.Assert(len(queryResult)).Eql(1)
			g.Assert(len(queryResult[0].Relations)).Eql(14)
			g.Assert(queryResult[0].Relations[0].RelatedEntity.ID).Eql(pref + ":person-15")
			g.Assert(queryResult[0].Relations[13].RelatedEntity.ID).Eql(pref + ":person-2")
			g.Assert(queryResult[0].Continuation.RelationIndexFromKey).IsNil()
		})
	})
}

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
