package server

import (
	"context"
	"fmt"
	"github.com/DataDog/datadog-go/v5/statsd"
	"github.com/franela/goblin"
	"go.uber.org/fx/fxtest"
	"go.uber.org/zap"
	"os"
	"testing"
	"time"

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
			peopleNamespacePrefix := setupData(store, dsm)
			result, err := store.GetManyRelatedEntitiesBatch(
				[]RelatedEntitiesContinuation{{StartUri: peopleNamespacePrefix + ":person-1"}},
				"*",
				false,
				nil,
				0,
			)
			g.Assert(err).IsNil()
			g.Assert(len(result)).Eql(1)
			g.Assert(len(result[0].Relations)).Eql(3, "person-1 points to p2, p3 and (empty) p4")
			seenCnt := 0
			g.Assert(result[0].Relations[0].RelatedEntity.ID).Eql(peopleNamespacePrefix + ":person-3")
			g.Assert(result[0].Relations[1].RelatedEntity.ID).Eql(peopleNamespacePrefix + ":person-4")
			g.Assert(result[0].Relations[2].RelatedEntity.ID).Eql(peopleNamespacePrefix + ":person-2")
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

			result, err = store.GetManyRelatedEntitiesBatch(
				[]RelatedEntitiesContinuation{{StartUri: peopleNamespacePrefix + ":person-1"}},
				"*",
				false,
				nil,
				1,
			)
			g.Assert(err).IsNil()
			g.Assert(len(result)).Eql(1)
			g.Assert(len(result[0].Relations)).Eql(1)

			result, err = store.GetManyRelatedEntitiesBatch(
				[]RelatedEntitiesContinuation{{StartUri: peopleNamespacePrefix + ":person-1"}},
				"*",
				false,
				nil,
				2,
			)
			g.Assert(err).IsNil()
			g.Assert(len(result)).Eql(1)
			g.Assert(len(result[0].Relations)).Eql(2)
		})
		g.It("Should respect limit for many start uris", func() {
			peopleNamespacePrefix := setupData(store, dsm)
			// get everything
			result, err := store.GetManyRelatedEntitiesBatch(
				[]RelatedEntitiesContinuation{
					{StartUri: peopleNamespacePrefix + ":person-1"},
					{StartUri: peopleNamespacePrefix + ":person-2"},
				},
				"*",
				false,
				nil,
				0,
			)
			g.Assert(err).IsNil()
			g.Assert(len(result)).Eql(2)
			g.Assert(len(result[0].Relations)).Eql(3)
			g.Assert(len(result[1].Relations)).Eql(2)

			// limit 1, should return first hit of first startUri
			result, err = store.GetManyRelatedEntitiesBatch(
				[]RelatedEntitiesContinuation{
					{StartUri: peopleNamespacePrefix + ":person-1"},
					{StartUri: peopleNamespacePrefix + ":person-2"},
				},
				"*",
				false,
				nil,
				1,
			)
			g.Assert(err).IsNil()
			g.Assert(len(result)).Eql(1)
			g.Assert(len(result[0].Relations)).Eql(1)

			// limit 4, room for all 3 relations of first startUri plus 1 from other startUri
			result, err = store.GetManyRelatedEntitiesBatch(
				[]RelatedEntitiesContinuation{
					{StartUri: peopleNamespacePrefix + ":person-1"},
					{StartUri: peopleNamespacePrefix + ":person-2"},
				},
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
			peopleNamespacePrefix := setupData(store, dsm)

			// get everything
			result, err := store.GetManyRelatedEntitiesBatch(
				[]RelatedEntitiesContinuation{
					{StartUri: peopleNamespacePrefix + ":person-1"},
					{StartUri: peopleNamespacePrefix + ":person-2"},
				},
				"*", true, nil, 0)
			g.Assert(err).IsNil()
			g.Assert(len(result)).Eql(2)
			g.Assert(len(result[0].Relations)).Eql(1)
			g.Assert(len(result[1].Relations)).Eql(2)
			// limit 1, should return first hit of first startUri
			result, err = store.GetManyRelatedEntitiesBatch(
				[]RelatedEntitiesContinuation{
					{StartUri: peopleNamespacePrefix + ":person-1"},
					{StartUri: peopleNamespacePrefix + ":person-2"},
				},
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
				[]RelatedEntitiesContinuation{
					{StartUri: peopleNamespacePrefix + ":person-1"},
					{StartUri: peopleNamespacePrefix + ":person-2"},
				},
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
			g.Timeout(1 * time.Hour)
			b := buildTestBatch(store, []testPerson{
				{id: 1, friends: []int{2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20}},
			})
			pref := persist(store, dsm, b)
			start := []RelatedEntitiesContinuation{{StartUri: pref + ":person-1"}}
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
			g.Assert(queryResult[0].Continuation.NextIdxKey).IsNotNil()
			startFromCont := []RelatedEntitiesContinuation{queryResult[0].Continuation}
			queryResult, err = store.GetManyRelatedEntitiesBatch(startFromCont, "*", false, nil, 3)
			g.Assert(err).IsNil()
			g.Assert(len(queryResult)).Eql(1)
			g.Assert(len(queryResult[0].Relations)).Eql(3)
			g.Assert(queryResult[0].Relations[0].RelatedEntity.ID).Eql(pref + ":person-18")
			g.Assert(queryResult[0].Relations[1].RelatedEntity.ID).Eql(pref + ":person-17")
			g.Assert(queryResult[0].Relations[2].RelatedEntity.ID).Eql(pref + ":person-16")
			g.Assert(queryResult[0].Continuation.NextIdxKey).IsNotNil()
			startFromCont = []RelatedEntitiesContinuation{queryResult[0].Continuation}
			queryResult, err = store.GetManyRelatedEntitiesBatch(startFromCont, "*", false, nil, 25)
			g.Assert(err).IsNil()
			g.Assert(len(queryResult)).Eql(1)
			g.Assert(len(queryResult[0].Relations)).Eql(14)
			g.Assert(queryResult[0].Relations[0].RelatedEntity.ID).Eql(pref + ":person-15")
			g.Assert(queryResult[0].Relations[13].RelatedEntity.ID).Eql(pref + ":person-2")
			g.Assert(queryResult[0].Continuation.NextIdxKey).IsNotNil()
			startFromCont = []RelatedEntitiesContinuation{queryResult[0].Continuation}
			queryResult, err = store.GetManyRelatedEntitiesBatch(startFromCont, "*", false, nil, 25)
			g.Assert(err).IsNil()
			g.Assert(len(queryResult)).Eql(1)
			g.Assert(len(queryResult[0].Relations)).Eql(0)
			g.Assert(queryResult[0].Continuation.NextIdxKey).IsNil()
		})
	})
}

func persist(store *Store, dsm *DsManager, b []*Entity) string {
	friendsDS, _ := dsm.CreateDataset("friends", nil)
	_ = friendsDS.StoreEntities(b)
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
		e.References[peopleNamespacePrefix+":Friend"] = friends
		e.IsDeleted = p.deleted
		result = append(result, e)
	}
	return result
}

/*
setting up friends dataset of persons with friend relationships between them.
*/
func setupData(store *Store, dsm *DsManager) string {
	peopleNamespacePrefix, _ := store.NamespaceManager.AssertPrefixMappingForExpansion(
		"http://data.mimiro.io/people/",
	)
	personX := NewEntityFromMap(map[string]any{
		"id":      peopleNamespacePrefix + ":person-x",
		"deleted": true,
		"props":   map[string]any{peopleNamespacePrefix + ":Name": "will be deleted"},
		"refs": map[string]any{
			peopleNamespacePrefix + ":Friend": []string{
				peopleNamespacePrefix + ":person-1",
				peopleNamespacePrefix + ":person-2",
				peopleNamespacePrefix + ":person-3",
				peopleNamespacePrefix + ":person-4",
			},
		},
	})
	personX.IsDeleted = true
	tmpPerson3 := NewEntityFromMap(map[string]any{
		"id":    peopleNamespacePrefix + ":person-3",
		"props": map[string]any{peopleNamespacePrefix + ":Name": "Heidi"},
		"refs": map[string]any{
			peopleNamespacePrefix + ":Friend": []string{
				peopleNamespacePrefix + ":person-2",
			},
		},
	})
	tmpPerson3.IsDeleted = true

	friendsDS, _ := dsm.CreateDataset("friends", nil)
	_ = friendsDS.StoreEntities([]*Entity{
		NewEntityFromMap(map[string]any{
			"id":    peopleNamespacePrefix + ":person-1",
			"props": map[string]any{peopleNamespacePrefix + ":Name": "Lisa"},
			"refs": map[string]any{
				peopleNamespacePrefix + ":Friend": []string{peopleNamespacePrefix + ":person-x"},
			},
		}),
		NewEntityFromMap(map[string]any{
			"id":    peopleNamespacePrefix + ":person-2",
			"props": map[string]any{peopleNamespacePrefix + ":Name": "Jim"},
			"refs": map[string]any{
				peopleNamespacePrefix + ":Friend": []string{
					peopleNamespacePrefix + ":person-1",
					peopleNamespacePrefix + ":person-4",
				},
			},
		}),
		tmpPerson3,
		NewEntityFromMap(map[string]any{
			"id":    peopleNamespacePrefix + ":person-x",
			"props": map[string]any{peopleNamespacePrefix + ":Name": "will be deleted"},
			"refs": map[string]any{
				peopleNamespacePrefix + ":Friend": []string{
					peopleNamespacePrefix + ":person-1",
					peopleNamespacePrefix + ":person-2",
					peopleNamespacePrefix + ":person-3",
					peopleNamespacePrefix + ":person-4",
				},
			},
		}),
		NewEntityFromMap(map[string]any{
			"id":    peopleNamespacePrefix + ":person-3",
			"props": map[string]any{peopleNamespacePrefix + ":Name": "Heidi"},
			"refs": map[string]any{
				peopleNamespacePrefix + ":Friend": []string{
					peopleNamespacePrefix + ":person-2",
				},
			},
		}),
		personX,
		NewEntityFromMap(map[string]any{
			"id":    peopleNamespacePrefix + ":person-1",
			"props": map[string]any{peopleNamespacePrefix + ":Name": "Lisa"},
			"refs": map[string]any{
				peopleNamespacePrefix + ":Friend": []string{
					peopleNamespacePrefix + ":person-3",
					peopleNamespacePrefix + ":person-2",
					peopleNamespacePrefix + ":person-4",
				},
			},
		}),
	})

	return peopleNamespacePrefix
}
