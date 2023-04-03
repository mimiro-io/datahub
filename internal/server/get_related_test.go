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
			peopleNamespacePrefix := setupData(store, dsm)
			result, err := store.GetManyRelatedEntitiesBatch(
				[]RelatedEntitiesContinuation{{StartUri: peopleNamespacePrefix + ":person-1"}},
				"*", false, nil, 0)
			g.Assert(err).IsNil()
			g.Assert(len(result)).Eql(1)
			g.Assert(len(result[0].Relations)).Eql(4)

			result, err = store.GetManyRelatedEntitiesBatch(
				[]RelatedEntitiesContinuation{{StartUri: peopleNamespacePrefix + ":person-1"}},
				"*", false, nil, 1)
			g.Assert(err).IsNil()
			g.Assert(len(result)).Eql(1)
			g.Assert(len(result[0].Relations)).Eql(1)

			result, err = store.GetManyRelatedEntitiesBatch(
				[]RelatedEntitiesContinuation{{StartUri: peopleNamespacePrefix + ":person-1"}},
				"*", false, nil, 2)
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
			g.Assert(len(result[0].Relations)).Eql(2)

			g.Assert(1).Eql(1)

			g.Assert(len(result[1].Relations)).
				Eql(2)
			// TODO: person-x is found, but its deleted? also in go-debug it does not find person-x???

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

			// limit 3, room for all 2 relations of first startUri plus 1 from other startUri
			result, err = store.GetManyRelatedEntitiesBatch(
				[]RelatedEntitiesContinuation{
					{StartUri: peopleNamespacePrefix + ":person-1"},
					{StartUri: peopleNamespacePrefix + ":person-2"},
				},
				"*",
				true,
				nil,
				3,
			)
			g.Assert(err).IsNil()
			g.Assert(len(result)).Eql(2)
			g.Assert(len(result[0].Relations)).Eql(2)
			g.Assert(len(result[1].Relations)).Eql(1)
		})
	})
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
				// TODO: this should not break the test??
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
