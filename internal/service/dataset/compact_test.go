package dataset

import (
	"encoding/binary"
	"encoding/json"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/DataDog/datadog-go/v5/statsd"
	"github.com/dgraph-io/badger/v4"
	"github.com/mimiro-io/datahub/internal/conf"
	"github.com/mimiro-io/datahub/internal/server"
	"github.com/mimiro-io/datahub/internal/service/entity"
	"github.com/mimiro-io/datahub/internal/service/scheduler"
	"github.com/mimiro-io/datahub/internal/service/store"
	"github.com/mimiro-io/datahub/internal/service/types"
	"go.uber.org/zap"
)

func TestCompact(t *testing.T) {
	var store *server.Store
	var dsm *server.DsManager
	var ba server.BadgerAccess
	var compactor *CompactionWorker
	log := zap.NewNop().Sugar()
	testCnt := 0
	setup := func() func() {
		storeLocation := "./test_service_dataset_compact_" + strconv.Itoa(testCnt)
		os.RemoveAll(storeLocation)
		// lc = fxtest.NewLifecycle(internal.FxTestLog(GinkgoT(), false))
		env := &conf.Config{
			Logger:        zap.NewNop().Sugar(),
			StoreLocation: storeLocation,
		}
		store = server.NewStore(env, &statsd.NoOpClient{})
		dsm = server.NewDsManager(env, store, server.NoOpBus())
		ba = server.NewBadgerAccess(store, dsm)
		compactor = NewCompactor(store, dsm, log)
		return func() {
			store.Close()
			os.RemoveAll(storeLocation)
		}
	}
	// entities is a list of arrays where the first element
	// is the entity id and the second is the deleted state.
	// e.g. ["1", false] is an entity with id "1" and not deleted.
	// optionally, a third element can be added to specify properties
	//	e.g. ["1", false, map[string]interface{}{"name": "John Doe"}]
	//  or ["1", false, `{"name": "John Doe"}`]
	// and a fourth element can be added to specify references.
	// 	e.g. ["1", false, `{"name": "John Doe"}`, `{"ref1": "2"}`]
	// or ["1", false, nil, map[string]interface{}{"ref1": "2"}]
	mkDs := func(t *testing.T, name string, store *server.Store, entities ...[]any) {
		t.Helper()
		ds, _ := dsm.CreateDataset(name, nil)
		pref, err := store.NamespaceManager.AssertPrefixMappingForExpansion("http://ns/")
		if err != nil {
			t.Fatalf("error asserting prefix mapping: %v", err)
		}
		_, _ = store.NamespaceManager.AssertPrefixMappingForExpansion("http://ns2/")
		var ents []*server.Entity
		for _, e := range entities {
			entity := server.NewEntity(strings.ReplaceAll(e[0].(string), "http://ns/", pref+":"), 0)
			if len(e) > 1 {
				entity.IsDeleted = e[1].(bool)
			}
			if len(e) > 2 && e[2] != nil {
				if props, ok := e[2].(map[string]interface{}); ok {
					entity.Properties = props
				} else {
					props := make(map[string]interface{})
					err := json.Unmarshal([]byte(e[2].(string)), &props)
					if err != nil {
						t.Fatalf("error unmarshalling properties: %v", err)
					}
					entity.Properties = props
				}
			}
			if len(e) > 3 && e[3] != nil {
				if refs, ok := e[3].(map[string]interface{}); ok {
					entity.References = refs
				} else {
					refs := make(map[string]interface{})
					err := json.Unmarshal([]byte(e[3].(string)), &refs)
					if err != nil {
						t.Fatalf("error unmarshalling references: %v", err)
					}
					entity.References = refs
				}
			}
			ents = append(ents, entity)
		}
		err = ds.StoreEntities(ents)
		if err != nil {
			t.Fatalf("error storing entities: %v", err)
		}
	}

	checkChanges := func(t *testing.T, store *server.Store, dataset string, expected []any) {
		t.Helper()
		ds := dsm.GetDataset(dataset)
		changes, err := ds.GetChanges(0, 1000, false)
		if err != nil {
			t.Fatalf("error getting changes")
		}
		latests, err := ds.GetEntities("", 1000)
		if len(changes.Entities) != len(expected) {
			t.Fatalf("expected %d entities, got %d", len(expected), len(changes.Entities))
		}
		var latestSeen map[string]bool = make(map[string]bool)
		for i, e := range changes.Entities {
			// exp is an array with the elements [id, deleted, props, refs]. only id is mandatory
			exp := expected[i].([]any)
			if e.ID != exp[0] {
				t.Errorf("expected entity id %s, got %s", exp[0], e.ID)
			}
			del := false
			if len(exp) > 1 {
				del = exp[1].(bool)
			}
			if e.IsDeleted != del {
				t.Errorf("expected entity id %s to be deleted=%t, got deleted=%t",
					exp[0], exp[1], e.IsDeleted)
			}
			if len(exp) > 2 && exp[2] != nil {
				props := make(map[string]interface{})
				err = json.Unmarshal([]byte(exp[2].(string)), &props)
				if err != nil {
					t.Fatalf("error unmarshalling properties: %v", err)
				}
				if !reflect.DeepEqual(e.Properties, props) {
					t.Errorf("expected properties %v, got %v", props, e.Properties)
				}
			}
			if len(exp) > 3 && exp[3] != nil {
				refs := make(map[string]interface{})
				err = json.Unmarshal([]byte(exp[3].(string)), &refs)
				if err != nil {
					t.Fatalf("error unmarshalling references: %v", err)
				}
				if !reflect.DeepEqual(e.References, refs) {
					t.Errorf("expected references %v, got %v", refs, e.References)
				}
			}

			for _, l := range latests.Entities {
				if l.InternalID == e.InternalID {
					latestSeen[e.ID] = true
				}
			}
		}
		for _, l := range latests.Entities {
			if _, ok := latestSeen[l.ID]; !ok {
				t.Errorf("latest entity %s not found in changes", l.ID)
			}
		}
	}
	t.Run("using deduplication", func(t *testing.T) {
		for _, flushThreshold := range []int{1, 2, 100000} {
			//if true {
			//	flushThreshold := 1
			strat := func() CompactionStrategy {
				s := DeduplicationStrategy()
				s.(*deduplicationStrategy).flushAfter = flushThreshold
				return s
			}
			t.Run("with flush threshold "+strconv.Itoa(flushThreshold), func(t *testing.T) {
				//t.Run("with flush threshold 1", func(t *testing.T) {

				t.Run("empty dataset", func(t *testing.T) {
					defer setup()()
					mkDs(t, "people", store)
					compactor.compact("people", strat())
					checkChanges(t, store, "people", []any{})
				})
				t.Run("single changes dataset", func(t *testing.T) {
					defer setup()()
					mkDs(t, "people", store,
						[]any{"1"},
						[]any{"2", true},
						[]any{"3"})
					if err := compactor.compact("people", strat()); err != nil {
						t.Fatalf("error compacting dataset: %v", err)
					}

					// no change expected
					checkChanges(t, store, "people", []any{
						[]any{"1"},
						[]any{"2", true},
						[]any{"3"},
					})
				})
				t.Run("one entity duplicate", func(t *testing.T) {
					defer setup()()
					// create a dataset with 3 entities
					mkDs(t, "people", store,
						[]any{"http://ns/1"},
						[]any{"http://ns/2", true},
						[]any{"http://ns/3"})
					// "hack" a duplicate entity into the dataset.  datahub prevents this from happening but older versions of datahub did not.
					duplicateEntityChange("people", "http://ns/1", ba, store, dsm, t)
					// verify that the duplicate is present
					checkChanges(t, store, "people", []any{
						[]any{"ns3:1"},
						[]any{"ns3:2", true},
						[]any{"ns3:3"},
						[]any{"ns3:1"},
					})

					// Now do the compaction
					if err := compactor.compact("people", strat()); err != nil {
						t.Fatalf("error compacting dataset: %v", err)
					}

					// verify that the duplicate is gone
					checkChanges(t, store, "people", []any{
						[]any{"ns3:1"},
						[]any{"ns3:2", true},
						[]any{"ns3:3"},
					})
				})
				t.Run("many entity duplicates", func(t *testing.T) {
					defer setup()()
					// create a dataset with 3 entities
					mkDs(t, "people", store,
						[]any{"http://ns/1", false, `{"ns3:name": "John Doe"}`, `{"ns3:ref1": "ns3:2"}`},
						[]any{"http://ns/2", true},
						[]any{"http://ns/3"})
					// "hack" a duplicate entity into the dataset
					duplicateEntityChange("people", "http://ns/1", ba, store, dsm, t)
					duplicateEntityChange("people", "http://ns/1", ba, store, dsm, t)
					duplicateEntityChange("people", "http://ns/1", ba, store, dsm, t)
					duplicateEntityChange("people", "http://ns/2", ba, store, dsm, t)
					duplicateEntityChange("people", "http://ns/1", ba, store, dsm, t)
					duplicateEntityChange("people", "http://ns/1", ba, store, dsm, t)
					duplicateEntityChange("people", "http://ns/2", ba, store, dsm, t)
					duplicateEntityChange("people", "http://ns/2", ba, store, dsm, t)
					duplicateEntityChange("people", "http://ns/2", ba, store, dsm, t)
					duplicateEntityChange("people", "http://ns/2", ba, store, dsm, t)
					duplicateEntityChange("people", "http://ns/2", ba, store, dsm, t)
					duplicateEntityChange("people", "http://ns/2", ba, store, dsm, t)
					duplicateEntityChange("people", "http://ns/2", ba, store, dsm, t)

					checkChanges(t, store, "people", []any{
						[]any{"ns3:1", false, `{"ns3:name": "John Doe"}`, `{"ns3:ref1": "ns3:2"}`},
						[]any{"ns3:2", true},
						[]any{"ns3:3"},
						[]any{"ns3:1", false, `{"ns3:name": "John Doe"}`, `{"ns3:ref1": "ns3:2"}`},
						[]any{"ns3:1", false, `{"ns3:name": "John Doe"}`, `{"ns3:ref1": "ns3:2"}`},
						[]any{"ns3:1", false, `{"ns3:name": "John Doe"}`, `{"ns3:ref1": "ns3:2"}`},
						[]any{"ns3:2", true},
						[]any{"ns3:1", false, `{"ns3:name": "John Doe"}`, `{"ns3:ref1": "ns3:2"}`},
						[]any{"ns3:1", false, `{"ns3:name": "John Doe"}`, `{"ns3:ref1": "ns3:2"}`},
						[]any{"ns3:2", true},
						[]any{"ns3:2", true},
						[]any{"ns3:2", true},
						[]any{"ns3:2", true},
						[]any{"ns3:2", true},
						[]any{"ns3:2", true},
						[]any{"ns3:2", true},
					})

					// count refs for entity 1 before compaction
					if rcOut, rcIn := refCount(t, store, ba, "ns3:1", "ns3:ref1"); rcOut != 6 || rcIn != 6 {
						t.Fatalf("expected 6 ref out and 6 ref in for entity 1, got %d and %d", rcOut, rcIn)
					}

					checkQuery(t, store, "ns3:1", "ns3:ref1", false, "ns3:2")
					checkQuery(t, store, "ns3:2", "ns3:ref1", true, "ns3:1")
					// Now do the compaction
					if err := compactor.compact("people", strat()); err != nil {
						t.Fatalf("error compacting dataset: %v", err)
					}

					// verify that the duplicate is gone
					checkChanges(t, store, "people", []any{
						[]any{"ns3:1", false, `{"ns3:name": "John Doe"}`, `{"ns3:ref1": "ns3:2"}`},
						[]any{"ns3:2", true},
						[]any{"ns3:3"},
					})
					// count refs for entity 1 after compaction
					if rcOut, rcIn := refCount(t, store, ba, "ns3:1", "ns3:ref1"); rcOut != 1 || rcIn != 1 {
						t.Fatalf("expected 1 ref out and 1 ref in, got %d and %d", rcOut, rcIn)
					}
					checkQuery(t, store, "ns3:1", "ns3:ref1", false, "ns3:2")
					checkQuery(t, store, "ns3:2", "ns3:ref1", true, "ns3:1")
				})
				t.Run("with duplicate ref in many versions", func(t *testing.T) {
					t.Run("stored in different batches", func(t *testing.T) {
						defer setup()()
						// create a dataset with 3 entities
						// entity 2 is deleted without prior version, so datahub should not store any refs for it
						mkDs(t, "people", store,
							[]any{"http://ns/1", false, nil, `{"ns3:ref1": "ns3:2", "ns4:r1": "ns4:2"}`},
							[]any{"http://ns/2", true, nil, `{ "ns4:r1": ["ns4:2", "ns4:3"]}`},
							[]any{"http://ns/3", false})
						time.Sleep(1 * time.Millisecond) // make sure the txTime is different
						// now update entity 2 to be not deleted
						mkDs(t, "people", store,
							[]any{"http://ns/1", false, `{"p": "a"}`, `{"ns3:ref1": "ns3:2", "ns4:r1": "ns4:2"}`},
							[]any{"http://ns/2", false, nil, `{ "ns4:r1": ["ns4:2", "ns4:3"]}`},
						)
						time.Sleep(1 * time.Millisecond) // make sure the txTime is different
						mkDs(t, "people", store,
							[]any{"http://ns/1", false, `{"p": "b"}`, `{"ns3:ref1": "ns3:2", "ns4:r1": "ns4:2"}`},
						)

						// verify that the duplicate is present
						checkChanges(t, store, "people", []any{
							[]any{"ns3:1", false, nil, `{"ns3:ref1":"ns3:2", "ns4:r1":"ns4:2"}`},
							[]any{"ns3:2", true, nil, `{"ns4:r1":["ns4:2", "ns4:3"]}`},
							[]any{"ns3:3", false},
							[]any{"ns3:1", false, nil, `{"ns3:ref1":"ns3:2", "ns4:r1":"ns4:2"}`},
							[]any{"ns3:2", false, nil, `{"ns4:r1":["ns4:2", "ns4:3"]}`},
							[]any{"ns3:1", false, nil, `{"ns3:ref1":"ns3:2", "ns4:r1":"ns4:2"}`},
						})
						if rcOut, rcIn := refCount(t, store, ba, "ns3:1", "ns3:ref1"); rcOut != 3 || rcIn != 3 {
							t.Fatalf("expected 3 ref out and 3 ref in, got %d and %d", rcOut, rcIn)
						}
						if rcOut, rcIn := refCount(t, store, ba, "ns3:1", "ns4:r1"); rcOut != 3 || rcIn != 3 {
							t.Fatalf("expected 3 ref out and 3 ref in, got %d and %d", rcOut, rcIn)
						}
						if rcOut, rcIn := refCount(t, store, ba, "ns3:2", "ns4:r1"); rcOut != 2 || rcIn != 2 {
							t.Fatalf("expected 2 ref out and 2 ref in, got %d and %d", rcOut, rcIn)
						}

						newStats := getStats(t, ba, store, log)
						peopleIncoming := newStats["refs"].(map[string]any)["INCOMING_REF_INDEX"].(map[string]any)["people"].(map[string]any)
						peopleOutgoing := newStats["refs"].(map[string]any)["OUTGOING_REF_INDEX"].(map[string]any)["people"].(map[string]any)
						if peopleIncoming["keys"] != 8.0 {
							t.Fatalf("expected 8 incoming ref keys, got %.0f", peopleIncoming["keys"])
						}
						if peopleOutgoing["keys"] != 8.0 {
							t.Fatalf("expected 8 outgoing ref keys, got %.0f", peopleOutgoing["keys"])
						}
						if newStats["refs"].(map[string]any)["INCOMING_REF_INDEX"].(map[string]any)["people (deleted)"] != nil {
							t.Fatalf("not expected incoming ref index for deleted entities")
						}
						if newStats["refs"].(map[string]any)["OUTGOING_REF_INDEX"].(map[string]any)["people (deleted)"] != nil {
							t.Fatalf("not expected outgoing ref index for deleted entities")
						}

						// query for the entity before the compaction
						checkQuery(t, store, "ns3:1", "ns3:ref1", false, "ns3:2")
						checkQuery(t, store, "ns3:2", "ns3:ref1", true, "ns3:1")

						// Now do the compaction
						if err := compactor.compact("people", strat()); err != nil {
							t.Fatalf("error compacting dataset: %v", err)
						}

						// query for the entity after the compaction
						checkQuery(t, store, "ns3:1", "ns3:ref1", false, "ns3:2")
						checkQuery(t, store, "ns3:2", "ns3:ref1", true, "ns3:1")
						// verify that the duplicate is gone
						checkChanges(t, store, "people", []any{
							[]any{"ns3:1", false, nil, `{"ns3:ref1":"ns3:2", "ns4:r1":"ns4:2"}`},
							[]any{"ns3:2", true, nil, `{"ns4:r1":["ns4:2", "ns4:3"]}`},
							[]any{"ns3:3", false},
							[]any{"ns3:1", false, nil, `{"ns3:ref1":"ns3:2", "ns4:r1":"ns4:2"}`},
							[]any{"ns3:2", false, nil, `{"ns4:r1":["ns4:2", "ns4:3"]}`},
							[]any{"ns3:1", false, nil, `{"ns3:ref1":"ns3:2", "ns4:r1":"ns4:2"}`},
						})

						// in this case, all reference duplicates were in the same batch, therefore they have the same key (sam txTime).
						if rcOut, rcIn := refCount(t, store, ba, "ns3:1", "ns3:ref1"); rcOut != 1 || rcIn != 1 {
							t.Fatalf("expected 1 ref out and 1 ref in, got %d and %d", rcOut, rcIn)
						}
						if rcOut, rcIn := refCount(t, store, ba, "ns3:1", "ns4:r1"); rcOut != 1 || rcIn != 1 {
							t.Fatalf("expected 1 ref out and 1 ref in, got %d and %d", rcOut, rcIn)
						}
						if rcOut, rcIn := refCount(t, store, ba, "ns3:2", "ns4:r1"); rcOut != 2 || rcIn != 2 {
							// t.Fatalf("expected 2 ref out and 2 ref in, got %d and %d", rcOut, rcIn)
						}
						newStats = getStats(t, ba, store, log)
						peopleIncoming = newStats["refs"].(map[string]any)["INCOMING_REF_INDEX"].(map[string]any)["people"].(map[string]any)
						peopleOutgoing = newStats["refs"].(map[string]any)["OUTGOING_REF_INDEX"].(map[string]any)["people"].(map[string]any)
						if peopleIncoming["keys"] != 4.0 {
							t.Fatalf("expected 4 incoming ref keys, got %.0f", peopleIncoming["keys"])
						}
						if peopleOutgoing["keys"] != 4.0 {
							t.Fatalf("expected 4 outgoing ref keys, got %.0f", peopleOutgoing["keys"])
						}
						if newStats["refs"].(map[string]any)["INCOMING_REF_INDEX"].(map[string]any)["people (deleted)"] != nil {
							t.Fatalf("not expected incoming ref index for deleted entities")
						}
						if newStats["refs"].(map[string]any)["OUTGOING_REF_INDEX"].(map[string]any)["people (deleted)"] != nil {
							t.Fatalf("not expected outgoing ref index for deleted entities")
						}
					})
					t.Run("stored in same batch", func(t *testing.T) {
						defer setup()()
						// create a dataset with 3 entities
						mkDs(t, "people", store, []any{"http://ns/1", false, nil, `{"ns3:ref1": "ns3:2"}`},
							[]any{"http://ns/2", true},
							[]any{"http://ns/1", false, `{"p": "a"}`, `{"ns3:ref1": "ns3:2"}`},
							[]any{"http://ns/1", false, `{"p": "b"}`, `{"ns3:ref1": "ns3:2"}`},
							[]any{"http://ns/3", false})

						// verify that the duplicate is present
						checkChanges(t, store, "people", []any{
							[]any{"ns3:1", false, nil, `{"ns3:ref1":"ns3:2"}`},
							[]any{"ns3:2", true},
							[]any{"ns3:1", false, nil, `{"ns3:ref1":"ns3:2"}`},
							[]any{"ns3:1", false, nil, `{"ns3:ref1":"ns3:2"}`},
							[]any{"ns3:3", false},
						})
						if rcOut, rcIn := refCount(t, store, ba, "ns3:1", "ns3:ref1"); rcOut != 1 || rcIn != 1 {
							t.Fatalf("expected 1 ref out and 1 ref in, got %d and %d", rcOut, rcIn)
						}
						newStats := getStats(t, ba, store, log)
						peopleIncoming := newStats["refs"].(map[string]any)["INCOMING_REF_INDEX"].(map[string]any)["people"].(map[string]any)
						peopleOutgoing := newStats["refs"].(map[string]any)["OUTGOING_REF_INDEX"].(map[string]any)["people"].(map[string]any)
						if peopleIncoming["keys"] != 1.0 {
							t.Fatalf("expected 1 incoming ref keys, got %.0f", peopleIncoming["keys"])
						}
						if peopleOutgoing["keys"] != 1.0 {
							t.Fatalf("expected 1 outgoing ref keys, got %.0f", peopleOutgoing["keys"])
						}

						// query for the entity before the compaction
						checkQuery(t, store, "ns3:1", "ns3:ref1", false, "ns3:2")
						checkQuery(t, store, "ns3:2", "ns3:ref1", true, "ns3:1")

						// Now do the compaction. it should not have to do anything since all refs have same txn time
						if err := compactor.compact("people", strat()); err != nil {
							t.Fatalf("error compacting dataset: %v", err)
						}

						// query for the entity after the compaction
						checkQuery(t, store, "ns3:1", "ns3:ref1", false, "ns3:2")
						checkQuery(t, store, "ns3:2", "ns3:ref1", true, "ns3:1")
						// verify that the duplicate is gone
						checkChanges(t, store, "people", []any{
							[]any{"ns3:1", false, nil, `{"ns3:ref1":"ns3:2"}`},
							[]any{"ns3:2", true},
							[]any{"ns3:1", false, nil, `{"ns3:ref1":"ns3:2"}`},
							[]any{"ns3:1", false, nil, `{"ns3:ref1":"ns3:2"}`},
							[]any{"ns3:3", false},
						})

						// in this case, all reference duplicates were in the same batch, therefore they have the same key (sam txTime).
						if rcOut, rcIn := refCount(t, store, ba, "ns3:1", "ns3:ref1"); rcOut != 1 || rcIn != 1 {
							t.Fatalf("expected 1 ref out and 1 ref in, got %d and %d", rcOut, rcIn)
						}
						newStats = getStats(t, ba, store, log)
						peopleIncoming = newStats["refs"].(map[string]any)["INCOMING_REF_INDEX"].(map[string]any)["people"].(map[string]any)
						peopleOutgoing = newStats["refs"].(map[string]any)["OUTGOING_REF_INDEX"].(map[string]any)["people"].(map[string]any)
						if peopleIncoming["keys"] != 1.0 {
							t.Fatalf("expected 1 incoming ref keys, got %.0f", peopleIncoming["keys"])
						}
						if peopleOutgoing["keys"] != 1.0 {
							t.Fatalf("expected 1 outgoing ref keys, got %.0f", peopleOutgoing["keys"])
						}
					})
					t.Run("with alternating delete state", func(t *testing.T) {
						defer setup()()
						var times []int64
						for _, e := range [][]any{
							{"http://ns/1", false, nil, `{"ns3:ref1": ["ns3:2", "ns3:3"]}`},
							{"http://ns/1", false, `{"p": "a"}`, `{"ns3:ref1": ["ns3:2", "ns3:3"]}`},
							{"http://ns/1", true, `{"p": "b"}`, `{"ns3:ref1": ["ns3:2", "ns3:3"]}`},
							{"http://ns/1", true, `{"p": "c"}`, `{"ns3:ref1": ["ns3:2", "ns3:3"]}`},
							{"http://ns/1", true, `{"p": "d"}`, `{"ns3:ref1": ["ns3:2", "ns3:3"]}`},
							{"http://ns/1", false, `{"p": "e"}`, `{"ns3:ref1": ["ns3:2", "ns3:3"]}`},
							{"http://ns/1", false, `{"p": "f"}`, `{"ns3:ref1": ["ns3:2", "ns3:3"]}`},
						} {
							mkDs(t, "people", store, e)
							times = append(times, time.Now().UnixNano())
							time.Sleep(1 * time.Millisecond) // make sure the txTime is different
						}

						// verify that the duplicate is present
						checkChanges(t, store, "people", []any{
							[]any{"ns3:1", false, nil, `{"ns3:ref1": ["ns3:2", "ns3:3"]}`},
							[]any{"ns3:1", false, `{"p": "a"}`, `{"ns3:ref1": ["ns3:2", "ns3:3"]}`},
							[]any{"ns3:1", true, `{"p": "b"}`, `{"ns3:ref1": ["ns3:2", "ns3:3"]}`},
							[]any{"ns3:1", true, `{"p": "c"}`, `{"ns3:ref1": ["ns3:2", "ns3:3"]}`},
							[]any{"ns3:1", true, `{"p": "d"}`, `{"ns3:ref1": ["ns3:2", "ns3:3"]}`},
							[]any{"ns3:1", false, `{"p": "e"}`, `{"ns3:ref1": ["ns3:2", "ns3:3"]}`},
							[]any{"ns3:1", false, `{"p": "f"}`, `{"ns3:ref1": ["ns3:2", "ns3:3"]}`},
						})
						if rcOut, rcIn := refCount(t, store, ba, "ns3:1", "ns3:ref1"); rcOut != 14 || rcIn != 14 {
							t.Fatalf("expected 14 ref out and 14 ref in, got %d and %d", rcOut, rcIn)
						}
						newStats := getStats(t, ba, store, log)
						peopleIncoming := newStats["refs"].(map[string]any)["INCOMING_REF_INDEX"].(map[string]any)["people"].(map[string]any)
						peopleOutgoing := newStats["refs"].(map[string]any)["OUTGOING_REF_INDEX"].(map[string]any)["people"].(map[string]any)
						if peopleIncoming["keys"] != 8.0 {
							t.Fatalf("expected 8 incoming ref keys, got %.0f", peopleIncoming["keys"])
						}
						if peopleOutgoing["keys"] != 8.0 {
							t.Fatalf("expected 8 outgoing ref keys, got %.0f", peopleOutgoing["keys"])
						}
						peopleIncoming = newStats["refs"].(map[string]any)["INCOMING_REF_INDEX"].(map[string]any)["people (deleted)"].(map[string]any)
						peopleOutgoing = newStats["refs"].(map[string]any)["OUTGOING_REF_INDEX"].(map[string]any)["people (deleted)"].(map[string]any)
						if peopleIncoming["keys"] != 6.0 {
							t.Fatalf("expected 6 incoming deleted ref keys, got %.0f", peopleIncoming["keys"])
						}
						if peopleOutgoing["keys"] != 6.0 {
							t.Fatalf("expected 6 outgoing ref keys, got %.0f", peopleOutgoing["keys"])
						}

						// query for the entity before the compaction
						queryResult, _ := store.GetManyRelatedEntities([]string{"ns3:1"}, "ns3:ref1", false, nil, false)
						if len(queryResult) != 2 {
							t.Fatalf("expected 2 related entities, got %d", len(queryResult))
						}
						if queryResult[0][2].(*server.Entity).ID != "ns3:3" {
							t.Fatalf("expected related entity id ns3:3, got %s", queryResult[0][2].(*server.Entity).ID)
						}
						if queryResult[1][2].(*server.Entity).ID != "ns3:2" {
							t.Fatalf("expected related entity id ns3:2, got %s", queryResult[1][2].(*server.Entity).ID)
						}
						checkQuery(t, store, "ns3:2", "ns3:ref1", true, "ns3:1")
						checkQuery(t, store, "ns3:3", "ns3:ref1", true, "ns3:1")

						// also do point in time queries
						// outgoing
						checkRelatedAtPointInTime(times[0]-int64(1*time.Second), t, store, "ns3:1", "ns3:ref1", false, []string{})
						checkRelatedAtPointInTime(times[0], t, store, "ns3:1", "ns3:ref1", false, []string{"ns3:3", "ns3:2"})
						checkRelatedAtPointInTime(times[1], t, store, "ns3:1", "ns3:ref1", false, []string{"ns3:3", "ns3:2"})
						checkRelatedAtPointInTime(times[2], t, store, "ns3:1", "ns3:ref1", false, []string{})
						checkRelatedAtPointInTime(times[3], t, store, "ns3:1", "ns3:ref1", false, []string{})
						checkRelatedAtPointInTime(times[4], t, store, "ns3:1", "ns3:ref1", false, []string{})
						checkRelatedAtPointInTime(times[5], t, store, "ns3:1", "ns3:ref1", false, []string{"ns3:3", "ns3:2"})
						checkRelatedAtPointInTime(times[6], t, store, "ns3:1", "ns3:ref1", false, []string{"ns3:3", "ns3:2"})
						checkRelatedAtPointInTime(times[6]+int64(1*time.Second), t, store, "ns3:1", "ns3:ref1", false, []string{"ns3:3", "ns3:2"})

						// incoming
						checkRelatedAtPointInTime(times[0]-int64(1*time.Second), t, store, "ns3:3", "ns3:ref1", true, nil)
						checkRelatedAtPointInTime(times[0], t, store, "ns3:3", "ns3:ref1", true, []string{"ns3:1"})
						checkRelatedAtPointInTime(times[1], t, store, "ns3:3", "ns3:ref1", true, []string{"ns3:1"})
						checkRelatedAtPointInTime(times[2], t, store, "ns3:3", "ns3:ref1", true, []string{})
						checkRelatedAtPointInTime(times[3], t, store, "ns3:3", "ns3:ref1", true, []string{})
						checkRelatedAtPointInTime(times[4], t, store, "ns3:3", "ns3:ref1", true, []string{})
						checkRelatedAtPointInTime(times[5], t, store, "ns3:3", "ns3:ref1", true, []string{"ns3:1"})
						checkRelatedAtPointInTime(times[6], t, store, "ns3:3", "ns3:ref1", true, []string{"ns3:1"})
						checkRelatedAtPointInTime(times[6]+int64(1*time.Second), t, store, "ns3:3", "ns3:ref1", true, []string{"ns3:1"})

						// Now do the compaction. it should not have to do anything since all refs have same txn time
						if err := compactor.compact("people", strat()); err != nil {
							t.Fatalf("error compacting dataset: %v", err)
						}

						// query for the entity after the compaction
						queryResult, _ = store.GetManyRelatedEntities([]string{"ns3:1"}, "ns3:ref1", false, nil, false)
						if len(queryResult) != 2 {
							t.Fatalf("expected 2 related entities, got %d", len(queryResult))
						}
						if queryResult[0][2].(*server.Entity).ID != "ns3:3" {
							t.Fatalf("expected related entity id ns3:3, got %s", queryResult[0][2].(*server.Entity).ID)
						}
						if queryResult[1][2].(*server.Entity).ID != "ns3:2" {
							t.Fatalf("expected related entity id ns3:2, got %s", queryResult[1][2].(*server.Entity).ID)
						}
						checkQuery(t, store, "ns3:2", "ns3:ref1", true, "ns3:1")
						checkQuery(t, store, "ns3:3", "ns3:ref1", true, "ns3:1")

						// verify that the changes are still there
						checkChanges(t, store, "people", []any{
							[]any{"ns3:1", false, nil, `{"ns3:ref1": ["ns3:2", "ns3:3"]}`},
							[]any{"ns3:1", false, `{"p": "a"}`, `{"ns3:ref1": ["ns3:2", "ns3:3"]}`},
							[]any{"ns3:1", true, `{"p": "b"}`, `{"ns3:ref1": ["ns3:2", "ns3:3"]}`},
							[]any{"ns3:1", true, `{"p": "c"}`, `{"ns3:ref1": ["ns3:2", "ns3:3"]}`},
							[]any{"ns3:1", true, `{"p": "d"}`, `{"ns3:ref1": ["ns3:2", "ns3:3"]}`},
							[]any{"ns3:1", false, `{"p": "e"}`, `{"ns3:ref1": ["ns3:2", "ns3:3"]}`},
							[]any{"ns3:1", false, `{"p": "f"}`, `{"ns3:ref1": ["ns3:2", "ns3:3"]}`},
						})

						// check that reference duplicates are gone.
						// 6 expected = 2 from first change, 2 from first delete (change nr3), 2 from undelete (change nr6)
						if rcOut, rcIn := refCount(t, store, ba, "ns3:1", "ns3:ref1"); rcOut != 6 || rcIn != 6 {
							t.Fatalf("expected 6 ref out and 6 ref in, got %d and %d", rcOut, rcIn)
						}
						newStats = getStats(t, ba, store, log)
						peopleIncoming = newStats["refs"].(map[string]any)["INCOMING_REF_INDEX"].(map[string]any)["people"].(map[string]any)
						peopleOutgoing = newStats["refs"].(map[string]any)["OUTGOING_REF_INDEX"].(map[string]any)["people"].(map[string]any)
						if peopleIncoming["keys"] != 4.0 {
							t.Fatalf("expected 4 incoming ref keys, got %.0f", peopleIncoming["keys"])
						}
						if peopleOutgoing["keys"] != 4.0 {
							t.Fatalf("expected 4 outgoing ref keys, got %.0f", peopleOutgoing["keys"])
						}
						peopleIncoming = newStats["refs"].(map[string]any)["INCOMING_REF_INDEX"].(map[string]any)["people (deleted)"].(map[string]any)
						peopleOutgoing = newStats["refs"].(map[string]any)["OUTGOING_REF_INDEX"].(map[string]any)["people (deleted)"].(map[string]any)
						if peopleIncoming["keys"] != 2.0 {
							t.Fatalf("expected 2 incoming deleted ref keys, got %.0f", peopleIncoming["keys"])
						}
						if peopleOutgoing["keys"] != 2.0 {
							t.Fatalf("expected 2 outgoing ref keys, got %.0f", peopleOutgoing["keys"])
						}

						// again do point in time queries
						// outgoing
						checkRelatedAtPointInTime(times[0]-int64(1*time.Second), t, store, "ns3:1", "ns3:ref1", false, []string{})
						checkRelatedAtPointInTime(times[0], t, store, "ns3:1", "ns3:ref1", false, []string{"ns3:3", "ns3:2"})
						checkRelatedAtPointInTime(times[1], t, store, "ns3:1", "ns3:ref1", false, []string{"ns3:3", "ns3:2"})
						checkRelatedAtPointInTime(times[2], t, store, "ns3:1", "ns3:ref1", false, []string{})
						checkRelatedAtPointInTime(times[3], t, store, "ns3:1", "ns3:ref1", false, []string{})
						checkRelatedAtPointInTime(times[4], t, store, "ns3:1", "ns3:ref1", false, []string{})
						checkRelatedAtPointInTime(times[5], t, store, "ns3:1", "ns3:ref1", false, []string{"ns3:3", "ns3:2"})
						checkRelatedAtPointInTime(times[6], t, store, "ns3:1", "ns3:ref1", false, []string{"ns3:3", "ns3:2"})
						checkRelatedAtPointInTime(times[6]+int64(1*time.Second), t, store, "ns3:1", "ns3:ref1", false, []string{"ns3:3", "ns3:2"})

						// incoming
						checkRelatedAtPointInTime(times[0]-int64(1*time.Second), t, store, "ns3:3", "ns3:ref1", true, nil)
						checkRelatedAtPointInTime(times[0], t, store, "ns3:3", "ns3:ref1", true, []string{"ns3:1"})
						checkRelatedAtPointInTime(times[1], t, store, "ns3:3", "ns3:ref1", true, []string{"ns3:1"})
						checkRelatedAtPointInTime(times[2], t, store, "ns3:3", "ns3:ref1", true, []string{})
						checkRelatedAtPointInTime(times[3], t, store, "ns3:3", "ns3:ref1", true, []string{})
						checkRelatedAtPointInTime(times[4], t, store, "ns3:3", "ns3:ref1", true, []string{})
						checkRelatedAtPointInTime(times[5], t, store, "ns3:3", "ns3:ref1", true, []string{"ns3:1"})
						checkRelatedAtPointInTime(times[6], t, store, "ns3:3", "ns3:ref1", true, []string{"ns3:1"})
						checkRelatedAtPointInTime(times[6]+int64(1*time.Second), t, store, "ns3:3", "ns3:ref1", true, []string{"ns3:1"})
					})
				})
			})
		}
	})
}

func checkRelatedAtPointInTime(when int64, t *testing.T, store *server.Store, from string, via string, inverse bool, expected []string) {
	t.Helper()
	fromEntity, err := store.GetEntity(from, nil, true)
	if err != nil {
		t.Fatalf("error getting from entity: %v", err)
	}
	var expEntities []*server.Entity
	for _, e := range expected {
		expEntity, err := store.GetEntity(e, nil, true)
		if err != nil {
			t.Fatalf("error getting to entity: %v", err)
		}
		expEntities = append(expEntities, expEntity)
	}
	entityInternalId := fromEntity.InternalID
	// 1. build a RelatedFrom query input
	searchBuffer := make([]byte, 10)
	if inverse {
		binary.BigEndian.PutUint16(searchBuffer, server.IncomingRefIndex)
	} else {
		binary.BigEndian.PutUint16(searchBuffer, server.OutgoingRefIndex)
	}
	binary.BigEndian.PutUint64(searchBuffer[2:], entityInternalId)

	predID, err := store.GetPredicateID(via, nil)
	if err != nil {
		t.Fatalf("error getting predicate id: %v", err)
	}
	relatedFrom := &server.RelatedFrom{
		RelationIndexFromKey: searchBuffer,
		Predicate:            predID,
		Inverse:              inverse,
		Datasets:             nil,
		At:                   when,
	}
	qresult, _, err := store.GetRelatedAtTime(relatedFrom, 100)
	if err != nil {
		t.Fatalf("error getting related entities: %v", err)
	}
	if len(qresult) != len(expEntities) {
		t.Fatalf("expected %d related entities, got %d", len(expEntities), len(qresult))
	}
	for i, e := range qresult {
		if e.EntityID != expEntities[i].InternalID {
			t.Fatalf("expected related entity id %v, got %v", expEntities[i].InternalID, e.EntityID)
		}
	}

}

func checkQuery(t *testing.T, store *server.Store, from string, via string, inverse bool, expectedID string) {
	t.Helper()
	queryResult, err := store.GetManyRelatedEntities([]string{from}, via, inverse, nil, false)
	if err != nil {
		t.Fatalf("error querying for related entities: %v", err)
	}
	if len(queryResult) != 1 {
		t.Fatalf("expected 1 related entity, got %d", len(queryResult))
	}
	if queryResult[0][2].(*server.Entity).ID != expectedID {
		t.Fatalf("expected related entity id %s, got %s", expectedID, queryResult[0][2].(*server.Entity).ID)
	}
}

func getStats(t *testing.T, ba server.BadgerAccess, store *server.Store, log *zap.SugaredLogger) map[string]any {
	t.Helper()
	// ba.GetDB().Flatten(4)
	// time.Sleep(10 * time.Second)
	su := scheduler.NewStatisticsUpdater(log, ba)
	su.Run()
	for su.State() != scheduler.TaskStateScheduled {
		// fmt.Println("waiting for stats update")
		time.Sleep(100 * time.Millisecond)
	}
	sr := &server.Statistics{
		Store:  store,
		Logger: log,
	}
	stringWriter := &strings.Builder{}
	sr.GetStatistics(stringWriter)
	var o map[string]any
	err := json.Unmarshal([]byte(stringWriter.String()), &o)
	if err != nil {
		t.Fatalf("error unmarshalling stats: %v", err)
	}

	return o
}

func refCount(t *testing.T, s *server.Store, ba server.BadgerAccess, entityID string, predicate string) (int, int) {
	t.Helper()
	cntOut := 0
	cntIn := 0
	err := ba.GetDB().View(func(txn *badger.Txn) error {
		pid, err := s.GetPredicateID(predicate, txn)
		if err != nil {
			t.Fatalf("error getting predicate id: %v", err)
		}
		internalEntityId, err := entity.Lookup{}.InternalIDForCURIE(txn, types.CURIE(entityID))
		searchBuffer := make([]byte, 10)
		binary.BigEndian.PutUint16(searchBuffer, server.OutgoingRefIndex)
		binary.BigEndian.PutUint64(searchBuffer[2:], uint64(internalEntityId))
		opts := badger.DefaultIteratorOptions
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Seek(searchBuffer); it.ValidForPrefix(searchBuffer); it.Next() {
			k := it.Item().Key()
			if binary.BigEndian.Uint64(k[18:26]) == pid {
				cntOut++
			}
		}

		searchBuffer = make([]byte, 2)
		binary.BigEndian.PutUint16(searchBuffer, server.IncomingRefIndex)
		it = txn.NewIterator(opts)
		defer it.Close()
		for it.Seek(searchBuffer); it.ValidForPrefix(searchBuffer); it.Next() {
			k := it.Item().Key()
			if binary.BigEndian.Uint64(k[26:34]) == pid && binary.BigEndian.Uint64(k[10:18]) == uint64(internalEntityId) {
				cntIn++
			}
		}

		// 0:2: incoming ref index, uint16
		// 2:10: other entity id, uint64
		// 10:18: this entity id, uint64
		// 18:26: txn time, uint64
		// 26:34: predicate, uint64
		// 34:36: is deleted, uint16
		// 36:40: dataset id, uint32
		return nil
	})
	if err != nil {
		t.Fatalf("error getting predicate id: %v", err)
	}
	return cntOut, cntIn
}

func duplicateEntityChange(dataset string, entityID string, ba store.BadgerStore, store *server.Store, dsm *server.DsManager, t *testing.T) {
	t.Helper()

	time.Sleep(1 * time.Millisecond) // make sure the txTime is different
	ent, err := store.GetEntity(entityID, []string{dataset}, true)
	if err != nil {
		t.Fatalf("error getting entity: %v", err)
	}
	ent.IsDeleted = !ent.IsDeleted
	ds := dsm.GetDataset(dataset)
	ds.StoreEntities([]*server.Entity{ent})
	// capture all the keys that will be deleted
	cnt, err := ds.ProcessChangesRaw(0, 1000, true, func(entity []byte) error {
		return nil
	})
	if err != nil {
		t.Fatalf("error processing changes: %v", err)
	}
	changLogKey := make([]byte, 22)
	binary.BigEndian.PutUint16(changLogKey, server.DatasetEntityChangeLog)
	binary.BigEndian.PutUint32(changLogKey[2:], ds.InternalID)
	binary.BigEndian.PutUint64(changLogKey[6:], uint64(cnt-1))
	binary.BigEndian.PutUint64(changLogKey[14:], ent.InternalID)

	var jsonKey []byte
	var refs [][]byte
	ba.GetDB().View(func(txn *badger.Txn) error {
		item, err := txn.Get(changLogKey)
		if err != nil {
			t.Fatalf("error getting item: %v", err)
		}
		jsonKey, err = item.ValueCopy(nil)
		if err != nil {
			t.Fatalf("error copying value: %v", err)
		}
		refs, err = findRefs(ent, jsonKey, txn, entity.Lookup{})
		if err != nil {
			t.Fatalf("error finding refs: %v", err)
		}
		return nil
	})
	// latestKey := mkLatestKey(jsonKey)

	ent.IsDeleted = !ent.IsDeleted
	ds.StoreEntities([]*server.Entity{ent})

	// delete keys
	wg := sync.WaitGroup{}
	wg.Add(1)
	ba.GetDB().Update(func(txn *badger.Txn) error {
		defer wg.Done()
		err := txn.Delete(jsonKey)
		if err != nil {
			t.Fatalf("error deleting json key: %v", err)
		}
		err = txn.Delete(changLogKey)
		if err != nil {
			t.Fatalf("error deleting change log key: %v", err)
		}
		//err = txn.Delete(latestKey)
		//if err != nil {
		//	t.Fatalf("error deleting latest key: %v", err)
		//}
		for _, ref := range refs {
			err = txn.Delete(ref)
			if err != nil {
				t.Fatalf("error deleting ref: %v", err)
			}
		}
		return nil
	})
	wg.Wait()
}
