package dataset

import (
	"encoding/binary"
	"encoding/json"
	"github.com/DataDog/datadog-go/v5/statsd"
	"github.com/dgraph-io/badger/v4"
	"github.com/mimiro-io/datahub/internal/conf"
	"github.com/mimiro-io/datahub/internal/server"
	"github.com/mimiro-io/datahub/internal/service/entity"
	"github.com/mimiro-io/datahub/internal/service/store"
	"github.com/mimiro-io/datahub/internal/service/types"
	"go.uber.org/zap"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestCompact(t *testing.T) {
	var store *server.Store
	var dsm *server.DsManager
	var ba server.BadgerAccess
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
		ents, err := ds.GetChanges(0, 1000, false)
		if err != nil {
			t.Fatalf("error getting changes")
		}
		if len(ents.Entities) != len(expected) {
			t.Fatalf("expected %d entities, got %d", len(expected), len(ents.Entities))
		}
		for i, e := range ents.Entities {
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
			if len(exp) > 2 {
				refs := make(map[string]interface{})
				err = json.Unmarshal([]byte(exp[2].(string)), &refs)
				if err != nil {
					t.Fatalf("error unmarshalling references: %v", err)
				}
				if !reflect.DeepEqual(e.References, refs) {
					t.Errorf("expected references %v, got %v", refs, e.References)
				}
			}
		}
	}
	t.Run("using DeduplicationStrategy", func(t *testing.T) {
		t.Run("empty dataset", func(t *testing.T) {
			defer setup()()
			mkDs(t, "people", store)
			Compact(ba, "people", DeduplicationStrategy)
			checkChanges(t, store, "people", []any{})
		})
		t.Run("single changes dataset", func(t *testing.T) {
			defer setup()()
			mkDs(t, "people", store,
				[]any{"1"},
				[]any{"2", true},
				[]any{"3"})
			if err := Compact(ba, "people", DeduplicationStrategy); err != nil {
				t.Fatalf("error compacting dataset: %v", err)
			}
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
			// "hack" a duplicate entity into the dataset
			duplicateEntityChange("people", "http://ns/1", ba, store, dsm, t)
			// verify that the duplicate is present
			checkChanges(t, store, "people", []any{
				[]any{"ns3:1"},
				[]any{"ns3:2", true},
				[]any{"ns3:3"},
				[]any{"ns3:1"},
			})

			// Now do the compaction
			if err := Compact(ba, "people", DeduplicationStrategy); err != nil {
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
				[]any{"http://ns/1"},
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

			// Now do the compaction
			if err := Compact(ba, "people", DeduplicationStrategy); err != nil {
				t.Fatalf("error compacting dataset: %v", err)
			}

			// verify that the duplicate is gone
			checkChanges(t, store, "people", []any{
				[]any{"ns3:1"},
				[]any{"ns3:2", true},
				[]any{"ns3:3"},
			})
		})
		t.Run("with duplicate ref in many versions", func(t *testing.T) {
			t.Run("stored in different batches", func(t *testing.T) {
				defer setup()()
				// create a dataset with 3 entities
				mkDs(t, "people", store,
					[]any{"http://ns/1", false, nil, `{"ns3:ref1": "ns3:2"}`},
					[]any{"http://ns/2", true},
					[]any{"http://ns/3", false})
				time.Sleep(1 * time.Millisecond) // make sure the txTime is different
				mkDs(t, "people", store,
					[]any{"http://ns/1", false, `{"p": "a"}`, `{"ns3:ref1": "ns3:2"}`},
				)
				time.Sleep(1 * time.Millisecond) // make sure the txTime is different
				mkDs(t, "people", store,
					[]any{"http://ns/1", false, `{"p": "b"}`, `{"ns3:ref1": "ns3:2"}`},
				)

				// verify that the duplicate is present
				checkChanges(t, store, "people", []any{
					[]any{"ns3:1", false, `{"ns3:ref1":"ns3:2"}`},
					[]any{"ns3:2", true},
					[]any{"ns3:3", false},
					[]any{"ns3:1", false, `{"ns3:ref1":"ns3:2"}`},
					[]any{"ns3:1", false, `{"ns3:ref1":"ns3:2"}`},
				})
				if rcOut, rcIn := refCount(t, store, ba, "ns3:1", "ns3:ref1"); rcOut != 3 || rcIn != 3 {
					t.Fatalf("expected 3 ref out and 3 ref in, got %d and %d", rcOut, rcIn)
				}

				// Now do the compaction
				if err := Compact(ba, "people", DeduplicationStrategy); err != nil {
					t.Fatalf("error compacting dataset: %v", err)
				}

				// verify that the duplicate is gone
				checkChanges(t, store, "people", []any{
					[]any{"ns3:1", false, `{"ns3:ref1":"ns3:2"}`},
					[]any{"ns3:2", true},
					[]any{"ns3:3", false},
					[]any{"ns3:1", false, `{"ns3:ref1":"ns3:2"}`},
					[]any{"ns3:1", false, `{"ns3:ref1":"ns3:2"}`},
				})

				// in this case, all reference duplicates were in the same batch, therefore they have the same key (sam txTime).
				if rcOut, rcIn := refCount(t, store, ba, "ns3:1", "ns3:ref1"); rcOut != 1 || rcIn != 1 {
					t.Fatalf("expected 1 ref out and 1 ref in, got %d and %d", rcOut, rcIn)
				}
			})
			t.Run("stored in same batch", func(t *testing.T) {
				defer setup()()
				// create a dataset with 3 entities
				mkDs(t, "people", store,
					[]any{"http://ns/1", false, nil, `{"ns3:ref1": "ns3:2"}`},
					[]any{"http://ns/2", true},
					[]any{"http://ns/1", false, `{"p": "a"}`, `{"ns3:ref1": "ns3:2"}`},
					[]any{"http://ns/1", false, `{"p": "b"}`, `{"ns3:ref1": "ns3:2"}`},
					[]any{"http://ns/3", false})

				// verify that the duplicate is present
				checkChanges(t, store, "people", []any{
					[]any{"ns3:1", false, `{"ns3:ref1":"ns3:2"}`},
					[]any{"ns3:2", true},
					[]any{"ns3:1", false, `{"ns3:ref1":"ns3:2"}`},
					[]any{"ns3:1", false, `{"ns3:ref1":"ns3:2"}`},
					[]any{"ns3:3", false},
				})
				if rcOut, rcIn := refCount(t, store, ba, "ns3:1", "ns3:ref1"); rcOut != 1 || rcIn != 1 {
					t.Fatalf("expected 1 ref out and 1 ref in, got %d and %d", rcOut, rcIn)
				}

				// Now do the compaction
				if err := Compact(ba, "people", DeduplicationStrategy); err != nil {
					t.Fatalf("error compacting dataset: %v", err)
				}

				// verify that the duplicate is gone
				checkChanges(t, store, "people", []any{
					[]any{"ns3:1", false, `{"ns3:ref1":"ns3:2"}`},
					[]any{"ns3:2", true},
					[]any{"ns3:1", false, `{"ns3:ref1":"ns3:2"}`},
					[]any{"ns3:1", false, `{"ns3:ref1":"ns3:2"}`},
					[]any{"ns3:3", false},
				})

				// in this case, all reference duplicates were in the same batch, therefore they have the same key (sam txTime).
				if rcOut, rcIn := refCount(t, store, ba, "ns3:1", "ns3:ref1"); rcOut != 1 || rcIn != 1 {
					t.Fatalf("expected 1 ref out and 1 ref in, got %d and %d", rcOut, rcIn)
				}
			})
		})
	})
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
	//latestKey := mkLatestKey(jsonKey)

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
