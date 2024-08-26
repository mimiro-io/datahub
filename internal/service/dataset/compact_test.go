package dataset

import (
	"encoding/binary"
	"github.com/DataDog/datadog-go/v5/statsd"
	"github.com/dgraph-io/badger/v4"
	"github.com/mimiro-io/datahub/internal/conf"
	"github.com/mimiro-io/datahub/internal/server"
	"github.com/mimiro-io/datahub/internal/service/entity"
	"github.com/mimiro-io/datahub/internal/service/store"
	"go.uber.org/zap"
	"os"
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
	mkDs := func(name string, store *server.Store, entities ...[]any) {
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
			if e.ID != expected[i].([]any)[0] {
				t.Errorf("expected entity id %s, got %s", expected[i].([]any)[0], e.ID)
			}
			if e.IsDeleted != expected[i].([]any)[1] {
				t.Errorf("expected entity id %s to be deleted=%t, got deleted=%t",
					expected[i].([]any)[0], expected[i].([]any)[1], e.IsDeleted)
			}
		}
	}
	t.Run("using DeduplicationStrategy", func(t *testing.T) {
		t.Run("empty dataset", func(t *testing.T) {
			defer setup()()
			mkDs("people", store)
			Compact(ba, "people", DeduplicationStrategy)
			checkChanges(t, store, "people", []any{})
		})
		t.Run("single changes dataset", func(t *testing.T) {
			defer setup()()
			mkDs("people", store,
				[]any{"1", false},
				[]any{"2", true},
				[]any{"3", false})
			if err := Compact(ba, "people", DeduplicationStrategy); err != nil {
				t.Fatalf("error compacting dataset: %v", err)
			}
			checkChanges(t, store, "people", []any{
				[]any{"1", false},
				[]any{"2", true},
				[]any{"3", false},
			})
		})
		t.Run("one complete entity dup", func(t *testing.T) {
			defer setup()()
			// create a dataset with 3 entities
			mkDs("people", store,
				[]any{"http://ns/1", false},
				[]any{"http://ns/2", true},
				[]any{"http://ns/3", false})
			// "hack" a duplicate entity into the dataset
			dupEnt("people", "http://ns/1", ba, store, dsm, t)
			// verify that the duplicate is present
			checkChanges(t, store, "people", []any{
				[]any{"ns3:1", false},
				[]any{"ns3:2", true},
				[]any{"ns3:3", false},
				[]any{"ns3:1", false},
			})

			// Now do the compaction
			if err := Compact(ba, "people", DeduplicationStrategy); err != nil {
				t.Fatalf("error compacting dataset: %v", err)
			}

			// verify that the duplicate is gone
			checkChanges(t, store, "people", []any{
				[]any{"ns3:1", false},
				[]any{"ns3:2", true},
				[]any{"ns3:3", false},
			})
		})
	})
}

func dupEnt(dataset string, entityID string, ba store.BadgerStore, store *server.Store, dsm *server.DsManager, t *testing.T) {
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
	time.Sleep(500 * time.Millisecond)
}
