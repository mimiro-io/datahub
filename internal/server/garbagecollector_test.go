package server

import (
	"context"
	"fmt"
	"github.com/DataDog/datadog-go/statsd"
	"github.com/dgraph-io/badger/v2"
	"github.com/franela/goblin"
	"github.com/mimiro-io/datahub/internal/conf"
	"go.uber.org/fx/fxtest"
	"go.uber.org/zap"
	"os"
	"testing"
)

func TestGC(t *testing.T) {
	g := goblin.Goblin(t)
	g.Describe("The GarbageCollector", func() {
		testCnt := 0
		var dsm *DsManager
		var store *Store
		var storeLocation string
		var gc *GarbageCollector

		g.BeforeEach(func() {
			testCnt += 1
			storeLocation = fmt.Sprintf("./test_gc_%v", testCnt)
			err := os.RemoveAll(storeLocation)
			g.Assert(err).IsNil("should be allowed to clean testfiles in " + storeLocation)
			e := &conf.Env{
				Logger:        zap.NewNop().Sugar(),
				StoreLocation: storeLocation,
			}

			devNull, _ := os.Open("/dev/null")
			oldErr := os.Stderr
			os.Stderr = devNull
			lc := fxtest.NewLifecycle(t)
			store = NewStore(lc, e, &statsd.NoOpClient{})
			dsm = NewDsManager(lc, e, store, NoOpBus())
			gc = NewGarbageCollector(lc, store, e)
			g.Assert(err).IsNil()

			err = lc.Start(context.Background())
			g.Assert(err).IsNil()
			os.Stderr = oldErr
		})
		g.AfterEach(func() {
			_ = store.Close()
			_ = os.RemoveAll(storeLocation)
		})
		g.It("Should not delete used data", func() {
			b := gc.store.database
			g.Assert(count(b)).Eql(16)

			ds, _ := dsm.CreateDataset("test")
			g.Assert(count(b)).Eql(24)

			_ = ds.StoreEntities([]*Entity{NewEntity("hei", 0)})
			g.Assert(count(b)).Eql(34)

			gc.GC()
			g.Assert(count(b)).Eql(34)

			gc.Cleandeleted()
			g.Assert(count(b)).Eql(34)
		})

		g.It("Should remove entities and indexes from deleted datasets", func() {
			b := store.database
			g.Assert(count(b)).Eql(16)

			ds, _ := dsm.CreateDataset("test")
			g.Assert(count(b)).Eql(24)

			ds2, _ := dsm.CreateDataset("delete.me")
			g.Assert(count(b)).Eql(32)

			_ = ds.StoreEntities([]*Entity{NewEntity("p1", 0)})
			g.Assert(count(b)).Eql(42)

			_ = ds2.StoreEntities([]*Entity{NewEntity("p1", 0)})
			g.Assert(count(b)).Eql(50)

			_ = dsm.DeleteDataset("delete.me")
			g.Assert(count(b)).Eql(54, "before cleanup, 4 new keys are expected (deleted dataset state)")

			err := gc.Cleandeleted()
			g.Assert(err).IsNil()
			g.Assert(count(b)).Eql(51, "3 keys should be gone now for the one referenced entity in "+
				"'delete.me': main index, latest and changes")
		})

		g.It("Should delete the correct incoming and outgoing references", func() {
			b := store.database
			peopleNamespacePrefix, _ := store.NamespaceManager.AssertPrefixMappingForExpansion("http://data.mimiro.io/people/")
			workPrefix, _ := store.NamespaceManager.AssertPrefixMappingForExpansion("http://data.mimiro.io/work/")
			g.Assert(count(b)).Eql(16)

			friendsDS, _ := dsm.CreateDataset("friends")
			g.Assert(count(b)).Eql(24)

			workDS, _ := dsm.CreateDataset("work")
			g.Assert(count(b)).Eql(32)

			_ = friendsDS.StoreEntities([]*Entity{
				NewEntityFromMap(map[string]interface{}{
					"id":    peopleNamespacePrefix + ":person-1",
					"props": map[string]interface{}{peopleNamespacePrefix + ":Name": "Lisa"},
					"refs":  map[string]interface{}{peopleNamespacePrefix + ":Friend": peopleNamespacePrefix + ":person-3"}}),
				NewEntityFromMap(map[string]interface{}{
					"id":    peopleNamespacePrefix + ":person-2",
					"props": map[string]interface{}{peopleNamespacePrefix + ":Name": "Bob"},
					"refs":  map[string]interface{}{peopleNamespacePrefix + ":Friend": peopleNamespacePrefix + ":person-1"}}),
			})
			g.Assert(count(b)).Eql(55)

			// check that we can query outgoing
			result, err := store.GetManyRelatedEntities(
				[]string{"http://data.mimiro.io/people/person-1"}, peopleNamespacePrefix+":Friend", false, nil)
			g.Assert(err).IsNil()
			g.Assert(len(result)).Eql(1)
			g.Assert(result[0][1]).Eql(peopleNamespacePrefix + ":Friend")
			g.Assert(result[0][2].(*Entity).ID).Eql(peopleNamespacePrefix + ":person-3")

			// check that we can query incoming
			result, err = store.GetManyRelatedEntities(
				[]string{"http://data.mimiro.io/people/person-1"}, peopleNamespacePrefix+":Friend", true, nil)
			g.Assert(err).IsNil()
			g.Assert(len(result)).Eql(1)
			g.Assert(result[0][1]).Eql(peopleNamespacePrefix + ":Friend")
			g.Assert(result[0][2].(*Entity).ID).Eql(peopleNamespacePrefix + ":person-2")

			_ = workDS.StoreEntities([]*Entity{
				NewEntityFromMap(map[string]interface{}{
					"id":    peopleNamespacePrefix + ":person-1",
					"props": map[string]interface{}{workPrefix + ":Wage": "110"},
					"refs":  map[string]interface{}{workPrefix + ":Coworker": peopleNamespacePrefix + ":person-2"}}),
				NewEntityFromMap(map[string]interface{}{
					"id":    peopleNamespacePrefix + ":person-2",
					"props": map[string]interface{}{workPrefix + ":Wage": "100"},
					"refs":  map[string]interface{}{workPrefix + ":Coworker": peopleNamespacePrefix + ":person-3"}}),
			})
			g.Assert(count(b)).Eql(74)

			// check that we can still query outgoing
			/*
			result, err = store.GetManyRelatedEntities(
				[]string{"http://data.mimiro.io/people/person-1"}, peopleNamespacePrefix+":Friend", false, nil)
			g.Assert(err).IsNil()
			g.Assert(len(result)).Eql(1, "Expected still to find person-3 as a friend")
			g.Assert(result[0][2].(*Entity).ID).Eql(peopleNamespacePrefix + ":person-3")

			// and incoming
			result, err = store.GetManyRelatedEntities(
				[]string{"http://data.mimiro.io/people/person-1"}, peopleNamespacePrefix+":Friend", true, nil)
			g.Assert(err).IsNil()
			g.Assert(len(result)).Eql(1, "Expected still to find person-2 as reverse friend")
			g.Assert(result[0][2].(*Entity).ID).Eql(peopleNamespacePrefix + ":person-2")
			*/

			_ = dsm.DeleteDataset("work")
			g.Assert(count(b)).Eql(78, "before cleanup, 4 new keys are expected (deleted dataset state)")

			err = gc.Cleandeleted()
			g.Assert(err).IsNil()
			g.Assert(count(b)).Eql(66, "two entities with 6 keys each should be removed now")

			// make sure we still can query
			result, err = store.GetManyRelatedEntities(
				[]string{"http://data.mimiro.io/people/person-1"}, "*", false, nil)
			g.Assert(err).IsNil()
			g.Assert(len(result)).Eql(1)
			g.Assert(result[0][1]).Eql(peopleNamespacePrefix + ":Friend")
			g.Assert(result[0][2].(*Entity).ID).Eql(peopleNamespacePrefix + ":person-3")

			result, err = store.GetManyRelatedEntities(
				[]string{"http://data.mimiro.io/people/person-2"}, "*", false, nil)
			g.Assert(err).IsNil()
			g.Assert(len(result)).Eql(1)
			g.Assert(result[0][1]).Eql(peopleNamespacePrefix + ":Friend")
			g.Assert(result[0][2].(*Entity).ID).Eql(peopleNamespacePrefix + ":person-1")
		})
	})
}

func count(b *badger.DB) int {
	items := 0
	_ = b.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)

		for it.Rewind(); it.Valid(); it.Next() {
			items++
			//os.Stdout.Write([]byte(fmt.Sprintf("%v\n",it.Item())))
		}

		it.Close()
		return nil
	})
	//os.Stdout.Write([]byte(fmt.Sprintf("%v\n\n\n",items)))
	return items
}
