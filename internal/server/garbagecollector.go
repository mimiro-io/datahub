package server

import (
	"github.com/dgraph-io/badger/v3"
	"github.com/mimiro-io/datahub/internal/conf"
	"go.uber.org/fx"
)

type GarbageCollector struct {
	store *Store
}

func NewGarbageCollector(lc fx.Lifecycle, store *Store, env *conf.Env) (*GarbageCollector, error) {
	gc := &GarbageCollector{ store: store }

	return gc, nil
}

func (garbageCollector *GarbageCollector) gc() error {
	err := garbageCollector.store.database.RunValueLogGC(0.5)
	return err
}

func (garbageCollector *GarbageCollector) cleandeleted() {

	// delete from entities index -- ENTITY_ID_TO_JSON_INDEX_ID
	/* 		binary.BigEndian.PutUint16(entityIdBuffer, ENTITY_ID_TO_JSON_INDEX_ID)
			binary.BigEndian.PutUint64(entityIdBuffer[2:], rid)
			binary.BigEndian.PutUint32(entityIdBuffer[10:], ds.InternalID)
			binary.BigEndian.PutUint64(entityIdBuffer[14:], uint64(txnTime))
			binary.BigEndian.PutUint16(entityIdBuffer[22:], uint16(jsonLength))
	*/

	// delete from change log
	/*
			binary.BigEndian.PutUint16(entityIdChangeTimeBuffer, DATASET_ENTITY_CHANGE_LOG)
			binary.BigEndian.PutUint32(entityIdChangeTimeBuffer[2:], ds.InternalID)
			binary.BigEndian.PutUint64(entityIdChangeTimeBuffer[6:], nextEntitySeq)
			binary.BigEndian.PutUint64(entityIdChangeTimeBuffer[14:], rid)
	*/

	/* 		binary.BigEndian.PutUint16(datasetEntitiesLatestVersionKey, DATASET_LATEST_ENTITIES)
			binary.BigEndian.PutUint32(datasetEntitiesLatestVersionKey[2:], ds.InternalID)
			binary.BigEndian.PutUint64(datasetEntitiesLatestVersionKey[6:], rid) */

	// delete from outgoing
	/*
			binary.BigEndian.PutUint16(outgoingBuffer, OUTGOING_REF_INDEX)
			binary.BigEndian.PutUint64(outgoingBuffer[2:], rid)
			binary.BigEndian.PutUint64(outgoingBuffer[10:], uint64(txnTime))
			binary.BigEndian.PutUint64(outgoingBuffer[18:], predid)
			binary.BigEndian.PutUint64(outgoingBuffer[26:], relatedid)
			binary.BigEndian.PutUint16(outgoingBuffer[34:], 0) // deleted.
			binary.BigEndian.PutUint32(outgoingBuffer[36:], ds.InternalID)
	 */

	// delete from incoming
	/*
			binary.BigEndian.PutUint16(incomingBuffer, INCOMING_REF_INDEX)
			binary.BigEndian.PutUint64(incomingBuffer[2:], relatedid)
			binary.BigEndian.PutUint64(incomingBuffer[10:], rid)
			binary.BigEndian.PutUint64(incomingBuffer[18:], uint64(txnTime))
			binary.BigEndian.PutUint64(incomingBuffer[26:], predid)
			binary.BigEndian.PutUint16(incomingBuffer[34:], 0) // deleted.
			binary.BigEndian.PutUint32(incomingBuffer[36:], ds.InternalID)
	*/
	
}

func (garbageCollector *GarbageCollector) DeleteByPrefixAndSelectorFunction(prefix []byte, selector func(key []byte) bool) {
	deleteKeys := func(keysForDelete [][]byte) error {
		if err := garbageCollector.store.database.Update(func(txn *badger.Txn) error {
			for _, key := range keysForDelete {
				if err := txn.Delete(key); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return err
		}
		return nil
	}

	collectSize := 10000
	garbageCollector.store.database.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.AllVersions = false
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		keysForDelete := make([][]byte, 0, collectSize)
		keysCollected := 0
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			key := it.Item().KeyCopy(nil)

			// if selector false continue
			if !selector(key) {
				continue
			}

			keysForDelete = append(keysForDelete, key)
			keysCollected++
			if keysCollected == collectSize {
				if err := deleteKeys(keysForDelete); err != nil {
					panic(err)
				}
				keysForDelete = make([][]byte, 0, collectSize)
				keysCollected = 0
			}
		}
		if keysCollected > 0 {
			if err := deleteKeys(keysForDelete); err != nil {
				panic(err)
			}
		}

		return nil
	})
}
