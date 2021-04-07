package server

import (
	"context"
	"encoding/binary"
	"github.com/dgraph-io/badger/v2"
	"github.com/mimiro-io/datahub/internal/conf"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"time"
)

type GarbageCollector struct {
	store  *Store
	logger *zap.SugaredLogger
}

func NewGarbageCollector(lc fx.Lifecycle, store *Store, env *conf.Env) *GarbageCollector {
	gc := &GarbageCollector{
		store:  store,
		logger: env.Logger.Named("garbagecollector"),
	}

	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			if env.GcOnStartup {
				gc.logger.Info("Starting inital GC in background")
				go func() {
					ts := time.Now()
					gc.logger.Info("Starting to clean deleted datasets")
					gc.Cleandeleted()
					gc.logger.Infof("Finished cleaning of deleted datasets after %v", time.Since(ts).Round(time.Millisecond))

					ts = time.Now()
					gc.logger.Info("Starting badger gc")
					err := gc.GC()
					gc.logger.Infof("Finished badger gc after %v", time.Since(ts).Round(time.Millisecond))
					if err != nil {
						gc.logger.Panic("badger gc failed", err)
					}
				}()
			} else {
				gc.logger.Info("GC_ON_STARTUP disabled")
			}
			return nil
		},
	})

	return gc
}

func (garbageCollector *GarbageCollector) GC() error {
again:
	err := garbageCollector.store.database.RunValueLogGC(0.5)
	if err == nil {
		goto again
	}
	return nil
}

func (garbageCollector *GarbageCollector) Cleandeleted() error {
	// delete from entities index -- ENTITY_ID_TO_JSON_INDEX_ID
	/* 		binary.BigEndian.PutUint16(entityIdBuffer, ENTITY_ID_TO_JSON_INDEX_ID)
	binary.BigEndian.PutUint64(entityIdBuffer[2:], rid)
	binary.BigEndian.PutUint32(entityIdBuffer[10:], ds.InternalID)
	binary.BigEndian.PutUint64(entityIdBuffer[14:], uint64(txnTime))
	binary.BigEndian.PutUint16(entityIdBuffer[22:], uint16(jsonLength))
	*/
	index := make([]byte, 2)
	binary.BigEndian.PutUint16(index, ENTITY_ID_TO_JSON_INDEX_ID)
	err := garbageCollector.deleteByPrefixAndSelectorFunction(index, func(key []byte) bool {
		dsInternalID := binary.BigEndian.Uint32(key[10:])
		return garbageCollector.store.deletedDatasets[dsInternalID]
	})

	if err != nil {
		return err
	}

	for deletedDsID := range garbageCollector.store.deletedDatasets {
		// delete from change log
		/*
			binary.BigEndian.PutUint16(entityIdChangeTimeBuffer, DATASET_ENTITY_CHANGE_LOG)
			binary.BigEndian.PutUint32(entityIdChangeTimeBuffer[2:], ds.InternalID)
			binary.BigEndian.PutUint64(entityIdChangeTimeBuffer[6:], nextEntitySeq)
			binary.BigEndian.PutUint64(entityIdChangeTimeBuffer[14:], rid)
		*/
		index = make([]byte, 6)
		binary.BigEndian.PutUint16(index, DATASET_ENTITY_CHANGE_LOG)
		binary.BigEndian.PutUint32(index[2:], deletedDsID)
		err = garbageCollector.deleteByPrefixAndSelectorFunction(index, func(key []byte) bool {
			return true
		})

		if err != nil {
			return err
		}

		/* 		binary.BigEndian.PutUint16(datasetEntitiesLatestVersionKey, DATASET_LATEST_ENTITIES)
		binary.BigEndian.PutUint32(datasetEntitiesLatestVersionKey[2:], ds.InternalID)
		binary.BigEndian.PutUint64(datasetEntitiesLatestVersionKey[6:], rid) */
		index = make([]byte, 6)
		binary.BigEndian.PutUint16(index, DATASET_LATEST_ENTITIES)
		binary.BigEndian.PutUint32(index[2:], deletedDsID)
		err = garbageCollector.deleteByPrefixAndSelectorFunction(index, func(key []byte) bool {
			return true
		})

		if err != nil {
			return err
		}
	}

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
	index = make([]byte, 2)
	binary.BigEndian.PutUint16(index, OUTGOING_REF_INDEX)
	err = garbageCollector.deleteByPrefixAndSelectorFunction(index, func(key []byte) bool {
		dsInternalID := binary.BigEndian.Uint32(key[36:])
		return garbageCollector.store.deletedDatasets[dsInternalID]
	})

	if err != nil {
		return err
	}

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
	index = make([]byte, 2)
	binary.BigEndian.PutUint16(index, INCOMING_REF_INDEX)
	err = garbageCollector.deleteByPrefixAndSelectorFunction(index, func(key []byte) bool {
		dsInternalID := binary.BigEndian.Uint32(key[36:])
		return garbageCollector.store.deletedDatasets[dsInternalID]
	})

	if err != nil {
		return err
	}

	return nil
}

func (garbageCollector *GarbageCollector) deleteByPrefixAndSelectorFunction(prefix []byte, selector func(key []byte) bool) error {
	deleteKeys := func(keysForDelete [][]byte) error {
		return garbageCollector.store.database.Update(func(txn *badger.Txn) error {
			for _, key := range keysForDelete {
				if err := txn.Delete(key); err != nil {
					return err
				}
			}
			return nil
		})
	}

	collectSize := 10000

	return garbageCollector.store.database.View(func(txn *badger.Txn) error {
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
					return err
				}
				keysForDelete = make([][]byte, 0, collectSize)
				keysCollected = 0
			}
		}
		if keysCollected > 0 {
			if err := deleteKeys(keysForDelete); err != nil {
				return err
			}
		}

		return nil
	})
}
