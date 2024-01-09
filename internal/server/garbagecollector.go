// Copyright 2021 MIMIRO AS
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
	"encoding/binary"
	"errors"
	"time"

	"github.com/dgraph-io/badger/v4"
	"go.uber.org/zap"

	"github.com/mimiro-io/datahub/internal/conf"
)

type GarbageCollector struct {
	store  *Store
	logger *zap.SugaredLogger
	quit   chan bool
	env    *conf.Config
}

func NewGarbageCollector(store *Store, env *conf.Config) *GarbageCollector {
	gc := &GarbageCollector{
		store:  store,
		logger: env.Logger.Named("garbagecollector"),
		quit:   make(chan bool, 1),
		env:    env,
	}

	gc.Start(context.Background())

	return gc
}

func (garbageCollector *GarbageCollector) Start(ctx context.Context) error {
	if garbageCollector.env.GcOnStartup {
		garbageCollector.logger.Info("Starting inital GC in background")
		go func() {
			ts := time.Now()
			var err error
			garbageCollector.logger.Info("Starting to clean deleted datasets")
			err = garbageCollector.Cleandeleted()
			if err != nil {
				garbageCollector.logger.Warnf("cleaning of deleted datasets failed: %v", err.Error())
				return
			} else {
				garbageCollector.logger.Infof("Finished cleaning of deleted datasets after %v", time.Since(ts).Round(time.Millisecond))
			}
			ts = time.Now()
			garbageCollector.logger.Info("Starting badger gc")
			err = garbageCollector.GC()
			if err != nil {
				garbageCollector.logger.Warn("badger gc failed: ", err)
			} else {
				garbageCollector.logger.Infof("Finished badger gc after %v", time.Since(ts).Round(time.Millisecond))
			}
		}()
	} else {
		garbageCollector.logger.Info("GC_ON_STARTUP disabled")
	}
	return nil
}

func (garbageCollector *GarbageCollector) Stop(ctx context.Context) error {
	go func() { garbageCollector.quit <- true }()
	return nil
}

func (garbageCollector *GarbageCollector) GC() error {
again:
	if garbageCollector.isCancelled() {
		return errors.New("gc cancelled")
	}
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
	binary.BigEndian.PutUint16(index, EntityIDToJSONIndexID)
	err := garbageCollector.deleteByPrefixAndSelectorFunction(index, func(key []byte) bool {
		dsInternalID := binary.BigEndian.Uint32(key[10:])
		return garbageCollector.store.deletedDatasets[dsInternalID]
	})
	if err != nil {
		return err
	}

	for deletedDsID := range garbageCollector.store.deletedDatasets {
		if garbageCollector.isCancelled() {
			return errors.New("gc cancelled")
		}
		// delete from change log
		/*
			binary.BigEndian.PutUint16(entityIdChangeTimeBuffer, DATASET_ENTITY_CHANGE_LOG)
			binary.BigEndian.PutUint32(entityIdChangeTimeBuffer[2:], ds.InternalID)
			binary.BigEndian.PutUint64(entityIdChangeTimeBuffer[6:], nextEntitySeq)
			binary.BigEndian.PutUint64(entityIdChangeTimeBuffer[14:], rid)
		*/
		index = make([]byte, 6)
		binary.BigEndian.PutUint16(index, DatasetEntityChangeLog)
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
		binary.BigEndian.PutUint16(index, DatasetLatestEntities)
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
	binary.BigEndian.PutUint16(index, OutgoingRefIndex)
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
	binary.BigEndian.PutUint16(index, IncomingRefIndex)
	err = garbageCollector.deleteByPrefixAndSelectorFunction(index, func(key []byte) bool {
		dsInternalID := binary.BigEndian.Uint32(key[36:])
		return garbageCollector.store.deletedDatasets[dsInternalID]
	})

	if err != nil {
		return err
	}

	return nil
}

func (garbageCollector *GarbageCollector) deleteByPrefixAndSelectorFunction(
	prefix []byte,
	selector func(key []byte) bool,
) error {
	deleteKeys := func(keysForDelete [][]byte) error {
		return garbageCollector.store.database.Update(func(txn *badger.Txn) error {
			for _, key := range keysForDelete {
				if err := txn.Delete(key); err != nil {
					return err
				}
				if garbageCollector.isCancelled() {
					return errors.New("gc cancelled")
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
		opts.Prefix = prefix
		it := txn.NewIterator(opts)
		defer it.Close()

		keysForDelete := make([][]byte, 0, collectSize)
		keysCollected := 0
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			if garbageCollector.isCancelled() {
				return errors.New("gc cancelled")
			}
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

func (garbageCollector *GarbageCollector) isCancelled() bool {
	select {
	case <-garbageCollector.quit:
		return true
	default:
		return false
	}
}
