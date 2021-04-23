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
	b64 "encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v3"
)

type fullSyncLease struct {
	ctx    context.Context
	cancel func()
}

// Dataset data structure
type Dataset struct {
	ID                  string `json:"id"`
	InternalID          uint32 `json:"internalId"`
	SubjectIdentifier   string `json:"subjectIdentifier"`
	store               *Store
	writeLock           sync.Mutex
	fullSyncStarted     bool
	fullSyncLease       *fullSyncLease
	fullSyncSeen        map[uint64]int
	isChangeCache       bool      // indicates if this is a local change cache
	dataChangeNotifiers []string  // list of endpoints to ping after new batch committed.
	cache               []*Entity // cache of recently updated entities.
	cacheStartOffset    uint32    // log position start in cache
	markedForDeletion   bool
	PublicNamespaces    []string `json:"publicNamespaces"`
	fullSyncID          string
}

// NewDataset Create a new dataset from the params provided
func NewDataset(store *Store, id string, internalID uint32, subjectIdentifier string) *Dataset {
	dataset := Dataset{}
	dataset.store = store
	dataset.ID = id
	dataset.InternalID = internalID
	dataset.SubjectIdentifier = subjectIdentifier
	return &dataset
}

// StartFullSync Indicates that a full sync is starting
func (ds *Dataset) StartFullSync() error {
	if ds.fullSyncStarted {
		if ds.fullSyncLease != nil && ds.fullSyncLease.cancel != nil {
			ds.fullSyncLease.cancel()
		}

		ds.fullSyncLease = nil
		ds.fullSyncID = ""
	}

	ds.fullSyncStarted = true
	ds.fullSyncSeen = make(map[uint64]int)

	return nil
}

func (ds *Dataset) StartFullSyncWithLease(fullSyncID string) error {
	if err := ds.StartFullSync(); err != nil {
		return err
	}
	ds.fullSyncID = fullSyncID

	return ds.RefreshFullSyncLease(fullSyncID)
}

func (ds *Dataset) RefreshFullSyncLease(fullSyncID string) error {
	if ds.fullSyncStarted {
		if fullSyncID == ds.fullSyncID {
			//cancel previous lease
			if ds.fullSyncLease != nil && ds.fullSyncLease.cancel != nil {
				ds.fullSyncLease.cancel()
			}

			//start new lease
			ctx, cancel := context.WithTimeout(context.Background(), ds.store.fullsyncLeaseTimeout)
			ds.fullSyncLease = &fullSyncLease{
				ctx,
				cancel,
			}

			go func() {
				currentFsID := ds.fullSyncID

				<-ctx.Done()
				endTime, ok := ctx.Deadline()
				// time out was the cause
				now := time.Now()
				if ok && now.After(endTime) && ds.fullSyncID == currentFsID {
					ds.fullSyncStarted = false
					ds.fullSyncSeen = make(map[uint64]int)
					ds.fullSyncID = ""
					ds.fullSyncLease = nil
				} // else, canceled by refresh. do nothing
			}()

			return nil
		}

		return fmt.Errorf("given fullsyncId %v does not match running fullsync id %v", fullSyncID, ds.fullSyncID)
	} else if fullSyncID != "" {
		return fmt.Errorf("fullsync with sync-id %v is not running", fullSyncID)
	}

	return nil
}

func (ds *Dataset) ReleaseFullSyncLease(fullSyncID string) error {
	if ds.fullSyncLease == nil {
		return errors.New("no active fullsync lease found, can't complete")
	}

	if ds.fullSyncLease != nil && ds.fullSyncLease.cancel != nil {
		ds.fullSyncLease.cancel()
	}
	return nil
}

// CompleteFullSync Full sync completed - mark unseen entities as deleted
func (ds *Dataset) CompleteFullSync() error {
	defer func() {
		ds.fullSyncStarted = false
		ds.fullSyncSeen = make(map[uint64]int) // release sync state
		ds.fullSyncLease = nil                 // unset lease
		ds.fullSyncID = ""                     // unset id
	}()

	// check all seen and mark deleted
	txn := ds.store.database.NewTransaction(true)
	defer txn.Discard()

	deleteBatch := make([]*Entity, 0)
	_, err := ds.MapEntities("", -1, func(e *Entity) error {
		if !e.IsDeleted {
			_, ok := ds.fullSyncSeen[e.InternalID]
			if !ok {
				// data no longer in source so delete it
				e.IsDeleted = true
				deleteBatch = append(deleteBatch, e)
			}
			if len(deleteBatch) == 1000 {
				err := ds.StoreEntities(deleteBatch)
				if err != nil {
					return err
				}
				deleteBatch = make([]*Entity, 0)
			}
		}
		return nil
	})

	if err != nil {
		return err
	}

	// store remaining
	if len(deleteBatch) > 0 {
		err := ds.StoreEntities(deleteBatch)
		if err != nil {
			return err
		}
	}

	return nil
}

// GetStorageKey gets storage key for storing dataset in database
func (ds *Dataset) getStorageKey() []byte {
	key := make([]byte, 2+len(ds.ID))
	binary.BigEndian.PutUint16(key, SYS_DATASETS_ID)
	copy(key[2:], ds.ID)
	return key
}

// StoreEntities
func (ds *Dataset) StoreEntities(entities []*Entity) (Error error) {
	tags := []string{
		"application:datahub",
		fmt.Sprintf("dataset:%s", ds.ID),
	}

	if len(entities) == 0 {
		return nil
	}

	_ = ds.store.statsdClient.Count("ds.added.items", int64(len(entities)), tags, 1)

	// get lock - only one writer to a dataset log
	ds.writeLock.Lock()
	// release lock at end regardless
	defer ds.writeLock.Unlock()

	// need this to ensure time moves forward in high perf environments.
	time.Sleep(time.Nanosecond * 1)

	// time now as uint64
	txnTime := time.Now().UnixNano()

	txn := ds.store.database.NewTransaction(true)
	defer txn.Discard()

	// get dataset seq
	// fixme: can be part of the dataset data structure no need to open each txn as the is a mutex protecting it
	datasetSeqKey := make([]byte, 6)
	binary.BigEndian.PutUint16(datasetSeqKey, SYS_DATASETS_SEQUENCES)
	binary.BigEndian.PutUint32(datasetSeqKey[2:], ds.InternalID)
	logseq, _ := ds.store.database.GetSequence(datasetSeqKey, 1000)
	defer func() {
		err := logseq.Release()
		if err != nil {
			Error = err
		}
	}()

	isnew := false
	var rid uint64

	idCache := make(map[string]uint64)

	rtxn := ds.store.database.NewTransaction(false)
	defer rtxn.Discard()

	var newitems int64 = 0

	for _, e := range entities {

		// entityIdBuffer buffer for lookup in main index
		// index_id;rid;dataset;time => blob
		// unit16;unit64;uint32;uint64
		entityIdBuffer := make([]byte, 24)
		uid := e.ID
		var err error
		rid, isnew, err = ds.store.assertIDForURI(uid, idCache)
		if err != nil {
			return err
		}
		e.InternalID = rid // set internal id on entity
		e.Recorded = uint64(txnTime)

		if ds.fullSyncStarted {
			ds.fullSyncSeen[e.InternalID] = 1
		}

		jsonData, _ := json.Marshal(e)
		jsonLength := len(jsonData)

		_ = ds.store.statsdClient.Count("ds.processed.bytes", int64(jsonLength), tags, 1)

		binary.BigEndian.PutUint16(entityIdBuffer, ENTITY_ID_TO_JSON_INDEX_ID)
		binary.BigEndian.PutUint64(entityIdBuffer[2:], rid)
		binary.BigEndian.PutUint32(entityIdBuffer[10:], ds.InternalID)
		binary.BigEndian.PutUint64(entityIdBuffer[14:], uint64(txnTime))
		binary.BigEndian.PutUint16(entityIdBuffer[22:], uint16(jsonLength))

		// assume different from a previous version
		isDifferent := true

		datasetEntitiesLatestVersionKey := make([]byte, 14)
		binary.BigEndian.PutUint16(datasetEntitiesLatestVersionKey, DATASET_LATEST_ENTITIES)
		binary.BigEndian.PutUint32(datasetEntitiesLatestVersionKey[2:], ds.InternalID)
		binary.BigEndian.PutUint64(datasetEntitiesLatestVersionKey[6:], rid)
		// fixme: optimise to have txntime in key and not need a value

		var previousEntityIdBuffer []byte
		var prevEntity *Entity
		if previousEntityIdBufferValue, err := rtxn.Get(datasetEntitiesLatestVersionKey); err == nil {
			previousEntityIdBuffer, err = previousEntityIdBufferValue.ValueCopy(nil)
			if err != nil {
				return err
			}
			prevJsonDataValue, err := rtxn.Get(previousEntityIdBuffer)
			if err != nil {
				return err
			}
			prevJsonData, err := prevJsonDataValue.ValueCopy(nil)
			_ = ds.store.statsdClient.Count("ds.read.bytes", prevJsonDataValue.ValueSize(), tags, 1)
			if err != nil {
				return err
			}
			prevEntity = &Entity{}
			err = json.Unmarshal(prevJsonData, prevEntity)
			if err != nil {
				return err
			}
		}

		if !isnew {
			if prevEntity != nil {
				if prevEntity.IsDeleted == e.IsDeleted &&
					reflect.DeepEqual(prevEntity.References, e.References) &&
					reflect.DeepEqual(prevEntity.Properties, e.Properties) {
					isDifferent = false
				}
			} else {
				newitems++
			}
		}

		// not new and not different
		if !isnew && !isDifferent {
			continue
		}

		// store entity and the log entry
		_ = ds.store.statsdClient.Count("ds.added.bytes", int64(jsonLength), tags, 1)
		_ = ds.store.statsdClient.Gauge("ds.throughput.bytes", float64(jsonLength), tags, 1)
		err = txn.Set(entityIdBuffer, jsonData)
		if err != nil {
			return err
		}

		// store change log
		entityIdChangeTimeBuffer := make([]byte, 22)
		nextEntitySeq, _ := logseq.Next()
		binary.BigEndian.PutUint16(entityIdChangeTimeBuffer, DATASET_ENTITY_CHANGE_LOG)
		binary.BigEndian.PutUint32(entityIdChangeTimeBuffer[2:], ds.InternalID)
		binary.BigEndian.PutUint64(entityIdChangeTimeBuffer[6:], nextEntitySeq)
		binary.BigEndian.PutUint64(entityIdChangeTimeBuffer[14:], rid)
		err = txn.Set(entityIdChangeTimeBuffer, entityIdBuffer)
		if err != nil {
			return err
		}

		// dataset all latest entities
		err = txn.Set(datasetEntitiesLatestVersionKey, entityIdBuffer)
		if err != nil {
			return err
		}

		// Process references
		// if new then insert else get latest resource and do diff of rels from previous
		if isnew {
			newitems++
			// iter refs and create rels
			// add association to indexes
			// outgoing buffer og-indexid:rid:time:predid:relatedid:deleted => nil
			//                 2:8:8:8:8:2
			// incoming buffer ic-indexid:relatedid:rid:time:predid:deleted => nil
			for k, stringOrArrayValue := range e.References {

				// need to check if v is string or []string
				refs, isArray := stringOrArrayValue.([]string)
				if !isArray {
					refs = []string{stringOrArrayValue.(string)}
				}

				for _, ref := range refs {
					outgoingBuffer := make([]byte, 40)
					incomingBuffer := make([]byte, 40)

					// assert uint64 id for predicate
					predid, _, err := ds.store.assertIDForURI(k, idCache)
					if err != nil {
						return err
					}

					// assert uint64 id for related entity URI
					relatedid, _, err := ds.store.assertIDForURI(ref, idCache)
					if err != nil {
						return err
					}

					binary.BigEndian.PutUint16(outgoingBuffer, OUTGOING_REF_INDEX)
					binary.BigEndian.PutUint64(outgoingBuffer[2:], rid)
					binary.BigEndian.PutUint64(outgoingBuffer[10:], uint64(txnTime))
					binary.BigEndian.PutUint64(outgoingBuffer[18:], predid)
					binary.BigEndian.PutUint64(outgoingBuffer[26:], relatedid)
					binary.BigEndian.PutUint16(outgoingBuffer[34:], 0) // deleted.
					binary.BigEndian.PutUint32(outgoingBuffer[36:], ds.InternalID)
					err = txn.Set(outgoingBuffer, []byte(""))
					if err != nil {
						return err
					}

					binary.BigEndian.PutUint16(incomingBuffer, INCOMING_REF_INDEX)
					binary.BigEndian.PutUint64(incomingBuffer[2:], relatedid)
					binary.BigEndian.PutUint64(incomingBuffer[10:], rid)
					binary.BigEndian.PutUint64(incomingBuffer[18:], uint64(txnTime))
					binary.BigEndian.PutUint64(incomingBuffer[26:], predid)
					binary.BigEndian.PutUint16(incomingBuffer[34:], 0) // deleted.
					binary.BigEndian.PutUint32(incomingBuffer[36:], ds.InternalID)
					err = txn.Set(incomingBuffer, []byte(""))
					if err != nil {
						return err
					}
				}
			}

		} else {

			oldRefs := make(map[uint64]uint64)
			if prevEntity != nil {
				// go through previous state
				// check for no longer there rels
				for k, stringOrArrayValue := range prevEntity.References {
					// need to check if v is string or []string
					refs, isArray := stringOrArrayValue.([]interface{})
					if !isArray {
						s := stringOrArrayValue.(interface{})
						refs = []interface{}{s}
					}

					for _, ref := range refs {
						// get predicate
						predid, _, err := ds.store.assertIDForURI(k, idCache)
						if err != nil {
							return err
						}

						// get related
						relatedid, _, err := ds.store.assertIDForURI(ref.(string), idCache)
						if err != nil {
							return err
						}

						oldRefs[predid] = relatedid
					}
				}
			}

			// go through new state
			for k, stringOrArrayValue := range e.References {

				// need to check if v is string or []string
				refs, isArray := stringOrArrayValue.([]string)
				if !isArray {
					refs = []string{stringOrArrayValue.(string)}
				}

				for _, ref := range refs {
					outgoingBuffer := make([]byte, 40)
					incomingBuffer := make([]byte, 40)

					// assert uint64 id for predicate
					predid, _, err := ds.store.assertIDForURI(k, idCache)
					if err != nil {
						return err
					}

					// assert uint64 id for related entity URI
					relatedid, _, err := ds.store.assertIDForURI(ref, idCache)
					if err != nil {
						return err
					}

					binary.BigEndian.PutUint16(outgoingBuffer, OUTGOING_REF_INDEX)
					binary.BigEndian.PutUint64(outgoingBuffer[2:], rid)
					binary.BigEndian.PutUint64(outgoingBuffer[10:], uint64(txnTime))
					binary.BigEndian.PutUint64(outgoingBuffer[18:], predid)
					binary.BigEndian.PutUint64(outgoingBuffer[26:], relatedid)
					binary.BigEndian.PutUint16(outgoingBuffer[34:], 0) // deleted.
					binary.BigEndian.PutUint32(outgoingBuffer[36:], ds.InternalID)
					err = txn.Set(outgoingBuffer, []byte(""))
					if err != nil {
						return err
					}

					binary.BigEndian.PutUint16(incomingBuffer, INCOMING_REF_INDEX)
					binary.BigEndian.PutUint64(incomingBuffer[2:], relatedid)
					binary.BigEndian.PutUint64(incomingBuffer[10:], rid)
					binary.BigEndian.PutUint64(incomingBuffer[18:], uint64(txnTime))
					binary.BigEndian.PutUint64(incomingBuffer[26:], predid)
					binary.BigEndian.PutUint16(incomingBuffer[34:], 0) // deleted.
					binary.BigEndian.PutUint32(incomingBuffer[36:], ds.InternalID)
					err = txn.Set(incomingBuffer, []byte(""))
					if err != nil {
						return err
					}

					// delete key from current state
					delete(oldRefs, predid)
				}
			}

			// iterate remaining keys of old and add them as deleted for incoming
			for p, e := range oldRefs {
				incomingBuffer := make([]byte, 40)

				binary.BigEndian.PutUint16(incomingBuffer, INCOMING_REF_INDEX)
				binary.BigEndian.PutUint64(incomingBuffer[2:], e)
				binary.BigEndian.PutUint64(incomingBuffer[10:], rid)
				binary.BigEndian.PutUint64(incomingBuffer[18:], uint64(txnTime))
				binary.BigEndian.PutUint64(incomingBuffer[26:], p)
				binary.BigEndian.PutUint16(incomingBuffer[34:], 1) // is deleted
				binary.BigEndian.PutUint32(incomingBuffer[36:], ds.InternalID)
				err := txn.Set(incomingBuffer, []byte(""))
				if err != nil {
					return err
				}
			}
		}
	}

	err := ds.store.commitIdTxn()
	if err != nil {
		return err
	}

	err = txn.Commit()
	if err != nil {
		return err
	}

	err = ds.updateDataset(newitems, entities)
	if err != nil {
		return err
	}

	return nil
}

func (ds *Dataset) updateDataset(newItemCount int64, entities []*Entity) error {
	if ds.ID == "core.Dataset" {
		for _, dsEntity := range entities {
			dsInfo, err := ds.store.NamespaceManager.GetDatasetNamespaceInfo()
			if err != nil {
				return err
			}
			newNamespaces := dsEntity.Properties[dsInfo.PublicNamespacesKey]
			if newNamespaces != nil {
				interfacesArray := newNamespaces.([]interface{})
				newNamespacesArray := make([]string, len(interfacesArray))
				for i, v := range interfacesArray {
					newNamespacesArray[i] = v.(string)
				}
				dsInterface, found := ds.store.datasets.Load(dsEntity.Properties[dsInfo.NameKey])
				if found {
					dataset := dsInterface.(*Dataset)
					dataset.PublicNamespaces = newNamespacesArray
					jsonData, err := json.Marshal(dataset)
					if err != nil {
						return err
					}
					err = ds.store.storeValue(dataset.getStorageKey(), jsonData)
					if err != nil {
						return err
					}
					ds.store.datasets.Store(dataset.ID, dataset)
				}
			}
		}
	} else if newItemCount > 0 {
		dsInfo, err := ds.store.NamespaceManager.GetDatasetNamespaceInfo()
		if err != nil {
			return err
		}
		var datasets []string // unlimited scope for best performance, we access an internal dataset here so no need for scoping
		dsEntity, err := ds.store.GetEntity(fmt.Sprintf("%s:%s", dsInfo.DatasetPrefix, ds.ID), datasets)
		if err != nil {
			return err
		}
		if dsEntity != nil {
			items, ok := dsEntity.Properties[dsInfo.ItemsKey]
			var count int64 = 0
			if ok {
				// so items is stored as an int64, but comes back as a float64 because of json
				existing := int64(items.(float64))
				count = existing + newItemCount
			} else {
				count = newItemCount
			}
			dsEntity.Properties[dsInfo.ItemsKey] = count
			tds, ok := ds.store.datasets.Load("core.Dataset")
			if ok {
				_ = tds.(*Dataset).StoreEntities([]*Entity{dsEntity})
			}
		}
	}
	return nil
}

/*
func (ds *Dataset) GetAllVersionsOfEntity(uri string) []*Entity {
	results := make([]*Entity, 0)
	ds.store.database.View(func(txn *badger.Txn) error {
		rid, ridExists := s.getIDForURI(txn, uri)
		if !ridExists {
			return nil
		}

		opts1 := badger.DefaultIteratorOptions
		entityIterator := txn.NewIterator(opts1)
		defer entityIterator.Close()

		searchBuffer := make([]byte, 10)
		binary.BigEndian.PutUint16(searchBuffer, INCOMING_REF_INDEX)
		binary.BigEndian.PutUint64(searchBuffer[2:], rid)
		searchBuffer = append(searchBuffer, 0xFF)

		for entityIterator.Seek(searchBuffer); entityIterator.ValidForPrefix(prefixBuffer); entityIterator.Next() {
		}

		return nil
	})
	return results
} */

type EntitiesResult struct {
	Context           *Context
	Entities          []*Entity
	ContinuationToken string
}

// GetEntities returns a batch of entities
func (ds *Dataset) GetEntities(from string, count int) (*EntitiesResult, error) {
	result := &EntitiesResult{}
	result.Context = ds.GetContext()
	result.Entities = make([]*Entity, 0)

	token, err := ds.MapEntities(from, count, func(entity *Entity) error {
		result.Entities = append(result.Entities, entity)
		return nil
	})

	if err != nil {
		return nil, err
	}

	result.ContinuationToken = token

	return result, nil
}

// MapEntities applies a function to all entities in the dataset
// returns the id of the last entity so that it can be used as a continuation token
func (ds *Dataset) MapEntities(from string, count int, processEntity func(entity *Entity) error) (string, error) {
	continuationToken, err := ds.MapEntitiesRaw(from, count, func(entityJson []byte) error {
		e := &Entity{}
		err := json.Unmarshal(entityJson, e)
		if err != nil {
			return err
		}

		return processEntity(e)
	})

	if err != nil {
		return "", err
	}

	// the continuation token
	return continuationToken, nil
}

// MapEntities applies a function to all entities in the dataset. the entities are provided as raw json bytes
// returns the id of the last entity so that it can be used as a continuation token
func (ds *Dataset) MapEntitiesRaw(from string, count int, processEntity func(json []byte) error) (string, error) {

	lastKeyAsContinuationToken := ""

	err := ds.store.database.View(func(txn *badger.Txn) error {
		opts1 := badger.DefaultIteratorOptions
		entityIterator := txn.NewIterator(opts1)
		defer entityIterator.Close()

		searchBufferPrefix := make([]byte, 10)
		binary.BigEndian.PutUint16(searchBufferPrefix, DATASET_LATEST_ENTITIES)
		binary.BigEndian.PutUint32(searchBufferPrefix[2:], ds.InternalID)
		var searchBuffer []byte
		if from == "" {
			searchBuffer = searchBufferPrefix // append(searchBufferPrefix, []byte(from)...)
		} else {
			searchBuffer, _ = b64.StdEncoding.DecodeString(from)
		}
		taken := 0

		entityIterator.Seek(searchBuffer)
		// the from matches the id of the last object found
		// so need to advanced to next
		if from != "" {
			lastKeyAsContinuationToken = from
			entityIterator.Next()
		}

		for ; entityIterator.ValidForPrefix(searchBufferPrefix); entityIterator.Next() {
			// store key into lastSeenKey
			lastKeyAsContinuationToken = b64.StdEncoding.EncodeToString(entityIterator.Item().Key())
			item := entityIterator.Item()
			taken++

			err := item.Value(func(val []byte) error {
				entityItem, _ := txn.Get(val)
				return entityItem.Value(func(entityJson []byte) error {
					return processEntity(entityJson)
				})
			})
			if err != nil {
				return err
			}

			if taken == count {
				break
			}
		}

		return nil
	})

	if err != nil {
		return "", err
	}

	// the continuation token
	return lastKeyAsContinuationToken, nil
}

// Changes Object
type Changes struct {
	Context   *Context
	Entities  []*Entity
	NextToken uint64
}

func NewChanges() *Changes {
	changes := &Changes{}
	changes.Entities = make([]*Entity, 0)
	changes.NextToken = 0
	return changes
}

func (ds *Dataset) GetChanges(since uint64, count int) (*Changes, error) {
	changes := NewChanges()
	changes.Context = ds.GetContext()
	changes.Entities = make([]*Entity, 0)
	var err error
	changes.NextToken, err = ds.ProcessChanges(since, count, func(entity *Entity) {
		changes.Entities = append(changes.Entities, entity)
	})
	if err != nil {
		return nil, err
	}
	return changes, nil
}

func (ds *Dataset) ProcessChanges(since uint64, count int, processChangedEntity func(entity *Entity)) (uint64, error) {
	return ds.ProcessChangesRaw(since, count, func(jsonData []byte) error {
		entity := &Entity{}
		err := json.Unmarshal(jsonData, entity)
		if err != nil {
			return err
		}

		processChangedEntity(entity)
		return nil
	})
}

func (ds *Dataset) ProcessChangesRaw(since uint64, count int, processChangedEntity func(entityJson []byte) error) (uint64, error) {

	lastSeen := since
	foundChanges := false

	err := ds.store.database.View(func(txn *badger.Txn) error {

		opts1 := badger.DefaultIteratorOptions
		changesIterator := txn.NewIterator(opts1)
		defer changesIterator.Close()

		searchBuffer := make([]byte, 14)
		binary.BigEndian.PutUint16(searchBuffer, DATASET_ENTITY_CHANGE_LOG)
		binary.BigEndian.PutUint32(searchBuffer[2:], ds.InternalID)
		binary.BigEndian.PutUint64(searchBuffer[6:], since)

		processed := 0
		for changesIterator.Seek(searchBuffer); changesIterator.ValidForPrefix(searchBuffer[:6]); changesIterator.Next() {
			foundChanges = true
			processed++
			item := changesIterator.Item()
			k := item.Key()

			// get current offset
			lastSeen = binary.BigEndian.Uint64(k[6:])

			err := item.Value(func(val []byte) error {
				entityItem, _ := txn.Get(val)
				return entityItem.Value(func(jsonVal []byte) error {
					return processChangedEntity(jsonVal)
				})
			})

			if err != nil {
				return err
			}

			if processed == count {
				break
			}
		}

		return nil
	})

	if err != nil {
		return 0, err
	}

	// if we returned something then move ahead to next seq
	if foundChanges {
		return lastSeen + 1, nil
	} else {
		return since, nil
	}
}

func (ds *Dataset) GetContext() *Context {
	return ds.store.NamespaceManager.GetContext(ds.PublicNamespaces)
}

func (ds *Dataset) FullSyncStarted() bool {
	return ds.fullSyncStarted
}
