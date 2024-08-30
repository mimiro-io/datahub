package dataset

import (
	"bytes"
	"encoding/binary"
	"github.com/dgraph-io/badger/v4"
	"github.com/mimiro-io/datahub/internal/server"
	"github.com/mimiro-io/datahub/internal/service/entity"
	"go.uber.org/zap"
	"reflect"
)

type deduplicationStrategy struct {
	prev            *server.Entity
	prevJsonKey     []byte
	prevEntityBytes []byte
	lookup          entity.Lookup
	counts          map[string]int
	changeBuffer    map[[24]byte]byte
	logger          *zap.SugaredLogger
	flushAfter      int
}

func (d *deduplicationStrategy) flushThreshold() int {
	if d.flushAfter > 0 {
		return d.flushAfter
	}
	return 100000 // 100k json keys, potentially duplicated with change log entries = 200k keys. fits in badger transaction
}

func (d *deduplicationStrategy) SetLogger(logger *zap.SugaredLogger) {
	d.logger = logger
}

// end is called after the last entity is processed. It should return all remaining keys that should be deleted (if any)
func (d *deduplicationStrategy) flush(txn *badger.Txn) ([][]byte, error) {
	if d.prevJsonKey != nil {
		return d.findChangeLogKeys(d.prevJsonKey, txn)
	}
	return nil, nil
}

func (d *deduplicationStrategy) stats() map[string]int {
	return d.counts
}

func (d *deduplicationStrategy) eval(
	e *server.Entity,
	entityBytes []byte,
	jsonKey []byte,
	isFirstVersion bool,
	isLatestVersion bool,
	txn *badger.Txn) (*compactionInstruction, error) {
	// if this is the first version of the entity, we just need to keep it
	if isFirstVersion {
		d.prevJsonKey = jsonKey
		d.prevEntityBytes = entityBytes
		d.prev = e
		return nil, nil
	}

	del := make([][]byte, 0)
	var rewriteKeys [][]byte
	var rewriteValues [][]byte
	// first, check if the whole entity is equal to the previous entity
	if server.IsEntityEqual(d.prevEntityBytes, entityBytes, d.prev, e) {
		// if to be deleted... delete 5 key types for each change version:
		// 1.delete json entry (key already in keysToDelete)
		del = append(del, jsonKey)
		d.counts["json"]++
		// 2.DO NOT delete latestVersion key, need to re-assign if we delete the latest version though
		if isLatestVersion {
			latestKey := mkLatestKey(jsonKey)
			rewriteKeys = append(rewriteKeys, latestKey)
			rewriteValues = append(rewriteValues, d.prevJsonKey)
		}

		// 3.delete change log entry (need to iterate over all change versions, match value with json key)
		//ts := time.Now()
		//del = append(del, d.findChangeLogKeys(jsonKey, txn, false)...)
		d.changeBuffer[[24]byte(jsonKey)] = 1
		d.counts["changeLog"]++
		//fmt.Printf("findChangeLogKeys took: %v\n", time.Since(ts))
		//
		// 4.delete outgoing references
		// 5.delete incoming references
		refs, err := findRefs(e, jsonKey, txn, d.lookup)
		if err != nil {
			return nil, err
		}
		del = append(del, refs...)
		d.counts["refs"] += len(refs)
	} else {
		// if the entity is not equal to the previous entity, we can still check for just reference duplicates
		for k, stringOrArrayValue := range e.References {
			if reflect.DeepEqual(d.prev.References[k], stringOrArrayValue) {
				// reference is identical to previous version, so we can delete the current reference

				// calculate the references to delete
				refsToDel, err := processRefs(e, jsonKey, txn, d.lookup, stringOrArrayValue, k)
				if err != nil {
					return nil, err
				}

				// also calculate the references to delete for the previous version, for extra safety check
				refsToDelPrev, err2 := processRefs(d.prev, d.prevJsonKey, txn, d.lookup, d.prev.References[k], k)
				if err2 != nil {
					return nil, err2
				}
				// only delete if the references have different txn times (they are not identical).
				// if they are identical, they were stored in the same batch and there is only one ref key, we must keep it
				identical := false
				if len(refsToDel) == len(refsToDelPrev) {
					identical = true
					for i, ref := range refsToDel {

						if !bytes.Equal(ref, refsToDelPrev[i]) {
							identical = false
							break
						}
					}
				}
				if !identical {
					del = append(del, refsToDel...)
					d.counts["refs"] += len(refsToDel)
				}
			}
		}
	}
	if len(del) > 0 {
		res := &compactionInstruction{
			DeleteKeys: del,
		}
		if len(rewriteKeys) > 0 {
			res.RewriteKeys = rewriteKeys
			res.RewriteValues = rewriteValues
		}
		return res, nil
	}
	d.prevJsonKey = jsonKey
	d.prevEntityBytes = entityBytes
	d.prev = e
	return nil, nil
}

// findChangeLogKeys finds the change log key for a given json key. format:
// TODO: calling this often in a strategy is not efficient, should be refactored so that we call this once after
// all deletion candidates (jsonKeys) are found, and the we can pick out all change log keys in one go.
//
//	0:2: dataset entity change log, uint16
//	2:6: dataset id, uint32
//	6:14: change id (ds seq), uint64
//	14:22: entity id, uint64
func (d *deduplicationStrategy) findChangeLogKeys(jsonKey []byte, txn *badger.Txn) ([][]byte, error) {
	if len(d.changeBuffer) > 0 {
		searchBuffer := make([]byte, 6)
		binary.BigEndian.PutUint16(searchBuffer, server.DatasetEntityChangeLog)
		copy(searchBuffer[2:], jsonKey[10:14])

		opts := badger.DefaultIteratorOptions
		opts.Prefix = searchBuffer
		it := txn.NewIterator(opts)
		defer it.Close()
		res := make([][]byte, 0)
		var val []byte
		var err error
		for it.Seek(searchBuffer); it.ValidForPrefix(searchBuffer); it.Next() {
			val, err = it.Item().ValueCopy(val)
			if err != nil {
				return nil, err
			}

			if d.changeBuffer[[24]byte(val)] == 1 {
				res = append(res, it.Item().KeyCopy(nil))
			}
		}
		d.changeBuffer = make(map[[24]byte]byte)
		return res, nil
	}
	return nil, nil
}
