package dataset

import (
	"bytes"
	"encoding/binary"
	"github.com/dgraph-io/badger/v4"
	"github.com/mimiro-io/datahub/internal/server"
	"github.com/mimiro-io/datahub/internal/service/entity"
	"reflect"
)

type deduplicationStrategy struct {
	prev            *server.Entity
	prevJsonKey     []byte
	prevEntityBytes []byte
	lookup          entity.Lookup
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
		// 2.DO NOT delete latestVersion key, need to re-assign if we delete the latest version though
		if isLatestVersion {
			latestKey := mkLatestKey(jsonKey)
			rewriteKeys = append(rewriteKeys, latestKey)
			rewriteValues = append(rewriteValues, d.prevJsonKey)
		}

		// 3.delete change log entry (need to iterate over all change versions, match value with json key)
		del = append(del, d.findChangeLogKey(jsonKey, txn))
		//
		// 4.delete outgoing references
		// 5.delete incoming references
		refs, err := findRefs(e, jsonKey, txn, d.lookup)
		if err != nil {
			return nil, err
		}
		del = append(del, refs...)
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
				refsToDelPrev, err := processRefs(d.prev, d.prevJsonKey, txn, d.lookup, stringOrArrayValue, k)
				if err != nil {
					return nil, err
				}
				// only delete if the references have different txn times (they are not identical).
				// if they are identical, they were stored in the same batch and there is only one ref key, we must keep it
				if !reflect.DeepEqual(refsToDel, refsToDelPrev) {
					del = append(del, refsToDel...)
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

// findChangeLogKey finds the change log key for a given json key. format:
// TODO: calling this often in a strategy is not efficient, should be refactored so that we call this once after
// all deletion candidates (jsonKeys) are found, and the we can pick out all change log keys in one go.
//
//	0:2: dataset entity change log, uint16
//	2:6: dataset id, uint32
//	6:14: change id (ds seq), uint64
//	14:22: entity id, uint64
func (d *deduplicationStrategy) findChangeLogKey(jsonKey []byte, txn *badger.Txn) []byte {
	searchBuffer := make([]byte, 6)
	binary.BigEndian.PutUint16(searchBuffer, server.DatasetEntityChangeLog)
	copy(searchBuffer[2:], jsonKey[10:14])

	opts := badger.DefaultIteratorOptions
	opts.Prefix = searchBuffer
	it := txn.NewIterator(opts)
	defer it.Close()
	v := make([]byte, 22)
	var found bool
	for it.Seek(searchBuffer); it.ValidForPrefix(searchBuffer); it.Next() {
		it.Item().Value(func(val []byte) error {
			if bytes.Equal(val, jsonKey) {
				v = it.Item().KeyCopy(v)
				found = true
			}
			return nil
		})
		if found {
			return v
		}
	}
	return nil
}
