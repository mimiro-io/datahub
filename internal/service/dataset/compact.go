package dataset

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/dgraph-io/badger/v4"
	"github.com/mimiro-io/datahub/internal/server"
	"github.com/mimiro-io/datahub/internal/service/store"
	"github.com/mimiro-io/datahub/internal/service/types"
)

func Compact(bs store.BadgerStore, datasetID string, strategy CompactionStrategy) error {

	dsId, b := bs.LookupDatasetID(datasetID)
	if !b {
		return fmt.Errorf("dataset %s not found", datasetID)
	}
	txn := bs.GetDB().NewTransaction(false)
	defer txn.Discard()

	// 1. go through these:
	//datasetEntitiesLatestVersionKey := make([]byte, 14)
	//binary.BigEndian.PutUint16(datasetEntitiesLatestVersionKey, DatasetLatestEntities)
	//binary.BigEndian.PutUint32(datasetEntitiesLatestVersionKey[2:], ds.InternalID)
	//binary.BigEndian.PutUint64(datasetEntitiesLatestVersionKey[6:], rid)
	seekLatestChanges := store.SeekLatestChanges(dsId)
	opts := badger.DefaultIteratorOptions
	opts.Prefix = seekLatestChanges
	it := txn.NewIterator(opts)
	defer it.Close()

	for it.Seek(seekLatestChanges); it.ValidForPrefix(seekLatestChanges); it.Next() {
		latestKey := it.Item().Key()
		internalEntityID := types.InternalID(binary.BigEndian.Uint64(latestKey[6:14]))

		// 2. for each entity id, go through change versions in given dataset (chronologically)
		err := forEntity(bs, dsId, internalEntityID, txn, strategy)
		if err != nil {
			return err
		}
	}

	return nil
}

func forEntity(bs store.BadgerStore, dsId types.InternalDatasetID, internalEntityID types.InternalID, txn *badger.Txn, strategy CompactionStrategy) error {
	entityLocatorPrefixBuffer := store.SeekEntityChanges(dsId, internalEntityID)
	opts1 := badger.DefaultIteratorOptions
	opts1.PrefetchValues = false
	opts1.Prefix = entityLocatorPrefixBuffer
	entityLocatorIterator := txn.NewIterator(opts1)
	defer entityLocatorIterator.Close()

	isFirstChange := true
	var keysToBeDeleted [][]byte
	changeEval := func(jsonBytes []byte, jsonKey []byte, isFirstChange bool, isLastChange bool) error {
		e, err := toEntity(jsonBytes)
		if err != nil {
			return err
		}
		instr, err2 := strategy.eval(e, jsonBytes, jsonKey, isFirstChange, isLastChange, txn)
		if err2 != nil {
			return err2
		}
		if instr == nil {
			return nil
		}
		keysToBeDeleted = append(keysToBeDeleted, instr.DeleteKeys...)

		err3 := doReWrites(instr, bs)
		if err3 != nil {
			return err3
		}
		return flushDeletes(bs, keysToBeDeleted, isLastChange)
	}
	var jsonKey []byte
	var jsonBytes []byte
	var err error
	for entityLocatorIterator.Seek(entityLocatorPrefixBuffer); entityLocatorIterator.ValidForPrefix(entityLocatorPrefixBuffer); entityLocatorIterator.Next() {
		// eval previous change. skip first change. we do it this way, so that we can determine what the last change is
		// when calling a final changeEval after the loop
		if jsonBytes != nil {
			err = changeEval(jsonBytes, jsonKey, isFirstChange, false)
			if err != nil {
				return err
			}
			isFirstChange = false
		}
		item := entityLocatorIterator.Item()

		// item.Key() is the the json key
		// capture the json key and json value for next iteration as comparison base
		jsonKey = item.KeyCopy(nil)
		jsonBytes, err = item.ValueCopy(nil)
		if err != nil {
			return err
		}
	}
	// eval last change
	return changeEval(jsonBytes, jsonKey, isFirstChange, true)
}

// depending on strategy, we may need to rewrite value.
// typically when the latest version of an entity is not the one we want to keep. in that case we need to rewrite the
// latest key to point to the correct version of the entity
func doReWrites(instr *compactionInstruction, bs store.BadgerStore) error {
	if instr.RewriteKeys != nil {
		err3 := bs.GetDB().Update(func(txn *badger.Txn) error {
			for i, key := range instr.RewriteKeys {
				err := txn.Set(key, instr.RewriteValues[i])
				if err != nil {
					return err
				}
			}
			return nil
		})
		if err3 != nil {
			return err3
		}
	}
	return nil
}

// for efficiency, we flush deletes in batches
func flushDeletes(bs store.BadgerStore, deleted [][]byte, finalFlush bool) error {
	if !finalFlush && len(deleted) < 1000 {
		return nil
	}
	if len(deleted) > 0 {
		err := bs.GetDB().Update(func(txn *badger.Txn) error {
			for _, key := range deleted {
				e := txn.Delete(key)
				if e != nil {
					return e
				}
			}
			return nil
		})
		if err != nil {
			return err
		}
		deleted = make([][]byte, 0)
	}
	return nil
}

func toRefs(stringOrArrayValue any) ([]string, error) {
	// need to check if v is string, []interface{} or []string
	var refs []string
	switch v := stringOrArrayValue.(type) {
	case string:
		refs = []string{v}
	case []string:
		refs = v
	case []interface{}:
		refs = make([]string, len(v))
		for i, v := range v {
			if val, ok := v.(string); ok {
				refs[i] = val
			} else {
				return nil, fmt.Errorf("encountered nil in ref array, cannot process ref value %v+", v)
			}
		}
	case nil:
		return nil, fmt.Errorf("encountered nil ref, cannot process ref value %v+", v)
	}
	return refs, nil
}

func toEntity(jsonBytes []byte) (*server.Entity, error) {
	ent := server.Entity{}
	e := json.Unmarshal(jsonBytes, &ent)
	if e != nil {
		return nil, e
	}
	return &ent, nil
}
