package dataset

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/mimiro-io/datahub/internal/server"
	"github.com/mimiro-io/datahub/internal/service/store"
	"github.com/mimiro-io/datahub/internal/service/types"
	"go.uber.org/zap"
)

type CompactionWorker struct {
	bs      store.BadgerStore
	logger  *zap.SugaredLogger
	running bool
}

func NewCompactor(store *server.Store, dsm *server.DsManager, logger *zap.SugaredLogger) *CompactionWorker {
	bs := server.NewBadgerAccess(store, dsm)
	return &CompactionWorker{
		bs:     bs,
		logger: logger.Named("compaction-worker"),
	}
}

func (c *CompactionWorker) CompactAsync(datasetID string, strategy CompactionStrategy) error {
	if !c.running {
		c.running = true
		go func() {
			defer func() {
				c.running = false
			}()
			err := c.compact(datasetID, strategy)
			if err != nil {
				c.logger.Errorf("compaction failed: %v", err)
			}
		}()
		return nil
	}
	return fmt.Errorf("there is already a compaction running. try again later")
}

func (c *CompactionWorker) compact(datasetID string, strategy CompactionStrategy) error {
	startTs := time.Now()
	strategy.SetLogger(c.logger)
	dsId, b := c.bs.LookupDatasetID(datasetID)
	if !b {
		return fmt.Errorf("dataset %s not found", datasetID)
	}
	txn := c.bs.GetDB().NewTransaction(false)
	defer txn.Discard()

	// 1. go through these:
	// datasetEntitiesLatestVersionKey := make([]byte, 14)
	// binary.BigEndian.PutUint16(datasetEntitiesLatestVersionKey, DatasetLatestEntities)
	// binary.BigEndian.PutUint32(datasetEntitiesLatestVersionKey[2:], ds.InternalID)
	// binary.BigEndian.PutUint64(datasetEntitiesLatestVersionKey[6:], rid)
	seekLatestChanges := store.SeekLatestChanges(dsId)
	opts := badger.DefaultIteratorOptions
	opts.Prefix = seekLatestChanges
	it := txn.NewIterator(opts)
	defer it.Close()

	ts := time.Now()
	cnt := 0
	ops := &compactionInstruction{}
	for it.Seek(seekLatestChanges); it.ValidForPrefix(seekLatestChanges); it.Next() {
		latestKey := it.Item().Key()
		internalEntityID := types.InternalID(binary.BigEndian.Uint64(latestKey[6:14]))

		// 2. for each entity id, go through change versions in given dataset (chronologically)
		err := c.forEntity(dsId, internalEntityID, txn, strategy, ops)
		if err != nil {
			return err
		}
		cnt++
		// keysToBeDeleted = append(keysToBeDeleted, newKeys...)
		if time.Since(ts) > 15*time.Second {
			ts = time.Now()
			c.logger.Infof("compacting dataset %v, processed %v entities, removed keys so far: %v", datasetID, cnt, strategy.stats())
		}

	}
	_, err := flushDeletes(c.bs, ops, true, strategy)
	if err != nil {
		return err
	}
	c.logger.Infof("compacted dataset %s in %v, removed keys: %v", datasetID, time.Since(startTs), strategy.stats())
	return nil
}

func (c *CompactionWorker) forEntity(dsId types.InternalDatasetID, internalEntityID types.InternalID, txn *badger.Txn,
	strategy CompactionStrategy, ops *compactionInstruction,
) error {
	entityLocatorPrefixBuffer := store.SeekEntityChanges(dsId, internalEntityID)
	opts1 := badger.DefaultIteratorOptions
	opts1.PrefetchValues = false
	opts1.Prefix = entityLocatorPrefixBuffer
	entityLocatorIterator := txn.NewIterator(opts1)
	defer entityLocatorIterator.Close()

	isFirstChange := true
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
		ops.append(instr)

		reset, err4 := flushDeletes(c.bs, ops, false, strategy)
		if reset {
			ops.reset()
		}
		return err4
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
	err = changeEval(jsonBytes, jsonKey, isFirstChange, true)
	return err
}

// for efficiency, we flush deletes in batches
func flushDeletes(bs store.BadgerStore, ops *compactionInstruction, finalFlush bool, strategy CompactionStrategy) (bool, error) {
	if !finalFlush && len(ops.DeleteKeys) < strategy.flushThreshold() {
		return false, nil
	}
	err := bs.GetDB().Update(func(txn *badger.Txn) error {
		bufferedKeys, err := strategy.flush(txn)
		if err != nil {
			return err
		}
		all := append(ops.DeleteKeys, bufferedKeys...)
		for _, key := range all {
			_, testErr := txn.Get(key)
			if testErr != nil {
				// if dataset was compacted before using dedup strat, we may have already deleted ref keys
				if (key[1] == 3) || (key[1] == 2) { // refs
					strategy.stats()["refs"]--
				}
			} else {
				e := txn.Delete(key)
				if e != nil {
					return e
				}
			}
		}
		// fmt.Println("deleted", len(all), "keys")
		for i, key := range ops.RewriteKeys {
			err2 := txn.Set(key, ops.RewriteValues[i])
			if err2 != nil {
				return err2
			}
		}
		// fmt.Println("rewritten", len(ops.RewriteKeys), "keys")
		return nil
	})
	if err != nil {
		return false, err
	}
	return true, nil
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
