package dataset

import (
	"encoding/binary"
	"fmt"
	"github.com/dgraph-io/badger/v4"
	"github.com/mimiro-io/datahub/internal/server"
	"github.com/mimiro-io/datahub/internal/service/entity"
	"github.com/mimiro-io/datahub/internal/service/types"
	"go.uber.org/zap"
)

// CompactionStrategy is an interface that can be implemented to define a strategy for compacting datasets.
// A strategy evaluates entities instances (changes) and decides which keys to delete.
// The strategy is called for each change version of an entity, and can decide to delete keys based on
// the entity, the json key, and whether it is the first or last version of the entity
// The strategy can be stateful, and may need to cache versions of entities to make decisions

type CompactionStrategy interface {
	// eval will be called for each change version of the current entity
	// eval should return an array of all keys that should be deleted. either on each call, or only
	// on the last call, depending on the strategy
	//
	// jsonKey is the key of the json entry for the entity
	// 0:2: entity id to json index id, uint16
	// 2:10: entity id, uint64
	// 10:14: dataset id, uint32
	// 14:22: txn time, uint64
	// 22:24: batch seq num, uint16
	// firstVersion is true if this is the first version of the current entity
	// lastVersion is true if this is the last version of the current entity
	//
	//
	// if to be deleted... delete 5 key types for each change version:
	// 1. json entry key
	// 2. NOT latestVersion key, but need to re-assign if we delete the latest version
	// 3. incoming ref key
	// 4. outgoing ref key
	// 5. entity change log key
	eval(e *server.Entity, entityBytes []byte, jsonKey []byte, isFirstVersion bool, isLatestVersion bool, txn *badger.Txn) (*compactionInstruction, error)
	stats() map[string]int
	flush(txn *badger.Txn) ([][]byte, error)
	SetLogger(logger *zap.SugaredLogger)
	flushThreshold() int
}

type compactionInstruction struct {
	DeleteKeys    [][]byte
	RewriteKeys   [][]byte
	RewriteValues [][]byte
}

func (i *compactionInstruction) append(instr *compactionInstruction) {
	i.DeleteKeys = append(i.DeleteKeys, instr.DeleteKeys...)
	i.RewriteKeys = append(i.RewriteKeys, instr.RewriteKeys...)
	i.RewriteValues = append(i.RewriteValues, instr.RewriteValues...)
}

func (i *compactionInstruction) reset() {
	i.DeleteKeys = make([][]byte, 0)
	i.RewriteKeys = make([][]byte, 0)
	i.RewriteValues = make([][]byte, 0)
}

type recordedStrategy struct{}
type maxVersionStrategy struct{}

var (
	DeduplicationStrategy = func() CompactionStrategy {
		return &deduplicationStrategy{counts: make(map[string]int), changeBuffer: make(map[[24]byte]byte)}
	}
	//RecordedStrategy      = func() CompactionStrategy { return &recordedStrategy{} }
	//MaxVersionStrategy    = func() CompactionStrategy { return &maxVersionStrategy{} }
)

func mkLatestKey(jsonKey []byte) []byte {
	//2:6, dataset id
	//6:14 entity id)
	datasetEntitiesLatestVersionKey := make([]byte, 14)
	binary.BigEndian.PutUint16(datasetEntitiesLatestVersionKey, server.DatasetLatestEntities)
	copy(datasetEntitiesLatestVersionKey[2:], jsonKey[10:14])
	copy(datasetEntitiesLatestVersionKey[6:], jsonKey[2:10])
	return datasetEntitiesLatestVersionKey
}
func findRefs(ent *server.Entity, jsonKey []byte, txn *badger.Txn, lookup entity.Lookup) ([][]byte, error) {
	refsToDel := make([][]byte, 0)
	for k, stringOrArrayValue := range ent.References {
		foundRefs, err := processRefs(ent, jsonKey, txn, lookup, stringOrArrayValue, k)
		if err != nil {
			return nil, err
		}
		refsToDel = append(refsToDel, foundRefs...)
	}
	return refsToDel, nil
}

func processRefs(
	ent *server.Entity,
	jsonKey []byte,
	txn *badger.Txn,
	lookup entity.Lookup,
	stringOrArrayValue interface{},
	k string,
) ([][]byte, error) {
	refs, err4 := toRefs(stringOrArrayValue)
	if err4 != nil {
		return nil, fmt.Errorf("error converting refs for entity %s: %w", ent.ID, err4)
	}
	result := make([][]byte, 0)

	for _, ref := range refs {
		// assert uint64 id for Predicate
		predid, err := lookup.InternalIDForCURIE(txn, types.CURIE(k))
		if err != nil {
			return nil, err
		}

		// assert uint64 id for related entity URI
		relatedid, er := lookup.InternalIDForCURIE(txn, types.CURIE(ref))
		if er != nil {
			return nil, er
		}

		//fmt.Println("building refs for entity", ent.InternalID, "pred", k, "related", relatedid, "recorded", ent.Recorded)
		// delete outgoing references
		// 0:2: outgoing ref index, uint16
		// 2:10: this entity id, uint64
		// 10:18: txn time, uint64
		// 18:26: predicate, uint64
		// 26:34: other entity id, uint64
		// 34:36: is deleted, uint16
		// 36:40: dataset id, uint32
		outgoingBuffer := make([]byte, 40)
		binary.BigEndian.PutUint16(outgoingBuffer, server.OutgoingRefIndex)
		binary.BigEndian.PutUint64(outgoingBuffer[2:], ent.InternalID)
		binary.BigEndian.PutUint64(outgoingBuffer[10:], ent.Recorded)
		binary.BigEndian.PutUint64(outgoingBuffer[18:], uint64(predid))
		binary.BigEndian.PutUint64(outgoingBuffer[26:], uint64(relatedid))
		if ent.IsDeleted {
			binary.BigEndian.PutUint16(outgoingBuffer[34:], 1) // deleted.
		} else {
			binary.BigEndian.PutUint16(outgoingBuffer[34:], 0) // deleted.
		}
		copy(outgoingBuffer[36:40], jsonKey[10:14])
		result = append(result, outgoingBuffer)

		// delete incoming references
		// 0:2: incoming ref index, uint16
		// 2:10: other entity id, uint64
		// 10:18: this entity id, uint64
		// 18:26: txn time, uint64
		// 26:34: predicate, uint64
		// 34:36: is deleted, uint16
		// 36:40: dataset id, uint32
		incomingBuffer := make([]byte, 40)
		binary.BigEndian.PutUint16(incomingBuffer, server.IncomingRefIndex)
		binary.BigEndian.PutUint64(incomingBuffer[2:], uint64(relatedid))
		binary.BigEndian.PutUint64(incomingBuffer[10:], ent.InternalID)
		binary.BigEndian.PutUint64(incomingBuffer[18:], ent.Recorded)
		binary.BigEndian.PutUint64(incomingBuffer[26:], uint64(predid))
		if ent.IsDeleted {
			binary.BigEndian.PutUint16(incomingBuffer[34:], 1) // deleted.
		} else {
			binary.BigEndian.PutUint16(incomingBuffer[34:], 0) // deleted.
		}
		copy(incomingBuffer[36:40], jsonKey[10:14])
		result = append(result, incomingBuffer)
	}
	return result, nil
}
