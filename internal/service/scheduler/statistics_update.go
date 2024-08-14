package scheduler

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/badger/v4/pb"
	"github.com/dgraph-io/ristretto/z"
	"github.com/mimiro-io/datahub/internal/service/store"
	"github.com/mimiro-io/datahub/internal/service/types"
	"go.uber.org/zap"
	"strings"
	"sync"
	"time"
)

const (
	URI_TO_ID_INDEX_ID         uint16 = 0
	ENTITY_ID_TO_JSON_INDEX_ID uint16 = 1
	INCOMING_REF_INDEX         uint16 = 2
	OUTGOING_REF_INDEX         uint16 = 3
	DATASET_ENTITY_CHANGE_LOG  uint16 = 4
	SYS_DATASETS_ID            uint16 = 5
	SYS_JOBS_ID                uint16 = 6
	SYS_DATASETS_SEQUENCES     uint16 = 7
	DATASET_LATEST_ENTITIES    uint16 = 8
	ID_TO_URI_INDEX_ID         uint16 = 9

	STORE_META_INDEX      uint16 = 10
	NAMESPACES_INDEX      uint16 = 11
	JOB_RESULT_INDEX      uint16 = 12
	JOB_DATA_INDEX        uint16 = 13
	JOB_CONFIGS_INDEX     uint16 = 14
	CONTENT_INDEX         uint16 = 15
	STORE_NEXT_DATASET_ID uint16 = 16
	LOGIN_PROVIDER_INDEX  uint16 = 17
)

func NewStatisticsUpdater(logger *zap.SugaredLogger, store store.BadgerStore) schedulable {

	stats := &Statistics{
		badger: store,
		Logger: logger,
	}

	statsKey := make([]byte, 2)
	binary.BigEndian.PutUint16(statsKey, STORE_META_INDEX)
	statsKey = append(statsKey, []byte("::stats")...)

	t := newSchedulableTask("scheduled_stats_update", true, logger, func() RunResult {
		ctx, cancel := context.WithCancel(context.Background())
		stats.cancel = cancel
		defer func() {
			if stats.cancel != nil {
				stats.cancel()
			}
			stats.cancel = nil
		}()
		stats.Logger.Infof("gathering counts for all datasets")
		o := stats.count(ctx)
		statBytes, err := json.Marshal(o)
		if err != nil {
			logger.Errorf("failed to marshal statistics: %v", err)
			return RunResult{state: RunResultFailed, timestame: time.Now()}
		}

		err = store.GetDB().Update(func(txn *badger.Txn) error {
			return txn.Set(statsKey, statBytes)
		})

		if err != nil {
			logger.Errorf("failed to store statistics: %v", err)
			return RunResult{state: RunResultFailed, timestame: time.Now()}
		}
		return RunResult{state: RunResultSuccess, timestame: time.Now()}
	})

	t.OnStop = func(ctx context.Context) error {
		logger.Info("Stopping statistics updater")
		return stats.Stop(ctx)

	}
	return t
}

type Statistics struct {
	badger store.BadgerStore
	Logger *zap.SugaredLogger
	cancel context.CancelFunc
}

func (stats *Statistics) count(ctx context.Context) map[string]any {
	s := stats.badger.GetDB().NewStream()
	//s.NumGo = 16
	counts := map[string]int64{}
	keySizes := map[string]int64{}
	valueSizes := map[string]int64{}
	mapLock := sync.RWMutex{}
	txn := stats.badger.GetDB().NewTransaction(false)
	defer txn.Discard()
	s.KeyToList = func(key []byte, itr *badger.Iterator) (*pb.KVList, error) {
		item := itr.Item()
		kv := &pb.KV{
			//Key: y.Copy(item.Key()),
			Key:   item.Key(),
			Value: binary.BigEndian.AppendUint64(nil, uint64(item.ValueSize())),
		}
		return &pb.KVList{
			Kv: []*pb.KV{kv},
		}, nil
	}
	INCOMING_REF_BYTES := make([]byte, 2)
	binary.BigEndian.PutUint16(INCOMING_REF_BYTES, INCOMING_REF_INDEX)
	OUTGOING_REF_BYTES := make([]byte, 2)
	binary.BigEndian.PutUint16(OUTGOING_REF_BYTES, OUTGOING_REF_INDEX)
	DATASET_ENTITY_CHANGE_LOG_BYTES := make([]byte, 2)
	binary.BigEndian.PutUint16(DATASET_ENTITY_CHANGE_LOG_BYTES, DATASET_ENTITY_CHANGE_LOG)
	DATASET_LATEST_ENTITIES_BYTES := make([]byte, 2)
	binary.BigEndian.PutUint16(DATASET_LATEST_ENTITIES_BYTES, DATASET_LATEST_ENTITIES)
	ENTITY_ID_TO_JSON_INDEX_BYTES := make([]byte, 2)
	binary.BigEndian.PutUint16(ENTITY_ID_TO_JSON_INDEX_BYTES, ENTITY_ID_TO_JSON_INDEX_ID)

	s.Send = func(buf *z.Buffer) error {
		mapLock.Lock()
		defer mapLock.Unlock()
		list, err := badger.BufferToKVList(buf)
		if err != nil {
			return err
		}
		for _, kv := range list.Kv {
			cntKey := make([]byte, 8)
			if kv.StreamDone {
				return nil
			}
			idx := kv.Key[:2]
			copy(cntKey[0:2], idx)

			if bytes.Equal(idx, INCOMING_REF_BYTES) || bytes.Equal(idx, OUTGOING_REF_BYTES) {
				copy(cntKey[2:6], kv.Key[36:40])
				copy(cntKey[6:8], kv.Key[34:36]) // deleted
			} else {
				if bytes.Equal(idx, DATASET_ENTITY_CHANGE_LOG_BYTES) || bytes.Equal(idx, DATASET_LATEST_ENTITIES_BYTES) {
					copy(cntKey[2:6], kv.Key[2:6])
				}
				if bytes.Equal(idx, ENTITY_ID_TO_JSON_INDEX_BYTES) {
					copy(cntKey[2:6], kv.Key[10:14])
				}
			}

			sKey := string(cntKey)
			counts[sKey] += 1
			keySizes[sKey] += int64(len(kv.Key))
			valueSizes[sKey] += int64(binary.BigEndian.Uint64(kv.Value))
		}
		return err
	}

	s.Orchestrate(ctx)
	mapLock.RLock()
	defer mapLock.RUnlock()
	out := map[string]any{}
	del := []byte{0, 1}
	for kS, v := range counts {
		k := []byte(kS)
		idx := binary.BigEndian.Uint16(k[:2])
		dsId := binary.BigEndian.Uint32(k[2:6])
		ds := "other"

		if dsName, ok := stats.badger.LookupDatasetName(types.InternalDatasetID(dsId)); ok {
			ds = dsName
		}
		if bytes.Equal(k[6:8], del) { //deleted
			ds += " (deleted)"
		}

		catAndIdx := idxToStr(idx)
		main, idxName := "_", "_"
		tokens := strings.Split(catAndIdx, ":")
		if len(tokens) == 2 {
			main, idxName = tokens[0], tokens[1]
		} else {
			main = catAndIdx
		}

		o, _ := out[main].(map[string]any)
		if o == nil {
			o = map[string]any{}
			out[main] = o
		}
		idxMap, _ := o[idxName].(map[string]any)
		if idxMap == nil {
			idxMap = map[string]any{}
			o[idxName] = idxMap
		}

		dsMap, _ := idxMap[ds].(map[string]any)
		if dsMap == nil {
			dsMap = map[string]any{}
			idxMap[ds] = dsMap
		}

		allMap, _ := idxMap["all"].(map[string]any)
		if main == "entity" || main == "refs" {
			if allMap == nil {
				allMap = map[string]any{}
				idxMap["all"] = allMap
				allMap["keys"] = int64(0)
				allMap["size-keys"] = int64(0)
				allMap["size-values"] = int64(0)
			}
		}

		dsMap["keys"] = v
		dsMap["size-keys"] = keySizes[kS]
		dsMap["size-values"] = valueSizes[kS]
		//dsMap["total-value-size"] = map[string]any{"size": ByteCountIEC(valueSizes[kS]), "raw": valueSizes[kS]}
		//dsMap["total-key-size"] = map[string]any{"size": ByteCountIEC(keySizes[kS]), "raw": keySizes[kS]}
		//dsMap["avg-value-size"] = map[string]any{"size": ByteCountIEC(valueSizes[kS] / v), "raw": valueSizes[kS] / v}
		if allMap != nil {
			allMap["keys"] = allMap["keys"].(int64) + v
			allMap["size-keys"] = allMap["size-keys"].(int64) + keySizes[kS]
			allMap["size-values"] = allMap["size-values"].(int64) + valueSizes[kS]
		}
	}

	return out
}

func (stats *Statistics) Stop(ctx context.Context) error {
	if stats.cancel != nil {
		stats.cancel()
	}
	return nil
}

func idxToStr(idx uint16) string {
	switch idx {
	case URI_TO_ID_INDEX_ID:
		return "urimap:URI_TO_ID_INDEX_ID"
	case ENTITY_ID_TO_JSON_INDEX_ID:
		return "entity:ENTITY_ID_TO_JSON_INDEX_ID"
	case INCOMING_REF_INDEX:
		return "refs:INCOMING_REF_INDEX"
	case OUTGOING_REF_INDEX:
		return "refs:OUTGOING_REF_INDEX"
	case DATASET_ENTITY_CHANGE_LOG:
		return "entity:DATASET_ENTITY_CHANGE_LOG"
	case SYS_DATASETS_ID:
		return "sys:SYS_DATASETS_ID"
	case SYS_JOBS_ID:
		return "sys:SYS_JOBS_ID"
	case SYS_DATASETS_SEQUENCES:
		return "sys:SYS_DATASETS_SEQUENCES"
	case DATASET_LATEST_ENTITIES:
		return "entity:DATASET_LATEST_ENTITIES"
	case ID_TO_URI_INDEX_ID:
		return "urimap:ID_TO_URI_INDEX_ID"
	case NAMESPACES_INDEX:
		return "sys:NAMESPACES_INDEX"
	case JOB_RESULT_INDEX:
		return "sys:JOB_RESULT_INDEX"
	case STORE_META_INDEX:
		return "sys:STORE_META_INDEX"
	case JOB_DATA_INDEX:
		return "sys:JOB_DATA_INDEX"
	case JOB_CONFIGS_INDEX:
		return "sys:JOB_CONFIGS_INDEX"
	case CONTENT_INDEX:
		return "sys:CONTENT_INDEX"
	case STORE_NEXT_DATASET_ID:
		return "sys:STORE_NEXT_DATASET_ID"
	case LOGIN_PROVIDER_INDEX:
		return "sys:LOGIN_PROVIDER_INDEX"
	default:
		return "unknown:other"
	}
}

func ByteCountIEC(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB",
		float64(b)/float64(div), "KMGTPE"[exp])
}
