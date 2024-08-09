package server

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/badger/v4/pb"
	"github.com/dgraph-io/ristretto/z"
	"go.uber.org/zap"
	"io"
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

type Statistics struct {
	Store  *Store
	Logger *zap.SugaredLogger
}

type keepAliveRoutine struct {
	ticker *time.Ticker
}

func (k *keepAliveRoutine) Close() error {
	k.ticker.Stop()
	return nil
}

func keepAlive(writer io.Writer) io.Closer {
	ticker := time.NewTicker(240 * time.Second)
	go func() {
		for range ticker.C {
			writer.Write([]byte(" "))
		}
	}()
	return &keepAliveRoutine{ticker: ticker}
}

func (stats Statistics) GetStatistics(writer io.Writer, ctx context.Context) error {
	defer keepAlive(writer).Close()
	stats.Logger.Infof("gathering counts for all datasets")
	datasetName := ""
	o := stats.perPrefixStats(ctx, INCOMING_REF_INDEX, datasetName)
	merge(o, stats.perPrefixStats(ctx, OUTGOING_REF_INDEX, datasetName))
	merge(o, stats.perPrefixStats(ctx, URI_TO_ID_INDEX_ID, datasetName))
	merge(o, stats.perPrefixStats(ctx, ENTITY_ID_TO_JSON_INDEX_ID, datasetName))
	merge(o, stats.perPrefixStats(ctx, DATASET_ENTITY_CHANGE_LOG, datasetName))
	merge(o, stats.perPrefixStats(ctx, SYS_DATASETS_ID, datasetName))
	merge(o, stats.perPrefixStats(ctx, SYS_JOBS_ID, datasetName))
	merge(o, stats.perPrefixStats(ctx, SYS_DATASETS_SEQUENCES, datasetName))
	merge(o, stats.perPrefixStats(ctx, DATASET_LATEST_ENTITIES, datasetName))
	merge(o, stats.perPrefixStats(ctx, ID_TO_URI_INDEX_ID, datasetName))
	merge(o, stats.perPrefixStats(ctx, STORE_META_INDEX, datasetName))
	merge(o, stats.perPrefixStats(ctx, NAMESPACES_INDEX, datasetName))
	merge(o, stats.perPrefixStats(ctx, JOB_RESULT_INDEX, datasetName))
	merge(o, stats.perPrefixStats(ctx, JOB_DATA_INDEX, datasetName))
	merge(o, stats.perPrefixStats(ctx, JOB_CONFIGS_INDEX, datasetName))
	merge(o, stats.perPrefixStats(ctx, CONTENT_INDEX, datasetName))
	merge(o, stats.perPrefixStats(ctx, STORE_NEXT_DATASET_ID, datasetName))
	merge(o, stats.perPrefixStats(ctx, LOGIN_PROVIDER_INDEX, datasetName))

	return json.NewEncoder(writer).Encode(o)
}

func (stats Statistics) GetStatisticsForDs(datasetName string, writer io.Writer, ctx context.Context) error {
	defer keepAlive(writer).Close()
	stats.Logger.Infof("gathering counts for dataset %v", datasetName)
	//o := map[string]any{}
	o := stats.perPrefixStats(ctx, INCOMING_REF_INDEX, datasetName)
	merge(o, stats.perPrefixStats(ctx, OUTGOING_REF_INDEX, datasetName))
	//merge(o, stats.perPrefixStats(ctx, URI_TO_ID_INDEX_ID, datasetName))
	merge(o, stats.perPrefixStats(ctx, ENTITY_ID_TO_JSON_INDEX_ID, datasetName))
	merge(o, stats.perPrefixStats(ctx, DATASET_ENTITY_CHANGE_LOG, datasetName))
	//merge(o, stats.perPrefixStats(ctx, SYS_DATASETS_ID, datasetName))
	//merge(o, stats.perPrefixStats(ctx, SYS_JOBS_ID, datasetName))
	//merge(o, stats.perPrefixStats(ctx, SYS_DATASETS_SEQUENCES, datasetName))
	merge(o, stats.perPrefixStats(ctx, DATASET_LATEST_ENTITIES, datasetName))
	//merge(o, stats.perPrefixStats(ctx, ID_TO_URI_INDEX_ID, datasetName))
	//merge(o, stats.perPrefixStats(ctx, STORE_META_INDEX, datasetName))
	//merge(o, stats.perPrefixStats(ctx, NAMESPACES_INDEX, datasetName))
	//merge(o, stats.perPrefixStats(ctx, JOB_RESULT_INDEX, datasetName))
	//merge(o, stats.perPrefixStats(ctx, JOB_DATA_INDEX, datasetName))
	//merge(o, stats.perPrefixStats(ctx, JOB_CONFIGS_INDEX, datasetName))
	//merge(o, stats.perPrefixStats(ctx, CONTENT_INDEX, datasetName))
	//merge(o, stats.perPrefixStats(ctx, STORE_NEXT_DATASET_ID, datasetName))
	//merge(o, stats.perPrefixStats(ctx, LOGIN_PROVIDER_INDEX, datasetName))

	return json.NewEncoder(writer).Encode(o)
}

func merge(o map[string]any, o2 map[string]any) {
	for k, v := range o2 {
		if o[k] == nil {
			o[k] = v
		} else {
			merge(o[k].(map[string]any), v.(map[string]any))
		}
	}
}
func (stats Statistics) perPrefixStats(ctx context.Context, pt uint16, datasetName string) map[string]any {
	stats.Logger.Infof("gathering counts for %v", idxToStr(pt))

	dsFilter := uint32(0)
	if datasetName != "" {
		ds, _ := stats.Store.datasets.Load(datasetName)
		dsFilter = ds.(*Dataset).InternalID
	}

	prefix := make([]byte, 2)
	binary.BigEndian.PutUint16(prefix, pt)
	if dsFilter != 0 && (pt == DATASET_ENTITY_CHANGE_LOG || pt == DATASET_LATEST_ENTITIES) {
		prefix = make([]byte, 6)
		binary.BigEndian.PutUint16(prefix, pt)
		binary.BigEndian.PutUint32(prefix[2:], dsFilter)
	}

	jsonViaChanges := false
	if jsonViaChanges && dsFilter != 0 && pt == ENTITY_ID_TO_JSON_INDEX_ID {
		prefix = make([]byte, 6)
		binary.BigEndian.PutUint16(prefix, DATASET_ENTITY_CHANGE_LOG)
		binary.BigEndian.PutUint32(prefix[2:], dsFilter)
	}
	s := stats.Store.database.NewStream()
	s.Prefix = prefix

	//s.NumGo = 12
	counts := map[string]int64{}
	keySizes := map[string]int64{}
	valueSizes := map[string]int64{}
	mapLock := sync.RWMutex{}
	jsonKey := make([]byte, 4)
	txn := stats.Store.database.NewTransaction(false)
	defer txn.Discard()
	s.KeyToList = func(key []byte, itr *badger.Iterator) (*pb.KVList, error) {
		item := itr.Item()
		k := item.Key()
		valSize := item.ValueSize()
		if jsonViaChanges && dsFilter != 0 && pt == ENTITY_ID_TO_JSON_INDEX_ID {
			jk, err := item.ValueCopy(jsonKey)
			if err != nil {
				return nil, err
			}
			jsonItem, err := txn.Get(jk)
			if err != nil {
				return nil, err
			}
			k = jsonItem.Key()
			valSize = jsonItem.ValueSize()
		}

		kv := &pb.KV{
			//Key: y.Copy(k),
			Key:   k,
			Value: binary.BigEndian.AppendUint64(nil, uint64(valSize)),
		}
		return &pb.KVList{
			Kv: []*pb.KV{kv},
		}, nil
	}
	dsFilterBytes := make([]byte, 4)
	empty := []byte{0, 0, 0, 0}
	binary.BigEndian.PutUint32(dsFilterBytes, dsFilter)
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

			if pt == INCOMING_REF_INDEX || pt == OUTGOING_REF_INDEX {
				dsBytes := kv.Key[36:40]

				if dsFilter != 0 && !bytes.Equal(dsFilterBytes, dsBytes) {
					continue
				}
				copy(cntKey[2:6], dsBytes)
				copy(cntKey[6:8], kv.Key[34:36]) // deleted
				//deleted := binary.BigEndian.Uint16(kv.Key[34:36])
			} else {
				if pt == DATASET_ENTITY_CHANGE_LOG || pt == DATASET_LATEST_ENTITIES {
					copy(cntKey[2:6], kv.Key[2:6])
				}
				if pt == ENTITY_ID_TO_JSON_INDEX_ID {
					copy(cntKey[2:6], kv.Key[10:14])
				}

				if dsFilter != 0 && !bytes.Equal(cntKey[2:6], empty) && !bytes.Equal(dsFilterBytes, cntKey[2:6]) {
					continue
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
		ds := ""
		if dsO, dsOK := stats.Store.datasetsByInternalID.Load(dsId); dsOK {
			ds = dsO.(*Dataset).ID
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

		dsMap["keys"] = v
		dsMap["total-value-size"] = ByteCountIEC(valueSizes[kS])
		dsMap["total-key-size"] = ByteCountIEC(keySizes[kS])
		dsMap["avg-value-size"] = ByteCountIEC(valueSizes[kS] / v)

	}
	return out
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
		return "sys:other"
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
