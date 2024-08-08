package server

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/ristretto/z"
	"go.uber.org/zap"
	"io"
	"strings"
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

func (stats Statistics) GetStatistics(writer io.Writer, ctx context.Context) error {
	datasetName := ""
	o := stats.perPrefixStats(ctx, INCOMING_REF_INDEX, datasetName)
	merge(o, stats.perPrefixStats(ctx, OUTGOING_REF_INDEX, datasetName))
	merge(o, stats.perPrefixStats(ctx, URI_TO_ID_INDEX_ID, datasetName))
	//merge(o, stats.perPrefixStats(ctx, ENTITY_ID_TO_JSON_INDEX_ID, datasetName))
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
	if dsFilter != 0 && pt == ENTITY_ID_TO_JSON_INDEX_ID {
		binary.BigEndian.PutUint16(prefix, DATASET_ENTITY_CHANGE_LOG)
	}
	s := stats.Store.database.NewStream()
	s.Prefix = prefix

	//s.NumGo = 12
	counts := map[string]int64{}
	keySizes := map[string]int64{}
	valueSizes := map[string]int64{}
	s.Send = func(buf *z.Buffer) error {
		list, err := badger.BufferToKVList(buf)
		if err != nil {
			return err
		}
		stats.Store.database.View(func(txn *badger.Txn) error {
			for _, kv := range list.Kv {
				if kv.StreamDone {
					return nil
				}
				// fmt.Printf("%+v\n\n", kv)
				idx := kv.Key[:2]
				idxNum := binary.BigEndian.Uint16(idx)
				if pt == ENTITY_ID_TO_JSON_INDEX_ID {
					idxNum = ENTITY_ID_TO_JSON_INDEX_ID
				}
				cntName := idxToStr(idxNum)
				if cntName == "other" {
					stats.Logger.Infof("Unknown index: %v", idxNum)
				}

				if pt == INCOMING_REF_INDEX || pt == OUTGOING_REF_INDEX {
					deleted := binary.BigEndian.Uint16(kv.Key[34:36])
					dsId := binary.BigEndian.Uint32(kv.Key[36:])

					if dsFilter != 0 && dsFilter != dsId {
						continue
					}

					ds, _ := stats.Store.datasetsByInternalID.Load(dsId)
					delStr := ""
					if deleted == 1 {
						delStr = "-deleted"
					}
					cntName = fmt.Sprintf("%v:%v%v", cntName, (ds.(*Dataset)).ID, delStr)

				} else {
					dsId := uint32(0)
					if pt == DATASET_ENTITY_CHANGE_LOG || pt == DATASET_LATEST_ENTITIES {
						dsId = binary.BigEndian.Uint32(kv.Key[2:6])
					}
					if pt == ENTITY_ID_TO_JSON_INDEX_ID {

						item, err := txn.Get(kv.Value)
						if err != nil {
							return err
						}
						item.Value(func(val []byte) error {
							kv.Key = kv.Value
							kv.Value = val
							return nil
						})
						dsId = binary.BigEndian.Uint32(kv.Key[10:14])
					}

					if dsFilter != 0 && dsId != 0 && dsFilter != dsId {
						continue
					}

					ds, ok := stats.Store.datasetsByInternalID.Load(dsId)
					if ok {
						cntName = fmt.Sprintf("%v:%v", cntName, (ds.(*Dataset)).ID)
					}
				}

				counts[cntName] += 1
				keySizes[cntName] += int64(len(kv.Key))
				valueSizes[cntName] += int64(len(kv.Value))
			}
			return nil
		})
		return err
	}

	s.Orchestrate(ctx)
	out := map[string]any{}
	for k := range counts {
		main, idxName, ds := "_", "_", "_"
		tokens := strings.Split(k, ":")
		if len(tokens) == 3 {
			main, idxName, ds = tokens[0], tokens[1], tokens[2]
		} else if len(tokens) == 2 {
			main, idxName = tokens[0], tokens[1]
		} else {
			main = k
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

		dsMap["keys"] = counts[k]
		dsMap["total-value-size"] = ByteCountIEC(valueSizes[k])
		dsMap["total-key-size"] = ByteCountIEC(keySizes[k])
		dsMap["avg-value-size"] = ByteCountIEC(valueSizes[k] / counts[k])

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
