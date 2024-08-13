package server

import (
	"encoding/json"
	"go.uber.org/zap"
	"io"
)

type Statistics struct {
	Store  *Store
	Logger *zap.SugaredLogger
}

func (stats Statistics) GetStatistics(writer io.Writer) error {
	o := map[string]any{}
	err := stats.Store.GetObject(StoreMetaIndex, "stats", &o)
	if err != nil {
		return err
	}
	return json.NewEncoder(writer).Encode(o)
}

func (stats Statistics) GetStatisticsForDs(datasetName string, writer io.Writer) error {
	o := map[string]any{}
	err := stats.Store.GetObject(StoreMetaIndex, "stats", &o)
	if err != nil {
		return err
	}
	for k, v := range o {
		if k == "entity" || k == "refs" {
			subMaps := v.(map[string]any)
			for _, v2 := range subMaps {
				for k3, _ := range v2.(map[string]any) {
					if k3 != datasetName {
						delete(v2.(map[string]any), k3)
					}
				}
			}
		} else {
			delete(o, k)
		}

	}
	return json.NewEncoder(writer).Encode(o)
}
