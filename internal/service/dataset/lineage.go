package dataset

import (
	"encoding/json"
	"github.com/mimiro-io/datahub/internal/jobs"
	"github.com/mimiro-io/datahub/internal/server"
	"github.com/mimiro-io/datahub/internal/service/types"
	"go.uber.org/zap"
	"slices"
)

type LineageBuilder struct {
	logger *zap.SugaredLogger
	store  *server.Store
	ba     server.BadgerAccess
}

type LineageRel struct {
	From string `json:"from"`
	To   string `json:"to"`
	Type string `json:"type"`
}
type Lineage []LineageRel

func (lb *LineageBuilder) ForDataset(datasetName string) (Lineage, error) {
	output := Lineage{}

	err2 := lb.store.IterateObjectsRaw(server.JobConfigsIndexBytes, func(jsonData []byte) error {
		jobConfig := &jobs.JobConfiguration{}
		err := json.Unmarshal(jsonData, jobConfig)
		if err != nil {
			lb.logger.Warnf("error parsing job from store: %s", err)
			return err
		}

		meta := &server.MetaContext{}
		err = lb.store.GetObject(server.JobMetaIndex, jobConfig.ID, &meta)
		if err != nil {
			return err
		}

		from, to, as := fromToAs(jobConfig)

		if to != "" {
			for _, f := range from {
				if f == datasetName || to == datasetName {
					output = append(output, LineageRel{From: f, To: to, Type: as})
				}
			}
		}

		hops := make(map[string]struct{})
		for dsId := range meta.QueriedDatasets {
			dsName, found := lb.ba.LookupDatasetName(types.InternalDatasetID(dsId))
			if !found {
				lb.logger.Warnf("dataset not found: %d", dsId)
				continue
			}
			hops[dsName] = struct{}{}
		}
		if to == datasetName {
			for f := range hops {
				output = append(output, LineageRel{From: f, To: to, Type: as + "-hop"})
			}
		}
		for f := range hops {
			if f == datasetName && to != "" {
				output = append(output, LineageRel{From: f, To: to, Type: as + "-hop"})
			}
		}

		for txn := range meta.TransactionSink {
			if txn == datasetName {
				for _, f := range from {
					output = append(output, LineageRel{From: f, To: txn, Type: as})
				}
				for f := range hops {
					output = append(output, LineageRel{From: f, To: txn, Type: as + "-hop"})
				}
			}
			for _, fr := range from {
				if fr == datasetName {
					output = append(output, LineageRel{From: fr, To: txn, Type: as})
				}
			}

			for f := range hops {
				if f == datasetName {
					output = append(output, LineageRel{From: f, To: txn, Type: as + "-hop"})
				}
			}
		}
		return nil
	})
	if err2 != nil {
		return nil, err2
	}

	sort(output)
	// return the lineage
	return output, nil
}

func (lb *LineageBuilder) ForAll() (Lineage, error) {
	output := Lineage{}

	err2 := lb.store.IterateObjectsRaw(server.JobConfigsIndexBytes, func(jsonData []byte) error {
		jobConfig := &jobs.JobConfiguration{}
		err := json.Unmarshal(jsonData, jobConfig)
		if err != nil {
			lb.logger.Warnf("error parsing job from store: %s", err)
			return err
		}

		meta := &server.MetaContext{}
		err = lb.store.GetObject(server.JobMetaIndex, jobConfig.ID, &meta)
		if err != nil {
			return err
		}
		from, to, as := fromToAs(jobConfig)
		if to != "" {
			for _, f := range from {
				output = append(output, LineageRel{From: f, To: to, Type: as})
			}
		}

		hops := make(map[string]struct{})
		for dsId := range meta.QueriedDatasets {
			dsName, found := lb.ba.LookupDatasetName(types.InternalDatasetID(dsId))
			if !found {
				lb.logger.Warnf("dataset not found: %d", dsId)
				continue
			}
			hops[dsName] = struct{}{}
		}

		for f := range hops {
			output = append(output, LineageRel{From: f, To: to, Type: as + "-hop"})
		}
		for txn := range meta.TransactionSink {
			for _, f := range from {
				output = append(output, LineageRel{From: f, To: txn, Type: as})
			}
			for f := range hops {
				output = append(output, LineageRel{From: f, To: txn, Type: as + "-hop"})
			}
		}
		return nil
	})
	if err2 != nil {
		return nil, err2
	}

	sort(output)
	return output, nil
}

func NewLineageBuilder(store *server.Store, dsm *server.DsManager, named *zap.SugaredLogger) *LineageBuilder {

	return &LineageBuilder{store: store, logger: named, ba: server.NewBadgerAccess(store, dsm)}
}

func sort(output Lineage) {
	slices.SortFunc(output, func(a, b LineageRel) int {
		if a.From < b.From {
			return -1
		}
		if a.From > b.From {
			return 1
		}
		if a.To < b.To {
			return -1
		}
		if a.To > b.To {
			return 1
		}
		if a.Type < b.Type {
			return -1
		}
		if a.Type > b.Type {
			return 1
		}
		return 0
	})
}

func fromToAs(jobConfig *jobs.JobConfiguration) ([]string, string, string) {
	var from []string
	var to string
	t := "copy"
	if jobConfig.Transform != nil {
		t = "transform"
	}

	if jobConfig.Source["Type"] == "DatasetSource" {
		from = []string{jobConfig.Source["Name"].(string)}
	}
	if jobConfig.Source["Type"] == "HttpDatasetSource" {
		from = []string{jobConfig.Source["Url"].(string)}
	}
	if jobConfig.Source["Type"] == "UnionDatasetSource" {
		from = []string{}
		for _, s := range jobConfig.Source["DatasetSources"].([]interface{}) {
			from = append(from, s.(map[string]interface{})["Name"].(string))
		}
	}

	if jobConfig.Sink["Type"] == "HttpDatasetSink" {
		to = jobConfig.Sink["Url"].(string)
	}
	if jobConfig.Sink["Type"] == "DatasetSink" {
		to = jobConfig.Sink["Name"].(string)
	}
	return from, to, t
}
