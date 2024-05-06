package server

import (
	"go.uber.org/zap"
)

type SmartDataset struct {
	*SmartDatasetConfig
	Store     *Store
	DsManager *DsManager
	Logger    *zap.SugaredLogger
	Wrap      func(d *SmartDataset, since string, f func(entity *Entity) error) (string, error)
}

// StreamChanges streams out entities, produced by the configured transform script. it returns a continuation token
func (d *SmartDataset) StreamChanges(since string, f func(entity *Entity) error) (string, error) {
	return d.Wrap(d, since, f)
}

func (ds *Dataset) IsSmart() bool {
	return ds.SmartDatasetConfig != nil && ds.SmartDatasetConfig.Transform != ""
}

func (ds *Dataset) AsSmartDataset(
	datasetManager *DsManager,
	wrap func(d *SmartDataset, since string, f func(entity *Entity) error) (string, error),
) *SmartDataset {
	res := &SmartDataset{
		SmartDatasetConfig: ds.SmartDatasetConfig,
		Store:              ds.store,
		DsManager:          datasetManager,
		Logger:             ds.store.logger.Named("smart-dataset"),
		Wrap:               wrap}
	return res
}
