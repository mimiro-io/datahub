package server

import (
	"encoding/json"
	"fmt"
	"go.uber.org/zap"
	"io"
)

type VirtualDataset struct {
	*VirtualDatasetConfig
	Store     *Store
	DsManager *DsManager
	Logger    *zap.SugaredLogger
	Wrap      func(d *VirtualDataset, params map[string]any, since string, f func(entity *Entity) error) (string, error)
}

// StreamChanges streams out entities, produced by the configured transform script. it returns a continuation token
func (d *VirtualDataset) StreamChanges(since string, body io.Reader, f func(entity *Entity) error) (string, error) {
	bstr := "{}"
	if body != nil {
		b, err := io.ReadAll(body)
		if err != nil {
			return "", err
		}
		if len(b) != 0 {
			bstr = string(b)
		}
	}

	params := map[string]any{}

	err := json.Unmarshal([]byte(fmt.Sprintf(`{"param": %v}`, bstr)), &params)
	if err != nil {
		return "", err
	}
	return d.Wrap(d, params, since, f)
}

func (ds *Dataset) IsVirtual() bool {
	return ds.VirtualDatasetConfig != nil && ds.VirtualDatasetConfig.Transform != ""
}

func (ds *Dataset) AsVirtualDataset(
	datasetManager *DsManager,
	wrap func(d *VirtualDataset, params map[string]any, since string, f func(entity *Entity) error) (string, error),
) *VirtualDataset {
	res := &VirtualDataset{
		VirtualDatasetConfig: ds.VirtualDatasetConfig,
		Store:                ds.store,
		DsManager:            datasetManager,
		Logger:               ds.store.logger.Named("virtual-dataset"),
		Wrap:                 wrap}
	return res
}
