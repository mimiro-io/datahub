package source

import (
	"errors"
	"strconv"

	"github.com/mimiro-io/datahub/internal/server"
)

type DatasetSource struct {
	DatasetName    string
	Store          *server.Store
	DatasetManager *server.DsManager
	isFullSync     bool
	LatestOnly     bool
}

func (datasetSource *DatasetSource) StartFullSync() {
	datasetSource.isFullSync = true
}

func (datasetSource *DatasetSource) EndFullSync() {
	datasetSource.isFullSync = false
}

func (datasetSource *DatasetSource) ReadEntities(since DatasetContinuation, batchSize int,
	processEntities func([]*server.Entity, DatasetContinuation) error) error {
	exists := datasetSource.DatasetManager.IsDataset(datasetSource.DatasetName)
	if !exists {
		return errors.New("dataset is missing")
	}
	dataset := datasetSource.DatasetManager.GetDataset(datasetSource.DatasetName)

	entities := make([]*server.Entity, 0)
	if datasetSource.isFullSync {
		continuation, err := dataset.MapEntities(since.GetToken(), batchSize, func(entity *server.Entity) error {
			entities = append(entities, entity)
			return nil
		})
		if err != nil {
			return err
		}

		err = processEntities(entities, &StringDatasetContinuation{continuation})
		if err != nil {
			return err
		}
	} else {
		continuation, err := dataset.ProcessChanges(since.AsIncrToken(), batchSize, datasetSource.LatestOnly,
			func(entity *server.Entity) {
				entities = append(entities, entity)
			})
		if err != nil {
			return err
		}

		err = processEntities(entities, &StringDatasetContinuation{strconv.Itoa(int(continuation))})
		if err != nil {
			return err
		}
	}

	return nil
}

func (datasetSource *DatasetSource) GetConfig() map[string]interface{} {
	config := make(map[string]interface{})
	config["Type"] = "DatasetSource"
	config["Name"] = datasetSource.DatasetName
	return config
}
