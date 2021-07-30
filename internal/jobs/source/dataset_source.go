package source

import (
	"errors"
	"github.com/mimiro-io/datahub/internal/server"
	"strconv"
)

type DatasetSource struct {
	DatasetName    string
	Store          *server.Store
	DatasetManager *server.DsManager
	isFullSync     bool
}

func (datasetSource *DatasetSource) StartFullSync() {
	datasetSource.isFullSync = true
}

func (datasetSource *DatasetSource) EndFullSync() {
	datasetSource.isFullSync = false
}

func (datasetSource *DatasetSource) ReadEntities(since string, batchSize int, processEntities func([]*server.Entity, string) error) error {
	exists := datasetSource.DatasetManager.IsDataset(datasetSource.DatasetName)
	if !exists {
		return errors.New("dataset is missing")
	}
	dataset := datasetSource.DatasetManager.GetDataset(datasetSource.DatasetName)

	sinceInt := 0

	entities := make([]*server.Entity, 0)
	if datasetSource.isFullSync {
		continuation, err := dataset.MapEntities(since, batchSize, func(entity *server.Entity) error {
			entities = append(entities, entity)
			return nil
		})
		if err != nil {
			return err
		}

		err = processEntities(entities, continuation)
		if err != nil {
			return err
		}
	} else {
		if since != "" {
			s, err := strconv.Atoi(since)
			if err != nil {
				return err
			}
			sinceInt = s
		}
		continuation, err := dataset.ProcessChanges(uint64(sinceInt), batchSize, func(entity *server.Entity) {
			entities = append(entities, entity)
		})
		if err != nil {
			return err
		}

		err = processEntities(entities, strconv.Itoa(int(continuation)))
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
