// Copyright 2021 MIMIRO AS
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package source

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	"github.com/mimiro-io/datahub/internal/server"
)

type DatasetSource struct {
	DatasetName           string
	Store                 *server.Store
	DatasetManager        *server.DsManager
	isFullSync            bool
	LatestOnly            bool
	AuthorizeProxyRequest func(authProviderName string) func(req *http.Request)
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
		return fmt.Errorf("dataset does not exist: %v", datasetSource.DatasetName)
	}
	dataset := datasetSource.DatasetManager.GetDataset(datasetSource.DatasetName)

	entities := make([]*server.Entity, 0)
	var err error
	var cont *StringDatasetContinuation
	if datasetSource.isFullSync {
		if dataset.IsProxy() {
			continuation, err := dataset.AsProxy(
				datasetSource.AuthorizeProxyRequest(dataset.ProxyConfig.AuthProviderName),
			).StreamEntitiesRaw(since.GetToken(), batchSize, func(jsonData []byte) error {
				e := &server.Entity{}
				err := json.Unmarshal(jsonData, e)
				if err != nil {
					return err
				}
				entities = append(entities, e)
				return nil
			}, nil)
			if err != nil {
				return err
			}
			cont = &StringDatasetContinuation{continuation}
		} else {
			continuation, err := dataset.MapEntities(since.GetToken(), batchSize, func(entity *server.Entity) error {
				entities = append(entities, entity)
				return nil
			})
			if err != nil {
				return err
			}
			cont = &StringDatasetContinuation{continuation}
		}
	} else {
		if dataset.IsProxy() {
			continuation, err := dataset.AsProxy(
				datasetSource.AuthorizeProxyRequest(dataset.ProxyConfig.AuthProviderName),
			).StreamChangesRaw(since.GetToken(), batchSize, func(jsonData []byte) error {
				e := &server.Entity{}
				err := json.Unmarshal(jsonData, e)
				if err != nil {
					return err
				}
				entities = append(entities, e)
				return nil
			}, nil)
			if err != nil {
				return err
			}
			cont = &StringDatasetContinuation{continuation}
		} else {
			continuation, err := dataset.ProcessChanges(since.AsIncrToken(), batchSize, datasetSource.LatestOnly,
				func(entity *server.Entity) {
					entities = append(entities, entity)
				})
			if err != nil {
				return err
			}
			cont = &StringDatasetContinuation{strconv.Itoa(int(continuation))}
		}
	}

	err = processEntities(entities, cont)
	if err != nil {
		return err
	}

	return nil
}

func (datasetSource *DatasetSource) GetConfig() map[string]interface{} {
	config := make(map[string]interface{})
	config["Type"] = "DatasetSource"
	config["Name"] = datasetSource.DatasetName
	return config
}
