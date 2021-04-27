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

package server

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"strings"

	"github.com/mimiro-io/datahub/internal/conf"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

const datasetCore = "core.Dataset"

type DsManager struct {
	store  *Store
	logger *zap.SugaredLogger
	eb     EventBus
}

type DatasetName struct {
	Name string `json:"Name"`
}

func NewDsManager(lc fx.Lifecycle, env *conf.Env, store *Store, eb EventBus) *DsManager {
	dsm := &DsManager{
		store:  store,
		logger: env.Logger.Named("ds-manager"),
		eb:     eb,
	}

	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			eb.Init(dsm.GetDatasetNames())
			// if we are missing core datasets, we add these here
			_, err := dsm.CreateDataset(datasetCore)
			if err != nil {
				dsm.logger.Warn(err)
			}
			return nil
		},
	})

	return dsm
}

func (dsm *DsManager) GetDatasetEntity(name string) *Entity {

	prefix, _ := dsm.store.NamespaceManager.AssertPrefixMappingForExpansion("http://data.mimiro.io/core/dataset/")
	core, _ := dsm.store.NamespaceManager.AssertPrefixMappingForExpansion("http://data.mimiro.io/core/")
	rdfNamespacePrefix, _ := dsm.store.NamespaceManager.AssertPrefixMappingForExpansion(RdfNamespaceExpansion)
	entity := NewEntity(prefix+":"+name, 0)
	entity.Properties[prefix+":name"] = name
	entity.Properties[prefix+":items"] = 0
	entity.References[rdfNamespacePrefix+":type"] = core + ":dataset"

	return entity
}

func (dsm *DsManager) storeEntity(dataset *Dataset, entity *Entity) error {
	entities := []*Entity{
		entity,
	}
	return dataset.StoreEntities(entities)
}

func (dsm *DsManager) CreateDataset(name string) (*Dataset, error) {
	// fixme: race condition needs a lock

	exists := dsm.IsDataset(name)
	if exists {
		return dsm.GetDataset(name), nil
	}

	// create a new one
	ds := NewDataset(dsm.store, name, dsm.store.nextDatasetID, "http://data.mimiro.io/datasets/"+name)

	// store next dataset-id
	dsm.store.nextDatasetID++
	nextDatasetIDBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(nextDatasetIDBytes, dsm.store.nextDatasetID)
	err := dsm.store.storeValue(STORE_NEXT_DATASET_ID_BYTES, nextDatasetIDBytes)
	if err != nil {
		return nil, err
	}

	jsonData, _ := json.Marshal(ds)
	err = dsm.store.storeValue(ds.getStorageKey(), jsonData)
	if err != nil {
		return nil, err
	}

	dsm.store.datasets.Store(name, ds)

	// need to add the event publisher topic
	dsm.logger.Infof("Registering dataset." + name)
	dsm.eb.RegisterTopic(name)

	// add the entity
	ent := dsm.GetDatasetEntity(name)
	core := dsm.GetDataset(datasetCore)
	err = dsm.storeEntity(core, ent)
	if err != nil {
		return ds, err
	}

	// making sure the event is triggered
	dsm.eb.Emit(context.Background(), "dataset.core.Dataset", nil)

	return ds, nil
}

// DeleteDataset deletes dataset if it exists
func (dsm *DsManager) DeleteDataset(name string) error {
	if name == datasetCore {
		return errors.New("cannot delete " + datasetCore)
	}

	// delete dataset metadata
	exists := dsm.IsDataset(name)
	if !exists {
		return errors.New("attempt to delete non existent dataset")
	}

	existingDataset := dsm.GetDataset(name)
	existingDataset.markedForDeletion = true

	// delete from local cache
	dsm.store.datasets.Delete(name)
	key := existingDataset.getStorageKey()
	err := dsm.store.deleteValue(key)
	if err != nil {
		return err
	}

	// record we deleted it. fixme: persist this list and reload
	dsm.store.deletedDatasets[existingDataset.InternalID] = true
	err = dsm.store.StoreObject(STORE_META_INDEX, "deleteddatasets", dsm.store.deletedDatasets)
	if err != nil {
		return err
	}

	dsm.eb.UnregisterTopic(name) // unregister event-handler on this topic. Note that subscriptions are left.

	// also delete the associated entity
	entity := dsm.GetDatasetEntity(name)
	entity.IsDeleted = true
	core := dsm.GetDataset(datasetCore)
	err = dsm.storeEntity(core, entity)
	if err != nil {
		return err
	}
	dsm.eb.Emit(context.Background(), "dataset.core.Dataset", nil)

	// fixme: schedule background job for cleaning up
	// delete all entities in dataset
	// delete all index entries related to entities in the dataset
	return nil
}

// GetDatasetNames returns a list of the dataset names
func (dsm *DsManager) GetDatasetNames() []DatasetName {
	names := make([]DatasetName, 0)
	dsm.store.datasets.Range(func(k interface{}, val interface{}) bool {
		names = append(names, DatasetName{Name: k.(string)})
		return true
	})
	return names
}

func (dsm *DsManager) GetDataset(id string) *Dataset {
	ds, ok := dsm.store.datasets.Load(id)
	if !ok {
		return nil
	}
	return ds.(*Dataset)
}

func (dsm *DsManager) GetDatasetDetails(name string) (*Entity, bool, error) {
	exist := dsm.IsDataset(name)
	if !exist {
		return nil, false, nil
	}

	dataset := dsm.GetDataset(datasetCore)
	entity := &Entity{}
	found := false
	_, err := dataset.MapEntitiesRaw("", 1000, func(jsonData []byte) error {
		e := &Entity{}
		err := json.Unmarshal(jsonData, e)
		if err == nil {
			// e.ID is in the format 'ns0:datasetname'. we must extract value after prefix to match requested name
			idElements := strings.SplitN(e.ID, ":", 2)
			if len(idElements) == 2 && idElements[1] == name {
				entity = e
				found = true
			}
		}

		return err
	})
	if err != nil {
		return nil, false, err
	}

	return entity, found, err
}

func (dsm *DsManager) IsDataset(name string) bool {
	dataset := dsm.GetDataset(name)
	if dataset != nil {
		return true
	}
	return false
}
