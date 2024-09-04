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
	"fmt"
	"strings"
	"sync"

	"go.uber.org/zap"

	"github.com/mimiro-io/datahub/internal/conf"
)

const datasetCore = "core.Dataset"

type DsManager struct {
	store  *Store
	logger *zap.SugaredLogger
	eb     EventBus
	lock   sync.Mutex
}

type DatasetName struct {
	Name string `json:"Name"`
}

func NewDsManager(env *conf.Config, store *Store, eb EventBus) *DsManager {
	dsm := &DsManager{
		store:  store,
		logger: env.Logger.Named("ds-manager"),
		eb:     eb,
	}

	eb.Init(dsm.GetDatasetNames())
	_, err := dsm.CreateDataset(datasetCore, nil)
	if err != nil {
		dsm.logger.Warn(err)
	}

	return dsm
}

func (dsm *DsManager) NewDatasetEntity(
	name string,
	proxyDatasetConfig *ProxyDatasetConfig,
	virtualDatasetConfig *VirtualDatasetConfig,
	publicNamespaces []string,
) *Entity {
	prefix, _ := dsm.store.NamespaceManager.AssertPrefixMappingForExpansion("http://data.mimiro.io/core/dataset/")
	core, _ := dsm.store.NamespaceManager.AssertPrefixMappingForExpansion("http://data.mimiro.io/core/")
	rdfNamespacePrefix, _ := dsm.store.NamespaceManager.AssertPrefixMappingForExpansion(RdfNamespaceExpansion)
	entity := NewEntity(prefix+":"+name, 0)
	entity.Properties[prefix+":name"] = name
	entity.Properties[prefix+":items"] = 0
	entity.References[rdfNamespacePrefix+":type"] = core + ":dataset"

	if proxyDatasetConfig != nil && proxyDatasetConfig.RemoteURL != "" {
		entity.References[rdfNamespacePrefix+":type"] = core + ":proxy-dataset"
		entity.Properties[prefix+":remoteUrl"] = proxyDatasetConfig.RemoteURL
		entity.Properties[prefix+":authProviderName"] = proxyDatasetConfig.AuthProviderName
		entity.Properties[prefix+":downstreamTransform"] = proxyDatasetConfig.DownstreamTransform
		entity.Properties[prefix+":upstreamTransform"] = proxyDatasetConfig.UpstreamTransform
		entity.Properties[prefix+":timeoutSeconds"] = proxyDatasetConfig.TimeoutSeconds
	}

	if virtualDatasetConfig != nil && virtualDatasetConfig.Transform != "" {
		entity.References[rdfNamespacePrefix+":type"] = core + ":virtual-dataset"
		entity.Properties[prefix+":transform"] = virtualDatasetConfig.Transform
	}

	if len(publicNamespaces) > 0 {
		entity.Properties[prefix+":publicNamespaces"] = publicNamespaces
	}

	return entity
}

func (dsm *DsManager) storeEntity(dataset *Dataset, entity *Entity) error {
	entities := []*Entity{
		entity,
	}
	return dataset.StoreEntities(entities)
}

type CreateDatasetConfig struct {
	ProxyDatasetConfig   *ProxyDatasetConfig   `json:"ProxyDatasetConfig"`
	VirtualDatasetConfig *VirtualDatasetConfig `json:"VirtualDatasetConfig"`
	PublicNamespaces     []string              `json:"publicNamespaces"`
}

type UpdateDatasetConfig struct {
	ID string // update id/name
}

func (dsm *DsManager) CreateDataset(name string, createDatasetConfig *CreateDatasetConfig) (*Dataset, error) {
	dsm.lock.Lock()
	defer dsm.lock.Unlock()
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
	err := dsm.store.storeValue(StoreNextDatasetIDBytes, nextDatasetIDBytes)
	if err != nil {
		return nil, err
	}
	if createDatasetConfig != nil {
		ds.ProxyConfig = createDatasetConfig.ProxyDatasetConfig
		ds.PublicNamespaces = createDatasetConfig.PublicNamespaces
		ds.VirtualDatasetConfig = createDatasetConfig.VirtualDatasetConfig
	}

	jsonData, _ := json.Marshal(ds)
	err = dsm.store.storeValue(ds.getStorageKey(), jsonData)
	if err != nil {
		return nil, err
	}

	dsm.store.datasets.Store(name, ds)
	dsm.store.datasetsByInternalID.Store(ds.InternalID, ds)

	// need to add the event publisher topic
	dsm.logger.Infof("Registering dataset." + name)
	dsm.eb.RegisterTopic(name)

	// add the entity
	ent := dsm.NewDatasetEntity(name, ds.ProxyConfig, ds.VirtualDatasetConfig, ds.PublicNamespaces)

	core := dsm.GetDataset(datasetCore)
	err = dsm.storeEntity(core, ent)
	if err != nil {
		return ds, err
	}

	// making sure the event is triggered
	dsm.eb.Emit(context.Background(), "dataset.core.Dataset", nil)

	return ds, nil
}

func (dsm *DsManager) UpdateDataset(name string, config *UpdateDatasetConfig) (*Dataset, error) {
	dsm.lock.Lock()
	defer dsm.lock.Unlock()
	if name == datasetCore {
		return nil, errors.New("cannot update " + datasetCore)
	}
	exists := dsm.IsDataset(name)
	if !exists {
		return nil, errors.New("attempt to update non existent dataset")
	}

	ds := dsm.GetDataset(name)
	ds.WriteLock.Lock()
	defer ds.WriteLock.Unlock()

	// new ID means rename
	if config.ID != name {
		newName := config.ID
		newExists := dsm.IsDataset(newName)
		if newExists {
			return nil, fmt.Errorf("cannot rename dataset %v to existing dataset %v", name, newName)
		}
		dsm.logger.Infof("renaming dataset %v to %v", name, newName)

		oldKey := ds.getStorageKey()
		// update stored datasets
		ds.ID = newName
		ds.SubjectIdentifier = "http://data.mimiro.io/datasets/" + newName
		jsonData, _ := json.Marshal(ds)
		newKey := ds.getStorageKey()
		err := dsm.store.moveValue(oldKey, newKey, jsonData)
		if err != nil {
			return nil, err
		}

		// update in local cache
		dsm.store.datasets.Delete(name)
		dsm.store.datasets.Store(newName, ds)
		dsm.store.datasetsByInternalID.Store(ds.InternalID, ds)

		// update eventbus
		dsm.eb.UnregisterTopic(name)
		dsm.eb.RegisterTopic(newName)

		// update core entity

		core := dsm.GetDataset(datasetCore)
		dsInfo, err := ds.store.NamespaceManager.GetDatasetNamespaceInfo()
		if err != nil {
			return nil, err
		}

		entity, err := dsm.store.GetEntity(dsInfo.DatasetPrefix+":"+name, []string{datasetCore}, true)
		if err != nil {
			return nil, err
		}
		entity.IsDeleted = true
		err = dsm.storeEntity(core, entity)
		if err != nil {
			return nil, err
		}
		entity.IsDeleted = false
		entity.ID = dsInfo.DatasetPrefix + ":" + newName
		entity.Properties[dsInfo.NameKey] = newName
		err = dsm.storeEntity(core, entity)
		if err != nil {
			return nil, err
		}
		dsm.eb.Emit(context.Background(), "dataset.core.Dataset", nil)
	}
	return ds, nil
}

// DeleteDataset deletes dataset if it exists
func (dsm *DsManager) DeleteDataset(name string) error {
	dsm.lock.Lock()
	defer dsm.lock.Unlock()
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
	dsm.store.datasetsByInternalID.Delete(existingDataset.InternalID)
	key := existingDataset.getStorageKey()
	err := dsm.store.deleteValue(key)
	if err != nil {
		return err
	}

	// record we deleted it.
	// swap map out with new modified copy of map to avoid concurrent read/write issues which can occur if
	// a user deletes a dataset while this map is iterated over (in garbagecollector for example)
	newDeletedDatasets := make(map[uint32]bool)
	for k, v := range dsm.store.deletedDatasets {
		newDeletedDatasets[k] = v
	}
	newDeletedDatasets[existingDataset.InternalID] = true
	dsm.store.deletedDatasets = newDeletedDatasets
	err = dsm.store.StoreObject(StoreMetaIndex, "deleteddatasets", dsm.store.deletedDatasets)
	if err != nil {
		return err
	}

	dsm.eb.UnregisterTopic(name) // unregister event-handler on this topic. Note that subscriptions are left.

	// also delete the associated entity
	entity, err2 := dsm.store.GetEntity(dsm.NewDatasetEntity(name, nil, nil, nil).ID, []string{datasetCore}, true)
	if err2 != nil {
		return err2
	}
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
	dsm.store.datasets.Range(func(k interface{}, _ interface{}) bool {
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
	return dsm.GetDataset(name) != nil
}
