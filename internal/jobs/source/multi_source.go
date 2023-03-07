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
	"errors"
	"fmt"
	"strconv"

	"github.com/mimiro-io/datahub/internal/server"
)

/*
*

	MultiSource only operates on changes, but accepts calls to FullsyncStart to avoid processing dependencies during initial load
*/
type Join struct {
	Dataset   string
	Predicate string
	Inverse   bool
}

type Dependency struct {
	Dataset string // name of the dependent dataset
	Joins   []Join
}

type MultiSource struct {
	DatasetName    string
	Dependencies   []Dependency // the Dependency queries
	Store          *server.Store
	DatasetManager *server.DsManager
	isFullSync     bool
	waterMarks     map[string]uint64
	changesCache   map[string]changeURIData
	LatestOnly     bool
}

type MultiDatasetContinuation struct {
	MainToken        string
	DependencyTokens map[string]*StringDatasetContinuation
	activeDS         string
}

func (c *MultiDatasetContinuation) Encode() (string, error) {
	result, err := json.Marshal(c)
	if nil != err {
		return "", err
	}
	return string(result), nil
}

func (c *MultiDatasetContinuation) GetToken() string {
	if c.activeDS != "" {
		return c.DependencyTokens[c.activeDS].GetToken()
	}

	return c.MainToken
}

func (c *MultiDatasetContinuation) AsIncrToken() uint64 {
	if c.activeDS != "" {
		return c.DependencyTokens[c.activeDS].AsIncrToken()
	}

	i, err := strconv.Atoi(c.MainToken)
	if err != nil {
		return 0
	}

	return uint64(i)
}

func (multiSource *MultiSource) GetConfig() map[string]interface{} {
	config := make(map[string]interface{})
	config["Type"] = "MultiSource"
	config["Name"] = multiSource.DatasetName

	return config
}

func (multiSource *MultiSource) StartFullSync() {
	multiSource.isFullSync = true
	_ = multiSource.grabWatermarks()
}

func (multiSource *MultiSource) EndFullSync() {
	multiSource.isFullSync = false
}

var ErrDatasetMissing = fmt.Errorf("dataset is missing")
var ErrTokenType = fmt.Errorf("continuation token is of wrong type")

func (multiSource *MultiSource) ReadEntities(since DatasetContinuation, batchSize int,
	processEntities func([]*server.Entity, DatasetContinuation) error) error {

	multiSource.resetChangesCache()
	defer multiSource.resetChangesCache()

	if exists := multiSource.DatasetManager.IsDataset(multiSource.DatasetName); !exists {
		return fmt.Errorf("dataset %v is missing, %w", multiSource.DatasetName, ErrDatasetMissing)
	}

	dataset := multiSource.DatasetManager.GetDataset(multiSource.DatasetName)

	d, ok := since.(*MultiDatasetContinuation)
	if !ok {
		return fmt.Errorf("continuation in multisource is not a *MulitDatasetContinuation, but %t, %w", since, ErrTokenType)
	}

	if !multiSource.isFullSync {
		for _, dep := range multiSource.Dependencies {
			err := multiSource.processDependency(dep, d, batchSize, processEntities)
			if err != nil {
				return err
			}
		}
	} else {
		//set watermarks before starting fullsync
		for depName, waterMark := range multiSource.waterMarks {
			if d.DependencyTokens == nil {
				d.DependencyTokens = make(map[string]*StringDatasetContinuation)
			}

			d.DependencyTokens[depName] = &StringDatasetContinuation{Token: strconv.Itoa(int(waterMark))}
		}
	}

	d.activeDS = ""

	return multiSource.incrementalRead(since, batchSize, processEntities, dataset)
}

func (multiSource *MultiSource) resetChangesCache() {
	multiSource.changesCache = make(map[string]changeURIData)
}

func (multiSource *MultiSource) processDependency(dep Dependency, d *MultiDatasetContinuation, batchSize int,
	processEntities func([]*server.Entity, DatasetContinuation) error) error {
	depDataset, err2 := multiSource.getDatasetFor(dep)
	if err2 != nil {
		return err2
	}
	d.activeDS = dep.Dataset

	depSince := d.DependencyTokens[d.activeDS]
	if depSince == nil {
		depSince = &StringDatasetContinuation{}
	}

	uris, continuation, err := multiSource.findChangeURIs(depDataset, depSince, batchSize)

	if err != nil {
		return fmt.Errorf("detecting changes in dependency dataset %+v failed, %w", dep, err)
	}

	entities := make([]*server.Entity, 0)
	//go through joins in sequence
	for idx, join := range dep.Joins {
		// TODO: create GetManyRelated variant that takes internal ids as "startUris" ?
		relatedEntities, err := multiSource.Store.GetManyRelatedEntities(uris, join.Predicate, join.Inverse, nil)
		if err != nil {
			return fmt.Errorf("GetManyRelatedEntities failed for Join %+v, %w", join, err)
		}

		// For non-inverse first-joins, we need to query back in time aswell,
		// to find relatedEntities that have had their relation removed since "then"
		if idx == 0 && !join.Inverse {
			//get last change of previous run (cont-token minus 1 should give the last change index of previous processing run)
			since := uint64(0)
			if depSince.AsIncrToken() > 0 {
				since = depSince.AsIncrToken() - 1
			}
			changes, err := depDataset.GetChanges(since, 1, multiSource.LatestOnly)
			if err != nil {
				return err
			}
			if len(changes.Entities) > 0 {

				timestamp := int64(changes.Entities[0].Recorded)

				prevRelatedEntities, err := multiSource.Store.GetManyRelatedEntitiesAtTime(
					uris, join.Predicate, join.Inverse, nil, timestamp)
				if err != nil {
					return fmt.Errorf("previous GetManyRelatedEntities failed for Join %+v at timestamp %v, %w", join, timestamp, err)
				}

				relatedEntities = append(relatedEntities, prevRelatedEntities...)
			}
		}

		dedupCache := map[uint64]bool{}
		if idx == len(dep.Joins)-1 {
			//we reached the end
			for _, r := range relatedEntities {
				e := r[2].(*server.Entity)
				if _, ok := dedupCache[e.InternalID]; !ok {
					dedupCache[e.InternalID] = true
					// we need to load the target entity only with target dataset scope, else we risk merged entities as result.
					//
					// normally this is rather costy, since GetEntity creates a new read transacton per call.
					// But multiSource is a slow source to begin with, so it's not that noticable in the sum of things going on.
					//
					// To make the whole source more performant, we could consider performing all read operations
					// in a single read transaction while also accessing badger indexes with more specific read keys.
					// (minimize the number of badger keys we iterate over in store.GetX operations for this source).
					//
					// For example:
					// GetEntity here goes through all datasets containing the desired entity ID. If we create
					// a new API function which includes the target dataset in the bagder search key this would be more performant in
					// cases where the ID exists in many datasets. Even better if we can have an API function version that allows
					// reuse of a common read transaction.
					mainEntity, err := multiSource.Store.GetEntity(e.ID, []string{multiSource.DatasetName})
					if err != nil {
						return fmt.Errorf("Could not load entity %+v from dataset %v", e, multiSource.DatasetName)
					}
					// GetEntity is a Query Function. As such it returns skeleton Entities which only contain an ID for
					// references to non-existing entities. Checking for recorded tells us whether this is a real entity hit
					// on an "open graph" reference.
					if mainEntity.Recorded > 0 {
						entities = append(entities, mainEntity)
					}
				}
			}
		} else {
			//prepare uris list for next join
			uris = make([]string, 0)
			for _, r := range relatedEntities {
				e := r[2].(*server.Entity)
				if _, ok := dedupCache[e.InternalID]; !ok {
					dedupCache[e.InternalID] = true
					uris = append(uris, e.ID)
				}
			}
		}
	}

	if d.DependencyTokens == nil {
		d.DependencyTokens = make(map[string]*StringDatasetContinuation)
	}

	d.DependencyTokens[dep.Dataset] = &StringDatasetContinuation{Token: strconv.Itoa(int(continuation))}

	err = processEntities(entities, d)
	if err != nil {
		return err
	}

	return nil
}

type changeURIData struct {
	URIs         []string
	continuation uint64
}

func (multiSource *MultiSource) findChangeURIs(depDataset *server.Dataset, depSince *StringDatasetContinuation,
	batchSize int) ([]string, uint64, error) {
	if changes, ok := multiSource.changesCache[depDataset.ID]; ok {
		return changes.URIs, changes.continuation, nil
	}

	uris := make([]string, 0)
	continuation, err := depDataset.ProcessChanges(depSince.AsIncrToken(), batchSize, multiSource.LatestOnly,
		func(entity *server.Entity) {
			uris = append(uris, entity.ID)
		})
	multiSource.changesCache[depDataset.ID] = changeURIData{uris, continuation}

	return uris, continuation, err
}

func (multiSource *MultiSource) getDatasetFor(dep Dependency) (*server.Dataset, error) {
	if exists := multiSource.DatasetManager.IsDataset(dep.Dataset); !exists {
		return nil, fmt.Errorf("dependency dataset is missing: %v, %w", dep.Dataset, ErrDatasetMissing)
	}

	depDataset := multiSource.DatasetManager.GetDataset(dep.Dataset)
	return depDataset, nil
}

func (multiSource *MultiSource) incrementalRead(since DatasetContinuation, batchSize int,
	processEntities func([]*server.Entity, DatasetContinuation) error, dataset *server.Dataset) error {
	entities := make([]*server.Entity, 0)
	continuation, err := dataset.ProcessChanges(since.AsIncrToken(), batchSize, multiSource.LatestOnly,
		func(entity *server.Entity) {
			entities = append(entities, entity)
		})
	if err != nil {
		return err
	}

	d := since.(*MultiDatasetContinuation)
	d.MainToken = strconv.Itoa(int(continuation))
	err = processEntities(entities, d)
	if err != nil {
		return err
	}
	return nil
}

// ParseDependencies populates MultiSource dependencies based on given json config
func (multiSource *MultiSource) ParseDependencies(dependenciesConfig interface{}) error {
	dataset := multiSource.DatasetManager.GetDataset(multiSource.DatasetName)
	if dataset != nil && dataset.IsProxy() {
		return fmt.Errorf("main dataset multiSource must not be a proxy dataset: %v", multiSource.DatasetName)
	}
	if depsList, ok := dependenciesConfig.([]interface{}); ok {
		for _, dep := range depsList {
			if m, ok := dep.(map[string]interface{}); ok {
				newDep := Dependency{}
				newDep.Dataset = m["dataset"].(string)
				depDataset := multiSource.DatasetManager.GetDataset(newDep.Dataset)
				if depDataset != nil && depDataset.IsProxy() {
					return fmt.Errorf("dependency dataset %v in multiSource %v must not be a proxy dataset", newDep.Dataset, multiSource.DatasetName)
				}

				for _, j := range m["joins"].([]interface{}) {
					newJoin := Join{}
					m := j.(map[string]interface{})
					newJoin.Dataset = m["dataset"].(string)
					newJoin.Predicate = m["predicate"].(string)
					newJoin.Inverse = m["inverse"].(bool)
					newDep.Joins = append(newDep.Joins, newJoin)
				}
				multiSource.Dependencies = append(multiSource.Dependencies, newDep)
			} else {
				return fmt.Errorf("dependency %+v must be json object structure, but is %t ", dep, dep)
			}
		}
	} else {
		return errors.New("dependenciesConfig must be array array")
	}

	return nil
}

func (multiSource *MultiSource) grabWatermarks() error {
	multiSource.waterMarks = make(map[string]uint64)
	for _, dep := range multiSource.Dependencies {
		dataset, err := multiSource.getDatasetFor(dep)
		if err != nil {
			return err
		}

		waterMark, err := dataset.GetChangesWatermark()
		if err != nil {
			return err
		}

		multiSource.waterMarks[dep.Dataset] = waterMark
	}

	return nil
}
