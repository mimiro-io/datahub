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
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"go.uber.org/zap"

	"github.com/mimiro-io/datahub/internal/server"
)

/*
*

	MultiSource only operates on changes, but accepts calls to FullsyncStart to avoid processing dependencies during initial load
*/
type (
	Join struct {
		Dataset   string
		Predicate string
		Inverse   bool
	}

	Dependency struct {
		Dataset string // name of the dependent dataset
		Joins   []Join
	}
	MultiSource struct {
		DatasetName      string
		Dependencies     []Dependency // the Dependency queries
		Store            *server.Store
		DatasetManager   *server.DsManager
		isFullSync       bool
		waterMarks       map[string]uint64
		changesCache     map[string]changeURIData
		LatestOnly       bool
		Logger           *zap.SugaredLogger
		AddTransformDeps func(string, DependencyRegistry) error
		depRegistries    []DependencyRegistry
	}

	MultiDatasetContinuation struct {
		MainToken        string
		DependencyTokens map[string]*StringDatasetContinuation
		activeDS         string
	}
)

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

var (
	ErrDatasetMissing = fmt.Errorf("dataset is missing")
	ErrTokenType      = fmt.Errorf("continuation token is of wrong type")
)

func (multiSource *MultiSource) ReadEntities(ctx context.Context, since DatasetContinuation, batchSize int, processEntities func([]*server.Entity, DatasetContinuation) error) error {
	multiSource.resetChangesCache()
	defer multiSource.resetChangesCache()

	if exists := multiSource.DatasetManager.IsDataset(multiSource.DatasetName); !exists {
		return fmt.Errorf("dataset %v is missing, %w", multiSource.DatasetName, ErrDatasetMissing)
	}

	dataset := multiSource.DatasetManager.GetDataset(multiSource.DatasetName)

	d, ok := since.(*MultiDatasetContinuation)
	if !ok {
		return fmt.Errorf(
			"continuation in multisource is not a *MulitDatasetContinuation, but %t, %w",
			since,
			ErrTokenType,
		)
	}

	if !multiSource.isFullSync {
		for _, dep := range multiSource.Dependencies {
			err := multiSource.processDependency(ctx, dep, d, batchSize, processEntities)
			if err != nil {
				return err
			}
		}
	} else {
		// set watermarks before starting fullsync
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

func (multiSource *MultiSource) processDependency(ctx context.Context, dep Dependency, d *MultiDatasetContinuation, batchSize int, processEntities func([]*server.Entity, DatasetContinuation) error) error {
	depDataset, err2 := multiSource.getDatasetFor(dep)
	targetDs := multiSource.Store.DatasetsToInternalIDs([]string{multiSource.DatasetName})
	if err2 != nil {
		return err2
	}
	d.activeDS = dep.Dataset

	if d.DependencyTokens == nil {
		d.DependencyTokens = make(map[string]*StringDatasetContinuation)
	}

	depSince := d.DependencyTokens[d.activeDS]
	if depSince == nil {
		depSince = &StringDatasetContinuation{}
		d.DependencyTokens[d.activeDS] = depSince
	}

	// When working through a dependency dataset, we first find all changes since the last run in that dependency
	startPoints, continuation, err := multiSource.findChanges(depDataset, depSince, batchSize)
	if err != nil {
		return fmt.Errorf("detecting changes in dependency dataset %+v failed, %w", dep, err)
	}

	queryTime := time.Now().UnixNano()
	entities := make([]*server.Entity, 0)
	// go through joins in sequence
	prevDataset := dep.Dataset
	for idx, join := range dep.Joins {
		joinLvlChan := make(chan uint64)
		errChan := make(chan error)
		currentStartPoints := startPoints // copy by value
		predID, err3 := multiSource.Store.GetPredicateID(join.Predicate, nil)
		if err3 != nil {
			// predicate does not exist (yet), so we can drop out
			startPoints = nil
			continue
		}
		datasets := multiSource.Store.DatasetsToInternalIDs([]string{prevDataset, join.Dataset})
		prevDataset = join.Dataset
		go func() {
			defer close(joinLvlChan)
			defer close(errChan)

			// we go through all changed entities in the current join
			for _, rid := range currentStartPoints {
				if ctx.Err() != nil {
					errChan <- ctx.Err()
					return
				}
				// 1. build a RelatedFrom query input
				searchBuffer := make([]byte, 10)
				if join.Inverse {
					binary.BigEndian.PutUint16(searchBuffer, server.IncomingRefIndex)
				} else {
					binary.BigEndian.PutUint16(searchBuffer, server.OutgoingRefIndex)
				}
				binary.BigEndian.PutUint64(searchBuffer[2:], rid)

				relatedFrom := &server.RelatedFrom{
					RelationIndexFromKey: searchBuffer,
					Predicate:            predID,
					Inverse:              join.Inverse,
					Datasets:             datasets,
					At:                   queryTime,
				}
				nextRelatedFrom := relatedFrom
			repeatQuery:
				if ctx.Err() != nil {
					errChan <- ctx.Err()
					return
				}
				// 2. run query
				relatedEntities, cont, err4 := multiSource.Store.GetRelatedAtTime(nextRelatedFrom, batchSize)
				if err4 != nil {
					errChan <- err4
					return
				}

				// 3. put all query results onto channel
				for _, r := range relatedEntities {
					joinLvlChan <- r.EntityID
				}
				// 4. if query result contained a continuation token, repeat from 2.
				if cont != nil {
					nextRelatedFrom = cont
					goto repeatQuery
				}

				// For non-inverse first-joins, we need to query back in time as well,
				// to find related entities that have had their relation removed since "then"
				if idx == 0 && !join.Inverse {
					if depSince.AsIncrToken() > 0 {
						// get last change of previous run (cont-token minus 1 should give the last change index of previous processing run)
						since := depSince.AsIncrToken() - 1

						// improve this using the previous seq-numbers of the specific entity to find timestamps.
						// also run queries for all changed versions of the entity, not just the last one,
						// with the respective recorded times of each change
						changes, err5 := depDataset.GetChanges(since, 1, false)
						if err5 != nil {
							errChan <- err5
							return
						}
						if len(changes.Entities) > 0 {
							timestamp := int64(changes.Entities[0].Recorded)
							// create a copy of relatedFrom with back-dated timestamp
							prevRelatedFrom := relatedFrom
							prevRelatedFrom.At = timestamp
						repeatPrevQuery:
							if ctx.Err() != nil {
								errChan <- ctx.Err()
								return
							}
							// same query paging logic as lines 213-227, just different point in time. duplicates may be put onto channel here
							prevRelatedEntities, c, err6 := multiSource.Store.GetRelatedAtTime(prevRelatedFrom, batchSize)
							if err6 != nil {
								errChan <- fmt.Errorf("previous GetRelatedAtTime failed for Join %+v at timestamp %v, %w", join, timestamp, err6)
								return
							}
							for _, r := range prevRelatedEntities {
								joinLvlChan <- r.EntityID
							}
							if c != nil {
								prevRelatedFrom = c
								goto repeatPrevQuery
							}

						}
					}
				}
			}
		}()

		dedupCache := map[uint64]bool{}
		startPoints = make([]uint64, 0)
	readRelations:
		for {

			select {
			case <-ctx.Done():
				return ctx.Err()

			// we continuously read from our query result channel
			case internalEntityID, ok := <-joinLvlChan:
				// ok means the channel was not closed
				if ok {
					// if we are in the middle of a join-chain, all we need to do here is to preapare a list of input entities for the next join
					if idx != len(dep.Joins)-1 {
						// prepare uris list for next join
						e := internalEntityID
						if _, o := dedupCache[e]; !o {
							dedupCache[e] = true
							startPoints = append(startPoints, e)
						}
					} else {
						// we reached the end of the join chain, here we can emit what we found
						e := internalEntityID
						if _, o := dedupCache[e]; !o {
							dedupCache[e] = true
							// we need to load the target entity only with target dataset scope, else we risk merged entities as result.
							//
							// normally this is rather costly, since GetEntity creates a new read transaction per call.
							// But multiSource is a slow source to begin with, so it's not that noticeable in the sum of things going on.
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
							mainEntity, err4 := multiSource.Store.GetEntityWithInternalID(e, targetDs)
							if err4 != nil {
								return fmt.Errorf("could not load entity %+v from dataset %v: %w", e, multiSource.DatasetName, err4)
							}
							// GetEntity is a Query Function. As such it returns skeleton Entities which only contain an ID for
							// references to non-existing entities. Checking for recorded tells us whether this is a real entity hit
							// on an "open graph" reference.
							if mainEntity.Recorded > 0 {
								entities = append(entities, mainEntity)
								// when desired batch size is reached, emit
								if len(entities) >= batchSize {
									err = processEntities(entities, d)
									if err != nil {
										return err
									}
									entities = make([]*server.Entity, 0)
								}
							}
						}
					}
				} else {
					// channel close from go routine means we can stop listening
					break readRelations
				}
			case err4, ok := <-errChan:
				if ok {
					return err4
				} else {
					// channel close from go routine means we can stop listening
					break readRelations
				}
			}
		}
	}

	d.DependencyTokens[dep.Dataset] = &StringDatasetContinuation{Token: strconv.Itoa(int(continuation))}
	// if there are still unemitted search results, emit them now
	if len(entities) > 0 {
		err = processEntities(entities, d)
		if err != nil {
			return err
		}
	}

	return nil
}

type changeURIData struct {
	ids          []uint64
	continuation uint64
}

// returns array of internal entity ids, changes-continuation, error
func (multiSource *MultiSource) findChanges(depDataset *server.Dataset, depSince *StringDatasetContinuation,
	batchSize int,
) ([]uint64, uint64, error) {
	if changes, ok := multiSource.changesCache[depDataset.ID]; ok {
		return changes.ids, changes.continuation, nil
	}

	ids := make([]uint64, 0)
	continuation, err := depDataset.ProcessChanges(depSince.AsIncrToken(), batchSize, multiSource.LatestOnly,
		func(entity *server.Entity) {
			ids = append(ids, entity.InternalID)
		})
	multiSource.changesCache[depDataset.ID] = changeURIData{ids, continuation}

	return ids, continuation, err
}

func (multiSource *MultiSource) getDatasetFor(dep Dependency) (*server.Dataset, error) {
	if exists := multiSource.DatasetManager.IsDataset(dep.Dataset); !exists {
		return nil, fmt.Errorf("dependency dataset is missing: %v, %w", dep.Dataset, ErrDatasetMissing)
	}

	depDataset := multiSource.DatasetManager.GetDataset(dep.Dataset)
	return depDataset, nil
}

func (multiSource *MultiSource) incrementalRead(since DatasetContinuation, batchSize int,
	processEntities func([]*server.Entity, DatasetContinuation) error, dataset *server.Dataset,
) error {
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