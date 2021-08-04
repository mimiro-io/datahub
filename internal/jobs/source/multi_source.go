package source

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	"github.com/mimiro-io/datahub/internal/server"
)

type join struct {
	Dataset   string
	Predicate string
	Inverse   bool
}

type dependency struct {
	Dataset string // name of the dependent dataset
	Joins   []join
}

type MultiSource struct {
	DatasetName    string
	Dependencies   []dependency // the dependency queries
	Store          *server.Store
	DatasetManager *server.DsManager
	isFullSync     bool
	waterMarks     map[string]uint64
}

type MultiDatasetContinuation struct {
	MainToken        string
	DependencyTokens map[string]*StringDatasetContinuation
	ActiveDS         string
}

func (c *MultiDatasetContinuation) Encode() (string, error) {
	result, err := json.Marshal(c)
	if nil != err {
		return "", err
	}
	return string(result), nil
}

func (c *MultiDatasetContinuation) GetToken() string {
	if c.ActiveDS != "" {
		return c.DependencyTokens[c.ActiveDS].GetToken()
	}

	return c.MainToken
}

func (c *MultiDatasetContinuation) AsIncrToken() uint64 {
	if c.ActiveDS != "" {
		return c.DependencyTokens[c.ActiveDS].AsIncrToken()
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

			d.DependencyTokens[depName] = &StringDatasetContinuation{token: strconv.Itoa(int(waterMark))}
		}
	}

	d.ActiveDS = ""

	return multiSource.incrementalRead(since, batchSize, processEntities, dataset)
}

func (multiSource *MultiSource) processDependency(dep dependency, d *MultiDatasetContinuation, batchSize int,
	processEntities func([]*server.Entity, DatasetContinuation) error) error {
	depDataset, err2 := multiSource.getDatasetFor(dep)
	if err2 != nil {
		return err2
	}
	d.ActiveDS = dep.Dataset

	depSince := d.DependencyTokens[dep.Dataset]
	if depSince == nil {
		depSince = &StringDatasetContinuation{}
	}

	uris := make([]string, 0)
	continuation, err := depDataset.ProcessChanges(depSince.AsIncrToken(), batchSize, func(entity *server.Entity) {
		uris = append(uris, entity.ID)
	})

	if err != nil {
		return fmt.Errorf("detecting changes in dependency dataset %+v failed, %w", dep, err)
	}

	entities := make([]*server.Entity, 0)
	//go through joins in sequence
	for idx, join := range dep.Joins {
		// TODO: create GetManyRelated variant that takes internal ids as "startUris" ?
		relatedEntities, err := multiSource.Store.GetManyRelatedEntities(uris, join.Predicate, join.Inverse, nil)
		if err != nil {
			return fmt.Errorf("GetManyRelatedEntities failed for join %+v, %w", join, err)
		}

		// For non-inverse first-joins, we need to query back in time aswell,
		// to find relatedEntities that have had their relation removed since "then"
		if idx == 0 && !join.Inverse {
			//get last change of previous run (cont-token minus 1 should give the last change index of previous processing run)
			since := uint64(0)
			if depSince.AsIncrToken() > 0 {
				since = depSince.AsIncrToken() - 1
			}
			changes, err := depDataset.GetChanges(since, 1)
			if err != nil {
				return err
			}

			timestamp := int64(changes.Entities[0].Recorded)

			prevRelatedEntities, err := multiSource.Store.GetManyRelatedEntitiesAtTime(
				uris, join.Predicate, join.Inverse, nil, timestamp)
			if err != nil {
				return fmt.Errorf("previous GetManyRelatedEntities failed for join %+v at timestamp %v, %w", join, timestamp, err)
			}

			relatedEntities = append(relatedEntities, prevRelatedEntities...)
		}

		dedupCache := map[uint64]bool{}
		if idx == len(dep.Joins)-1 {
			//we reached the end
			for _, r := range relatedEntities {
				e := r[2].(*server.Entity)
				if _, ok := dedupCache[e.InternalID]; !ok {
					dedupCache[e.InternalID] = true
					entities = append(entities, e)
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

	d.DependencyTokens[dep.Dataset] = &StringDatasetContinuation{token: strconv.Itoa(int(continuation))}

	err = processEntities(entities, d)
	if err != nil {
		return err
	}

	return nil
}

func (multiSource *MultiSource) getDatasetFor(dep dependency) (*server.Dataset, error) {
	if exists := multiSource.DatasetManager.IsDataset(dep.Dataset); !exists {
		return nil, fmt.Errorf("dependency dataset is missing: %v, %w", dep.Dataset, ErrDatasetMissing)
	}

	depDataset := multiSource.DatasetManager.GetDataset(dep.Dataset)
	return depDataset, nil
}

func (multiSource *MultiSource) incrementalRead(since DatasetContinuation, batchSize int,
	processEntities func([]*server.Entity, DatasetContinuation) error, dataset *server.Dataset) error {
	entities := make([]*server.Entity, 0)
	continuation, err := dataset.ProcessChanges(since.AsIncrToken(), batchSize, func(entity *server.Entity) {
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
	if depsList, ok := dependenciesConfig.([]interface{}); ok {
		for _, dep := range depsList {
			if m, ok := dep.(map[string]interface{}); ok {
				newDep := dependency{}
				newDep.Dataset = m["dataset"].(string)

				for _, j := range m["joins"].([]interface{}) {
					newJoin := join{}
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
