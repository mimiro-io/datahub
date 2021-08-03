package source

import (
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

func (multiSource *MultiSource) DecodeToken(token string) DatasetContinuation {
	panic("implement me")
}

func (multiSource *MultiSource) EncodeToken(token DatasetContinuation) string {
	panic("implement me")
}

type MultiDatasetContinuation struct {
	mainToken        string
	dependencyTokens map[string]*StringDatasetContinuation
	ActiveDS         string
}

func (c *MultiDatasetContinuation) GetToken() string {
	if c.ActiveDS != "" {
		return c.dependencyTokens[c.ActiveDS].GetToken()
	}

	return c.mainToken
}

func (c *MultiDatasetContinuation) AsIncrToken() uint64 {
	if c.ActiveDS != "" {
		return c.dependencyTokens[c.ActiveDS].AsIncrToken()
	}

	i, err := strconv.Atoi(c.mainToken)
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
		//set watermarks
		for depName, waterMark := range multiSource.waterMarks {
			if d.dependencyTokens == nil {
				d.dependencyTokens = make(map[string]*StringDatasetContinuation)
			}

			d.dependencyTokens[depName] = &StringDatasetContinuation{token: strconv.Itoa(int(waterMark))}
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

	depSince := d.dependencyTokens[dep.Dataset]
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

		//we reached the end
		if idx == len(dep.Joins)-1 {
			for _, r := range relatedEntities {
				entities = append(entities, r[2].(*server.Entity))
			}
		} else {
			uris = make([]string, 0)
			for _, r := range relatedEntities {
				uris = append(uris, r[2].(*server.Entity).ID)
			}
		}
	}

	if d.dependencyTokens == nil {
		d.dependencyTokens = make(map[string]*StringDatasetContinuation)
	}

	d.dependencyTokens[dep.Dataset] = &StringDatasetContinuation{token: strconv.Itoa(int(continuation))}

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
	d.mainToken = strconv.Itoa(int(continuation))
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
