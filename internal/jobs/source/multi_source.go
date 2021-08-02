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
}

func (multiSource *MultiSource) EndFullSync() {
	multiSource.isFullSync = false
}

func (multiSource *MultiSource) ReadEntities(since DatasetContinuation, batchSize int, processEntities func([]*server.Entity, DatasetContinuation) error) error {
	exists := multiSource.DatasetManager.IsDataset(multiSource.DatasetName)
	if !exists {
		return errors.New("dataset is missing")
	}
	dataset := multiSource.DatasetManager.GetDataset(multiSource.DatasetName)

	d := since.(*MultiDatasetContinuation)
	if !multiSource.isFullSync {
		for _, dep := range multiSource.Dependencies {
			exists := multiSource.DatasetManager.IsDataset(dep.Dataset)
			if !exists {
				return errors.New("dependency dataset is missing: "+dep.Dataset)
			}
			depDataset := multiSource.DatasetManager.GetDataset(dep.Dataset)
			d.ActiveDS = dep.Dataset
			depSince := d.dependencyTokens[dep.Dataset]
			if depSince == nil {
				depSince = &StringDatasetContinuation{}
			}
			uris := make([]string, 0)
			continuation, err := depDataset.ProcessChanges(depSince.AsIncrToken(), batchSize, func(entity *server.Entity) {
				uris = append(uris, entity.ID)
			})
			//TODO: create GetManyRelated variant that takes internal ids as "starturis" ?
			join := dep.Joins[0]
			relatedEntities, err := multiSource.Store.GetManyRelatedEntities(uris, join.Predicate, join.Inverse, []string{join.Dataset, dep.Dataset})
			if err != nil {
				return err
			}
			entities := make([]*server.Entity, 0)
			for _,r := range relatedEntities {
				entities = append(entities, r[2].(*server.Entity))
			}
			if d.dependencyTokens == nil {
				d.dependencyTokens = make(map[string]*StringDatasetContinuation)
			}
			d.dependencyTokens[dep.Dataset] = &StringDatasetContinuation{token: strconv.Itoa(int(continuation))}
			err = processEntities(entities, since)
			if err != nil {
				return err
			}
		}
	}
	d.ActiveDS = ""
	err := multiSource.incrementalRead(since, batchSize, processEntities, dataset)
	if err != nil {
		return err
	}

	return nil
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
				return errors.New(fmt.Sprintf("dependency %+v must be json object structure, but is %t ", dep, dep))
			}
		}
	} else {
		return errors.New("dependenciesConfig must be array array")
	}
	return nil
}
