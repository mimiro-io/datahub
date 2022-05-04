package source

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/mimiro-io/datahub/internal/server"
	"strconv"
)

type UnionDatasetSource struct {
	DatasetSources []*DatasetSource
}

func (s *UnionDatasetSource) ReadEntities(token DatasetContinuation, batchSize int,
	processEntities func(entities []*server.Entity, token DatasetContinuation) error) error {

	d, ok := token.(*UnionDatasetContinuation)
	if !ok {
		return fmt.Errorf("continuation in UnionDatasetSource is not a *UnionDatasetContinuation, but %t, %w", token, ErrTokenType)
	}

	d.activeIdx = 0
	if len(d.Tokens) == 0 {
		for _, dss := range s.DatasetSources {
			d.Tokens = append(d.Tokens, &StringDatasetContinuation{})
			d.DatasetNames = append(d.DatasetNames, dss.DatasetName)
		}
	}

	if len(d.Tokens) != len(s.DatasetSources) {
		return fmt.Errorf("UnionDatasetSource has %v datasets, but UnionDatasetContinuation "+
			"has %v tokens.", len(s.DatasetSources), len(d.Tokens))
	}

	for i, n := range d.DatasetNames {
		if s.DatasetSources[i].DatasetName != n {
			return fmt.Errorf("UnionDatasetSource: detected change in dataset order. Must reset token.")
		}
	}
	var err error
	keepGoing := true
	for keepGoing {

		datasetSource := s.DatasetSources[d.activeIdx]
		exists := datasetSource.DatasetManager.IsDataset(datasetSource.DatasetName)
		if !exists {
			return errors.New("dataset is missing")
		}
		dataset := datasetSource.DatasetManager.GetDataset(datasetSource.DatasetName)

		entities := make([]*server.Entity, 0)
		if datasetSource.isFullSync {
			cont, err := dataset.MapEntities(d.GetToken(), batchSize, func(entity *server.Entity) error {
				entities = append(entities, entity)
				return nil
			})
			if err != nil {
				return err
			}
			keepGoing = d.Update(cont)
		} else {
			cont, err := dataset.ProcessChanges(d.AsIncrToken(), batchSize, datasetSource.LatestOnly,
				func(entity *server.Entity) {
					entities = append(entities, entity)
				})
			if err != nil {
				return err
			}
			keepGoing = d.Update(strconv.Itoa(int(cont)))
		}
		if len(entities) > 0 || !keepGoing {
			err = processEntities(entities, d)
			if err != nil {
				return err
			}
		}

	}
	return nil
}

func (s *UnionDatasetSource) EndFullSync() {
	for _, ds := range s.DatasetSources {
		ds.EndFullSync()
	}
}

func (s *UnionDatasetSource) StartFullSync() {
	for _, ds := range s.DatasetSources {
		ds.StartFullSync()
	}
}
func (s *UnionDatasetSource) GetConfig() map[string]interface{} {
	name := ""
	for i, s := range s.DatasetSources {
		if i > 0 {
			name = name + ","
		}
		name = name + s.DatasetName
	}
	config := make(map[string]interface{})
	config["Type"] = "UnionDatasetSource"
	config["Name"] = name
	return config
}

type UnionDatasetContinuation struct {
	Tokens       []*StringDatasetContinuation
	activeIdx    int
	DatasetNames []string
}

func (c *UnionDatasetContinuation) Encode() (string, error) {
	result, err := json.Marshal(c)
	if nil != err {
		return "", err
	}
	return string(result), nil
}

func (c *UnionDatasetContinuation) GetToken() string {
	return c.ActiveToken().GetToken()
}

func (c *UnionDatasetContinuation) AsIncrToken() uint64 {
	i, err := strconv.Atoi(c.GetToken())
	if err != nil {
		return 0
	}
	return uint64(i)
}

func (c *UnionDatasetContinuation) ActiveToken() DatasetContinuation {
	if len(c.Tokens) == 0 {
		c.Tokens = []*StringDatasetContinuation{{}}
	}
	return c.Tokens[c.activeIdx]
}

func (c *UnionDatasetContinuation) Update(newToken string) bool {
	t := c.ActiveToken()
	prevString := t.GetToken()
	c.Tokens[c.activeIdx] = &StringDatasetContinuation{newToken}
	if newToken == prevString {
		if c.activeIdx < len(c.Tokens)-1 {
			c.activeIdx = c.activeIdx + 1
			return true
		}
		return false
	}
	return true
}
