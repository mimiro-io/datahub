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
	"math"
	"strconv"

	"github.com/mimiro-io/datahub/internal/server"
)

type SampleSource struct {
	NumberOfEntities int
	BatchSize        int
	Store            *server.Store
}

func (source *SampleSource) StartFullSync() {
	// empty for now (this makes sonar not complain)
}

func (source *SampleSource) EndFullSync() {
	// empty for now (this makes sonar not complain)
}

func (source *SampleSource) GetConfig() map[string]interface{} {
	config := make(map[string]interface{})
	config["Type"] = "SampleSource"
	config["NumberOfEntities"] = source.NumberOfEntities
	return config
}

func (source *SampleSource) ReadEntities(ctx context.Context, since DatasetContinuation, batchSize int, processEntities func([]*server.Entity, DatasetContinuation) error) error {
	var err error
	sinceOffset := int(since.AsIncrToken())
	if source.NumberOfEntities < batchSize {
		batchSize = source.NumberOfEntities
	}
	if sinceOffset >= source.NumberOfEntities {
		batchSize = 0
	}

	// assert sample source namespace
	prefix, err := source.Store.NamespaceManager.AssertPrefixMappingForExpansion("http://data.samplesource.org/")
	if err != nil {
		return err
	}

	endIndex := int(math.Min(float64(sinceOffset+batchSize), float64(source.NumberOfEntities)))

	entities := make([]*server.Entity, 0)
	for i := sinceOffset; i < endIndex; i++ {
		e := server.NewEntity(prefix+":e-"+strconv.Itoa(i), 0)
		entities = append(entities, e)
	}

	sinceToken := &StringDatasetContinuation{strconv.Itoa(endIndex)}
	return processEntities(entities, sinceToken)
}