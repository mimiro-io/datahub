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
	"strconv"
	"time"

	"github.com/mimiro-io/datahub/internal/server"
)

type SlowSource struct {
	BatchSize int
	Sleep     string
}

func (source *SlowSource) StartFullSync() {
	// empty for now (this makes sonar not complain)
}

func (source *SlowSource) EndFullSync() {
	// empty for now (this makes sonar not complain)
}

func (source *SlowSource) GetConfig() map[string]interface{} {
	config := make(map[string]interface{})
	config["Type"] = "SlowSource"
	config["BatchSize"] = source.BatchSize
	config["Sleep"] = source.Sleep

	return config
}

func (source *SlowSource) ReadEntities(since DatasetContinuation, batchSize int, processEntities func([]*server.Entity, DatasetContinuation) error) error {
	// assert sample source namespace

	entities := make([]*server.Entity, source.BatchSize)
	for i := 0; i < source.BatchSize; i++ {
		e := server.NewEntity("test:e-"+strconv.Itoa(i), 0)
		entities[i] = e
	}
	d, err := time.ParseDuration(source.Sleep)
	if err != nil {
		return err
	}
	time.Sleep(d)

	return processEntities(entities, &StringDatasetContinuation{""})
}
