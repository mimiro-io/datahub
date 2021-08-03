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

	"github.com/mimiro-io/datahub/internal/server"
)

// Source interface for pulling data
type Source interface {
	GetConfig() map[string]interface{}
	ReadEntities(since DatasetContinuation, batchSize int, processEntities func([]*server.Entity, DatasetContinuation) error) error
	StartFullSync()
	EndFullSync()
	DecodeToken(token string) DatasetContinuation
	EncodeToken(token DatasetContinuation) string
}

type DatasetContinuation interface {
	GetToken() string
	AsIncrToken() uint64
}

type StringDatasetContinuation struct {
	token string
}

func (c *StringDatasetContinuation) GetToken() string {
	return c.token
}
func (c *StringDatasetContinuation) AsIncrToken() uint64 {
	if c.token != "" {
		i, err := strconv.Atoi(c.token)
		if err != nil {
			return 0
		}
		return uint64(i)
	}
	return 0
}