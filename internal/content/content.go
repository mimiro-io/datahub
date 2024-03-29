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

package content

import (
	"encoding/json"

	"github.com/DataDog/datadog-go/v5/statsd"
	"go.uber.org/zap"

	"github.com/mimiro-io/datahub/internal/conf"
	"github.com/mimiro-io/datahub/internal/server"
)

type Content struct {
	ID   string                 `json:"id"`
	Data map[string]interface{} `json:"data"`
}

type Service struct {
	store  *server.Store
	logger *zap.SugaredLogger
	statsd statsd.ClientInterface
}

func NewContentService(env *conf.Config, store *server.Store, statsd statsd.ClientInterface) *Service {
	return &Service{
		store:  store,
		logger: env.Logger.Named("content"),
		statsd: statsd,
	}
}

// AddContent adds or replaces a new content entity to the store
func (contentConfig *Service) AddContent(id string, payload *Content) error {
	return contentConfig.store.StoreObject(server.ContentIndex, id, payload)
}

// UpdateContent updates an existing content (by calling AddContent)
// added the extra function for readability and future expansion
func (contentConfig *Service) UpdateContent(id string, payload *Content) error {
	return contentConfig.AddContent(id, payload)
}

// GetContentByID returns a single content by its id
func (contentConfig *Service) GetContentByID(id string) (*Content, error) {
	content := &Content{}
	err := contentConfig.store.GetObject(server.ContentIndex, id, &content)
	if err != nil {
		return nil, err
	}
	if content.ID == "" {
		return nil, nil
	}
	return content, nil
}

// ListContents returns all content entities, in random order
func (contentConfig *Service) ListContents() ([]*Content, error) {
	var contents []*Content
	err := contentConfig.store.IterateObjectsRaw(server.ContentIndexBytes, func(bytes []byte) error {
		content := &Content{}
		err := json.Unmarshal(bytes, content)
		if err != nil {
			return err
		}
		contents = append(contents, content)
		return nil
	})

	return contents, err
}

// DeleteContent deletes a single content from the store
func (contentConfig *Service) DeleteContent(id string) error {
	return contentConfig.store.DeleteObject(server.ContentIndex, id)
}
