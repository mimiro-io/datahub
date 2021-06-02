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

package secrets

import "go.uber.org/zap"

type SecretStore interface {
	HasKey(key string) bool
	Value(key string) (string, bool)
	Params() *map[string]interface{}
}

type NoopStore struct{}

func (s *NoopStore) HasKey(key string) bool {
	return false
}

func (s *NoopStore) Value(key string) (string, bool) {
	return "", false
}

func (s *NoopStore) Params() *map[string]interface{} {
	params := make(map[string]interface{})
	return &params
}

func NewManager(managerType string, profile string, logger *zap.SugaredLogger) (SecretStore, error) {
	if managerType == "ssm" {
		// attempt at loading values from ssm
		logger.Info("Using AWS SSM secrets manager")
		return NewSsm(&SsmManagerConfig{
			Env:    profile,
			Key:    "/application/datahub/",
			Logger: logger,
		})
	} else {
		logger.Info("Using NOOP secrets manager")
		return &NoopStore{}, nil
	}
}
