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

package security

import (
	"github.com/mimiro-io/datahub/internal/conf"
	"go.uber.org/zap"
)

type TokenProvider interface {
	Token() (string, error)
}

type TokenProviders struct {
	Providers map[string]interface{}
}

// NewTokenProviders provides a map of token providers, keyed on the
// name of the token provider struct as lower_case.
func NewTokenProviders(logger *zap.SugaredLogger, conf *conf.Env) *TokenProviders {
	log := logger.Named("security")
	var providers = make(map[string]interface{}, 0)
	config := NewDlJwtConfig(log, conf)
	providers["auth0tokenprovider"] = config
	providers["jwttokenprovider"] = config

	return &TokenProviders{
		Providers: providers,
	}

}
