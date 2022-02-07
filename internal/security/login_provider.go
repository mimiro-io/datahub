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
	"context"
	"encoding/json"
	"errors"
	"github.com/mimiro-io/datahub/internal/conf"
	"github.com/mimiro-io/datahub/internal/conf/secrets"
	"github.com/mimiro-io/datahub/internal/server"
	"github.com/spf13/viper"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"net/http"
	"os"
)

var (
	ErrLoginProviderNotFound = errors.New("login provider not found")
)

type Provider interface {
	Authorize(req *http.Request)
}

type ProviderManager struct {
	env   *conf.Env
	store *server.Store
	log   *zap.SugaredLogger
	sm    secrets.SecretStore
}

func NewProviderManager(lc fx.Lifecycle, env *conf.Env, store *server.Store, log *zap.SugaredLogger, sm secrets.SecretStore) *ProviderManager {
	pm := &ProviderManager{
		env:   env,
		store: store,
		log:   log.Named("login-provider"),
	}

	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			return pm.addComp()
		},
	})

	return pm
}

// addComp makes sure we still support the old version by adding the
// jwt configuration from the env if present.
func (pm *ProviderManager) addComp() error {
	if pm.env.DlJwtConfig.ClientId != "" {
		provider := ProviderConfig{
			Name: "jwttokenprovider",
			Type: "bearer",
			ClientId: &ValueReader{
				Type:  "env",
				Value: "DL_JWT_CLIENT_ID",
			},
			ClientSecret: &ValueReader{
				Type:  "env",
				Value: "DL_JWT_CLIENT_SECRET",
			},
			Audience: &ValueReader{
				Type:  "env",
				Value: "DL_JWT_AUDIENCE",
			},
			GrantType: &ValueReader{
				Type:  "env",
				Value: "DL_JWT_GRANT_TYPE",
			},
			Endpoint: &ValueReader{
				Type:  "env",
				Value: "DL_JWT_ENDPOINT",
			},
		}
		return pm.AddProvider(provider)
	}
	return nil
}

func (pm *ProviderManager) LoadValue(vp *ValueReader) string {
	switch vp.Type {
	case "text":
		return vp.Value
	case "env":
		v := os.Getenv(vp.Value)
		if v == "" {
			return viper.GetString(vp.Value)
		} else {
			return v
		}
	case "ssm":
		if v, ok := pm.sm.Value(vp.Value); ok {
			return v
		}
	}

	return ""
}

func (pm *ProviderManager) ListProviders() ([]ProviderConfig, error) {
	providers := make([]ProviderConfig, 0)
	err := pm.store.IterateObjectsRaw(server.LOGIN_PROVIDER_INDEX_BYTES, func(bytes []byte) error {
		provider := ProviderConfig{}
		if err := json.Unmarshal(bytes, &provider); err != nil {
			return err
		}
		providers = append(providers, provider)
		return nil
	})
	return providers, err
}

func (pm *ProviderManager) AddProvider(providerConfig ProviderConfig) error {
	return pm.store.StoreObject(server.LOGIN_PROVIDER_INDEX, providerConfig.Name, providerConfig)
}

func (pm *ProviderManager) UpdateProvider(name string, providerConfig ProviderConfig) error {
	if p, err := pm.FindByName(name); err != nil {
		return err
	} else if p == nil {
		return ErrLoginProviderNotFound
	}

	providerConfig.Name = name
	return pm.AddProvider(providerConfig)
}

func (pm *ProviderManager) DeleteProvider(name string) error {
	return pm.store.DeleteObject(server.LOGIN_PROVIDER_INDEX, name)
}

func (pm *ProviderManager) FindByName(name string) (*ProviderConfig, error) {
	config := &ProviderConfig{}
	if err := pm.store.GetObject(server.LOGIN_PROVIDER_INDEX, name, config); err != nil {
		return nil, err
	} else {
		if config.Name == "" { // does not exist
			return nil, nil
		}
		return config, err
	}

}

type ProviderConfig struct {
	Name         string       `json:"name"`
	Type         string       `json:"type"`
	User         *ValueReader `json:"user,omitempty"`
	Password     *ValueReader `json:"password,omitempty"`
	ClientId     *ValueReader `json:"key,omitempty"`
	ClientSecret *ValueReader `json:"secret,omitempty"`
	Audience     *ValueReader `json:"audience,omitempty"`
	GrantType    *ValueReader `json:"grantType,omitempty"`
	Endpoint     *ValueReader `json:"endpoint,omitempty"`
}

type ValueReader struct {
	Type  string `json:"type"`
	Value string `json:"value"`
}

type BasicProvider struct {
	User     string
	Password string
}

func (p BasicProvider) Authorize(req *http.Request) {
	req.SetBasicAuth(p.User, p.Password)
}
