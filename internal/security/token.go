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
	"strings"

	"go.uber.org/fx"
	"go.uber.org/zap"
)

type TokenProviders struct {
	Providers   *map[string]Provider
	log         *zap.SugaredLogger
	pm          *ProviderManager
	ServiceCore *ServiceCore
}

func (providers *TokenProviders) Get(providerName string) (Provider, bool) {
	pmap := *providers.Providers
	if p, ok := pmap[providerName]; ok {
		return p, true
	} else {
		return nil, false
	}
}

func (providers *TokenProviders) Add(providerConfig ProviderConfig) error {
	pmap := *providers.Providers
	pmap[strings.ToLower(providerConfig.Name)] = providers.toProvider(providerConfig)
	err := providers.pm.AddProvider(providerConfig)
	if err != nil {
		return err
	}
	return nil
}

// NewTokenProviders provides a map of token providers, keyed on the
// name of the token provider struct as lower_case.
func NewTokenProviders(
	lc fx.Lifecycle,
	logger *zap.SugaredLogger,
	providerManager *ProviderManager,
	serviceCore *ServiceCore,
) *TokenProviders {
	log := logger.Named("security")
	providers := make(map[string]Provider)

	tp := &TokenProviders{
		log:         log,
		pm:          providerManager,
		ServiceCore: serviceCore,
		Providers:   &providers,
	}

	lc.Append(fx.Hook{
		OnStart: func(_ context.Context) error {
			if providerList, err := providerManager.ListProviders(); err != nil {
				log.Warn(err)
			} else {
				for _, provider := range providerList {
					providers[strings.ToLower(provider.Name)] = tp.toProvider(provider)
				}
			}
			tp.Providers = &providers
			return nil
		},
	})

	return tp
}

func (providers *TokenProviders) toProvider(provider ProviderConfig) Provider {
	providers.log.Infof("Adding login provider '%s'", provider.Name)
	if strings.ToLower(provider.Type) == "bearer" {
		return NewTokenProvider(providers.log, provider, providers.pm)
	} else if strings.ToLower(provider.Type) == "nodebearer" {
		return NewNodeJwtBearerProvider(providers.log, providers.ServiceCore, provider)
	} else {
		return BasicProvider{
			User:     providers.pm.LoadValue(provider.User),
			Password: providers.pm.LoadValue(provider.Password),
		}
	}
}

func (providers *TokenProviders) ListProviders() ([]ProviderConfig, error) {
	return providers.pm.ListProviders()
}

func (providers *TokenProviders) DeleteProvider(name string) error {
	if _, ok := providers.Get(name); !ok {
		return ErrLoginProviderNotFound
	}
	delete(*providers.Providers, name)
	return providers.pm.DeleteProvider(name)
}

func (providers *TokenProviders) GetProviderConfig(name string) (*ProviderConfig, error) {
	return providers.pm.FindByName(name)
}

func (providers *TokenProviders) UpdateProvider(name string, provider ProviderConfig) error {
	if _, err := providers.GetProviderConfig(name); err != nil {
		return ErrLoginProviderNotFound
	}
	provider.Name = name
	return providers.Add(provider)
}
