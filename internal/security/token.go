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
	"go.uber.org/fx"
	"go.uber.org/zap"
	"strings"
)

type TokenProviders struct {
	Providers *map[string]Provider
	log       *zap.SugaredLogger
	pm        *ProviderManager
}

func (providers *TokenProviders) Get(providerName string) (Provider, bool) {
	pmap := *providers.Providers
	if p, ok := pmap[providerName]; ok {
		return p, true
	} else {
		return nil, false
	}
}

// NewTokenProviders provides a map of token providers, keyed on the
// name of the token provider struct as lower_case.
func NewTokenProviders(lc fx.Lifecycle, logger *zap.SugaredLogger, providerManager *ProviderManager) *TokenProviders {
	log := logger.Named("security")
	var providers = make(map[string]Provider)
	//config := NewDlJwtConfig(log, conf)
	//providers["auth0tokenprovider"] = config
	//providers["jwttokenprovider"] = config

	tp := &TokenProviders{
		log: log,
		pm:  providerManager,
	}

	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
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
		return NewDlJwtConfig(providers.log, provider, providers.pm)
	} else {
		return BasicProvider{
			User:     providers.pm.LoadValue(provider.User),
			Password: providers.pm.LoadValue(provider.Password),
		}
	}
}
