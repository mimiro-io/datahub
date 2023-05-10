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
	"fmt"
	"go.uber.org/zap"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"
	"net/http"
	"net/url"
)

// ClientCredentialsProvider contains the auth0 configuration
type ClientCredentialsProvider struct {
	logger      *zap.SugaredLogger
	tokenSource oauth2.TokenSource
}

// NewClientCredentialsProvider creates a new ClientCredentialsProvider struct, populated with the values from Viper.
func NewClientCredentialsProvider(logger *zap.SugaredLogger, conf ProviderConfig, pm *ProviderManager) *ClientCredentialsProvider {
	params := url.Values{"audience": []string{pm.LoadValue(conf.Audience)}}
	cc := clientcredentials.Config{
		ClientID:       pm.LoadValue(conf.ClientID),
		ClientSecret:   pm.LoadValue(conf.ClientSecret),
		TokenURL:       pm.LoadValue(conf.Endpoint),
		EndpointParams: params,
	}
	tokenSource := cc.TokenSource(context.Background())

	config := &ClientCredentialsProvider{
		logger:      logger.Named("jwt"),
		tokenSource: oauth2.ReuseTokenSource(nil, tokenSource),
	}

	return config
}

func (tp *ClientCredentialsProvider) Authorize(req *http.Request) {
	if token, err := tp.token(); err != nil {
		tp.logger.Warnf("Error getting token: %s", err)
	} else {
		if token != nil {
			req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", token.AccessToken))
		}
	}
}

// Token returns a valid token, or an error caused when getting one
// Successive calls to this will return cached values, where the cache
// validity equals the token validity. The token expiry has a grace
// period of 10 seconds to avoid race conditions.
func (tp *ClientCredentialsProvider) token() (*oauth2.Token, error) {
	if tp.tokenSource == nil {
		return nil, nil
	}

	return tp.tokenSource.Token()
}
