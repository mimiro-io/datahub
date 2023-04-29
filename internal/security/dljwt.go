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
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/gojektech/heimdall/v6/httpclient"
	"go.uber.org/zap"
)

// JwtBearerProvider contains the auth0 configuration
type JwtBearerProvider struct {
	ClientID     string `json:"client_id"`
	ClientSecret string `json:"client_secret"`
	Audience     string `json:"audience"`
	GrantType    string `json:"grant_type"`
	endpoint     string
	logger       *zap.SugaredLogger
	cache        *cache
}

type cache struct {
	until time.Time
	token string
}

type auth0Response struct {
	AccessToken string `json:"access_token"`
	Scope       string `json:"scope"`
	ExpiresIn   int64  `json:"expires_in"`
	TokenType   string `json:"token_type"`
}

// NewDlJwtConfig creates a new JwtBearerProvider struct, populated with the values from Viper.
func NewDlJwtConfig(logger *zap.SugaredLogger, conf ProviderConfig, pm *ProviderManager) *JwtBearerProvider {
	config := &JwtBearerProvider{
		ClientID:     pm.LoadValue(conf.ClientID),
		ClientSecret: pm.LoadValue(conf.ClientSecret),
		Audience:     pm.LoadValue(conf.Audience),
		GrantType:    pm.LoadValue(conf.GrantType),
		endpoint:     pm.LoadValue(conf.Endpoint),
		logger:       logger.Named("jwt"),
	}

	return config
}

func (auth0 *JwtBearerProvider) Authorize(req *http.Request) {
	if bearer, err := auth0.token(); err != nil {
		auth0.logger.Warn(err)
	} else {
		req.Header.Add("Authorization", bearer)
	}
}

// Token returns a valid token, or an error caused when getting one
// Successive calls to this will return cached values, where the cache
// validity equals the token validity. Experience-wise, this can lead to
// race conditions when the caller asks for a valid token that is about to
// run out, and then it runs out before it can be used.
// If the cache is invalid, there is also no protection against cache stampedes,
// so if many calls are calling at the same time, they will hit Auth0 where the can
// be rate limited.
func (auth0 *JwtBearerProvider) token() (string, error) {
	token, err := auth0.generateOrGetToken()
	if err != nil {
		auth0.logger.Warnf("Error getting token: %w", err.Error())
		return "", err
	}
	return fmt.Sprintf("Bearer %s", token), nil
}

func (auth0 *JwtBearerProvider) generateOrGetToken() (string, error) {
	now := time.Now()
	if auth0.cache == nil || now.After(auth0.cache.until) {
		// first run
		res, err := auth0.callRemote()
		if err != nil {
			return "", err
		}
		auth0.cache = &cache{
			until: time.Now().Add(time.Duration(res.ExpiresIn) * time.Second),
			token: res.AccessToken,
		}
	}

	return auth0.cache.token, nil
}

func (auth0 *JwtBearerProvider) callRemote() (*auth0Response, error) {
	timeout := 1000 * time.Millisecond
	client := httpclient.NewClient(httpclient.WithHTTPTimeout(timeout))

	requestBody, err := json.Marshal(map[string]string{
		"client_id":     auth0.ClientID,
		"client_secret": auth0.ClientSecret,
		"audience":      auth0.Audience,
		"grant_type":    auth0.GrantType,
	})
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("POST", auth0.endpoint, bytes.NewBuffer(requestBody))
	if err != nil {
		return nil, err
	}
	req.Header.Add("Content-Type", "application/json")

	auth0.logger.Debugf("Calling auth0: %s", auth0.endpoint)
	res, err := client.Do(req)
	if err != nil {
		return nil, err
	}

	auth0Response := &auth0Response{}
	err = json.NewDecoder(res.Body).Decode(auth0Response)
	if err != nil {
		return nil, err
	}
	return auth0Response, nil
}
