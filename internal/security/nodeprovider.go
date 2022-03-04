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
	"encoding/json"
	"fmt"
	"github.com/golang-jwt/jwt"
	"go.uber.org/zap"
	"net/http"
	"net/url"
	"time"
)

// NodeJwtBearerProvider contains the auth0 configuration
type NodeJwtBearerProvider struct {
	serviceCore *ServiceCore
	endpoint    string
	audience    string
	logger      *zap.SugaredLogger
	cache       *cache
}

func NewNodeJwtBearerProvider(logger *zap.SugaredLogger, serviceCore *ServiceCore, conf ProviderConfig) *NodeJwtBearerProvider {
	provider := &NodeJwtBearerProvider{
		serviceCore: serviceCore,
		endpoint:    conf.Endpoint.Value,
		audience:    conf.Audience.Value,
		logger:      logger.Named("jwt"),
	}

	return provider
}

func (nodeTokenProvider *NodeJwtBearerProvider) Authorize(req *http.Request) {
	token, err := nodeTokenProvider.getToken()
	if err != nil {
		nodeTokenProvider.logger.Warn(err)
	}
	req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", token))
}

func (nodeTokenProvider *NodeJwtBearerProvider) getToken() (string, error) {
	now := time.Now()
	if nodeTokenProvider.cache == nil || now.After(nodeTokenProvider.cache.until) {
		token, err := nodeTokenProvider.callRemoteNodeEndpoint()
		if err != nil {
			return "", err
		}
		nodeTokenProvider.cache = &cache{
			until: time.Unix(0, token.Claims.(*CustomClaims).ExpiresAt),
			token: token.Raw,
		}
	}

	return nodeTokenProvider.cache.token, nil
}

func (nodeTokenProvider *NodeJwtBearerProvider) callRemoteNodeEndpoint() (*jwt.Token, error) {
	requestToken, err := nodeTokenProvider.serviceCore.CreateJWTForTokenRequest(nodeTokenProvider.audience)
	if err != nil {
		return nil, err
	}
	requestFormData := url.Values{}
	requestFormData.Set("grant_type", "client_credentials")
	requestFormData.Set("client_assertion_type", "urn:ietf:params:oauth:grant-type:jwt-bearer")
	requestFormData.Set("client_assertion", requestToken)

	requestUrl := nodeTokenProvider.endpoint
	res, err := http.PostForm(requestUrl, requestFormData)

	decoder := json.NewDecoder(res.Body)
	response := make(map[string]interface{})
	err = decoder.Decode(&response)
	rawToken := response["access_token"].(string)

	token, err := jwt.ParseWithClaims(rawToken, &CustomClaims{}, func(token *jwt.Token) (interface{}, error) {
		return nil, nil
	})

	return token, nil
}