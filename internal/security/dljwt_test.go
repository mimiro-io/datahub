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
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/franela/goblin"
	"go.uber.org/zap"
)

func TestDlJwt(t *testing.T) {
	g := goblin.Goblin(t)
	g.Describe("DL Jwt configuration", func() {
		g.It("Should be correctly built from env properties", func() {
			provider := ProviderConfig{
				Name: "jwt",
				Type: "bearer",
				ClientId: &ValueReader{
					Type:  "text",
					Value: "id1",
				},
				ClientSecret: &ValueReader{
					Type:  "text",
					Value: "some-secret",
				},
				Audience: &ValueReader{
					Type:  "text",
					Value: "mimiro",
				},
				GrantType: &ValueReader{
					Type:  "text",
					Value: "test_grant",
				},
				Endpoint: &ValueReader{
					Type:  "text",
					Value: "http://localhost",
				},
			}

			config := NewDlJwtConfig(zap.NewNop().Sugar(), provider, &ProviderManager{})
			g.Assert(config.ClientId).Eql("id1")
			g.Assert(config.ClientSecret).Eql("some-secret")
		})

		g.It("Should call a remote token endpoint", func() {
			srv := serverMock()

			config := JwtBearerProvider{
				ClientId:     "123",
				ClientSecret: "456",
				Audience:     "",
				GrantType:    "",
				endpoint:     srv.URL + "/oauth/token",
				logger:       zap.NewNop().Sugar(),
			}

			res, err := config.callRemote()
			g.Assert(err).IsNil()
			g.Assert(res.AccessToken).Eql("hello-world", "remote mock server should answer hello-world")

			srv.Close()
		})

		g.It("Should use token cache if configured", func() {
			srv := serverMock()

			config := JwtBearerProvider{
				ClientId:     "123",
				ClientSecret: "456",
				Audience:     "",
				GrantType:    "",
				endpoint:     srv.URL + "/oauth/token",
				logger:       zap.NewNop().Sugar(),
			}

			// cache is nil, so it should generate a fresh one
			res, err := config.generateOrGetToken()
			g.Assert(err).IsNil()
			g.Assert(res).Eql("hello-world", "remote mock server should generate hello-world")

			// cache is set and time is in the future, so it should return the cached version
			config = JwtBearerProvider{
				ClientId:     "123",
				ClientSecret: "456",
				Audience:     "",
				GrantType:    "",
				endpoint:     srv.URL + "/oauth/token",
				logger:       zap.NewNop().Sugar(),
				cache: &cache{
					until: time.Now().Add(time.Duration(1000) * time.Second),
					token: "cached token",
				},
			}

			res, err = config.generateOrGetToken()
			g.Assert(err).IsNil()
			g.Assert(res).Eql("cached token", "cache should be used")

			// cache is set but time is in the past, so it should return a new token
			config = JwtBearerProvider{
				ClientId:     "123",
				ClientSecret: "456",
				Audience:     "",
				GrantType:    "",
				endpoint:     srv.URL + "/oauth/token",
				logger:       zap.NewNop().Sugar(),
				cache: &cache{
					until: time.Now().Add(time.Duration(-1000) * time.Second),
					token: "cached token",
				},
			}
			res, err = config.generateOrGetToken()
			g.Assert(err).IsNil()
			g.Assert(res).Eql("hello-world", "cache is stale, new remote token should be fetched")

			srv.Close()
		})
	})
}

func serverMock() *httptest.Server {
	handler := http.NewServeMux()
	handler.HandleFunc("/oauth/token", responseMock)

	srv := httptest.NewServer(handler)

	return srv
}

func responseMock(w http.ResponseWriter, r *http.Request) {
	j := ` {
	  "access_token": "hello-world",
	  "scope": "datahub:r",
	  "expires_in": 86400,
	  "token_type": "Bearer"
	} `
	_, _ = w.Write(bytes.NewBufferString(j).Bytes())
}
