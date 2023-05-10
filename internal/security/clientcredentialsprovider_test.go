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
	"context"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/zap"
	"golang.org/x/oauth2/clientcredentials"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestSecurity(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Security Suite")
}

var _ = Describe("DL Jwt configuration", func() {
	It("Should be correctly built from env properties", func() {
		provider := ProviderConfig{
			Name: "jwt",
			Type: "bearer",
			ClientID: &ValueReader{
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
			Endpoint: &ValueReader{
				Type:  "text",
				Value: "http://localhost",
			},
		}

		config := NewClientCredentialsProvider(zap.NewNop().Sugar(), provider, &ProviderManager{})
		Expect(config).ToNot(BeNil())
	})

	It("Should call a remote token endpoint", func() {
		srv := serverMock()

		cc := clientcredentials.Config{
			ClientID:     "123",
			ClientSecret: "456",
			TokenURL:     srv.URL + "/oauth2/token",
		}
		config := ClientCredentialsProvider{
			tokenSource: cc.TokenSource(context.Background()),
			logger:      zap.NewNop().Sugar(),
		}

		res, err := config.token()
		Expect(err).To(BeNil())
		Expect(res.AccessToken).To(Equal("hello-world"), "remote mock server should answer hello-world")

		srv.Close()
	})

})

func serverMock() *httptest.Server {
	handler := http.NewServeMux()
	handler.HandleFunc("/oauth2/token", responseMock)

	srv := httptest.NewServer(handler)

	return srv
}

func responseMock(w http.ResponseWriter, r *http.Request) {
	j := ` {
	  "access_token": "hello-world",
	  "scope": "datahub:r",
	  "expires_in": 86400,
	  "token_type": "bearer"
	} `
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(bytes.NewBufferString(j).Bytes())
}
