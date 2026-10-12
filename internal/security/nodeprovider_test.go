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
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/golang-jwt/jwt/v4"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

// newRemoteNodeProvider returns a provider for a local node whose remote token endpoint
// returns remoteToken, and the observer for warnings the provider logs.
func newRemoteNodeProvider(t *testing.T, remoteToken string) (*NodeJwtBearerProvider, *observer.ObservedLogs) {
	localKey, _ := rsa.GenerateKey(rand.Reader, 2048)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]string{"access_token": remoteToken})
	}))
	t.Cleanup(srv.Close)

	core, logs := observer.New(zap.WarnLevel)
	return &NodeJwtBearerProvider{
		serviceCore: &ServiceCore{NodeInfo: NewNodeInfo("local", []*KeyPair{NewKeyPair(localKey, &localKey.PublicKey, true)})},
		endpoint:    srv.URL,
		audience:    "remote",
		logger:      zap.New(core).Sugar(),
	}, logs
}

func Test_node_token_signed_by_remote_key_logs_no_warning(t *testing.T) {
	remoteKey, _ := rsa.GenerateKey(rand.Reader, 2048)
	remoteToken, err := CreateJWTForTokenRequest("remote", "local", remoteKey)
	if err != nil {
		t.Fatal(err)
	}
	provider, logs := newRemoteNodeProvider(t, remoteToken)

	token, err := provider.getToken()
	if err != nil {
		t.Fatalf("should get token : %+v", err)
	}
	if token != remoteToken {
		t.Fatalf("token = %s, want %s", token, remoteToken)
	}
	if provider.cache.until.IsZero() {
		t.Fatal("should cache the token until its expiry")
	}
	if logs.Len() != 0 {
		t.Fatalf("should not log warnings : %+v", logs.All())
	}
}

func Test_node_token_without_exp_is_not_cached(t *testing.T) {
	remoteKey, _ := rsa.GenerateKey(rand.Reader, 2048)
	remoteToken, err := jwt.NewWithClaims(jwt.SigningMethodRS256, jwt.RegisteredClaims{Subject: "remote"}).SignedString(remoteKey)
	if err != nil {
		t.Fatal(err)
	}
	provider, _ := newRemoteNodeProvider(t, remoteToken)

	token, err := provider.getToken()
	if err != nil {
		t.Fatalf("should get token : %+v", err)
	}
	if token != remoteToken {
		t.Fatalf("token = %s, want %s", token, remoteToken)
	}
	if !provider.cache.until.IsZero() {
		t.Fatalf("should not cache a token without exp, cached until %v", provider.cache.until)
	}
}
