// Copyright 2022 MIMIRO AS
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
	"strings"

	"github.com/golang-jwt/jwt/v4"
)

type CustomClaims struct {
	Scope    string   `json:"scope"`
	Scp      []string `json:"scp"`
	Gty      string   `json:"gty"`
	Adm      bool     `json:"adm"`
	Roles    []string `json:"roles"`
	ClientID string   `json:"client_id"`
	jwt.RegisteredClaims
}

func (claims CustomClaims) Scopes() []string {
	if claims.Scope != "" {
		return strings.Split(claims.Scope, ",")
	}
	return claims.Scp
}
