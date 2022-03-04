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

package middlewares

import (
	"crypto/rsa"
	"encoding/json"
	"errors"
	"github.com/mimiro-io/datahub/internal/security"
	"net/http"
	"time"

	"github.com/goburrow/cache"
	"github.com/golang-jwt/jwt"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
)

type (
	JwtConfig struct {
		// Skipper defines a function to skip middleware.
		Skipper middleware.Skipper

		// BeforeFunc defines a function which is executed just before the middleware.
		BeforeFunc middleware.BeforeFunc

		Cache     cache.LoadingCache
		Wellknown string
		Audience  string
		Issuer    string

		// This is set if Node security is enabled
		NodePublicKey *rsa.PublicKey
	}
)

/* type CustomClaims struct {
	Scope string `json:"scope"`
	Gty   string `json:"gty"`
	Adm   bool   `json:"adm"`
	Roles []string `json:"roles"`
	jwt.StandardClaims
} */

type Response struct {
	Message string `json:"message"`
}

type Jwks struct {
	Keys []JSONWebKeys `json:"keys"`
}

type JSONWebKeys struct {
	Kty string   `json:"kty"`
	Kid string   `json:"kid"`
	Use string   `json:"use"`
	N   string   `json:"n"`
	E   string   `json:"e"`
	X5c []string `json:"x5c"`
}

// Errors
var (
	ErrJWTMissing = echo.NewHTTPError(http.StatusBadRequest, "missing or malformed jwt")
	parser        = jwt.Parser{
		ValidMethods: []string{"RS256"},
	}
)

func newCache(wellknown string) cache.LoadingCache {
	load := func(k cache.Key) (cache.Value, error) {
		resp, err := http.Get(wellknown)
		if err != nil {
			return nil, err
		}

		defer resp.Body.Close()

		var jwks = Jwks{}
		err = json.NewDecoder(resp.Body).Decode(&jwks)

		return jwks, err
	}

	lc := cache.NewLoadingCache(load,
		cache.WithMaximumSize(10),
		cache.WithExpireAfterAccess(10*time.Second),
		cache.WithRefreshAfterWrite(60*time.Second),
	)
	return lc
}

func JWTHandler(config *JwtConfig) echo.MiddlewareFunc {
	if config.Cache == nil {
		config.Cache = newCache(config.Wellknown)
	}

	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			if config.Skipper(c) {
				return next(c)
			}

			if config.BeforeFunc != nil {
				config.BeforeFunc(c)
			}

			auth, err := extractToken(c)
			if err != nil {
				return echo.NewHTTPError(http.StatusUnauthorized, err.Error())
			}

			token, err := parser.ParseWithClaims(auth, &security.CustomClaims{}, func(token *jwt.Token) (interface{}, error) {
				if config.NodePublicKey != nil {
					return config.NodePublicKey, nil
				} else {
					cert, err := getPemCert(token, config)
					if err != nil {
						return nil, err
					}
					result, err := jwt.ParseRSAPublicKeyFromPEM([]byte(cert))
					if err != nil {
						return nil, err
					}
					return result, nil
				}
			})

			if !token.Valid {
				return echo.NewHTTPError(http.StatusUnauthorized, err.Error())
			}

			if err != nil {
				return echo.NewHTTPError(http.StatusUnauthorized, err.Error())
			}

			// we need to handle compatibility between our and auth0 tokens
			var (
				audience string
				issuer   string
			)
			claims := token.Claims.(*security.CustomClaims)
			audience = config.Audience
			issuer = config.Issuer

			// verify the audience is correct, audience must be set
			checkAud := claims.VerifyAudience(audience, true)
			if !checkAud {
				err = errors.New("invalid audience")
			}

			// verify the issuer of the token, issuer must be set
			checkIss := claims.VerifyIssuer(issuer, true)
			if !checkIss {
				err = errors.New("invalid issuer")
			}

			if err == nil {
				c.Set("user", token)
				return next(c)
			}

			return echo.NewHTTPError(http.StatusUnauthorized, err.Error())
		}
	}
}

func (config *JwtConfig) SigningKey() func(token *jwt.Token) string {
	return func(token *jwt.Token) string {
		cert, err := getPemCert(token, config)
		if err != nil {
			return ""
		}
		result, _ := jwt.ParseRSAPublicKeyFromPEM([]byte(cert))
		return result.N.String()
	}
}

func getPemCert(token *jwt.Token, config *JwtConfig) (string, error) {
	cert := ""
	result, err := config.Cache.Get("well-known")

	if err != nil {
		return cert, err
	}

	switch jwks := result.(type) {
	case Jwks:
		for k := range jwks.Keys {
			if token.Header["kid"] == jwks.Keys[k].Kid {
				cert = "-----BEGIN CERTIFICATE-----\n" + jwks.Keys[k].X5c[0] + "\n-----END CERTIFICATE-----"
			}
		}

		if cert == "" {
			err := errors.New("unable to find appropriate key")
			return cert, err
		}

		return cert, nil
	default:
		err := errors.New("no Jwks returned")
		return cert, err
	}

}

func extractToken(c echo.Context) (string, error) {
	auth := c.Request().Header.Get("Authorization")
	l := len("Bearer")
	if len(auth) > l+1 && auth[:l] == "Bearer" {
		return auth[l+1:], nil
	}
	return "", ErrJWTMissing
}
