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
	"context"
	"crypto/rsa"
	"errors"
	"github.com/golang-jwt/jwt/v4"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/lestrrat-go/jwx/v2/jwk"
	"github.com/mimiro-io/datahub/internal/security"
	"net/http"
)

type (
	JwtConfig struct {
		// Skipper defines a function to skip middleware.
		Skipper middleware.Skipper

		// BeforeFunc defines a function which is executed just before the middleware.
		BeforeFunc middleware.BeforeFunc

		Cache     *jwk.Cache
		Wellknown string
		Audience  []string
		Issuer    []string

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
	ErrJWTInvalid = echo.NewHTTPError(http.StatusUnauthorized, "invalid or expired jwt")
)

func JWTHandler(config *JwtConfig) echo.MiddlewareFunc {
	if config.Cache == nil {
		c := jwk.NewCache(context.Background())
		if err := c.Register(config.Wellknown); err != nil {
			panic(err)
		}
		config.Cache = c
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

			token, err := config.ValidateToken(auth)

			if err == nil {
				c.Set("user", token)
				return next(c)
			}

			return echo.NewHTTPError(http.StatusUnauthorized, err.Error())
		}
	}
}

func (config *JwtConfig) ValidateToken(auth string) (*jwt.Token, error) {
	token, err := jwt.ParseWithClaims(auth, &security.CustomClaims{}, func(token *jwt.Token) (interface{}, error) {
		if config.NodePublicKey != nil {
			return config.NodePublicKey, nil
		} else {
			set, err := config.Cache.Get(context.Background(), config.Wellknown)
			if err != nil {
				return nil, errors.New("unable to load well-known from cache")
			}

			switch jwks := set.(type) {
			case jwk.Set:
				kid := token.Header["kid"].(string)
				k, ok := jwks.LookupKeyID(kid)
				if !ok {
					return nil, errors.New("kid not found in jwks")
				}

				// Check for x5c. If present, convert to RSA Public key.
				// If not present, create raw key.
				der, ok := k.X509CertChain().Get(0)
				if ok {
					pem := "-----BEGIN CERTIFICATE-----\n" + string(der) + "\n-----END CERTIFICATE-----"
					return jwt.ParseRSAPublicKeyFromPEM([]byte(pem))
				} else {
					var pk any
					err = k.Raw(&pk)
					return pk, err
				}
			default:
				return nil, errors.New("unknown type in well-known cache")
			}
		}
	})

	if err != nil {
		return nil, err
	}

	if !token.Valid {
		return token, ErrJWTInvalid
	}

	claims := token.Claims.(*security.CustomClaims)
	audience := config.Audience
	issuer := config.Issuer

	checkAud := false
	for _, aud := range audience {
		if claims.VerifyAudience(aud, false) {
			checkAud = true
		}
	}
	if !checkAud {
		err = errors.New("invalid audience")
	}

	checkIss := false
	for _, iss := range issuer {
		if claims.VerifyIssuer(iss, false) {
			checkIss = true
		}
	}
	if !checkIss {
		err = errors.New("invalid issuer")
	}

	checkSigningMethod := token.Method.Alg() == "RS256"
	if !checkSigningMethod {
		err = errors.New("non matching signing method")
	}

	return token, err
}

func extractToken(c echo.Context) (string, error) {
	auth := c.Request().Header.Get("Authorization")
	l := len("Bearer")
	if len(auth) > l+1 && auth[:l] == "Bearer" {
		return auth[l+1:], nil
	}
	return "", ErrJWTMissing
}
