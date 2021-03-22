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
	"net/http"
	"strings"

	"github.com/dgrijalva/jwt-go"
	"github.com/juliangruber/go-intersect"
	"github.com/labstack/echo/v4"
	"go.uber.org/zap"
)

func JwtAuthorizer(logger *zap.SugaredLogger, scopes ...string) echo.MiddlewareFunc {

	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			if c.Get("user") == nil { // user never got set, oops
				return echo.NewHTTPError(http.StatusForbidden, "user not set")
			}
			token := c.Get("user").(*jwt.Token)

			claims := token.Claims.(*CustomClaims)
			if claims.Gty == "client-credentials" { // this is a machine or an application token
				var claimScopes []string
				if len(claims.scopes()) > 0 {
					claimScopes = strings.Split(claims.scopes()[0], " ")
				}
				res := intersect.Simple(claimScopes, scopes).([]interface{})
				if len(res) == 0 { // no intersection
					logger.Debugw("User attempted login with missing or wrong scope",
						"subject", token.Claims.(*CustomClaims).Subject,
						"scopes", claimScopes,
						"userScopes", scopes)
					return echo.NewHTTPError(http.StatusForbidden, "user attempted login with missing or wrong scope")

				}
			} else {
				// this is a user
				if !claims.Adm { // this will only be set for system admins, we only support mimiro Adm at the moment
					// if not, we need to see if the url requested contains the user id
					subject := claims.Subject
					// it needs the subject in the url
					uri := c.Request().RequestURI
					if strings.Index(uri, subject) == -1 { // not present, so forbidden
						return echo.NewHTTPError(http.StatusForbidden, "user has no access to path")
					}
				}
			}

			return next(c)
		}
	}
}

func NoOpAuthorizer(logger *zap.SugaredLogger, scopes ...string) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			return next(c)
		}
	}
}
