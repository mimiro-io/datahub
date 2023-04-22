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

package web

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/url"

	"github.com/labstack/echo/v4"
	"go.uber.org/fx"
	"go.uber.org/zap"

	"github.com/mimiro-io/datahub/internal/security"
)

func NewSecurityHandler(
	lc fx.Lifecycle,
	e *echo.Echo,
	logger *zap.SugaredLogger,
	mw *Middleware,
	core *security.ServiceCore,
) {
	log := logger.Named("web")
	handler := &SecurityHandler{}
	handler.serviceCore = core
	handler.logger = log

	lc.Append(fx.Hook{
		OnStart: func(_ context.Context) error {
			// query
			e.POST("/security/token", handler.tokenRequestHandler)
			e.POST("/security/clients", handler.clientRegistrationRequestHandler, mw.authorizer(log, datahubWrite))
			e.GET("/security/clients", handler.getClientsRequestHandler, mw.authorizer(log, datahubWrite))

			e.POST(
				"/security/clients/:clientid/acl",
				handler.setClientAccessControlsRequestHandler,
				mw.authorizer(log, datahubWrite),
			)
			e.GET(
				"/security/clients/:clientid/acl",
				handler.getClientAccessControlsRequestHandler,
				mw.authorizer(log, datahubWrite),
			)
			e.DELETE(
				"/security/clients/:clientid/acl",
				handler.deleteClientAccessControlsRequestHandler,
				mw.authorizer(log, datahubWrite),
			)

			/* jwtMiddleware := MakeJWTMiddleware(core.GetActiveKeyPair().PublicKey, "node:" + core.NodeInfo.NodeId, "node:" + core.NodeInfo.NodeId)

			e.POST("/security/clients", handler.registerClientRequestHandler, jwtMiddleware, MakeRoleCheckMiddleware("admin"))
			e.POST("/security/clientclaims", handler.setClientClaimsRequestHandler, jwtMiddleware, MakeRoleCheckMiddleware("admin"))
			e.POST("/security/clientacl", handler.setClientClaimsRequestHandler, jwtMiddleware, MakeRoleCheckMiddleware("admin"))
			*/

			return nil
		},
	})
}

type SecurityHandler struct {
	serviceCore *security.ServiceCore
	logger      *zap.SugaredLogger
}

// TokenResponse is a OAuth2 JWT Token Response
type TokenResponse struct {
	AccessToken string `json:"access_token"`
	TokenType   string `json:"token_type"`
}

func (handler *SecurityHandler) getClientsRequestHandler(c echo.Context) error {
	return c.JSON(http.StatusOK, handler.serviceCore.GetClients())
}

// oidc - oauth2 compliant token request handler
func (handler *SecurityHandler) tokenRequestHandler(c echo.Context) error {
	// url form encoded params
	grantType := c.FormValue("grant_type")
	clientAssertionType := c.FormValue("client_assertion_type")

	if grantType == "client_credentials" && clientAssertionType == "urn:ietf:params:oauth:grant-type:jwt-bearer" {
		assertion := c.FormValue("client_assertion")
		token, err := handler.serviceCore.ValidateClientJWTMakeJWTAccessToken(assertion)
		if err != nil {
			return echo.NewHTTPError(http.StatusUnauthorized, "invalid credentials")
		}
		response := &TokenResponse{}
		response.AccessToken = token
		response.TokenType = "Bearer"
		return c.JSON(http.StatusOK, response)

	} else if grantType == "client_credentials" {
		clientID := c.FormValue("client_id")
		clientSecret := c.FormValue("client_secret")

		token, err := handler.serviceCore.MakeAdminJWT(clientID, clientSecret)
		if err != nil {
			return echo.NewHTTPError(http.StatusUnauthorized, "invalid credentials")
		}
		response := &TokenResponse{}
		response.AccessToken = token
		response.TokenType = "Bearer"
		return c.JSON(http.StatusOK, response)
	}

	return nil
}

func (handler *SecurityHandler) clientRegistrationRequestHandler(c echo.Context) error {
	body, err := ioutil.ReadAll(c.Request().Body)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "missing body")
	}

	clientInfo := &security.ClientInfo{}
	err = json.Unmarshal(body, clientInfo)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "missing body")
	}
	handler.serviceCore.RegisterClient(clientInfo)

	return c.NoContent(http.StatusOK)
}

func (handler *SecurityHandler) getClientAccessControlsRequestHandler(c echo.Context) error {
	clientID, err := url.QueryUnescape(c.Param("clientid"))
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "missing client id")
	}
	return c.JSON(http.StatusOK, handler.serviceCore.GetAccessControls(clientID))
}

func (handler *SecurityHandler) deleteClientAccessControlsRequestHandler(c echo.Context) error {
	clientID, err := url.QueryUnescape(c.Param("clientid"))
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "missing client id")
	}
	handler.serviceCore.DeleteClientAccessControls(clientID)
	return c.NoContent(http.StatusOK)
}

func (handler *SecurityHandler) setClientAccessControlsRequestHandler(c echo.Context) error {
	body, err := ioutil.ReadAll(c.Request().Body)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "missing body")
	}

	clientID, err := url.QueryUnescape(c.Param("clientid"))
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "missing client id")
	}

	var acls []*security.AccessControl
	err = json.Unmarshal(body, &acls)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "missing body")
	}
	handler.serviceCore.SetClientAccessControls(clientID, acls)

	return c.NoContent(http.StatusOK)
}
