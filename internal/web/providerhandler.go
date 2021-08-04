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

package web

import (
	"context"
	"github.com/labstack/echo/v4"
	"github.com/mimiro-io/datahub/internal/security"
	"github.com/mimiro-io/datahub/internal/server"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"net/http"
	"net/url"
)

type providerHandler struct {
	log      *zap.SugaredLogger
	provider *security.ProviderManager
}

func NewProviderHandler(lc fx.Lifecycle, e *echo.Echo, log *zap.SugaredLogger, mw *Middleware, provider *security.ProviderManager) {
	handler := &providerHandler{
		log:      log.Named("web"),
		provider: provider,
	}

	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			e.POST("/provider/logins", handler.loginCreate, mw.authorizer(log, datahubWrite))
			e.GET("/provider/logins", handler.loginList, mw.authorizer(log, datahubRead))
			e.POST("/provider/login/:providerName", handler.loginUpdate, mw.authorizer(log, datahubWrite))
			e.GET("/provider/login/:providerName", handler.loginGet, mw.authorizer(log, datahubRead))
			e.DELETE("/provider/login/:providerName", handler.loginDelete, mw.authorizer(log, datahubRead))
			return nil
		},
	})
}

func (handler *providerHandler) loginCreate(c echo.Context) error {
	var provider security.ProviderConfig
	if err := c.Bind(&provider); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, server.HttpGenericErr(err).Error())
	}

	if err := handler.provider.AddProvider(provider); err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, server.HttpGenericErr(err).Error())
	}

	return c.NoContent(http.StatusOK)
}

func (handler *providerHandler) loginUpdate(c echo.Context) error {
	providerName, err := url.QueryUnescape(c.Param("providerName"))
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, server.HttpGenericErr(err).Error())
	}
	var provider security.ProviderConfig
	if err := c.Bind(&provider); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, server.HttpGenericErr(err).Error())
	}

	if err := handler.provider.UpdateProvider(providerName, provider); err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, server.HttpGenericErr(err).Error())
	}

	return c.NoContent(http.StatusOK)
}

func (handler *providerHandler) loginList(c echo.Context) error {
	if providers, err := handler.provider.ListProviders(); err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, server.HttpGenericErr(err).Error())
	} else {
		return c.JSON(http.StatusOK, providers)
	}
}

func (handler *providerHandler) loginGet(c echo.Context) error {
	providerName, err := url.QueryUnescape(c.Param("providerName"))
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, server.HttpGenericErr(err).Error())
	}
	if provider, err := handler.provider.FindByName(providerName); err != nil {
		return c.NoContent(http.StatusNotFound)
	} else {
		return c.JSON(http.StatusOK, provider)
	}
}

func (handler *providerHandler) loginDelete(c echo.Context) error {
	providerName, err := url.QueryUnescape(c.Param("providerName"))
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, server.HttpGenericErr(err).Error())
	}
	if err := handler.provider.DeleteProvider(providerName); err != nil {
		return c.NoContent(http.StatusNotFound)
	} else {
		return c.NoContent(http.StatusOK)
	}
}
