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
	"encoding/json"
	"errors"
	"io/ioutil"
	"net/http"
	"net/url"

	"github.com/labstack/echo/v4"
	"github.com/mimiro-io/datahub/internal/server"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

type Filter struct {
	property string
	operator string
	value    interface{}
}

type Hop struct {
	label   string
	refType string
	inverse bool
	filters []Filter
	Select  []string
}

type Query struct {
	EntityId         string   `json:"entityId"`
	StartingEntities []string `json:"startingEntities"`
	Predicate        string   `json:"predicate"`
	Inverse          bool     `json:"inverse"`
	Datasets         []string `json:"datasets"`
}

type NamespacePrefix struct {
	Prefix    string `json:"prefix"`
	Expansion string `json:"expansion"`
}

type EmptyEntity struct {
	Id string `json:"id"`
}

type queryHandler struct {
	store  *server.Store
	logger *zap.SugaredLogger
}

func NewQueryHandler(lc fx.Lifecycle, e *echo.Echo, logger *zap.SugaredLogger, mw *Middleware, store *server.Store) {
	log := logger.Named("web")
	handler := &queryHandler{
		store:  store,
		logger: log,
	}

	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			// query
			e.GET("/query", handler.queryHandler, mw.authorizer(log, datahubRead))
			e.POST("/query", handler.queryHandler, mw.authorizer(log, datahubRead))
			e.GET("/query/namespace", handler.queryNamespacePrefix, mw.authorizer(log, datahubRead))
			return nil
		},
	})

}

func (handler *queryHandler) queryNamespacePrefix(c echo.Context) error {
	urlExpansion, err := url.QueryUnescape(c.QueryParam("expansion"))
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, errors.New("problem mapping namespace prefix"))
	}

	prefix, err := handler.store.NamespaceManager.GetPrefixMappingForExpansion(urlExpansion)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, errors.New("problem mapping namespace prefix"))
	}
	if prefix == "" {
		return c.NoContent(http.StatusNotFound)
	}

	return c.JSON(http.StatusOK, &NamespacePrefix{Prefix: prefix, Expansion: urlExpansion})

}

func (handler *queryHandler) queryHandler(c echo.Context) error {
	query := &Query{}
	body, err := ioutil.ReadAll(c.Request().Body)
	if err != nil {
		handler.logger.Warn("Unable to read body")
		return echo.NewHTTPError(http.StatusBadRequest, server.HttpBodyMissingErr(err).Error())
	}
	err = json.Unmarshal(body, &query)
	if err != nil {
		handler.logger.Warn("Unable to parse json")
		return echo.NewHTTPError(http.StatusBadRequest, server.HttpJsonParsingErr(err).Error())
	}

	if query.EntityId != "" {
		entity, err := handler.store.GetEntity(query.EntityId, query.Datasets)

		if err != nil {
			return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
		}

		result := make([]interface{}, 2)
		//TODO https://mimiro.atlassian.net/browse/MIM-670
		//a returned Entity can be the product of multiple entities in multiple datasets with the same ID
		//To get the correct namespace context, we'd have to use the supplied list of dataset names (query.Datasets)
		//and merge their respective contexts to our result context here.
		result[0] = handler.store.GetGlobalContext()

		if entity == nil {
			entity := &EmptyEntity{}
			entity.Id = query.EntityId
			result[1] = entity
		} else {
			result[1] = entity
		}

		// return result as JSON
		return c.JSON(http.StatusOK, result)
	} else {
		// do query
		queryresult, err := handler.store.GetManyRelatedEntities(query.StartingEntities, query.Predicate, query.Inverse, query.Datasets)
		if err != nil {
			return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
		}

		result := make([]interface{}, 2)
		//TODO https://mimiro.atlassian.net/browse/MIM-670
		//To get the correct namespace context, we'd have to use the supplied list of dataset names (query.Datasets)
		//and merge their respective contexts to our result context here.
		result[0] = handler.store.GetGlobalContext()
		result[1] = queryresult

		// return result as JSON
		return c.JSON(http.StatusOK, result)
	}
}
