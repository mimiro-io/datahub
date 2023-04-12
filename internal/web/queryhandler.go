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
	"github.com/mimiro-io/datahub/internal/jobs"
	"io"
	"net/http"
	"net/url"

	"github.com/labstack/echo/v4"
	"github.com/mimiro-io/datahub/internal/server"
	ent "github.com/mimiro-io/datahub/internal/service/entity"
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
	Details          bool     `json:"details"`
}

type NamespacePrefix struct {
	Prefix    string `json:"prefix"`
	Expansion string `json:"expansion"`
}

type EmptyEntity struct {
	Id string `json:"id"`
}

type queryHandler struct {
	store          *server.Store
	datasetManager *server.DsManager
	logger         *zap.SugaredLogger
}

func NewQueryHandler(lc fx.Lifecycle, e *echo.Echo, logger *zap.SugaredLogger, mw *Middleware, store *server.Store, datasetManager *server.DsManager) {
	log := logger.Named("web")
	handler := &queryHandler{
		store:          store,
		datasetManager: datasetManager,
		logger:         log,
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

type JavascriptQuery struct {
	Query string `json:"query"`
}

// Implements interface for query response writer
type HttpQueryResponseWriter struct {
	context      echo.Context
	writtenFirst bool
}

func NewHttpQueryResponseWriter(context echo.Context) *HttpQueryResponseWriter {
	return &HttpQueryResponseWriter{
		context:      context,
		writtenFirst: false,
	}
}

func (w *HttpQueryResponseWriter) WriteObject(object interface{}) error {
	if !w.writtenFirst {
		jsonObject, _ := json.Marshal(object)
		_, _ = w.context.Response().Write(jsonObject)
		w.writtenFirst = true
	} else {
		_, _ = w.context.Response().Write([]byte(","))
		jsonObject, _ := json.Marshal(object)
		_, _ = w.context.Response().Write(jsonObject)
	}

	return nil
}

func (handler *queryHandler) queryHandler(c echo.Context) error {

	// get content type
	contentType := c.Request().Header.Get("Content-Type")
	if contentType == "application/x-javascript-query" {
		query := &JavascriptQuery{}
		body, err := io.ReadAll(c.Request().Body)
		if err != nil {
			handler.logger.Warn("Unable to read body")
			return echo.NewHTTPError(http.StatusBadRequest, server.HttpBodyMissingErr(err).Error())
		}
		err = json.Unmarshal(body, &query)

		if err != nil {
			handler.logger.Warn("Unable to parse json")
			return echo.NewHTTPError(http.StatusBadRequest, server.HttpJsonParsingErr(err).Error())
		}

		jsQuery, err := jobs.NewJavascriptTransform(handler.logger, query.Query, handler.store, handler.datasetManager)
		if err != nil {
			handler.logger.Warn("Unable to parse javascript query " + err.Error())
			return echo.NewHTTPError(http.StatusBadRequest, err.Error())
		}

		c.Response().Header().Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
		c.Response().WriteHeader(http.StatusOK)
		c.Response().Write([]byte("["))

		writer := NewHttpQueryResponseWriter(c)
		err = jsQuery.ExecuteQuery(writer)
		if err != nil {
			handler.logger.Warn("Error executing javascript query " + err.Error())
			return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
		}

		c.Response().Write([]byte("]"))
		c.Response().Flush()

		return nil
	}

	query := &Query{}
	body, err := io.ReadAll(c.Request().Body)
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
		//a returned Entity can be the product of multiple entities in multiple datasets with the same ID
		//To get the correct namespace context, we'd have to use the supplied list of dataset names (query.Datasets)
		//and merge their respective contexts to our result context here.
		result[0] = handler.store.GetGlobalContext()

		if entity == nil {
			entity := &EmptyEntity{}
			entity.Id = query.EntityId
			result[1] = entity
		} else {
			if query.Details {
				l, _ := ent.NewLookup(server.NewBadgerAccess(handler.store, handler.datasetManager))
				details, _ := l.Details(query.EntityId, query.Datasets)
				entity.Properties["datahub_details"] = details
			}
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
		//To get the correct namespace context, we'd have to use the supplied list of dataset names (query.Datasets)
		//and merge their respective contexts to our result context here.
		result[0] = handler.store.GetGlobalContext()
		result[1] = queryresult

		// return result as JSON
		return c.JSON(http.StatusOK, result)
	}
}
