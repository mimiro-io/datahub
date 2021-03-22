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
	"io/ioutil"
	"net/http"

	"github.com/labstack/echo/v4"
	"github.com/mimiro-io/datahub/internal/content"
	"github.com/mimiro-io/datahub/internal/server"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

type contentHandler struct {
	content *content.Config
}

func NewContentHandler(lc fx.Lifecycle, e *echo.Echo, logger *zap.SugaredLogger, mw *Middleware, content *content.Config) {
	log := logger.Named("web")
	handler := &contentHandler{
		content: content,
	}

	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			// datas
			e.GET("/content", handler.contentList, mw.authorizer(log, datahubRead))
			e.POST("/content", handler.contentAdd, mw.authorizer(log, datahubWrite))
			e.GET("/content/:contentId", handler.contentShow, mw.authorizer(log, datahubRead))
			e.PUT("/content/:contentId", handler.contentUpdate, mw.authorizer(log, datahubWrite))
			e.DELETE("/content/:contentId", handler.contentDelete, mw.authorizer(log, datahubWrite))

			return nil
		},
	})
}

func (handler *contentHandler) contentList(c echo.Context) error {
	res, err := handler.content.ListContents()
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, server.HttpGenericErr.Error())
	}

	return c.JSON(http.StatusOK, res)
}

func (handler *contentHandler) contentAdd(c echo.Context) error {
	// read json
	body, err := ioutil.ReadAll(c.Request().Body)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, server.HttpBodyMissingErr.Error())
	}

	content := &content.Content{}
	err = json.Unmarshal(body, content)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, server.HttpJsonParsingErr.Error())
	}

	err = handler.content.AddContent(content.Id, content)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, server.HttpContentStoreErr.Error())
	}

	return c.NoContent(http.StatusOK)
}

func (handler *contentHandler) contentShow(c echo.Context) error {
	contentId := c.Param("contentId")
	res, err := handler.content.GetContentById(contentId)
	if err != nil {
		return c.NoContent(http.StatusNotFound)
	}
	if res == nil { // no object found
		return c.NoContent(http.StatusNotFound)
	}

	return c.JSON(http.StatusOK, res)
}

func (handler *contentHandler) contentUpdate(c echo.Context) error {
	body, err := ioutil.ReadAll(c.Request().Body)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, server.HttpBodyMissingErr.Error())
	}
	contentId := c.Param("contentId")
	payload := &content.Content{}
	err = json.Unmarshal(body, payload)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, server.HttpJsonParsingErr.Error())
	}

	err = handler.content.UpdateContent(contentId, payload)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, server.HttpContentStoreErr.Error())
	}

	return c.NoContent(http.StatusOK)
}

func (handler *contentHandler) contentDelete(c echo.Context) error {
	contentId := c.Param("contentId")
	_, err := handler.content.GetContentById(contentId)
	if err != nil {
		return c.NoContent(http.StatusNotFound)
	}

	err = handler.content.DeleteContent(contentId)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, server.HttpContentStoreErr.Error())
	}

	return c.NoContent(http.StatusOK)
}
