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
	"net/http"

	"github.com/labstack/echo/v4"
	"go.uber.org/zap"

	"github.com/mimiro-io/datahub/internal/server"
)

type namespaceHandler struct {
	store *server.Store
}

func RegisterNamespaceHandler(
	e *echo.Echo,
	logger *zap.SugaredLogger,
	mw *Middleware,
	store *server.Store,
) {
	handler := namespaceHandler{store: store}
	e.GET("/namespaces", handler.getNamespaces, mw.authorizer(logger.Named("web"), datahubRead))
}

func (handler *namespaceHandler) getNamespaces(c echo.Context) error {
	v := handler.store.GetGlobalContext(false)

	return c.JSON(http.StatusOK, v.Namespaces)
}
