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
	"net/http"

	"github.com/labstack/echo/v4"
	"go.uber.org/fx"
	"go.uber.org/zap"

	"github.com/mimiro-io/datahub/internal/server"
)

func NewTxnHandler(lc fx.Lifecycle, e *echo.Echo, logger *zap.SugaredLogger, mw *Middleware, store *server.Store) {
	log := logger.Named("web")
	handler := &txnHandler{
		store:  store,
		logger: log,
	}

	lc.Append(fx.Hook{
		OnStart: func(_ context.Context) error {
			e.POST("/transactions", handler.processTransaction, mw.authorizer(log, datahubWrite))
			return nil
		},
	})
}

type txnHandler struct {
	store  *server.Store
	logger *zap.SugaredLogger
}

func (txnHandler *txnHandler) processTransaction(c echo.Context) error {
	// parse transaction
	esp := server.NewEntityStreamParser(txnHandler.store)
	txn, err := esp.ParseTransaction(c.Request().Body)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, server.AttemptStoreEntitiesErr(err).Error())
	}

	// execute transaction
	err = txnHandler.store.ExecuteTransaction(txn)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, server.AttemptStoreEntitiesErr(err).Error())
	}

	return c.NoContent(http.StatusOK)
}
