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
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strconv"

	"go.uber.org/fx"
	"go.uber.org/zap"

	"github.com/labstack/echo/v4"
	"github.com/mimiro-io/datahub/internal/server"
)

type datasetHandler struct {
	datasetManager *server.DsManager
	store          *server.Store
	eventBus       server.EventBus
}

func NewDatasetHandler(lc fx.Lifecycle, e *echo.Echo, logger *zap.SugaredLogger, mw *Middleware, dm *server.DsManager, store *server.Store, eb server.EventBus) {
	log := logger.Named("web")
	handler := &datasetHandler{
		datasetManager: dm,
		store:          store,
		eventBus:       eb,
	}

	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			e.GET("/datasets", handler.datasetList, mw.authorizer(log, datahubRead))
			e.GET("/datasets/:dataset/entities", handler.getEntitiesHandler, mw.authorizer(log, datahubRead))
			e.GET("/datasets/:dataset/changes", handler.getChangesHandler, mw.authorizer(log, datahubRead))
			e.POST("/datasets/:dataset/entities", handler.storeEntitiesHandler, mw.authorizer(log, datahubWrite))

			e.GET("/datasets/:dataset", handler.datasetGet, mw.authorizer(log, datahubRead))
			e.POST("/datasets/:dataset", handler.datasetCreate, mw.authorizer(log, datahubWrite))
			e.DELETE("/datasets/:dataset", handler.deleteDatasetHandler, mw.authorizer(log, datahubWrite))
			e.DELETE("/datasets", handler.deleteAllDatasets, mw.authorizer(log, datahubWrite))
			return nil
		},
	})

}

// datasetList
func (handler *datasetHandler) datasetList(c echo.Context) error {
	accessible := c.Get("datasets")

	datasets := make([]server.DatasetName, 0)

	if accessible == nil { // it's not set, so allow all
		datasets = handler.datasetManager.GetDatasetNames()
	} else {
		whitelist := accessible.([]string)
		if len(whitelist) > 0 { // an empty list here doesn't need to do anything
			if whitelist[0] == "*" { // this is a catch all, the user has access to all datasets
				datasets = handler.datasetManager.GetDatasetNames()
			} else { // we need to do some filtering
				datasets = whitelistDatasets(handler.datasetManager.GetDatasetNames(), whitelist)
			}
		}
	}

	return c.JSON(http.StatusOK, datasets)
}

func whitelistDatasets(datasets []server.DatasetName, whitelist []string) []server.DatasetName {
	whitelisted := make([]server.DatasetName, 0)
	for _, item := range datasets {
		for _, w := range whitelist {
			if item.Name == w {
				whitelisted = append(whitelisted, item)
			}
		}
	}

	return whitelisted
}

// datasetCreate
func (handler *datasetHandler) datasetCreate(c echo.Context) error {
	datasetName := c.Param("dataset")

	_, err := handler.datasetManager.CreateDataset(datasetName)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed creating dataset")
	}

	// TODO: remove the part under here
	// this is done to potentially update "old" datasets
	core := handler.datasetManager.GetDataset("core.Dataset")
	entity := handler.datasetManager.GetDatasetEntity(datasetName)
	entities := []*server.Entity{
		entity,
	}
	err = core.StoreEntities(entities)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed creating dataset")
	}

	return c.NoContent(http.StatusOK)
}

func (handler *datasetHandler) datasetGet(c echo.Context) error {
	datasetName := c.Param("dataset")

	entity, ok, err := handler.datasetManager.GetDatasetDetails(datasetName)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed getting dataset")
	}
	if !ok {
		return c.NoContent(http.StatusNotFound)
	}

	return c.JSON(http.StatusOK, entity)
}

// deleteDatasetHandler
func (handler *datasetHandler) deleteDatasetHandler(c echo.Context) error {
	datasetName := c.Param("dataset")
	err := handler.datasetManager.DeleteDataset(datasetName)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, err.Error())
	}

	return c.NoContent(http.StatusOK)
}

func (handler *datasetHandler) deleteAllDatasets(c echo.Context) error {
	err := handler.store.Delete()
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, errors.New("boink").Error())
	}

	return c.NoContent(http.StatusOK)
}

// getEntitiesHandler
// path param dataset
// query param continuationToken
func (handler *datasetHandler) getEntitiesHandler(c echo.Context) error {
	datasetName, err := url.QueryUnescape(c.Param("dataset"))
	if err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	var (
		l    int
		from []byte
	)
	limit := c.QueryParam("limit")
	if limit != "" {
		f, err := strconv.ParseInt(limit, 10, 64)
		if err != nil {
			return echo.NewHTTPError(http.StatusBadRequest, server.HttpQueryParamErr.Error())
		}
		l = int(f)
	}

	f := c.QueryParam("from")
	if f != "" {
		from, err = base64.StdEncoding.DecodeString(f)
		if err != nil {
			return echo.NewHTTPError(http.StatusBadRequest, server.SinceParseErr.Error())
		}
	}

	// check dataset exists
	dataset := handler.datasetManager.GetDataset(datasetName)
	if dataset == nil {
		return c.NoContent(http.StatusNotFound)
	}

	c.Response().Header().Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
	c.Response().WriteHeader(http.StatusOK)

	_, err = c.Response().Write([]byte("["))
	if err != nil {
		return err
	}

	// write context
	jsonContext, _ := json.Marshal(dataset.GetContext())
	_, err = c.Response().Write(jsonContext)
	if err != nil {
		return err
	}

	continuationToken, err := dataset.MapEntitiesRaw(string(from), l, func(jsonData []byte) error {
		_, err := c.Response().Write([]byte(","))
		if err != nil {
			return err
		}
		_, err = c.Response().Write(jsonData)
		return err
	})

	// write the continuation token and end the array of entities
	_, _ = c.Response().Write([]byte(", {\"id\":\"@continuation\",\"token\":\"" + continuationToken + "\"}]"))
	c.Response().Flush()

	return nil
}

func (handler *datasetHandler) getChangesHandler(c echo.Context) error {
	datasetName := c.Param("dataset")
	limit := c.QueryParam("limit")
	since := c.QueryParam("since")
	var (
		l int
	)
	if limit != "" {
		f, err := strconv.ParseInt(limit, 10, 64)
		if err != nil {
			return echo.NewHTTPError(http.StatusBadRequest, server.HttpQueryParamErr.Error())
		}
		l = int(f)
	}

	sinceNum, err := decodeSince(since)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, server.SinceParseErr.Error())
	}

	// check dataset exists
	dataset := handler.datasetManager.GetDataset(datasetName)
	if dataset == nil {
		return c.NoContent(http.StatusNotFound)
	}

	c.Response().Header().Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
	c.Response().WriteHeader(http.StatusOK)

	_, _ = c.Response().Write([]byte("["))

	// write context
	jsonContext, _ := json.Marshal(dataset.GetContext())
	_, _ = c.Response().Write(jsonContext)

	continuationToken, err := dataset.ProcessChangesRaw(sinceNum, l, func(jsonData []byte) error {
		_, _ = c.Response().Write([]byte(","))
		_, _ = c.Response().Write(jsonData)
		return nil
	})

	// write the continuation token and end the array of entities
	_, _ = c.Response().Write([]byte(", {\"id\":\"@continuation\",\"token\":\"" + encodeSince(continuationToken) + "\"}]"))
	c.Response().Flush()

	return nil
}

// storeEntitiesHandler
func (handler *datasetHandler) storeEntitiesHandler(c echo.Context) error {
	datasetName := c.Param("dataset")
	fsID := c.Request().Header.Get("universal-data-api-full-sync-id")
	fsStart := c.Request().Header.Get("universal-data-api-full-sync-start")
	fsEnd := c.Request().Header.Get("universal-data-api-full-sync-end")

	return handler.processEntities(c, datasetName, fsStart == "true", fsID, "true" == fsEnd)
}

func (handler *datasetHandler) processEntities(c echo.Context, datasetName string, fullSyncStart bool, fullSyncID string, fullSyncEnd bool) error {
	var err error
	// check dataset exists
	ok := handler.datasetManager.IsDataset(datasetName)
	if !ok {
		return echo.NewHTTPError(http.StatusInternalServerError, errors.New("dataset does not exists").Error())
	}

	dataset := handler.datasetManager.GetDataset(datasetName)

	// start new fullsync if requested
	if fullSyncStart {
		err := dataset.StartFullSyncWithLease(fullSyncID)
		if err != nil {
			return echo.NewHTTPError(http.StatusConflict, server.HttpFullsyncRunning.Error())
		}
	} else if dataset.FullSyncStarted() {
		err = dataset.RefreshFullSyncLease(fullSyncID)
		if err != nil {
			return echo.NewHTTPError(http.StatusConflict, server.HttpFullsyncRunning.Error())
		}
	}

	batchSize := 10
	entities := make([]*server.Entity, 0)
	esp := server.NewEntityStreamParser(handler.store)
	count := 0
	// this should be returning an error
	err = esp.ParseStream(c.Request().Body, func(e *server.Entity) error {
		entities = append(entities, e)
		count++
		if count == batchSize {
			err := dataset.StoreEntities(entities)
			if err != nil {
				return echo.NewHTTPError(http.StatusInternalServerError, server.AttemptStoreEntitiesErr.Error())
			}
			count = 0
			entities = make([]*server.Entity, 0)
		}
		return nil
	})

	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, server.AttemptStoreEntitiesErr.Error())
	}

	if count > 0 {
		err := dataset.StoreEntities(entities)
		if err != nil {
			return echo.NewHTTPError(http.StatusInternalServerError, server.AttemptStoreEntitiesErr.Error())
		}
	}

	if fullSyncEnd {
		if err := dataset.ReleaseFullSyncLease(fullSyncID);err != nil {
			return echo.NewHTTPError(http.StatusGone, server.HttpGenericErr.Error())
		}
		if err := dataset.CompleteFullSync(); err != nil {
			return echo.NewHTTPError(http.StatusInternalServerError, server.HttpGenericErr.Error())
		}
	}
	// we have to emit the dataset, so that subscribers can react to the event
	ctx := context.Background()
	handler.eventBus.Emit(ctx, "dataset."+datasetName, nil)
	handler.eventBus.Emit(ctx, "dataset.core.Dataset", nil)

	return c.NoContent(http.StatusOK)
}

func decodeSince(since string) (uint64, error) {
	if since == "" {
		return 0, nil
	} else {
		s, err := base64.StdEncoding.DecodeString(since)
		if err != nil {
			return 0, err
		}
		sinceNum, err := strconv.ParseUint(string(s), 10, 64)
		if err != nil {
			return 0, err
		}
		return sinceNum, nil
	}
}

func encodeSince(since uint64) string {
	continuationString := strconv.FormatUint(since, 10)
	return base64.StdEncoding.EncodeToString([]byte(continuationString))
}
