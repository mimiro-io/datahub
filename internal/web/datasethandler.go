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
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/golang-jwt/jwt/v4"
	"github.com/labstack/echo/v4"
	"go.uber.org/fx"
	"go.uber.org/zap"

	"github.com/mimiro-io/datahub/internal/security"
	"github.com/mimiro-io/datahub/internal/server"
	ds "github.com/mimiro-io/datahub/internal/service/dataset"
	"github.com/mimiro-io/datahub/internal/service/types"
)

type datasetHandler struct {
	datasetManager *server.DsManager
	store          *server.Store
	eventBus       server.EventBus
	tokenProviders *security.TokenProviders
}

func NewDatasetHandler(
	lc fx.Lifecycle,
	e *echo.Echo,
	logger *zap.SugaredLogger,
	mw *Middleware,
	dm *server.DsManager,
	store *server.Store,
	eb server.EventBus,
	tokenProviders *security.TokenProviders,
) {
	log := logger.Named("web")
	handler := &datasetHandler{
		datasetManager: dm,
		store:          store,
		eventBus:       eb,
		tokenProviders: tokenProviders,
	}

	lc.Append(fx.Hook{
		OnStart: func(_ context.Context) error {
			e.GET("/datasets", handler.datasetList, mw.authorizer(log, datahubRead))
			e.GET("/datasets/:dataset/entities", handler.getEntitiesHandler, mw.authorizer(log, datahubRead))
			e.GET("/datasets/:dataset/changes", handler.getChangesHandler, mw.authorizer(log, datahubRead))
			e.POST("/datasets/:dataset/entities", handler.storeEntitiesHandler, mw.authorizer(log, datahubWrite))

			e.GET("/datasets/:dataset", handler.datasetGet, mw.authorizer(log, datahubRead))
			e.POST("/datasets/:dataset", handler.datasetCreate, mw.authorizer(log, datahubWrite))
			e.PATCH("/datasets/:dataset", handler.datasetUpdate, mw.authorizer(log, datahubWrite))
			e.DELETE("/datasets/:dataset", handler.deleteDatasetHandler, mw.authorizer(log, datahubWrite))
			e.DELETE("/datasets", handler.deleteAllDatasets, mw.authorizer(log, datahubWrite))
			return nil
		},
	})
}

// datasetList
func (handler *datasetHandler) datasetList(c echo.Context) error {
	// this is only set by OPA auth
	// if not set and local auth is enabled then use that
	accessible := c.Get("datasets")
	var err error

	datasets := make([]server.DatasetName, 0)

	if accessible == nil {
		datasets = handler.datasetManager.GetDatasetNames()
		if handler.tokenProviders.ServiceCore.IsLocalAuthEnabled {
			user := c.Get("user")
			token := user.(*jwt.Token)
			claims := token.Claims.(*security.CustomClaims)
			roles := claims.Roles
			isAdmin := false

			for _, role := range roles {
				if role == "admin" {
					isAdmin = true
					break
				}
			}

			if !isAdmin {
				datasets, err = handler.tokenProviders.ServiceCore.FilterDatasets(datasets, claims.Subject)
				if err != nil {
					return err
				}
			}
		}
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
	isProxy := c.QueryParam("proxy")
	exist := handler.datasetManager.IsDataset(datasetName)
	if exist {
		return echo.NewHTTPError(http.StatusBadRequest, "Dataset already exist")
	}
	createDatasetConfig := &server.CreateDatasetConfig{}
	jsonDecoder := json.NewDecoder(c.Request().Body)
	err := jsonDecoder.Decode(createDatasetConfig)
	if err != nil && err != io.EOF { // eof means body was empty.
		return echo.NewHTTPError(http.StatusInternalServerError, "Failed to create dataset: "+err.Error())
	}
	if isProxy == "true" {
		if createDatasetConfig.ProxyDatasetConfig == nil ||
			createDatasetConfig.ProxyDatasetConfig.RemoteURL == "" {
			return echo.NewHTTPError(http.StatusBadRequest, "invalid proxy configuration provided")
		}
		_, err = handler.datasetManager.CreateDataset(datasetName, createDatasetConfig)
	} else {
		createDatasetConfig.ProxyDatasetConfig = nil // make sure we don't accidently store invalid proxy config
		_, err = handler.datasetManager.CreateDataset(datasetName, createDatasetConfig)
	}
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "Failed creating dataset")
	}
	/*
		// TODO: remove the part under here
		// this is done to potentially update "old" datasets
		core := handler.datasetManager.GetDataset("core.Dataset")
		entity := handler.datasetManager.NewDatasetEntity(datasetName, nil, nil)
		entities := []*server.Entity{
			entity,
		}
		err = core.StoreEntities(entities)
		if err != nil {
			return echo.NewHTTPError(http.StatusInternalServerError, "Failed creating dataset")
		}
	*/
	return c.NoContent(http.StatusOK)
}

func (handler *datasetHandler) datasetUpdate(c echo.Context) error {
	datasetName := c.Param("dataset")
	exist := handler.datasetManager.IsDataset(datasetName)
	if !exist {
		return echo.NewHTTPError(http.StatusBadRequest, "Dataset does not exist")
	}
	updateDatasetConfig := &server.UpdateDatasetConfig{}
	jsonDecoder := json.NewDecoder(c.Request().Body)
	err := jsonDecoder.Decode(updateDatasetConfig)
	if err != nil {
		if err != io.EOF { // eof means body was empty.
			return echo.NewHTTPError(http.StatusBadRequest, "update dataset request without payload: "+err.Error())
		}
		return echo.NewHTTPError(http.StatusBadRequest, "Could not parse update dataset payload: "+err.Error())
	}
	_, err = handler.datasetManager.UpdateDataset(datasetName, updateDatasetConfig)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "Failed updating dataset")
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

	var l int
	limit := c.QueryParam("limit")
	if limit != "" {
		f, err2 := strconv.ParseInt(limit, 10, 64)
		if err2 != nil {
			return echo.NewHTTPError(http.StatusBadRequest, server.HTTPQueryParamErr(err2).Error())
		}
		l = int(f)
	}

	f := c.QueryParam("from")
	reverse := c.QueryParam("reverse")
	if reverse != "" {
		return echo.NewHTTPError(http.StatusBadRequest, fmt.Errorf("reverse parameter only supported for changes"))
	}

	// check dataset exists
	dataset := handler.datasetManager.GetDataset(datasetName)
	if dataset == nil {
		return c.NoContent(http.StatusNotFound)
	}

	preStream := func() error {
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
		return nil
	}
	var continuationToken string
	if dataset.IsProxy() {
		continuationToken, err = dataset.AsProxy(
			handler.lookupAuth(dataset.ProxyConfig.AuthProviderName),
		).StreamEntitiesRaw(f, l, func(jsonData []byte) error {
			_, _ = c.Response().Write([]byte(","))
			_, _ = c.Response().Write(jsonData)
			return nil
		}, preStream)
		if err != nil {
			return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
		}
		if continuationToken != "" {
			// write the continuation token and end the array of entities
			_, _ = c.Response().Write([]byte(", {\"id\":\"@continuation\",\"token\":\"" + continuationToken + "\"}]"))
		} else {
			// write only array closing bracket
			_, _ = c.Response().Write([]byte("]"))
		}
	} else {
		if f != "" {
			_, err = base64.StdEncoding.DecodeString(f)
			if err != nil {
				return echo.NewHTTPError(http.StatusBadRequest, server.SinceParseErr(err).Error())
			}
		}
		err = preStream()
		if err != nil {
			return err
		}
		continuationToken, err = dataset.MapEntitiesRaw(f, l, func(jsonData []byte) error {
			_, err2 := c.Response().Write([]byte(","))
			if err2 != nil {
				return err2
			}
			_, err2 = c.Response().Write(jsonData)
			return err2
		})
		if err != nil {
			return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
		}
		// write the continuation token and end the array of entities
		_, _ = c.Response().Write([]byte(", {\"id\":\"@continuation\",\"token\":\"" + continuationToken + "\"}]"))
	}

	c.Response().Flush()

	return nil
}

func (handler *datasetHandler) lookupAuth(authProviderName string) func(req *http.Request) {
	if provider, ok := handler.tokenProviders.Get(strings.ToLower(authProviderName)); ok {
		return provider.Authorize
	}

	// if no authProvider es found, fall back to no auth for backend requests
	return func(req *http.Request) {
		// noop
	}
}

func (handler *datasetHandler) getChangesHandler(c echo.Context) error {
	datasetName := c.Param("dataset")
	limit := c.QueryParam("limit")
	since := c.QueryParam("since")
	reverse := c.QueryParam("reverse") == "true"
	latestOnly := c.QueryParam("latestOnly") == "true"

	var l int
	if limit != "" {
		f, err := strconv.ParseInt(limit, 10, 64)
		if err != nil {
			return echo.NewHTTPError(http.StatusBadRequest, server.HTTPQueryParamErr(err).Error())
		}
		l = int(f)
	}

	// check dataset exists
	dataset := handler.datasetManager.GetDataset(datasetName)
	if dataset == nil {
		return c.NoContent(http.StatusNotFound)
	}

	preStream := func() {
		c.Response().Header().Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
		c.Response().WriteHeader(http.StatusOK)

		_, _ = c.Response().Write([]byte("["))

		// write context
		jsonContext, _ := json.Marshal(dataset.GetContext())
		_, _ = c.Response().Write(jsonContext)
	}
	if dataset.IsProxy() {
		continuationToken, err := dataset.AsProxy(
			handler.lookupAuth(dataset.ProxyConfig.AuthProviderName),
		).StreamChangesRaw(since, l, latestOnly, reverse, func(jsonData []byte) error {
			_, _ = c.Response().Write([]byte(","))
			_, _ = c.Response().Write(jsonData)
			return nil
		}, preStream)
		if err != nil {
			return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
		}
		if continuationToken != "" {
			// write the continuation token and end the array of entities
			_, _ = c.Response().Write([]byte(", {\"id\":\"@continuation\",\"token\":\"" + continuationToken + "\"}]"))
		} else {
			// write only array closing bracket
			_, _ = c.Response().Write([]byte("]"))
		}
	} else {
		sinceNum, err := decodeSince(since)
		if err != nil {
			return echo.NewHTTPError(http.StatusBadRequest, server.SinceParseErr(err).Error())
		}
		preStream()
		if reverse {
			continuationToken := sinceNum
			of, err := ds.Of(server.NewBadgerAccess(handler.store, handler.datasetManager), dataset.ID)
			if err != nil {
				return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
			}
			it, err := of.At(types.DatasetOffset(sinceNum))
			if err != nil {
				return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
			}
			it = it.Inverse()
			defer it.Close()
			cnt := 0
			for it.Next() {
				jsonData := it.Item()
				_, _ = c.Response().Write([]byte(","))
				_, _ = c.Response().Write(jsonData)
				cnt++
				if cnt == l {
					break
				}
			}
			continuationToken = it.NextOffset()
			if it.Error() != nil {
				return echo.NewHTTPError(http.StatusInternalServerError, it.Error().Error())
			}
			if reverse && continuationToken == 0 {
				_, _ = c.Response().Write([]byte("]"))
			} else {
				_, _ = c.Response().Write([]byte(", {\"id\":\"@continuation\",\"token\":\"" + encodeSince(continuationToken) + "\"}]"))
			}
		} else {
			continuationToken, err := dataset.ProcessChangesRaw(uint64(sinceNum), l, latestOnly, func(jsonData []byte) error {
				_, _ = c.Response().Write([]byte(","))
				_, _ = c.Response().Write(jsonData)
				return nil
			})
			if err != nil {
				return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
			}
			_, _ = c.Response().Write([]byte(", {\"id\":\"@continuation\",\"token\":\"" + encodeSince(types.DatasetOffset(continuationToken)) + "\"}]"))
		}
	}
	// write the continuation token and end the array of entities
	c.Response().Flush()

	return nil
}

// storeEntitiesHandler
func (handler *datasetHandler) storeEntitiesHandler(c echo.Context) error {
	datasetName := c.Param("dataset")
	fsID := c.Request().Header.Get("universal-data-api-full-sync-id")
	fsStart := c.Request().Header.Get("universal-data-api-full-sync-start")
	fsEnd := c.Request().Header.Get("universal-data-api-full-sync-end")

	return handler.processEntities(c, datasetName, fsStart == "true", fsID, fsEnd == "true")
}

func (handler *datasetHandler) processEntities(
	c echo.Context,
	datasetName string,
	fullSyncStart bool,
	fullSyncID string,
	fullSyncEnd bool,
) error {
	var err error
	// check dataset exists
	ok := handler.datasetManager.IsDataset(datasetName)
	if !ok {
		return echo.NewHTTPError(http.StatusInternalServerError, errors.New("dataset does not exists").Error())
	}

	dataset := handler.datasetManager.GetDataset(datasetName)

	if dataset.IsProxy() {
		return dataset.AsProxy(
			handler.lookupAuth(dataset.ProxyConfig.AuthProviderName),
		).ForwardEntities(c.Request().Body, c.Request().Header)
	}
	// start new fullsync if requested
	if fullSyncStart {
		err2 := dataset.StartFullSyncWithLease(fullSyncID)
		if err2 != nil {
			return echo.NewHTTPError(http.StatusConflict, server.HTTPFullsyncErr(err2).Error())
		}
	} else if dataset.FullSyncStarted() {
		err = dataset.RefreshFullSyncLease(fullSyncID)
		if err != nil {
			return echo.NewHTTPError(http.StatusConflict, server.HTTPFullsyncErr(err).Error())
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
			err2 := dataset.StoreEntities(entities)
			if err2 != nil {
				return echo.NewHTTPError(http.StatusInternalServerError, server.AttemptStoreEntitiesErr(err2).Error())
			}
			count = 0
			entities = make([]*server.Entity, 0)
		}
		return nil
	})

	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, server.AttemptStoreEntitiesErr(err).Error())
	}

	if count > 0 {
		err := dataset.StoreEntities(entities)
		if err != nil {
			return echo.NewHTTPError(http.StatusInternalServerError, server.AttemptStoreEntitiesErr(err).Error())
		}
	}

	if fullSyncEnd {
		if err := dataset.ReleaseFullSyncLease(fullSyncID); err != nil {
			return echo.NewHTTPError(http.StatusGone, server.HTTPGenericErr(err).Error())
		}
		if err := dataset.CompleteFullSync(); err != nil {
			return echo.NewHTTPError(http.StatusInternalServerError, server.HTTPGenericErr(err).Error())
		}
	}
	// we have to emit the dataset, so that subscribers can react to the event
	ctx := context.Background()
	handler.eventBus.Emit(ctx, "dataset."+datasetName, nil)
	handler.eventBus.Emit(ctx, "dataset.core.Dataset", nil)

	return c.NoContent(http.StatusOK)
}

func decodeSince(since string) (types.DatasetOffset, error) {
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
		return types.DatasetOffset(sinceNum), nil
	}
}

func encodeSince(since types.DatasetOffset) string {
	continuationString := strconv.FormatUint(uint64(since), 10)
	return base64.StdEncoding.EncodeToString([]byte(continuationString))
}
