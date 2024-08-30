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
	"github.com/mimiro-io/datahub/internal/security"
	"html/template"
	"io"
	"net/http"

	"github.com/DataDog/datadog-go/v5/statsd"
	"github.com/labstack/echo/v4"
	"go.uber.org/zap"

	"github.com/mimiro-io/datahub/internal/conf"
	"github.com/mimiro-io/datahub/internal/content"
	"github.com/mimiro-io/datahub/internal/jobs"
	"github.com/mimiro-io/datahub/internal/server"
)

type Handler struct {
	Logger         *zap.SugaredLogger
	Port           string
	Store          *server.Store
	JobScheduler   *jobs.Scheduler
	ContentConfig  *content.Service
	StatsDClient   statsd.ClientInterface
	DatasetManager *server.DsManager
	EventBus       server.EventBus
	Profile        string
}

type Template struct {
	templates *template.Template
}

type ServiceInfo struct {
	Name     string `json:"name"`
	Location string `json:"location"`
}

const (
	mimiroIcon   = "/mimiro-favicon.png"
	favIcon      = "/favicon.ico"
	datahubRead  = "datahub:r"
	datahubWrite = "datahub:w"
)

func (t *Template) Render(w io.Writer, name string, data interface{}, c echo.Context) error {
	return t.templates.ExecuteTemplate(w, name, data)
}

type WebService struct {
	env    *conf.Config
	logger *zap.SugaredLogger
	statsd statsd.ClientInterface
	echo   *echo.Echo
}

func (ws *WebService) Start(ctx context.Context) error {
	ws.logger.Infof("Starting Http server on :%s", ws.env.Port)
	go func() {
		_ = ws.echo.Start(":" + ws.env.Port)
	}()
	return nil
}

func (ws *WebService) Stop(ctx context.Context) error {
	ws.logger.Infof("Shutting down Http server")
	return ws.echo.Shutdown(ctx)
}

// ServiceContext is the injection of all deps grouped nicely together
// expectation is that this can become an interface and also the things it contains
// should provide interfaces and not structs with funcs
type ServiceContext struct {
	Env            *conf.Config
	Logger         *zap.SugaredLogger
	Statsd         statsd.ClientInterface
	SecurityCore   *security.ServiceCore
	ContentService *content.Service
	DatasetManager *server.DsManager
	Store          *server.Store
	EventBus       server.EventBus
	TokenProviders *security.TokenProviders
	JobsScheduler  *jobs.Scheduler
	Port           string
}

func NewWebService(serviceContext *ServiceContext) (*WebService, error) {
	webService := &WebService{}
	webService.env = serviceContext.Env
	webService.logger = serviceContext.Logger
	webService.statsd = serviceContext.Statsd

	e := echo.New()
	e.HideBanner = true
	webService.echo = e

	NewStatusHandler(e, serviceContext.Port)

	mw := NewMiddleware(serviceContext.Env, e, serviceContext.SecurityCore, serviceContext.Logger, serviceContext.Statsd)
	logger := serviceContext.Logger
	store := serviceContext.Store

	// call all handler registrations
	RegisterContentHandler(e, logger, mw, serviceContext.ContentService)
	RegisterDatasetHandler(e, logger, mw, serviceContext.DatasetManager, store, serviceContext.EventBus, serviceContext.TokenProviders)
	RegisterTxnHandler(e, logger, mw, store)
	RegisterQueryHandler(e, logger, mw, store, serviceContext.DatasetManager)
	RegisterJobOperationHandler(e, logger, mw, serviceContext.JobsScheduler)
	RegisterJobsHandler(e, logger, mw, serviceContext.JobsScheduler)
	RegisterNamespaceHandler(e, logger, mw, store)
	RegisterProviderHandler(e, logger, mw, serviceContext.TokenProviders)
	RegisterSecurityHandler(e, logger, mw, serviceContext.SecurityCore)
	RegisterStatisticsHandler(e, logger, mw, store)
	RegisterCompactionHandler(e, logger, mw, serviceContext.DatasetManager, store)
	return webService, nil
}

func NewStatusHandler(e *echo.Echo, port string) {
	e.GET("/health", func(c echo.Context) error {
		return c.String(http.StatusOK, "UP")
	})
	e.GET("/", func(c echo.Context) error {
		serviceInfo := &ServiceInfo{"DataHub", "server:" + port}
		return c.JSON(http.StatusOK, serviceInfo)
	})
}
