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
	"github.com/DataDog/datadog-go/v5/statsd"
	"html/template"
	"io"
	"net/http"

	"github.com/labstack/echo/v4"
	"github.com/mimiro-io/datahub/internal/conf"
	"github.com/mimiro-io/datahub/internal/content"
	"github.com/mimiro-io/datahub/internal/jobs"
	"go.uber.org/fx"
	"go.uber.org/zap"

	"github.com/mimiro-io/datahub/internal/server"
)

type Handler struct {
	Logger         *zap.SugaredLogger
	Port           string
	Store          *server.Store
	JobScheduler   *jobs.Scheduler
	ContentConfig  *content.Config
	StatsDClient   statsd.ClientInterface
	DatasetManager *server.DsManager
	EventBus       server.EventBus
	Profile        string
}

type WebHandler struct {
	Logger       *zap.SugaredLogger
	Port         string
	StatsDClient statsd.ClientInterface
	Profile      string
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

func NewWebServer(lc fx.Lifecycle, env *conf.Env, logger *zap.SugaredLogger, statsd statsd.ClientInterface) (*WebHandler, *echo.Echo) {
	e := echo.New()
	e.HideBanner = true

	l := logger.Named("web")

	handler := &WebHandler{
		Logger:       l,
		Port:         env.Port,
		StatsDClient: statsd,
		Profile:      env.Env,
	}

	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			l.Infof("Starting Http server on :%s", env.Port)
			go func() {
				_ = e.Start(":" + env.Port)
			}()
			return nil
		},
		OnStop: func(ctx context.Context) error {
			l.Infof("Shutting down Http server")
			return e.Shutdown(ctx)
		},
	})
	return handler, e
}

func Register(e *echo.Echo, handler *WebHandler) {
	// this sets up the main chain
	e.GET("/health", handler.health)
	e.GET("/", handler.serviceInfoHandler)
}

func (handler *WebHandler) health(c echo.Context) error {
	return c.String(http.StatusOK, "UP")
}

// serviceInfoHandler
func (handler *WebHandler) serviceInfoHandler(c echo.Context) error {
	serviceInfo := &ServiceInfo{"DataHub", "server:" + handler.Port}
	return c.JSON(http.StatusOK, serviceInfo)
}
