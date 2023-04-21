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

package middlewares

import (
	"fmt"
	"strings"
	"time"

	"github.com/DataDog/datadog-go/v5/statsd"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"go.uber.org/zap"
)

type LoggerConfig struct {
	// Skipper defines a function to skip middleware.
	Skipper middleware.Skipper

	// BeforeFunc defines a function which is executed just before the middleware.
	BeforeFunc middleware.BeforeFunc

	Logger *zap.Logger

	StatsdClient statsd.ClientInterface
}

func LoggerFilter(config LoggerConfig) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			if config.Skipper(c) {
				return next(c)
			}

			if config.BeforeFunc != nil {
				config.BeforeFunc(c)
			}

			start := time.Now()
			req := c.Request()
			res := c.Response()

			tags := []string{
				"application:datahub",
				fmt.Sprintf("method:%s", strings.ToLower(req.Method)),
				fmt.Sprintf("url:%s", strings.ToLower(req.RequestURI)),
				fmt.Sprintf("status:%d", res.Status),
			}

			timed := time.Since(start)

			err := config.StatsdClient.Incr("http.count", tags, 1)
			if err != nil {
				config.Logger.Warn("Error with statsd", zap.String("error", fmt.Sprintf("%s", err)))
			}
			err = config.StatsdClient.Timing("http.time", timed, tags, 1)
			if err != nil {
				config.Logger.Warn("Error with statsd", zap.String("error", fmt.Sprintf("%s", err)))
			}
			err = config.StatsdClient.Gauge("http.size", float64(res.Size), tags, 1)
			if err != nil {
				config.Logger.Warn("Error with statsd", zap.String("error", fmt.Sprintf("%s", err)))
			}

			err = next(c)
			if err != nil {
				c.Error(err)
			}

			id := req.Header.Get(echo.HeaderXRequestID)
			if id == "" {
				id = res.Header().Get(echo.HeaderXRequestID)
			}
			msg := fmt.Sprintf("%d - %s %s (time: %s, size: %d, user_agent: %s, request_id: %s)",
				res.Status, req.Method, req.RequestURI, timed.String(), res.Size, req.UserAgent(), id)

			config.Logger.Info(msg)

			return nil
		}
	}
}
