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
	"github.com/DataDog/datadog-go/v5/statsd"
	"strings"

	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"go.uber.org/zap"

	"github.com/mimiro-io/datahub/internal/conf"
	"github.com/mimiro-io/datahub/internal/security"
	"github.com/mimiro-io/datahub/internal/web/middlewares"
)

type Middleware struct {
	logging    echo.MiddlewareFunc
	cors       echo.MiddlewareFunc
	jwt        echo.MiddlewareFunc
	recover    echo.MiddlewareFunc
	authorizer func(logger *zap.SugaredLogger, scopes ...string) echo.MiddlewareFunc
	// 	handler    *WebHandler
	logger *zap.SugaredLogger
	env    *conf.Config
}

func NewMiddleware(env *conf.Config, e *echo.Echo, core *security.ServiceCore, logger *zap.SugaredLogger, statsd statsd.ClientInterface) *Middleware {
	skipper := func(c echo.Context) bool {
		// don't secure health endpoints
		if strings.HasPrefix(c.Request().URL.Path, "/health") {
			return true
		}
		if strings.HasPrefix(c.Request().URL.Path, mimiroIcon) {
			return true
		}
		if strings.HasPrefix(c.Request().URL.Path, favIcon) {
			return true
		}
		if strings.HasPrefix(c.Request().URL.Path, "/api") {
			return true
		}
		if strings.HasPrefix(c.Request().URL.Path, "/static") {
			return true
		}
		if strings.HasPrefix(c.Request().URL.Path, "/security/token") {
			return true
		}
		return false
	}

	mw := &Middleware{
		logging:    setupLogger(logger, statsd, skipper),
		cors:       setupCors(),
		jwt:        setupJWT(env, core, skipper),
		recover:    setupRecovery(logger),
		authorizer: NewAuthorizer(env, logger, core),
		env:        env,
		logger:     logger,
	}

	mw.configure(e, logger)

	/*lc.Append(fx.Hook{
		OnStart: func(_ context.Context) error {
			mw.configure(e)
			return nil
		},
	}) */

	return mw
}

func NewAuthorizer(env *conf.Config, logger *zap.SugaredLogger, core *security.ServiceCore) func(logger *zap.SugaredLogger, scopes ...string) echo.MiddlewareFunc {
	log := logger.Named("authorizer")

	switch env.Auth.Middleware {
	case "local", "opa", "on":
		if env.AdminUserName == "" || env.AdminPassword == "" {
			log.Panicf("Admin password or username not set")
		} else {
			log.Infof("Adding node security Authorizer")
			return middlewares.Authorizer(core)
		}
	case "noop":
		fallthrough
	default:
		log.Infof("WARNING: Adding NoOp Authorizer")
		return middlewares.NoOpAuthorizer
	}
	return nil

}

func (middleware *Middleware) configure(e *echo.Echo, logger *zap.SugaredLogger) {
	e.Use(middleware.logging)

	if middleware.env.Auth.Middleware == "noop" { // don't enable local security if noop is enabled
		middleware.logger.Infof("WARNING: Security is disabled")
	} else {
		e.Use(middleware.cors)
		e.Use(middleware.jwt)
	}
	e.Use(middleware.recover)
}

func setupJWT(env *conf.Config, core *security.ServiceCore, skipper func(c echo.Context) bool) echo.MiddlewareFunc {
	config := &middlewares.JwtConfig{
		Skipper:   skipper,
		Audience:  env.Auth.Audience,
		Issuer:    env.Auth.Issuer,
		Wellknown: env.Auth.WellKnown,
	}

	// if security is enabled
	if env.Auth.Middleware == "local" || env.Auth.Middleware == "opa" || env.Auth.Middleware == "on" {
		config.NodePublicKey = core.NodeInfo.KeyPairs[0].PublicKey
		config.NodeIssuer = []string{"node:" + core.NodeInfo.NodeID}
		config.NodeAudience = []string{"node:" + core.NodeInfo.NodeID}
	}

	return middlewares.JWTHandler(config)
}

func setupLogger(logger *zap.SugaredLogger, statsd statsd.ClientInterface, skipper func(c echo.Context) bool) echo.MiddlewareFunc {
	return middlewares.LoggerFilter(middlewares.LoggerConfig{
		Skipper:      skipper,
		Logger:       logger.Desugar(),
		StatsdClient: statsd,
	})
}

func setupCors() echo.MiddlewareFunc {
	return middleware.CORSWithConfig(middleware.CORSConfig{
		AllowOrigins: []string{"https://api.mimiro.io", "https://platform.mimiro.io"},
		AllowHeaders: []string{echo.HeaderOrigin, echo.HeaderContentType, echo.HeaderAccept},
	})
}

func setupRecovery(logger *zap.SugaredLogger) echo.MiddlewareFunc {
	return middlewares.RecoverWithConfig(middlewares.DefaultRecoverConfig, logger)
}
