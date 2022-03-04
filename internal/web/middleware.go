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
	"strings"

	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/mimiro-io/datahub/internal/conf"
	"github.com/mimiro-io/datahub/internal/web/middlewares"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

type Middleware struct {
	logger     echo.MiddlewareFunc
	cors       echo.MiddlewareFunc
	jwt        echo.MiddlewareFunc
	recover    echo.MiddlewareFunc
	authorizer func(logger *zap.SugaredLogger, scopes ...string) echo.MiddlewareFunc
	handler    *WebHandler
	env        *conf.Env
}

func NewMiddleware(lc fx.Lifecycle, env *conf.Env, handler *WebHandler, e *echo.Echo, auth *AuthorizerConfig, core *security.ServiceCore) *Middleware {
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
		logger:     setupLogger(handler, skipper),
		cors:       setupCors(),
		jwt:        setupJWT(env, core, skipper),
		recover:    setupRecovery(handler),
		authorizer: auth.authorizer,
		handler:    handler,
		env:        env,
	}

	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			mw.configure(e)
			return nil
		},
	})

	return mw
}

type AuthorizerConfig struct {
	authorizer func(logger *zap.SugaredLogger, scopes ...string) echo.MiddlewareFunc
}

func NewAuthorizer(env *conf.Env, logger *zap.SugaredLogger, core *security.ServiceCore) *AuthorizerConfig {
	log := logger.Named("authorizer")
	var mws func(logger *zap.SugaredLogger, scopes ...string) echo.MiddlewareFunc

	switch env.Auth.Middleware {
	case "local":
		log.Infof("Adding node security Authorizer")
		if env.AdminUserName == "" || env.AdminPassword == "" {
			log.Warnf("Admin password or username not set")
		} else {
			mws = middlewares.LocalAuthorizer(core)
		}
	case "noop":
		log.Infof("WARNING: Adding NoOp Authorizer")
		mws = middlewares.NoOpAuthorizer
	case "auth0", "jwt":
		log.Infof("Adding JWT Authorizer")
		mws = middlewares.JwtAuthorizer
	case "opa":
		log.Infof("Adding OPA Authorizer")
		mws = middlewares.OpaAuthorizer
	}

	return &AuthorizerConfig{authorizer: mws}
}

func (middleware *Middleware) configure(e *echo.Echo) {
	e.Use(middleware.logger)

	if middleware.env.Auth.Middleware == "noop" { // don't enable local security if noop is enabled
		middleware.handler.Logger.Infof("WARNING: Security is disabled")
	} else {
		e.Use(middleware.cors)
		e.Use(middleware.jwt)
	}
	e.Use(middleware.recover)
}

func setupJWT(env *conf.Env, core *security.ServiceCore, skipper func(c echo.Context) bool) echo.MiddlewareFunc {
	config := &middlewares.JwtConfig{
		Skipper:   skipper,
		Audience:  env.Auth.Audience,
		Issuer:    env.Auth.Issuer,
		Wellknown: env.Auth.WellKnown}

	// if node security is enabled
	if env.Auth.Middleware == "local" {
		config.NodePublicKey = core.NodeInfo.KeyPairs[0].PublicKey
		config.Issuer = "node:" + core.NodeInfo.NodeId
		config.Audience = "node:" + core.NodeInfo.NodeId
	}

	return middlewares.JWTHandler(config)
}

func setupLogger(handler *WebHandler, skipper func(c echo.Context) bool) echo.MiddlewareFunc {
	return middlewares.LoggerFilter(middlewares.LoggerConfig{
		Skipper:      skipper,
		Logger:       handler.Logger.Desugar(),
		StatsdClient: handler.StatsDClient,
	})
}

func setupCors() echo.MiddlewareFunc {
	return middleware.CORSWithConfig(middleware.CORSConfig{
		AllowOrigins: []string{"https://api.mimiro.io", "https://platform.mimiro.io"},
		AllowHeaders: []string{echo.HeaderOrigin, echo.HeaderContentType, echo.HeaderAccept},
	})
}

func setupRecovery(handler *WebHandler) echo.MiddlewareFunc {
	return middlewares.RecoverWithConfig(middlewares.DefaultRecoverConfig, handler.Logger)
}
