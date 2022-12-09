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

package datahub

import (
	"context"
	"time"

	"github.com/mimiro-io/datahub/internal/conf"
	"github.com/mimiro-io/datahub/internal/conf/secrets"
	"github.com/mimiro-io/datahub/internal/content"
	"github.com/mimiro-io/datahub/internal/jobs"
	"github.com/mimiro-io/datahub/internal/security"
	"github.com/mimiro-io/datahub/internal/server"
	"github.com/mimiro-io/datahub/internal/web"
	"go.uber.org/fx"
)

func wire() *fx.App {
	return fx.New(
		fx.Options(
			fx.StartTimeout(60*time.Second),
		),
		fx.Provide(
			conf.NewEnv,
			secrets.NewManager,
			conf.NewMetricsClient,
			conf.NewLogger,
			server.NewBus,
			server.NewStore,
			server.NewDsManager,
			security.NewProviderManager,
			security.NewTokenProviders,
			jobs.NewRunner,
			jobs.NewScheduler,
			jobs.NewDataHubJobStore,
			content.NewContent,
			web.NewAuthorizer,
			web.NewWebServer,
			web.NewMiddleware,
			security.NewServiceCore,
		),
		fx.Invoke( // no other functions are using these, so they need to be invoked to kick things off
			conf.NewMemoryReporter,
			web.Register,
			web.NewContentHandler,
			web.NewDatasetHandler,
			web.NewTxnHandler,
			web.NewQueryHandler,
			web.NewJobOperationHandler,
			web.NewJobsHandler,
			web.NewNamespaceHandler,
			web.NewProviderHandler,
			server.NewBackupManager,
			server.NewGarbageCollector,
			web.NewSecurityHandler,
		),
	)
}

func Run() {
	wire().Run()
}

func Start(ctx context.Context) (*fx.App, error) {
	app := wire()
	err := app.Start(ctx)
	return app, err
}
