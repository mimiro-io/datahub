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

package main

import (
	"time"

	"github.com/mimiro-io/datahub"
	"github.com/mimiro-io/datahub/internal/conf"
	"github.com/mimiro-io/datahub/internal/content"
	"github.com/mimiro-io/datahub/internal/jobs"
	"github.com/mimiro-io/datahub/internal/security"
	"github.com/mimiro-io/datahub/internal/server"
	"github.com/mimiro-io/datahub/internal/web"
	"go.uber.org/fx"
)

func main() {
	app := fx.New(
		fx.Options(
			fx.StartTimeout(60*time.Second),
		),
		fx.Provide(
			datahub.NewEnv,
			conf.NewMetricsClient,
			conf.NewLogger,
			server.NewBus,
			server.NewStore,
			server.NewDsManager,
			security.NewTokenProviders,
			jobs.NewRunnerConfig,
			jobs.NewRunner,
			jobs.NewScheduler,
			content.NewContent,
			web.NewAuthorizer,
			web.NewWebServer,
			web.NewMiddleware,
		),
		fx.Invoke( // no other functions are using these, so they need to be invoked to kick things off
			conf.NewMemoryReporter,
			web.Register,
			web.NewContentHandler,
			web.NewDatasetHandler,
			web.NewQueryHandler,
			web.NewJobOperationHandler,
			web.NewJobsHandler,
			web.NewNamespaceHandler,
			server.NewBackupManager,
			server.NewGarbageCollector,
		),
	)

	app.Run()
}
