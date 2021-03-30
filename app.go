package datahub

import (
	"context"
	"github.com/mimiro-io/datahub/internal/conf"
	"github.com/mimiro-io/datahub/internal/content"
	"github.com/mimiro-io/datahub/internal/jobs"
	"github.com/mimiro-io/datahub/internal/security"
	"github.com/mimiro-io/datahub/internal/server"
	"github.com/mimiro-io/datahub/internal/web"
	"go.uber.org/fx"
	"time"
)

func wire() *fx.App {
	return fx.New(
		fx.Options(
			fx.StartTimeout(60*time.Second),
		),
		fx.Provide(
			conf.NewEnv,
			conf.NewStatsD,
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
