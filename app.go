package postgres

import (
	"context"
	"github.com/mimiro-io/postgresql-datalayer/internal/conf"
	"github.com/mimiro-io/postgresql-datalayer/internal/layers"
	"github.com/mimiro-io/postgresql-datalayer/internal/web"
	"go.uber.org/fx"
	"time"
)

func wire() *fx.App {
	app := fx.New(
		fx.StartTimeout(600*time.Second),
		fx.Provide(
			conf.NewEnv,
			conf.NewTokenProviders,
			conf.NewConfigurationManager,
			conf.NewStatsd,
			conf.NewLogger,
			web.NewWebServer,
			web.NewMiddleware,
			layers.NewLayer,
			layers.NewPostLayer,
		),
		fx.Invoke(
			web.Register,
			web.NewDatasetHandler,
			web.NewPostHandler,
		),
	)
	return app
}

func Run() {
	wire().Run()
}

func Start(ctx context.Context) (*fx.App, error) {
	app := wire()
	err := app.Start(ctx)
	return app, err
}
