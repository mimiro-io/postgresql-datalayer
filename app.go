package postgres

import (
	"context"
	conf2 "github.com/mimiro-io/postgresql-datalayer/internal/legacy/conf"
	layers2 "github.com/mimiro-io/postgresql-datalayer/internal/legacy/layers"
	"github.com/mimiro-io/postgresql-datalayer/internal/legacy/web"
	"go.uber.org/fx"
	"time"
)

func wire() *fx.App {
	app := fx.New(
		fx.StartTimeout(600*time.Second),
		fx.Provide(
			conf2.NewEnv,
			conf2.NewTokenProviders,
			conf2.NewConfigurationManager,
			conf2.NewStatsd,
			conf2.NewLogger,
			web.NewWebServer,
			web.NewMiddleware,
			layers2.NewLayer,
			layers2.NewPostLayer,
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
