package web

import (
	"context"
	"embed"
	_ "embed"
	"encoding/json"
	"github.com/DataDog/datadog-go/v5/statsd"
	"github.com/labstack/echo/v4"
	"github.com/mimiro-io/postgresql-datalayer/internal/legacy/conf"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"net/http"
	"runtime"
)

//go:embed *
var versionDir embed.FS

type Handler struct {
	Logger       *zap.SugaredLogger
	Port         string
	StatsDClient statsd.ClientInterface
	Profile      string
}

func NewWebServer(lc fx.Lifecycle, env *conf.Env, logger *zap.SugaredLogger, statsd statsd.ClientInterface) (*Handler, *echo.Echo) {
	e := echo.New()
	e.HideBanner = true

	l := logger.Named("web")

	handler := &Handler{
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

func Register(e *echo.Echo, env *conf.Env) {
	// this sets up the main chain
	env.Logger.Infof("Registering endpoints")
	e.GET("/health", health)

}

func health(c echo.Context) error {
	versionFile, err := versionDir.ReadFile("VERSION.json")
	var version map[string]string
	if err == nil {
		json.Unmarshal(versionFile, &version)
	} else {
		version = map[string]string{"version": "Only available on released binaries"}
	}
	version["go_version"] = runtime.Version()

	return c.JSON(http.StatusOK, version)
}
