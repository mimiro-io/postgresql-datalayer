package web

import (
	"context"
	"github.com/bcicen/jstream"
	"github.com/labstack/echo/v4"
	"github.com/mimiro-io/internal-go-util/pkg/uda"
	"github.com/mimiro-io/postgresql-datalayer/internal/db"
	"github.com/mimiro-io/postgresql-datalayer/internal/layers"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"net/http"
	"net/url"
)

type postHandler struct {
	logger    *zap.SugaredLogger
	postLayer *layers.PostLayer
}

func NewPostHandler(lc fx.Lifecycle, e *echo.Echo, mw *Middleware, logger *zap.SugaredLogger, layer *layers.PostLayer) {
	log := logger.Named("web")

	handler := &postHandler{
		logger:    log,
		postLayer: layer,
	}

	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			e.POST("/datasets/:dataset/changes", handler.storeEntities, mw.authorizer(log, "datahub:w"))
			e.POST("/datasets/:dataset/entities", handler.storeEntities, mw.authorizer(log, "datahub:w"))
			return nil
		},
	})

}

func (handler *postHandler) storeEntities(c echo.Context) error {
	datasetName, _ := url.QueryUnescape(c.Param("dataset"))
	handler.logger.Debugf("Working on dataset %s", datasetName)

	dataset, err := handler.postLayer.Dataset(db.DatasetRequest{DatasetName: datasetName})
	if err != nil {
		handler.logger.Warn(err)
		return echo.NewHTTPError(http.StatusBadRequest, err.Error())
	}
	entities := make(chan *uda.Entity)
	stopCh := make(chan struct{})

	// set up our 2 workers guarded by an errgroup
	group, ctx := errgroup.WithContext(context.Background())
	group.Go(func() error {
		body := c.Request().Body
		defer func() {
			close(entities)
		}()
		isFirst := true
		return uda.ParseStream(body, func(value *jstream.MetaValue) error {
			if isFirst {
				isFirst = false
			} else {
				e := uda.AsEntity(value)
				if e.ID != "@continuation" {
					select { // a bit of fiddling to make sure we don't write to a closed channel if the second go routine fails
					case <-stopCh:
						return nil
					case entities <- e:
					}
				}
			}
			return nil
		})
	})
	group.Go(func() error {
		err := dataset.Write(ctx, entities)
		if err != nil { // by using a stopCh for control here, we make sure that the first goroutine can detect that something has happened
			close(stopCh)
		}
		return err
	})

	err = group.Wait() // wait for both routines to finnish
	if err != nil {
		handler.logger.Warn(err)
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}
	return c.NoContent(200)
}
