package web

import (
	"context"
	"encoding/json"
	"github.com/labstack/echo/v4"
	"github.com/mimiro-io/internal-go-util/pkg/uda"
	"github.com/mimiro-io/postgresql-datalayer/internal/db"
	"github.com/mimiro-io/postgresql-datalayer/internal/layers"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"net/http"
	"net/url"
	"sort"
	"strconv"
)

type ServiceInfo struct {
	Name     string
	Location string
}

type DatasetName struct {
	Name string   `json:"name"`
	Type []string `json:"type"`
}

type datasetHandler struct {
	logger *zap.SugaredLogger
	layer  *layers.Layer
}

func NewDatasetHandler(lc fx.Lifecycle, e *echo.Echo, logger *zap.SugaredLogger, mw *Middleware, layer *layers.Layer) {
	log := logger.Named("web")

	dh := &datasetHandler{
		logger: log,
		layer:  layer,
	}
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			e.GET("/datasets", dh.listDatasetsHandler, mw.authorizer(log, "datahub:r"))
			e.GET("/datasets/:dataset/changes", dh.getEntities, mw.authorizer(log, "datahub:r"))
			e.GET("/datasets/:dataset/entities", dh.getEntities, mw.authorizer(log, "datahub:r"))
			return nil
		},
	})

}

// Handlers

// listDatasetsHandler
func (handler *datasetHandler) listDatasetsHandler(c echo.Context) error {
	names := make([]DatasetName, 0)
	datasets := handler.layer.GetDatasetNames()
	postDatasets := handler.layer.GetDatasetPostNames()
	sort.Strings(datasets)
	for _, v := range datasets {
		names = append(names, DatasetName{Name: v, Type: []string{"GET"}})
	}
	sort.Strings(postDatasets)
	for _, v := range postDatasets {
		names = append(names, DatasetName{Name: v, Type: []string{"POST"}})
	}

	return c.JSON(http.StatusOK, names)
}

func (handler *datasetHandler) getEntities(c echo.Context) error {
	datasetName, err := url.QueryUnescape(c.Param("dataset"))
	if err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	limit := c.QueryParam("limit")
	var l int64
	if limit != "" {
		f, _ := strconv.ParseInt(limit, 10, 64)
		l = f
	}

	// check dataset exists
	if !handler.layer.DoesDatasetExist(datasetName) {
		return c.NoContent(http.StatusNotFound)
	}

	request := db.DatasetRequest{
		DatasetName: datasetName,
		Limit:       l,
	}
	reader, err := handler.layer.Dataset(request)
	if err != nil {
		return c.NoContent(http.StatusInternalServerError)
	}

	c.Response().Header().Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
	c.Response().WriteHeader(http.StatusOK)
	enc := json.NewEncoder(c.Response())
	c.Response().Write([]byte("["))

	// make and send context as the first object
	context := handler.layer.GetContext(datasetName)

	_ = enc.Encode(context)

	entities := make(chan *uda.Entity)
	group, ctx := errgroup.WithContext(c.Request().Context())
	group.Go(func() error {
		defer close(entities)
		err := reader.Read(ctx, entities)
		if err != nil {
			return err
		}
		return nil
	})

	select {

	case <-ctx.Done():
		break
	case e, ok := <-entities:
		if !ok {
			break
		}
		c.Response().Write([]byte(","))
		_ = enc.Encode(e)
	}

	_ = group.Wait()
	c.Response().Flush()
	c.Response().Write([]byte("]"))
	c.Response().Flush()
	return nil
}
