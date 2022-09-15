package layers

import (
	"context"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/mimiro-io/postgresql-datalayer/internal/conf"
	"github.com/mimiro-io/postgresql-datalayer/internal/db"
	"go.uber.org/zap"
)

type PostLayer struct {
	logger   *zap.SugaredLogger
	cmgr     *conf.ConfigurationManager
	PostRepo *PostRepository //exported because it needs to deferred from main??
}

type PostRepository struct {
	DB           *pgxpool.Pool
	ctx          context.Context
	postTableDef *conf.PostMapping
	digest       [16]byte
}

func NewPostLayer(cmgr *conf.ConfigurationManager, logger *zap.SugaredLogger) *PostLayer {
	layer := &PostLayer{
		cmgr:   cmgr,
		logger: logger.Named("layer"),
	}

	return layer
}

func (postLayer *PostLayer) Dataset(request db.DatasetRequest) (WriteableDataset, error) {
	pg, err := postLayer.connect(postLayer.cmgr.Datalayer, db.DatasetName(request.DatasetName))
	if err != nil {
		return nil, err
	}
	table, err := db.NewWriteTable(postLayer.cmgr.Datalayer, db.DatasetName(request.DatasetName))
	if err != nil {
		return nil, err
	}
	return NewPostgresDataset(pg, nil, table, request), nil
}

func (postLayer *PostLayer) connect(layer *conf.Datalayer, name db.DatasetName) (*pgxpool.Pool, error) {
	var tableMap *conf.PostMapping
	for _, table := range layer.PostMappings {
		if table.DatasetName == string(name) {
			tableMap = table
			break
		} else if table.TableName == string(name) { // fallback
			tableMap = table
		}
	}

	u := postLayer.cmgr.Datalayer.GetUrl(tableMap)

	conn, err := pgxpool.Connect(context.Background(), u.String())
	if err != nil {
		postLayer.logger.Warn("Error creating connection pool: ", err.Error())
		return nil, err
	}

	return conn, nil
}
