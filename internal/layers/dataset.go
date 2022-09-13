package layers

import (
	"context"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/mimiro-io/internal-go-util/pkg/uda"
	"github.com/mimiro-io/postgresql-datalayer/internal/conf"
)

type ReadableDataset interface {
	Read(ctx context.Context, entities chan<- *uda.Entity) (string, error)
}

type WriteableDataset interface {
	Write(ctx context.Context, entities <-chan *uda.Entity) error
}

type DatasetName string

type PostgresDataset struct {
	pg    pgxpool.Pool
	name  DatasetName
	table *conf.TableMapping
}

func (ds *PostgresDataset) Read(ctx context.Context, entities chan<- *uda.Entity) (string, error) {

}

var _ ReadableDataset = (*PostgresDataset)(nil)