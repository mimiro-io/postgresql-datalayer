package layers

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/mimiro-io/internal-go-util/pkg/uda"
	"github.com/mimiro-io/postgresql-datalayer/internal/conf"
	"github.com/mimiro-io/postgresql-datalayer/internal/db"
)

type ReadableDataset interface {
	Read(ctx context.Context, entities chan<- *uda.Entity) (string, error)
}

type WriteableDataset interface {
	Write(ctx context.Context, entities <-chan *uda.Entity) error
}

type PostgresDataset struct {
	pg    *pgxpool.Pool
	table *db.Table
	since string
	limit int64
}

func NewPostgresDataset(pg *pgxpool.Pool, table *db.Table, request db.DatasetRequest) *PostgresDataset {
	return &PostgresDataset{
		pg:    pg,
		table: table,
		since: request.Since,
		limit: request.Limit,
	}
}

func (ds *PostgresDataset) Read(ctx context.Context, entities chan<- *uda.Entity) (string, error) {
	since, err := serverSince(ds.pg)
	if err != nil {
		return "", err
	}

	rows, err := ds.pg.Query(ctx, ds.table.Query())
	if err != nil {
		return "", err
	}
	defer func() {
		rows.Close()
	}()

	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return "", err
		}
		nullableRowData := buildRowType(rows.FieldDescriptions(), values, rows.RawValues())
		entity := toEntity(nullableRowData, ds.table.ColumnMappings)

		if entity != nil {
			// add types to entity
			if len(ds.table.Types) == 1 {
				entity.References["rdf:type"] = ds.table.Types[0]
			} else if len(ds.table.Types) > 1 {
				// multiple types...
				// fix me
			}

			entities <- entity
		}
	}
	if ds.table.CDCEnabled {
		entity := uda.NewEntity()
		entity.ID = "@continuation"
		entity.Properties["token"] = since
		entities <- entity
	}
	return since, nil
}

var _ ReadableDataset = (*PostgresDataset)(nil)

func toEntity(data map[string]any, columns []*conf.ColumnMapping) *uda.Entity {
	entity := uda.NewEntity()

	props := make(map[string]any)

	colDefs := mapColumns(columns)

	for k, v := range data {
		colMapping := colDefs[k]
		colName := "ns0:" + k
		value := v
		if colMapping != nil {
			if colMapping.IgnoreColumn {
				continue
			}

			if colMapping.PropertyName != "" {
				colName = colMapping.PropertyName
			}

			if colMapping.IsIdColumn && v != nil {
				entity.ID = fmt.Sprintf(colMapping.IdTemplate, v)
			}

			if colMapping.IsReference && v != nil {
				entity.References[colName] = fmt.Sprintf(colMapping.ReferenceTemplate, v)
			}

			if colMapping.IsEntity {
				switch v.(type) {
				case map[string]any:
					// is an object
					value = toEntity(v.(map[string]any), colMapping.ColumnMappings)
				case []interface{}:
					// is a list of objects
					ents := make([]*uda.Entity, 0)
					for _, obj := range v.([]any) {
						ret := toEntity(obj.(map[string]any), colMapping.ColumnMappings)
						if ret != nil {
							ents = append(ents, ret)
						}
					}
					value = ents
				default:
					// probably a nil
					value = v
				}
			}
		}
		props[colName] = value
	}
	entity.Properties = props

	if entity.IsDeleted {
		entity.Properties = make(map[string]any)
		entity.References = make(map[string]any)
	}
	if entity.ID == "" { // this is invalid
		return nil
	}

	return entity
}
