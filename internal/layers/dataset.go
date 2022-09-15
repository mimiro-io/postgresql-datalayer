package layers

import (
	"context"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/mimiro-io/internal-go-util/pkg/uda"
	"github.com/mimiro-io/postgresql-datalayer/internal/conf"
	"github.com/mimiro-io/postgresql-datalayer/internal/db"
	"strings"
)

const batchChunkSize = 1000

type ReadableDataset interface {
	Read(ctx context.Context, entities chan<- *uda.Entity) error
	ReadChanges(ctx context.Context, since string, entities chan<- *uda.Entity) (string, error)
}

type WriteableDataset interface {
	Write(ctx context.Context, entities <-chan *uda.Entity) error
}

type PostgresDataset struct {
	pg         *pgxpool.Pool
	table      *db.ReadTable
	writeTable *db.WriteTable
	since      string
	limit      int64
}

func NewPostgresDataset(pg *pgxpool.Pool, table *db.ReadTable, writeTable *db.WriteTable, request db.DatasetRequest) *PostgresDataset {
	return &PostgresDataset{
		pg:         pg,
		table:      table,
		writeTable: writeTable,
		since:      request.Since,
		limit:      request.Limit,
	}
}

// Write takes a chan of uda.Entity and queues this in a batch request. Once the batch request has
// batchChunkSize in entities, it attempts to commit the batch in a transaction. If the transaction
// fails, an error is returned from the writer, and control is returned to the caller.
func (ds *PostgresDataset) Write(ctx context.Context, entities <-chan *uda.Entity) error {
	defer func() {
		ds.pg.Close() // since write creates a new pool, we should close it when done
	}()
	if ds.writeTable == nil { // we could do a table check against the database here
		return errors.New("missing write table")
	}
	// we want to chunk this into blocks of data, then commit each chunk
	chunk := make([]*uda.Entity, batchChunkSize)
	batch := &pgx.Batch{}
	var count int32
	for e := range entities {
		count++
		chunk[count-1] = e
		if count == batchChunkSize {
			ds.queueAll(batch, chunk)
			err := ds.pg.BeginFunc(ctx, func(tx pgx.Tx) error {
				batchRequest := tx.SendBatch(ctx, batch)
				defer func() {
					batchRequest.Close() // make sure to always close the batch request
				}()
				_, err := batchRequest.Exec()
				if err != nil {
					return err
				}
				return nil
			})
			if err != nil {
				return err
			}
			// reset the counter
			count = 0
			chunk = make([]*uda.Entity, batchChunkSize)
		}
	}

	// this deals with the leftover chunk
	if count > 0 && count < batchChunkSize {
		ds.queueAll(batch, chunk[0:count])
		return ds.pg.BeginFunc(ctx, func(tx pgx.Tx) error {
			batchRequest := tx.SendBatch(ctx, batch)
			defer func() {
				batchRequest.Close()
			}()
			_, err := batchRequest.Exec()
			if err != nil {
				return err
			}
			return nil
		})
	}

	return nil
}

// queueAll takes a list if entities, and queues the in the pgx.Batch
func (ds *PostgresDataset) queueAll(batch *pgx.Batch, entities []*uda.Entity) {
	for _, entity := range entities {
		props := entity.StripPrefixes()
		args := make([]any, len(ds.writeTable.Fields)+1)
		if ds.writeTable.IdColumn != "" {
			args = make([]any, len(ds.writeTable.Fields))
		} else {
			args[0] = strings.SplitAfter(entity.ID, ":")[1]
		}

		for i, field := range ds.writeTable.Fields {
			if ds.writeTable.IdColumn != "" {
				args[i] = props[field.FieldName]
			} else {
				args[i+1] = props[field.FieldName]
			}
		}

		if entity.IsDeleted {
			batch.Queue(ds.writeTable.DeletionQuery(), args[0])
		} else {
			batch.Queue(ds.writeTable.Query(), args...)
		}
	}

}

// Read reads from a postgres query result, and emits an uda.Entity to the entities chan
func (ds *PostgresDataset) Read(ctx context.Context, entities chan<- *uda.Entity) error {
	if ds.table == nil {
		return errors.New("missing read table")
	}
	rows, err := ds.pg.Query(ctx, ds.table.Query(ds.limit))
	if err != nil {
		return err
	}
	defer func() {
		rows.Close()
	}()

	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return err
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

	return nil
}

func (ds *PostgresDataset) ReadChanges(ctx context.Context, since string, entities chan<- *uda.Entity) (string, error) {
	return "", errors.New("not implemented")
}

var _ ReadableDataset = (*PostgresDataset)(nil)
var _ WriteableDataset = (*PostgresDataset)(nil)

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
