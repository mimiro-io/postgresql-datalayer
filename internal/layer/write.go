package layer

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	common "github.com/mimiro-io/common-datalayer"
	egdm "github.com/mimiro-io/entity-graph-data-model"
)

func (d *Dataset) FullSync(ctx context.Context, batchInfo common.BatchInfo) (common.DatasetWriter, common.LayerError) {
	// TODO not supported (yet?)
	return nil, ErrNotSupported
}

func (d *Dataset) Incremental(ctx context.Context) (common.DatasetWriter, common.LayerError) {
	writer, err := d.newPgsqlWriter(ctx)
	if err != nil {
		return nil, err
	}

	berr := writer.begin()
	return writer, common.Err(berr, common.LayerErrorInternal)
}

func (d *Dataset) newPgsqlWriter(ctx context.Context) (*PgsqlWriter, common.LayerError) {
	mapper := common.NewMapper(d.logger, d.datasetDefinition.IncomingMappingConfig, d.datasetDefinition.OutgoingMappingConfig)
	db := d.db.db
	tableName, ok := d.datasetDefinition.SourceConfig[TableName].(string)
	if !ok {
		return nil, ErrGeneric("table name not found in source config for dataset %s", d.datasetDefinition.DatasetName)
	}
	flushThreshold := 1000
	flushThresholdOverride, ok := d.datasetDefinition.SourceConfig[FlushThreshold]
	if ok {
		flushThresholdF, ok := flushThresholdOverride.(float64)
		if !ok {
			return nil, ErrGeneric("flush threshold must be an integer")
		}
		flushThreshold = int(flushThresholdF)
	}
	idColumn := "id"
	for _, m := range d.datasetDefinition.IncomingMappingConfig.PropertyMappings {
		if m.IsIdentity {
			idColumn = m.Property
			break
		}
	}

	sinceColumn, _ := d.datasetDefinition.SourceConfig[SinceColumn].(string)

	return &PgsqlWriter{
		logger:         d.logger,
		mapper:         mapper,
		sinceColumn:    sinceColumn,
		db:             db,
		ctx:            ctx,
		table:          tableName,
		flushThreshold: flushThreshold,
		appendMode:     d.datasetDefinition.SourceConfig[AppendMode] == true,
		idColumn:       idColumn,
	}, nil
}

type PgsqlWriter struct {
	logger         common.Logger
	ctx            context.Context
	mapper         *common.Mapper
	db             *sql.DB
	tx             *sql.Tx
	table          string
	idColumn       string
	sinceColumn    string
	batch          strings.Builder
	deleteBatch    strings.Builder
	batchSize      int
	flushThreshold int
	appendMode     bool
}

func (o *PgsqlWriter) Write(entity *egdm.Entity) common.LayerError {
	item := &RowItem{Map: map[string]any{}}
	err := o.mapper.MapEntityToItem(entity, item)
	if err != nil {
		return common.Err(err, common.LayerErrorInternal)
	}
	// set the deleted flag, we always need this to do the right thing in upsert mode
	item.deleted = entity.IsDeleted

	// add delete statement to delete batch
	if o.deleteBatch.Len() == 0 {
		o.deleteBatch.WriteString("DELETE FROM ")
		o.deleteBatch.WriteString(o.table)
		o.deleteBatch.WriteString(" WHERE ")
	} else {
		o.deleteBatch.WriteString(" OR ")
	}
	o.deleteBatch.WriteString(o.idColumn)
	o.deleteBatch.WriteString(" = ")
	o.deleteBatch.WriteString(sqlVal(item.Map[o.idColumn]))

	// if the entity is deleted continue
	if entity.IsDeleted {
		return nil
	}

	err = o.insert(item)

	if err != nil {
		return common.Err(err, common.LayerErrorInternal)
	}
	if o.batchSize >= o.flushThreshold {
		err = o.flush()
		if err != nil {
			return common.Err(err, common.LayerErrorInternal)
		}
		o.batchSize = 0
		o.batch.Reset()
		o.deleteBatch.Reset()
	}
	return nil
}

func (o *PgsqlWriter) Close() common.LayerError {
	err := o.flush()
	if err != nil {
		return common.Err(err, common.LayerErrorInternal)
	}
	if o.tx != nil {
		err = o.tx.Commit()
		if err != nil {
			return common.Err(err, common.LayerErrorInternal)
		}
		o.logger.Debug("Transaction committed")
	}

	return nil
}

func sqlVal(v any) string {
	switch v.(type) {
	case string:
		return fmt.Sprintf("'%s'", v)
	case nil:
		return "NULL"
	case bool:
		return fmt.Sprintf("'%t'", v)
	default:
		return fmt.Sprintf("%v", v)
	}
}

func (o *PgsqlWriter) flush() error {
	if o.batchSize == 0 {
		return nil
	}

	// execute the batch
	delstmt := o.deleteBatch.String()
	stmt := o.batch.String()
	stmt = "BEGIN;\n\n" + delstmt + ";\n" + stmt
	stmt += ";\nCOMMIT;"

	o.logger.Debug(stmt)
	_, err := o.tx.ExecContext(o.ctx, stmt)
	if err != nil {
		if o.tx != nil {
			err2 := o.tx.Rollback()
			if err2 != nil {
				o.logger.Error("Failed to rollback transaction")
				return fmt.Errorf("failed to rollback transaction: %w, underlying: %w", err2, err)
			}
			o.logger.Debug("Transaction rolled back")
		}
		return err
	}

	return nil
}

func (o *PgsqlWriter) insert(item *RowItem) error {
	if o.batch.Len() == 0 {
		// Start building the INSERT statement
		o.batch.WriteString("INSERT INTO ")
		o.batch.WriteString(o.table)
		o.batch.WriteString(" (")
		for i, col := range item.Columns {
			if i > 0 {
				o.batch.WriteString(", ")
			}
			o.batch.WriteString("\"")
			o.batch.WriteString(strings.ToLower(col))
			o.batch.WriteString("\"")
		}

		if o.sinceColumn != "" {
			o.batch.WriteString(", \"")
			o.batch.WriteString(strings.ToLower(o.sinceColumn))
			o.batch.WriteString("\"")
		}

		o.batch.WriteString(") VALUES ")
	} else {
		// Add a comma before next set of values
		o.batch.WriteString(",")
	}

	// Build a single row of values in parentheses
	o.batch.WriteString(" (")
	for i, val := range item.Values {
		if i > 0 {
			o.batch.WriteString(", ")
		}
		o.batch.WriteString(sqlVal(val))
	}

	if o.sinceColumn != "" {
		o.batch.WriteString(", NOW()")
	}

	o.batch.WriteString(")")

	o.batchSize++
	return nil
}

func (o *PgsqlWriter) begin() error {
	tx, err := o.db.Begin()
	if err != nil {
		return err
	}
	o.tx = tx
	o.logger.Debug("Transaction started")
	return nil
}
