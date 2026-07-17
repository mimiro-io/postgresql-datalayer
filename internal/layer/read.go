package layer

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	cdl "github.com/mimiro-io/common-datalayer"
	egdm "github.com/mimiro-io/entity-graph-data-model"
	"reflect"
	"strconv"
	"strings"
	"time"
)

func (d *Dataset) Changes(since string, limit int, latestOnly bool) (cdl.EntityIterator, cdl.LayerError) {
	if latestOnly {
		// the layer does not know if the given table is a "change" table or not, so we cannot support this mode with confidence
		return nil, cdl.Err(fmt.Errorf("latest only operation not supported"), cdl.LayerNotSupported)
	}

	mapper := cdl.NewMapper(d.logger, d.datasetDefinition.IncomingMappingConfig, d.datasetDefinition.OutgoingMappingConfig)
	iter, err := d.newIterator(mapper, since, limit)
	if err != nil {
		return nil, err
	}

	return iter, nil
}

func (d *Dataset) Entities(from string, limit int) (cdl.EntityIterator, cdl.LayerError) {
	// the layer does not know if the given table is a "change" table or not, so implement /entities as /changes
	// TODO: consider adding source config options to allow for different behavior
	return d.Changes(from, limit, false)
}

func getStringConfigProperty(config map[string]interface{}, key string) string {
	val, ok := config[key]
	if !ok {
		return ""
	}
	valStr, ok := val.(string)
	if !ok {
		return ""
	}
	return valStr
}

func getBooleanConfigProperty(config map[string]interface{}, key string) bool {
	val, ok := config[key]
	if !ok {
		return false
	}

	valBool, ok := val.(bool)
	if !ok {
		return false
	}

	return valBool
}

func getNextSinceValue(rows *sql.Rows, datatype string) (string, error) {
	if datatype == "int" {
		var maxValue sql.NullInt64
		err := rows.Scan(&maxValue)
		if err != nil {
			return "", err
		}
		return strconv.FormatInt(maxValue.Int64, 10), nil
	} else if datatype == "float" {
		var maxValue sql.NullFloat64
		err := rows.Scan(&maxValue)
		if err != nil {
			return "", err
		}
		return strconv.FormatFloat(maxValue.Float64, 'f', -1, 64), nil
	} else if datatype == "string" {
		var maxValue sql.NullString
		err := rows.Scan(&maxValue)
		if err != nil {
			return "", err
		}
		return maxValue.String, nil
	} else if datatype == "time" {
		var maxValue sql.NullTime
		err := rows.Scan(&maxValue)
		if err != nil {
			return "", err
		}
		if maxValue.Valid {
			return maxValue.Time.Format("2006-01-02 15:04:05.000000"), nil
		} else {
			return "", nil
		}
	}
	return "", fmt.Errorf("unsupported datatype: %s", datatype)
}

func (d *Dataset) newIterator(mapper *cdl.Mapper, since string, limit int) (*dbIterator, cdl.LayerError) {
	entityColumn := getStringConfigProperty(d.datasetDefinition.SourceConfig, EntityColumn)
	sinceCol := getStringConfigProperty(d.datasetDefinition.SourceConfig, SinceColumn)
	sinceDatatype := getStringConfigProperty(d.datasetDefinition.SourceConfig, SinceDatatype)

	ctx := context.Background() // no timeout because we want to support long running stream operations

	db := d.db.db

	var nextToken string
	var maxSince string

	if sinceCol != "" {
		if sinceDatatype == "" {
			d.logger.Error("since datatype not set in source config")
			return nil, cdl.Err(fmt.Errorf("since datatype not set in source config"), cdl.LayerErrorInternal)
		}

		// since table
		sinceTable := getStringConfigProperty(d.datasetDefinition.SourceConfig, SinceTable)
		if sinceTable == "" {
			sinceTable = d.datasetDefinition.SourceConfig[TableName].(string)
		}

		// build max since query
		maxSinceQuery := "SELECT MAX(" + sinceCol + ") AS \"_MAX_SINCE\" FROM " + sinceTable
		rows, err := db.QueryContext(ctx, maxSinceQuery)
		if err != nil {
			return nil, cdl.Err(err, cdl.LayerErrorInternal)
		}
		defer func() {
			rows.Close()
		}()

		hasData := rows.Next()
		if !hasData {
			d.logger.Error("failed to get max since", "error", "no data")
			return nil, cdl.Err(fmt.Errorf("failed to get max since"), cdl.LayerErrorInternal)
		}

		newSince, err := getNextSinceValue(rows, sinceDatatype)
		if err != nil {
			d.logger.Error("failed to get max since", "error", err)
			return nil, cdl.Err(err, cdl.LayerErrorInternal)
		}

		// create encoded since
		nextToken = base64.URLEncoding.EncodeToString([]byte(newSince))

		// set max since
		maxSince = newSince
	}

	// build the query
	query, err := buildQuery(d.datasetDefinition, since, maxSince, sinceDatatype, limit)
	d.logger.Debug(fmt.Sprintf("changes query for dataset %s: %s", d.Name(), query), "dataset", d.Name())
	if err != nil {
		d.logger.Error("failed to build query", "error", err)
		return nil, ErrQuery(err)
	}

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		d.logger.Error("failed to execute query", "error", err)
		return nil, ErrQuery(err)
	}
	cts, err := rows.ColumnTypes()
	if err != nil {
		d.logger.Error("failed to get column types", "error", err)
		return nil, ErrQuery(err)
	}
	columns, err := rows.Columns()
	if err != nil {
		d.logger.Error("failed to get columns", "error", err)
		return nil, ErrQuery(err)
	}

	// lower case all the columne
	for i, col := range columns {
		columns[i] = strings.ToLower(col)
	}

	rowBuf := make([]any, 0, len(cts))
	for _, ct := range cts {
		dbType := ct.DatabaseTypeName()
		if dbType == "JSONB" || dbType == "JSON" {
			rowBuf = append(rowBuf, &json.RawMessage{})
			continue
		}

		st := ct.ScanType()
		if st == nil {
			d.logger.Error("no scan type for column", "column", ct.Name())
			return nil, ErrQuery(fmt.Errorf("no scan type for column %s", ct.Name()))
		}
		ex := reflect.New(st).Interface()
		switch ex.(type) {
		case *bool:
			rowBuf = append(rowBuf, &sql.NullBool{})
		case *int, *int32, *int64:
			rowBuf = append(rowBuf, &sql.NullInt64{})
		case *float32, *float64:
			rowBuf = append(rowBuf, &sql.NullFloat64{})
		case *time.Time:
			rowBuf = append(rowBuf, &sql.NullTime{})
		case *sql.NullInt32:
			rowBuf = append(rowBuf, &sql.NullInt32{})
		case *sql.NullTime:
			rowBuf = append(rowBuf, &sql.NullTime{})
		default:
			rowBuf = append(rowBuf, &sql.NullString{})
		}
	}

	return &dbIterator{
		logger:       d.logger,
		since:        since,
		limit:        limit,
		mapper:       mapper,
		rows:         rows,
		currentToken: nextToken,
		colTypes:     cts,
		columns:      columns,
		rowBuf:       rowBuf,
		sinceColumn:  sinceCol,
		entityColumn: entityColumn,
	}, nil
}

func buildQuery(definition *cdl.DatasetDefinition, since string, maxSince string, sinceDataType string, limit int) (string, error) {
	entityColumn := getStringConfigProperty(definition.SourceConfig, EntityColumn)
	sinceColumn := getStringConfigProperty(definition.SourceConfig, SinceColumn)
	sinceTable := getStringConfigProperty(definition.SourceConfig, SinceTable)
	dataQuery := getStringConfigProperty(definition.SourceConfig, DataQuery)

	cols := "*"
	if definition.OutgoingMappingConfig == nil {
		if entityColumn != "" {
			cols = "*"
		} else {
			return "", fmt.Errorf("outgoing mapping config is missing")
		}
	} else {
		if !definition.OutgoingMappingConfig.MapAll {
			cols = ""
			for _, pm := range definition.OutgoingMappingConfig.PropertyMappings {
				if len(cols) > 0 {
					cols = cols + ", "
				}
				cols = cols + pm.Property
			}
		}
	}

	var q string
	if dataQuery != "" {
		q = dataQuery
	} else {
		q = "SELECT " + cols + " FROM " + definition.SourceConfig[TableName].(string)
	}

	if maxSince != "" {
		if sinceTable != "" {
			connectTerm := " AND "
			if !strings.Contains(q, "WHERE") {
				connectTerm = " WHERE "
			}

			if since != "" {
				sinceValStr, err := base64.URLEncoding.DecodeString(since)
				if err != nil {
					return "", err
				}

				term := connectTerm + " %s.%s > %s AND %s.%s <= %s"
				q += fmt.Sprintf(term,
					sinceTable, sinceColumn, getQuotedValue(string(sinceValStr), sinceDataType),
					sinceTable, sinceColumn, getQuotedValue(maxSince, sinceDataType))
			} else {
				term := connectTerm + " %s.%s <= %s"
				q += fmt.Sprintf(term,
					sinceTable, sinceColumn, getQuotedValue(maxSince, sinceDataType))
			}
		} else if sinceColumn != "" {
			if since != "" {
				sinceValStr, err := base64.URLEncoding.DecodeString(since)
				if err != nil {
					return "", err
				}

				q += fmt.Sprintf(" WHERE %s.%s > %s AND %s.%s <= %s",
					definition.SourceConfig[TableName], definition.SourceConfig[SinceColumn], getQuotedValue(string(sinceValStr), sinceDataType),
					definition.SourceConfig[TableName], definition.SourceConfig[SinceColumn], getQuotedValue(maxSince, sinceDataType))
			} else {
				q += fmt.Sprintf(" WHERE %s.%s <= %s",
					definition.SourceConfig[TableName], definition.SourceConfig[SinceColumn], getQuotedValue(maxSince, sinceDataType))
			}
		}
	}
	if limit != 0 {
		q += " LIMIT " + strconv.Itoa(limit)
	}
	return q, nil
}

func getQuotedValue(val string, datatype string) string {
	switch datatype {
	case "int", "float":
		return val
	case "string":
		return "'" + val + "'"
	case "time":
		return "'" + val + "'"
	default:
		return "'" + val + "'"
	}
}

type dbIterator struct {
	logger       cdl.Logger
	mapper       *cdl.Mapper
	rows         *sql.Rows
	since        string
	currentToken string
	colTypes     []*sql.ColumnType
	rowBuf       []any
	columns      []string
	limit        int
	sinceColumn  string
	entityColumn string
}

func (it *dbIterator) Context() *egdm.Context {
	ctx := egdm.NewNamespaceContext()
	return ctx.AsContext()
}

func (it *dbIterator) Next() (*egdm.Entity, cdl.LayerError) {
	if it.rows.Next() {
		err := it.rows.Scan(it.rowBuf...)
		if err != nil {
			it.logger.Error("failed to scan row", "error", err)
			return nil, cdl.Err(err, cdl.LayerErrorInternal)
		}

		var entity *egdm.Entity
		if it.entityColumn == "" {

			entity = egdm.NewEntity()
			ri := &RowItem{
				Columns: it.columns,
				// Values:  it.rowBuf,
				Map: make(map[string]any),
			}
			for i, col := range it.columns {
				ri.Map[strings.ToLower(col)] = it.rowBuf[i]
			}

			err = it.mapper.MapItemToEntity(ri, entity)
			if err != nil {
				it.logger.Error("failed to map row", "error", err, "row", fmt.Sprintf("%+v", ri))
				return nil, cdl.Err(err, cdl.LayerErrorInternal)
			}
		} else {
			// read the entity column
			data := ""
			for i, col := range it.columns {
				if col == it.entityColumn {
					datax, err := json.Marshal(it.rowBuf[i])
					if err != nil {
						it.logger.Error("failed to marshal entity column", "error", err)
						return nil, cdl.Err(err, cdl.LayerErrorInternal)
					}
					data = string(datax)
					break
				}
			}

			// parse this into an entity
			parser := egdm.NewEntityParser(egdm.NewNamespaceContext()).WithExpandURIs()

			// add context to the entity json
			data = fmt.Sprintf("[{\"id\" : \"@context\", \"namespaces\" : {} }, %s ]", data)

			// create a reader from the data varaible
			datastream := strings.NewReader(data)

			err = parser.Parse(datastream, func(ent *egdm.Entity) error {
				entity = ent
				return nil
			}, func(continuation *egdm.Continuation) {

			})

			if err != nil {
				it.logger.Error("failed to parse entity", "error", err)
				return nil, cdl.Err(err, cdl.LayerErrorInternal)
			}

			if entity == nil {
				it.logger.Error("failed to parse entity", "error", "no entity")
				return nil, cdl.Err(fmt.Errorf("no entity"), cdl.LayerErrorInternal)
			}

		}

		return entity, nil

	} else {
		// exhausted or failed
		if it.rows.Err() != nil {
			it.logger.Error("failed to read rows", "error", it.rows.Err())
			return nil, cdl.Err(it.rows.Err(), cdl.LayerErrorInternal)
		}
		return nil, nil // end of result set
	}
}

func (it *dbIterator) Token() (*egdm.Continuation, cdl.LayerError) {
	cont := egdm.NewContinuation()
	if it.currentToken != "" {
		cont.Token = it.currentToken
	}
	return cont, nil
}

func (it *dbIterator) Close() cdl.LayerError {
	it.logger.Debug("closing iterator and db rows")
	err := it.rows.Close()
	it.logger.Debug("db rows closed")
	if err != nil {
		return cdl.Err(err, cdl.LayerErrorInternal)
	}
	return nil
}
