package layers

import (
	"context"
	"database/sql"
	"encoding/base64"
	"fmt"
	"github.com/jackc/pgproto3/v2"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/mimiro-io/postgresql-datalayer/internal/conf"
	"github.com/mimiro-io/postgresql-datalayer/internal/db"
	"go.uber.org/fx"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
)

type Layer struct {
	cmgr   *conf.ConfigurationManager
	logger *zap.SugaredLogger
	Repo   *Repository //exported because it needs to deferred from main
}

type Repository struct {
	DB       *pgxpool.Pool
	ctx      context.Context
	tableDef *conf.TableMapping
	digest   [16]byte
}

type DatasetRequest struct {
	DatasetName string
	Since       string
	Limit       int64
}

const jsonNull = "null"

func NewLayer(lc fx.Lifecycle, cmgr *conf.ConfigurationManager, env *conf.Env) *Layer {
	layer := &Layer{}
	layer.cmgr = cmgr
	layer.logger = env.Logger.Named("layer")
	layer.Repo = &Repository{
		ctx: context.Background(),
	}
	_ = layer.ensureConnection() // ok with error here

	lc.Append(fx.Hook{
		OnStop: func(ctx context.Context) error {
			if layer.Repo.DB != nil {
				layer.Repo.DB.Close()
			}
			return nil
		},
	})

	return layer
}

func (l *Layer) GetDatasetPostNames() []string {
	names := make([]string, 0)
	for _, table := range l.cmgr.Datalayer.PostMappings {
		names = append(names, table.DatasetName)
	}
	return names
}
func (l *Layer) GetDatasetNames() []string {
	names := make([]string, 0)
	for _, table := range l.cmgr.Datalayer.TableMappings {
		names = append(names, table.TableName)
	}
	return names
}

func (l *Layer) GetTableDefinition(datasetName string) *conf.TableMapping {
	for _, table := range l.cmgr.Datalayer.TableMappings {
		if table.TableName == datasetName {
			return table
		}
	}
	return nil
}

func (l *Layer) GetContext(datasetName string) map[string]interface{} {
	tableDef := l.GetTableDefinition(datasetName)
	ctx := make(map[string]interface{})
	namespaces := make(map[string]string)

	namespace := tableDef.TableName
	if tableDef.NameSpace != "" {
		namespace = tableDef.NameSpace
	}

	namespaces["ns0"] = l.cmgr.Datalayer.BaseNameSpace + namespace + "/"
	namespaces["rdf"] = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
	ctx["namespaces"] = namespaces
	ctx["id"] = "@context"
	return ctx
}

func (l *Layer) DoesDatasetExist(datasetName string) bool {
	names := l.GetDatasetNames()
	for _, name := range names {
		if name == datasetName {
			return true
		}
	}
	return false
}

func (l *Layer) ChangeSet(request db.DatasetRequest, callBack func(*Entity)) error {
	tableDef := l.GetTableDefinition(request.DatasetName)
	if tableDef == nil {
		l.er(fmt.Errorf("could not find defined dataset: %s", request.DatasetName))
		return nil
	}

	err := l.ensureConnection()
	if err != nil {
		return err
	}

	query := db.NewQuery(request, tableDef, l.cmgr.Datalayer)

	var rows pgx.Rows

	since, err := serverSince(l.Repo.DB)
	if err != nil {
		l.er(err)
		return err
	}

	rows, err = l.Repo.DB.Query(l.Repo.ctx, query.BuildQuery())
	defer func() {
		rows.Close()
	}()

	if err != nil {
		l.er(err)
		return err
	}

	// set up the row interface from the returned types

	for rows.Next() {
		//err = rows.Scan(nullableRowData...)
		values, err := rows.Values()

		if err != nil {
			l.er(err)
		} else {
			nullableRowData := buildRowType(rows.FieldDescriptions(), values, rows.RawValues())
			// map it
			entity := l.toEntity(nullableRowData, tableDef.ColumnMappings)

			if entity != nil {
				// add types to entity
				if len(tableDef.Types) == 1 {
					entity.References["rdf:type"] = tableDef.Types[0]
				} else if len(tableDef.Types) > 1 {
					// multiple types...
					// fix me
				}

				// call back function
				callBack(entity)
			}
		}

	}

	// only add continuation token if enabled
	if tableDef.CDCEnabled {
		entity := NewEntity()
		entity.ID = "@continuation"
		entity.Properties["token"] = since

		callBack(entity)
	}

	if err := rows.Err(); err != nil {
		l.er(err)
		return nil // this is already at the end, we don't care about this error now
	}

	// clean it up
	return nil
}

func buildRowType(colTypes []pgproto3.FieldDescription, values []interface{}, rawValues [][]byte) map[string]interface{} {
	nullableRowData := make(map[string]interface{})

	for i, d := range colTypes {
		switch d.DataTypeOID {
		case 2950:
			b, _ := uuid.FromBytes(rawValues[i])
			nullableRowData[string(d.Name)] = b
		default:
			nullableRowData[string(d.Name)] = values[i]
		}

	}

	return nullableRowData
}

func (l *Layer) er(err error) {
	l.logger.Warnf("Got error %s", err)
}

func (l *Layer) ensureConnection() error {
	l.logger.Debug("Ensuring connection")
	if l.cmgr.State.Digest != l.Repo.digest {
		l.logger.Debug("Configuration has changed need to reset connection")
		if l.Repo.DB != nil {
			l.Repo.DB.Close() // don't really care about the error, as long as it is closed
		}
		db, err := l.connect() // errors are already logged
		if err != nil {
			return err
		}
		l.Repo.DB = db
		l.Repo.digest = l.cmgr.State.Digest
	}
	return nil
}

func (l *Layer) connect() (*pgxpool.Pool, error) {

	u := &url.URL{
		Scheme: "postgresql",
		User:   url.UserPassword(l.cmgr.Datalayer.User, l.cmgr.Datalayer.Password),
		Host:   fmt.Sprintf("%s:%s", l.cmgr.Datalayer.DatabaseServer, l.cmgr.Datalayer.Port),
		Path:   l.cmgr.Datalayer.Database,
	}

	conn, err := pgxpool.Connect(context.Background(), u.String())
	if err != nil {
		l.logger.Warn("Error creating connection pool: ", err.Error())
		return nil, err
	}

	return conn, nil
}

// mapColumns remaps the ColumnMapping into Column
func mapColumns(columns []*conf.ColumnMapping) map[string]*conf.ColumnMapping {
	cms := make(map[string]*conf.ColumnMapping)

	for _, cm := range columns {
		cms[cm.FieldName] = cm
	}
	return cms

}

func (l *Layer) toEntity(data map[string]interface{}, columns []*conf.ColumnMapping) *Entity {
	entity := NewEntity()

	props := make(map[string]interface{})

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
				case map[string]interface{}:
					// is an object
					value = l.toEntity(v.(map[string]interface{}), colMapping.ColumnMappings)
				case []interface{}:
					// is a list of objects
					ents := make([]*Entity, 0)
					for _, obj := range v.([]interface{}) {
						ret := l.toEntity(obj.(map[string]interface{}), colMapping.ColumnMappings)
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
		entity.Properties = make(map[string]interface{})
		entity.References = make(map[string]interface{})
	}
	if entity.ID == "" { // this is invalid
		return nil
	}

	return entity
}

// serverSince queries the server for its time, this will be used as the source of the since to return
// when using cdc. The return value is Base64 encoded
func serverSince(db *pgxpool.Pool) (string, error) {
	var dt sql.NullTime
	err := db.QueryRow(context.Background(), "select current_timestamp;").Scan(&dt)
	if err != nil {
		return "", err
	}
	s := fmt.Sprintf("%s", dt.Time.Format(time.RFC3339))
	return base64.StdEncoding.EncodeToString([]byte(s)), nil
}

func toInt64(payload sql.RawBytes) (int64, error) {
	content := reflect.ValueOf(payload).Interface().(sql.RawBytes)
	data := string(content)                  //convert to string
	i, err := strconv.ParseInt(data, 10, 64) // convert to int or your preferred data type
	if err != nil {
		return 0, err
	}
	return i, nil
}

func ignoreColumn(column string, tableDef *conf.TableMapping) bool {

	if tableDef.CDCEnabled && strings.HasPrefix(column, "__$") {
		return true
	}
	return false
}
