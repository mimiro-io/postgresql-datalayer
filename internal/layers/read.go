package layers

import (
	"context"
	"github.com/google/uuid"
	"github.com/jackc/pgproto3/v2"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/mimiro-io/postgresql-datalayer/internal/conf"
	"github.com/mimiro-io/postgresql-datalayer/internal/db"
	"go.uber.org/fx"
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

func NewLayer(lc fx.Lifecycle, cmgr *conf.ConfigurationManager, env *conf.Env) *Layer {
	layer := &Layer{}
	layer.cmgr = cmgr
	layer.logger = env.Logger.Named("layer")
	layer.Repo = &Repository{
		ctx: context.Background(),
	}
	_ = layer.ensureConnection("") // ok with error here

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

func (l *Layer) GetContext(datasetName string) map[string]any {
	tableDef := l.GetTableDefinition(datasetName)

	ctx := make(map[string]any)
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

func (l *Layer) Dataset(request db.DatasetRequest) (ReadableDataset, error) {
	err := l.ensureConnection(request.DatasetName)
	if err != nil {
		return nil, err
	}
	table := db.NewTable(l.cmgr.Datalayer, db.DatasetName(request.DatasetName))
	return NewPostgresDataset(l.Repo.DB, table, nil, request), nil
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

func (l *Layer) ensureConnection(dataset string) error {
	l.logger.Debug("Ensuring connection")
	if l.cmgr.State.Digest != l.Repo.digest {
		l.logger.Debug("Configuration has changed need to reset connection")
		if l.Repo.DB != nil {
			l.Repo.DB.Close() // don't really care about the error, as long as it is closed
		}
		db, err := l.connect(dataset) // errors are already logged
		if err != nil {
			return err
		}
		l.Repo.DB = db
		l.Repo.digest = l.cmgr.State.Digest
	}
	return nil
}

func (l *Layer) connect(dataset string) (*pgxpool.Pool, error) {
	var tableMap = l.Repo.tableDef
	for _, table := range l.cmgr.Datalayer.TableMappings {
		if table.TableName == dataset {
			tableMap = table
			break
		}
	}
	u := l.cmgr.Datalayer.GetUrl(nil, tableMap)
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
