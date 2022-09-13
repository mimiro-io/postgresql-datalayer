package layers

import (
	"context"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/mimiro-io/postgresql-datalayer/internal/conf"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"sort"
	"strings"
)

type PostLayer struct {
	cmgr     *conf.ConfigurationManager //
	logger   *zap.SugaredLogger
	PostRepo *PostRepository //exported because it needs to deferred from main??
}
type PostRepository struct {
	DB           *pgxpool.Pool
	ctx          context.Context
	postTableDef *conf.PostMapping
	digest       [16]byte
}

func NewPostLayer(lc fx.Lifecycle, cmgr *conf.ConfigurationManager, logger *zap.SugaredLogger) *PostLayer {
	postLayer := &PostLayer{logger: logger.Named("layer")}
	postLayer.cmgr = cmgr
	postLayer.PostRepo = &PostRepository{
		ctx: context.Background(),
	}

	_ = postLayer.ensureConnection()

	lc.Append(fx.Hook{
		OnStop: func(ctx context.Context) error {
			if postLayer.PostRepo.DB != nil {
				postLayer.PostRepo.DB.Close()
			}
			return nil
		},
	})

	return postLayer
}

func (postLayer *PostLayer) connect() (*pgxpool.Pool, error) {

	u := postLayer.cmgr.Datalayer.GetUrl(postLayer.PostRepo.postTableDef)

	conn, err := pgxpool.Connect(context.Background(), u.String())
	if err != nil {
		postLayer.logger.Warn("Error creating connection pool: ", err.Error())
		return nil, err
	}

	return conn, nil
}

func (postLayer *PostLayer) PostEntities(datasetName string, entities []*Entity) error {

	postLayer.PostRepo.postTableDef = postLayer.GetTableDefinition(datasetName)

	if postLayer.PostRepo.postTableDef == nil {
		return errors.New(fmt.Sprintf("No configuration found for dataset: %s", datasetName))
	}

	if postLayer.PostRepo.DB == nil {
		db, err := postLayer.connect() // errors are already logged
		if err != nil {
			return err
		}
		postLayer.PostRepo.DB = db
	}

	query := postLayer.PostRepo.postTableDef.Query
	if query == "" {
		postLayer.logger.Errorf("Please add query in config for %s in ", datasetName)
		return errors.New(fmt.Sprintf("no query found in config for dataset: %s", datasetName))
	}
	postLayer.logger.Debug(query)

	queryDel := postLayer.CreateDeleteStatement(strings.ToLower(postLayer.PostRepo.postTableDef.TableName), strings.ToLower(postLayer.PostRepo.postTableDef.IdColumn))
	postLayer.logger.Debug(queryDel)

	fields := postLayer.PostRepo.postTableDef.FieldMappings

	if len(fields) == 0 {
		postLayer.logger.Errorf("Please define all fields in config that is involved in dataset %s and query: %s", datasetName, query)
		return errors.New("fields needs to be defined in the configuration")
	}

	//Only Sort Fields if SortOrder is set
	count := 0
	for _, field := range fields {
		if field.SortOrder == 0 {
			count++
		}
	}
	if count >= 2 {
		postLayer.logger.Warn("No sort order is defined for fields in config, this might corrupt the query")
	} else {
		sort.SliceStable(fields, func(i, j int) bool {
			return fields[i].SortOrder < fields[j].SortOrder
		})
	}

	batch := &pgx.Batch{}
	postLayer.CreateBatch(entities, fields, batch, query, queryDel, postLayer.PostRepo.postTableDef.IdColumn)

	br := postLayer.PostRepo.DB.SendBatch(context.Background(), batch)
	_, err := br.Exec()
	if err != nil {
		err2 := br.Close()
		if err2 != nil {
			postLayer.logger.Error(err)
			return err2
		}
		return err
	}

	err = br.Close()
	if err != nil {
		return err
	}
	return nil
}

func (postLayer *PostLayer) CreateBatch(entities []*Entity, fields []*conf.FieldMapping, batch *pgx.Batch, query string, queryDel string, idColumn string) {
	for _, entity := range entities {
		s := entity.StripProps()
		args := make([]interface{}, len(fields)+1)
		if idColumn != "" {
			args = make([]interface{}, len(fields))
		} else {
			args[0] = strings.SplitAfter(entity.ID, ":")[1]
		}

		for i, field := range fields {
			if idColumn != "" {
				args[i] = s[field.FieldName]
			} else {
				args[i+1] = s[field.FieldName]
			}
		}
		if !entity.IsDeleted { //If is deleted True --> Do not store
			batch.Queue(query, args...)
		} else { //Should be deleted if it exists
			batch.Queue(queryDel, args[0])
		}
	}
}

func (PostLayer *PostLayer) CreateDeleteStatement(TableName string, IdColumn string) string {
	if IdColumn != "" {
		return fmt.Sprintf(`DELETE FROM %s WHERE %s = $1;`, TableName, IdColumn)
	} else {
		return fmt.Sprintf(`DELETE FROM %s WHERE id = $1;`, TableName)
	}
}

func (postLayer *PostLayer) GetTableDefinition(datasetName string) *conf.PostMapping {
	for _, table := range postLayer.cmgr.Datalayer.PostMappings {
		if table.DatasetName == datasetName {
			return table
		} else if table.TableName == datasetName { // fallback
			return table
		}
	}
	return nil
}

func (postLayer *PostLayer) ensureConnection() error {
	postLayer.logger.Debug("Ensuring connection")
	if postLayer.cmgr.State.Digest != postLayer.PostRepo.digest {
		postLayer.logger.Debug("Configuration has changed need to reset connection")
		if postLayer.PostRepo.DB != nil {
			postLayer.PostRepo.DB.Close() // don't really care about the error, as long as it is closed
		}
		db, err := postLayer.connect() // errors are already logged
		if err != nil {
			return err
		}
		postLayer.PostRepo.DB = db
		postLayer.PostRepo.digest = postLayer.cmgr.State.Digest
	}
	return nil
}
