package layer

import (
	"context"
	common "github.com/mimiro-io/common-datalayer"
	"os"
	"sort"
)

type PgsqlDatalayer struct {
	db       *pgsqlDB
	datasets map[string]*Dataset
	config   *common.Config
	logger   common.Logger
	metrics  common.Metrics
}

type Dataset struct {
	logger            common.Logger
	db                *pgsqlDB
	datasetDefinition *common.DatasetDefinition
}

func (d *Dataset) MetaData() map[string]any {
	return d.datasetDefinition.SourceConfig
}

func (d *Dataset) Name() string {
	return d.datasetDefinition.DatasetName
}

func (dl *PgsqlDatalayer) Stop(ctx context.Context) error {
	err := dl.db.db.Close()
	if err != nil {
		return err
	}

	return nil
}

func (dl *PgsqlDatalayer) Dataset(dataset string) (common.Dataset, common.LayerError) {
	ds, found := dl.datasets[dataset]
	if found {
		return ds, nil
	}
	return nil, ErrDatasetNotFound(dataset)
}

func (dl *PgsqlDatalayer) DatasetDescriptions() []*common.DatasetDescription {
	var datasetDescriptions []*common.DatasetDescription
	for key := range dl.datasets {
		datasetDescriptions = append(datasetDescriptions, &common.DatasetDescription{Name: key})
	}
	sort.Slice(datasetDescriptions, func(i, j int) bool {
		return datasetDescriptions[i].Name < datasetDescriptions[j].Name
	})
	return datasetDescriptions
}

func NewPgsqlDataLayer(conf *common.Config, logger common.Logger, metrics common.Metrics) (common.DataLayerService, error) {
	oracledb, err := newPgsqlDB(conf)
	if err != nil {
		return nil, err
	}
	l := &PgsqlDatalayer{
		datasets: map[string]*Dataset{},
		logger:   logger,
		metrics:  metrics,
		config:   conf,
		db:       oracledb,
	}
	err = l.UpdateConfiguration(conf)
	if err != nil {
		return nil, err
	}
	return l, nil
}

func EnrichConfig(config *common.Config) error {
	// read env for user and password
	user := os.Getenv("PGSQL_USER")
	password := os.Getenv("PGSQL_PASSWORD")
	database := os.Getenv("PGSQL_DATABASE")
	host := os.Getenv("PGSQL_HOST")
	port := os.Getenv("PGSQL_PORT")

	if user != "" {
		config.NativeSystemConfig["user"] = user
	}

	if password != "" {
		config.NativeSystemConfig["password"] = password
	}

	if database != "" {
		config.NativeSystemConfig["database"] = database
	}

	if host != "" {
		config.NativeSystemConfig["host"] = host
	}

	if port != "" {
		config.NativeSystemConfig["port"] = port
	}

	return nil
}
