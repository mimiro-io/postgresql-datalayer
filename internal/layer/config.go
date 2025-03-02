package layer

import (
	"encoding/json"
	"fmt"
	cdl "github.com/mimiro-io/common-datalayer"
	"strings"
)

const (
	// dataset mapping config
	TableName      = "table_name"
	FlushThreshold = "flush_threshold"
	AppendMode     = "append_mode"
	SinceColumn    = "since_column"
	EntityColumn   = "entity_column"
)

type PgsqlConf struct {
	Hostname string `json:"host"`
	Port     string `json:"port"`
	Database string `json:"database"`
	User     string `json:"user"`
	Password string `json:"password"`
	Schema   string `json:"schema"`
}

func newPgsqlConf(config *cdl.Config) (*PgsqlConf, cdl.LayerError) {
	c := &PgsqlConf{}
	nativeConfig := config.NativeSystemConfig
	configJson, _ := json.Marshal(nativeConfig)
	err := json.Unmarshal(configJson, c)

	if err != nil {
		return nil, cdl.Err(fmt.Errorf("could not unmarshal native system config because %s", err.Error()), cdl.LayerErrorInternal)
	}

	return c, nil
}

func (dl *PgsqlDatalayer) UpdateConfiguration(config *cdl.Config) cdl.LayerError {
	// update database connection
	dl.db, _ = newPgsqlDB(config)

	existingDatasets := map[string]bool{}
	// update existing datasets
	for k, v := range dl.datasets {
		for _, dsd := range config.DatasetDefinitions {
			if k == dsd.DatasetName {
				existingDatasets[k] = true
				v.datasetDefinition = dsd
				v.db = dl.db
			}
		}
	}

	// remove deleted datasets
	for k := range dl.datasets {
		if _, found := existingDatasets[k]; !found {
			delete(dl.datasets, k)
		}
	}

	// add new datasets
	for _, dsd := range config.DatasetDefinitions {
		if _, found := existingDatasets[dsd.DatasetName]; !found {
			dl.datasets[dsd.DatasetName] = &Dataset{
				logger:            dl.logger,
				db:                dl.db,
				datasetDefinition: dsd,
			}
		}
	}

	// convert all column names to uppercase
	for _, ds := range dl.datasets {
		if ds.datasetDefinition.OutgoingMappingConfig != nil {
			for _, pm := range ds.datasetDefinition.OutgoingMappingConfig.PropertyMappings {
				pm.Property = strings.ToLower(pm.Property)
			}
		}
	}

	return nil
}
