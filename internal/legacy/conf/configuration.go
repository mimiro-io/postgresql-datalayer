package conf

import (
	"fmt"
	"net/url"
	"os"
)

type Datalayer struct {
	Id             string          `json:"id" yaml:"id"`
	DatabaseServer string          `json:"databaseServer" yaml:"databaseServer"`
	BaseUri        string          `json:"baseUri" yaml:"baseUri"`
	Database       string          `json:"database" yaml:"database"`
	Port           string          `json:"port" yaml:"port"`
	Schema         string          `json:"schema" yaml:"schema"`
	BaseNameSpace  string          `json:"baseNameSpace" yaml:"baseNameSpace"`
	User           string          `json:"user" yaml:"user"`
	Password       string          `json:"password" yaml:"password"`
	TableMappings  []*TableMapping `json:"tableMappings" yaml:"tableMappings"`
	PostMappings   []*PostMapping  `json:"postMappings" yaml:"postMappings"`
}

type TableMapping struct {
	TableName           string           `json:"tableName" yaml:"tableName"`
	NameSpace           string           `json:"nameSpace" yaml:"nameSpace"`
	CustomQuery         string           `json:"query" yaml:"customQuery"`
	CDCEnabled          bool             `json:"cdcEnabled" yaml:"cdcEnabled"`
	EntityIdConstructor string           `json:"entityIdConstructor" yaml:"entityIdConstructor"`
	Types               []string         `json:"types" yaml:"types"`
	ColumnMappings      []*ColumnMapping `json:"columnMappings" yaml:"columnMappings"`
	Config              *TableConfig     `json:"config" yaml:"config"`
	Columns             map[string]*ColumnMapping
}

type ColumnMapping struct {
	FieldName         string           `json:"fieldName" yaml:"fieldName"`
	PropertyName      string           `json:"propertyName" yaml:"propertyName"`
	IsIdColumn        bool             `json:"isIdColumn" yaml:"isIdColumn"`
	IsReference       bool             `json:"isReference" yaml:"isReference"`
	IsEntity          bool             `json:"isEntity" yaml:"isEntity"`
	ReferenceTemplate string           `json:"referenceTemplate" yaml:"referenceTemplate"`
	IgnoreColumn      bool             `json:"ignoreColumn" yaml:"ignoreColumn"`
	IdTemplate        string           `json:"idTemplate" yaml:"idTemplate"`
	ColumnMappings    []*ColumnMapping `json:"columnMappings" yaml:"columnMappings"`
}

type PostMapping struct {
	DatasetName   string          `json:"datasetName" yaml:"datasetName"`
	TableName     string          `json:"tableName" yaml:"tableName"`
	Query         string          `json:"query" yaml:"query"`
	Config        *TableConfig    `json:"config" yaml:"config"`
	FieldMappings []*FieldMapping `json:"fieldMappings" yaml:"fieldMappings"`
	IdColumn      string          `json:"idColumn" yaml:"idColumn"`
}

type TableConfig struct {
	DatabaseServer *string         `json:"databaseServer" yaml:"databaseServer"`
	Database       *string         `json:"database" yaml:"database"`
	Port           *string         `json:"port" yaml:"port"`
	Schema         *string         `json:"schema" yaml:"schema"`
	User           *VariableGetter `json:"user" yaml:"user"`
	Password       *VariableGetter `json:"password" yaml:"password"`
}

type FieldMapping struct {
	FieldName        string `json:"fieldName" yaml:"fieldName"`
	ToPostgresField  string `json:"toPostgresField" yaml:"toPostgresField"`
	SortOrder        int    `json:"order" yaml:"sortOrder"`
	Type             string `json:"type" yaml:"type"`
	ResolveNamespace bool   `json:"resolveNamespace" yaml:"resolveNamespace"`
}

type VariableGetter struct {
	Type string `json:"type" yaml:"type"`
	Key  string `json:"key" yaml:"key"`
}

func (v *VariableGetter) GetValue() string {
	switch v.Type {
	case "direct":
		return v.Key
	default:
		return os.Getenv(v.Key)
	}
}

func (layer *Datalayer) GetUrl(postMapping *PostMapping, tableMapping *TableMapping) *url.URL {
	u := &url.URL{}
	if postMapping != nil {
		database := layer.Database
		port := layer.Port
		server := layer.DatabaseServer
		user := layer.User
		password := layer.Password
		scheme := layer.Schema
		if scheme == "" {
			scheme = "postgresql"
		}

		if postMapping.Config != nil {
			if postMapping.Config.Schema != nil {
				scheme = *postMapping.Config.Schema
			}
			if postMapping.Config.Database != nil {
				database = *postMapping.Config.Database
			}
			if postMapping.Config.Port != nil {
				port = *postMapping.Config.Port
			}
			if postMapping.Config.DatabaseServer != nil {
				server = *postMapping.Config.DatabaseServer
			}
			if postMapping.Config.User != nil {
				user = postMapping.Config.User.GetValue()
			}
			if postMapping.Config.Password != nil {
				password = postMapping.Config.Password.GetValue()
			}
		}

		u = &url.URL{
			Scheme: scheme,
			User:   url.UserPassword(user, password),
			Host:   fmt.Sprintf("%s:%s", server, port),
			Path:   database,
		}
	} else if tableMapping != nil {
		database := layer.Database
		port := layer.Port
		server := layer.DatabaseServer
		user := layer.User
		password := layer.Password
		scheme := layer.Schema
		if scheme == "" {
			scheme = "postgresql"
		}

		if tableMapping.Config != nil {
			if tableMapping.Config.Schema != nil {
				scheme = *tableMapping.Config.Schema
			}
			if tableMapping.Config.Database != nil {
				database = *tableMapping.Config.Database
			}
			if tableMapping.Config.Port != nil {
				port = *tableMapping.Config.Port
			}
			if tableMapping.Config.DatabaseServer != nil {
				server = *tableMapping.Config.DatabaseServer
			}
			if tableMapping.Config.User != nil {
				user = tableMapping.Config.User.GetValue()
			}
			if tableMapping.Config.Password != nil {
				password = tableMapping.Config.Password.GetValue()
			}
		}
		u = &url.URL{
			Scheme: scheme,
			User:   url.UserPassword(user, password),
			Host:   fmt.Sprintf("%s:%s", server, port),
			Path:   database,
		}
	}
	return u
}
