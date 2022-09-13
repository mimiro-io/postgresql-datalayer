package db

import "github.com/mimiro-io/postgresql-datalayer/internal/conf"

type Table struct {
	ColumnMappings []*conf.ColumnMapping
	Types          []string
	CDCEnabled     bool
	Name           DatasetName
}

type DatasetName string

func NewTable(layer *conf.Datalayer, name DatasetName) *Table {
	var tableMap *conf.TableMapping
	for _, table := range layer.TableMappings {
		if table.TableName == string(name) {
			tableMap = table
			break
		}
	}
	if tableMap == nil {
		return nil
	}

	return &Table{
		ColumnMappings: tableMap.ColumnMappings,
		Types:          tableMap.Types,
		CDCEnabled:     tableMap.CDCEnabled,
	}
}

func (*Table) Query() string {
	return ""
}
