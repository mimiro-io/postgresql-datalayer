package db

import (
	"errors"
	"fmt"
	"github.com/mimiro-io/postgresql-datalayer/internal/legacy/conf"
	"sort"
)

type ReadTable struct {
	ColumnMappings []*conf.ColumnMapping
	Types          []string
	Name           DatasetName
	query          string
}

type DatasetRequest struct {
	DatasetName string
	Since       string
	Limit       int64
}

type DatasetName string

func NewTable(layer *conf.Datalayer, name DatasetName) *ReadTable {
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

	return &ReadTable{
		Name:           name,
		ColumnMappings: tableMap.ColumnMappings,
		Types:          tableMap.Types,
		query:          tableMap.CustomQuery,
	}
}

func (t *ReadTable) Query(limit int64) string {
	limitQuery := ""
	if limit > 0 {
		limitQuery = fmt.Sprintf(" limit %d ", limit)
	}
	query := fmt.Sprintf("select * from %s%s;", t.Name, limitQuery)
	if t.query != "" {
		query = fmt.Sprintf(t.query, limitQuery)
	}

	return query
}

type WriteTable struct {
	Name          DatasetName
	TableName     string
	Fields        []*conf.FieldMapping
	IdColumn      string
	query         string
	deletionQuery string
}

func NewWriteTable(layer *conf.Datalayer, name DatasetName) (*WriteTable, error) {
	var tableMap *conf.PostMapping
	for _, table := range layer.PostMappings {
		if table.DatasetName == string(name) {
			tableMap = table
			break
		} else if table.TableName == string(name) { // fallback
			tableMap = table
		}
	}

	deletionQuery := fmt.Sprintf(`DELETE FROM %s WHERE id = $1;`, tableMap.TableName)
	if tableMap.IdColumn != "" {
		deletionQuery = fmt.Sprintf(`DELETE FROM %s WHERE %s = $1;`, tableMap.TableName, tableMap.IdColumn)
	}

	// sort and set up the fields, if any are present
	fields := tableMap.FieldMappings

	if len(fields) == 0 {
		return nil, errors.New("fields needs to be defined in the configuration")
	}

	//Only Sort Fields if SortOrder is set
	count := 0
	for _, field := range fields {
		if field.SortOrder == 0 {
			count++
		}
	}

	sort.SliceStable(fields, func(i, j int) bool {
		return fields[i].SortOrder < fields[j].SortOrder
	})

	return &WriteTable{
		Name:          name,
		TableName:     tableMap.TableName,
		IdColumn:      tableMap.IdColumn,
		Fields:        fields,
		query:         tableMap.Query,
		deletionQuery: deletionQuery,
	}, nil
}

func (t *WriteTable) Query() string {
	return t.query
}

func (t *WriteTable) DeletionQuery() string {
	return t.deletionQuery
}
