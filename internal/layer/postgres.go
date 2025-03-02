package layer

import (
	"database/sql"
	_ "database/sql/driver"
	"fmt"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/stdlib"
	common "github.com/mimiro-io/common-datalayer"
)

type pgsqlDB struct {
	db *sql.DB
}

func newPgsqlDB(conf *common.Config) (*pgsqlDB, error) {
	c, err := newPgsqlConf(conf)
	if err != nil {
		return nil, err
	}

	connStr := fmt.Sprintf(
		"postgres://%s:%s@%s:%s/%s?sslmode=disable",
		c.User,
		c.Password,
		c.Hostname,
		c.Port,
		c.Database,
	)

	// config, cerr := pgx.ParseConfig("postgres://user:pass@localhost:5432/dbname?sslmode=disable")
	config, cerr := pgx.ParseConfig(connStr)
	if cerr != nil {
		return nil, ErrConnection(err)
	}

	// Create a driver.Connector from the pgx config.
	connector := stdlib.GetConnector(*config)

	// Use sql.OpenDB to get a *sql.DB from the connector.
	db := sql.OpenDB(connector)

	// Ping the database to verify DSN provided by the user.
	perr := db.Ping()
	if perr != nil {
		return nil, ErrConnection(perr)
	}

	return &pgsqlDB{db}, nil
}

type RowItem struct {
	Map     map[string]any
	Columns []string
	Values  []any
	deleted bool
}

func (r *RowItem) GetValue(name string) any {
	val := r.Map[name]
	switch v := val.(type) {
	case *sql.NullBool:
		return v.Valid && v.Bool
	case *sql.NullString:
		if v.Valid {
			return v.String
		} else {
			return nil
		}
	case *sql.NullInt64:
		if v.Valid {
			return v.Int64
		} else {
			return nil
		}
	case *sql.NullFloat64:
		if v.Valid {
			if v.Float64 == float64(int64(v.Float64)) {
				return int64(v.Float64)
			} else {
				return v.Float64
			}

		} else {
			return nil
		}
	case nil:
		return nil
	default:
		return "invalid type"
	}
}

func (r *RowItem) SetValue(name string, value any) {
	r.Columns = append(r.Columns, name)
	r.Values = append(r.Values, value)
	r.Map[name] = value
}

func (r *RowItem) NativeItem() any {
	return r.Map
}

func (r *RowItem) GetPropertyNames() []string {
	return r.Columns
}
