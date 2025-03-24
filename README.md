# PostgreSQL Data Layer

A Data Layer for PostgreSQL (https://www.postgresql.org/) that conforms to the Universal Data API specification (<https://open.mimiro.io/specifications/uda/latest.html>). This data layer can be used in conjunction with the MIMIRO data hub (<https://github.com/mimiro-io/datahub>) to create a modern data fabric. The PostgreSQL data layer can be configured to expose tables and views from a PostgreSQL database as a stream of changes or a current snapshot. Rows in a table are represented in JSON according the Entity Graph Data model that is described in the UDA specification. This data layer can be run as a standalone binary or as a docker container.

Releases of this data layer are published to docker hub in the repository: `mimiro/postgresql-datalayer`

This layer supports both common configuration and the legacy configuration structure. It is recommended that any new deployments use the common config version.

## Testing

To test run:

```bash
make test
```

## Run

For legacy:

Ensure a config file exists in the location configured in the CONFIG_LOCATION variable

```bash
make docker
docker run -d -p 4343:4343 -v $(pwd)/local.config.json:/root/config.json -e PROFILE=dev -e CONFIG_LOCATION=file://config.json postgresql-datalayer
```

For common config (recommended):

Ensure a config file exists in the location configured in the DATALAYER_CONFIG_PATH variable

```bash
make docker
docker run -p 4343:4343 -v $(pwd)/resources/layer/config.json:/root/config/config.json -e DATALAYER_CONFIG_PATH=/root/config postgresql-datalayer /root/pgsql-layer
```

## Configuration

The service configuration is as follows:

```json5
{
    "layer_config": {
        "port": "17777",
        "service_name": "pgsql_service",
        "log_level": "DEBUG",
        "log_format": "json",
        "config_refresh_interval": "200s"
    },
    "system_config": {
        "user": "postgres",
        "password": "postgres",
        "database": "psql_test",
        "host": "localhost",
        "port": "5432"
    },
    "dataset_definitions": []
}
```

Dataset definitions are as follows:

```json5
{
    "name": "products",
    "source_config": {
        "table_name": "The name of the table to be exposed or written to",
        "since_column": "Optional. The name of the column to use to detect changes MUST be of type DateTime in the database",
        "flush_threshold": "int value with number of entities to update in a batch. recommended is 100 - 1000 depending on number of columns.",
        "entity_column" : "If the data being mapped contains a JSONB column that contains compliant entity graph data model entity it can be used by naming the column here. When doing so, incoming and outgoing mapped config MUST be omitted."
    },
    "incoming_mapping_config": {},
    "outgoing_mapping_config": {}
}
```

An alternative to naming the table is to provide a data query, the name of the table that contains the last modified timestamp and the column in that table with the modified column. The configuration in this scenario looks like this:

```json5
{
    "name": "products",
    "source_config": {
        "data_query" : "The query used to select data ",
        "since_table": "Optional. The name of the table containing the DateTime column that should be used for getting latest changes",
        "since_column": "Optional (unless since_table is provided). The name of the column to use to detect changes MUST be of type DateTime in the database",
        "flush_threshold": "int value with number of entities to update in a batch. recommended is 100 - 1000 depending on number of columns.",
        "entity_column" : "If the data being mapped contains a JSONB column that contains compliant entity graph data model entity it can be used by naming the column here. When doing so, incoming and outgoing mapped config MUST be omitted."
    },
    "incoming_mapping_config": {},
    "outgoing_mapping_config": {}
}
```

Please refer to the common config docs for incoming and outgoing config mappings.

Here is a complete config example:

```json5
{
    "layer_config": {
        "port": "17777",
        "service_name": "pgsql_service",
        "log_level": "DEBUG",
        "log_format": "json",
        "config_refresh_interval": "200s"
    },
    "system_config": {
        "user" : "postgres",
        "password" : "postgres",
        "database" : "psql_test",
        "host" : "localhost",
        "port" : "5432"
    },
    "dataset_definitions": [
        {
            "name": "products",
            "source_config": {
                "table_name" : "Product",
                "since_column" : "Timestamp",
                "flush_threshold": 5
            },
            "incoming_mapping_config": {
                "base_uri": "http://data.test.io/newtestnamespace/product/",
                "property_mappings": [
                    {
                        "property": "id",
                        "is_identity": true,
                        "strip_ref_prefix": true
                    },
                    {
                        "entity_property": "Product_Id",
                        "property": "product_id"
                    },
                    {
                        "entity_property": "ProductPrice",
                        "property": "productprice"
                    },
                    {
                        "entity_property": "Date",
                        "property": "date"
                    },
                    {
                        "entity_property": "Reporter",
                        "property": "reporter"
                    },
                    {
                        "entity_property": "Version",
                        "property": "version"
                    }
                ]
            },
            "outgoing_mapping_config": {
                "base_uri": "http://data.sample.org/",
                "property_mappings": [
                    {
                        "property": "id",
                        "is_identity": true,
                        "uri_value_pattern": "http://data.sample.org/things/{value}"
                    },
                    {
                        "entity_property": "product_id",
                        "property": "product_id"
                    }
                ]
            }
        },
        {
            "name": "customers",
            "source_config": {
                "table_name" : "Customer",
                "since_column" : "last_modified",
                "entity_column" : "entity"
            }
        }
    ]
}
```

Environment variables overrides for connection to Postgresql instance:

```bash
PGSQL_USER      # username
PGSQL_PASSWORD  # password
PGSQL_DATABASE  # database name
PGSQL_HOST      # database server
PGSQL_PORT      # port of database
```

## Legacy Configuration

By default, the service will read a configuration file from "local/settings.yaml". This is a convenience for local testing,
but can also be used for production if wanted. You can override this location by starting the server with the config param.

```bash
bin/server --config="/path/to/settings.yaml"
```

A complete example of a configuration file is provided as "example_settings.yaml" in the root directory.

The preferred method of configuring a production server is through environment variables.

All keys in the yaml file has their equivalent env key. The yaml keys are flattened, upper-cased, and snake_cased.

```bash
# the default server port, this will be overridden to 8080 in AWS
SERVER_PORT=4343

# how verbose the logger should be
LOG_LEVEL=INFO

# setting up token integration with Auth0
TOKEN_WELL_KNOWN=https://auth.yoursite.io/jwks/.well-known/jwks.json
TOKEN_AUDIENCE=https://api.yoursite.io
TOKEN_ISSUER=https://yoursite.auth0.com/

# statsd agent location, if left empty, statsd collection is turned off
DD_AGENT_HOST=

# if config is read from the file system, refer to the file here, for example "file://.config.json"
CONFIG_LOCATION=

# how often should the system look for changes in the configuration. This uses the cron system to
# schedule jobs at the given interval. If ommitted, the default is every 60s.
CONFIG_REFRESH_INTERVAL=@every 60s

#optional mssql db user and password. These should be provided from secrets injection, but they need
# to be here to be able to be picked up with viper.
POSTGRES_DB_USER=
POSTGRES_DB_PASSWORD=

```

By default the PROFILE is set to local, to easier be able to run on local machines. This also disables
security features, and must NOT be set to local in AWS. It should be PROFILE=dev or PROFILE=prod.

This also changes the loggers.

## Configuration

The server will start with a bad or missing configuration file, it has an empty
default file under resources/ that it will load instead, and in general a call
to a misconfigured server should just return empty results or 404's.

Every 60s (or otherwise configured) the server will look for updated config's, and
load these if it detect changes. It should also then "fix" it's connection if changed.

It supports configuration locations that either start with "file://" or "http(s)://".

The service is configured with either a local json file or a remote variant of the same.
It is strongly recommended leaving the Password and User fields empty.

A complete example can be found under "resources/test/test-config.json"

```json
{
  "DatabaseServer" : "[DB SERVER]",
  "Database" : "[DBNAME]",
  "Password" : "[ADD PASSWORD HERE]",
  "User" : "[USERNAME]",
  "BaseUri" : "http://data.test.io/testnamespace/",
  "Port" : "1433",
  "Schema" : "SalesLT",

  "TableMappings" : [
      {
          "TableName" : "Address",
          "EntityIdConstructor" : "addresses/%s",
          "Types" : [ "http://data.test.io/testnamespace/Address" ],
          "ColumnMappings" : {
              "AddressId" : {
                  "IsIdColumn" : true
              }
          }
      },
      {
          "TableName" : "Product",
          "EntityIdConstructor" : "products/%s",
          "Types" : [ "http://data.test.io/testnamespace/Product" ],
          "ColumnMappings" : {
              "ProductId" : {
                  "IsIdColumn" : true
              },
              "ProductCategoryID" : {
                  "IsReference" : true,
                  "ReferenceTemplate" : "http://data.test.io/testnamespace/categories/%s"
              }
          }
      },
      {
          "TableName" : "Customer",
          "EntityIdConstructor" : "customers/%s",
          "Types" : [ "http://data.test.io/testnamespace/Customer" ],
          "ColumnMappings" : {
              "CustomerId" : {
                  "IsIdColumn" : true
              },
              "PasswordHash" : {
                  "IgnoreColumn" : true
              },
              "PasswordSalt" : {
                  "IgnoreColumn" : true
              },
              "SalesPerson" : {
                  "PropertyName" : "SalesPersonName"
              }
          }
      }
  ] ,
    "postMappings": [
        {
            "idColumn": "used to define what column is the id column in the database table (will use id from entity if not defined)",
            "datasetName": "datahub.Testdata",
            "tableName": "Testdata",
            "query": "INSERT INTO Testdata (id, foo, bar) VALUES ($1, $2, $3) ON CONFLICT (id) DO UPDATE SET foo=$2, bar=$3;",
            "config": {
                "databaseServer": "[DB SERVER]",
                "database": "[DBNAME]",
                "port": "1234",
                "schema": "SalesLT",

                "user": {
                    "type": "direct",
                    "key": "[USERNAME]"
                },
                "password": {
                    "type": "direct",
                    "key": "[PASSWORD]"
                }
            },
            "fieldMappings": [
                {
                    "fieldName": "foo",
                    "order": 1
                },
                {
                    "fieldName": "bar",
                    "order": 2
                }
            ]
        }
    ]
}
```

Support for writing to postgres has been implemented.
Configuration for this is added to the above mentioned config.json file in the section postMappings.

The config section is optional, and is available to allows the layer to read and write to different databases.
The user and password can be retrieved from the environment or set directly in the config.
The latter is achieved by setting the type to direct, the value is then retrieved from the key.
When the config section in postMappings is empty or omitted, the top level database configuration will be used.

The order parameter in fieldMappings is used to retain the order of the fields, in regard to the query.
The id in the query is obtained from the entities' id, with the namespace stripped.
