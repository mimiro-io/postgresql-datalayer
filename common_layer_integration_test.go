package postgres

import (
	"context"
	"github.com/docker/go-connections/nat"
	"github.com/jackc/pgx/v4"
	common "github.com/mimiro-io/common-datalayer"
	egdm "github.com/mimiro-io/entity-graph-data-model"
	pgl "github.com/mimiro-io/postgresql-datalayer/internal/layer"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"
)

var (
	conn             *pgx.Conn
	service          *common.ServiceRunner
	layerUrl         = "http://localhost:17777/datasets/products"
	customerLayerUrl = "http://localhost:17777/datasets/customers"
)

func setup(t *testing.T) testcontainers.Container {
	ctx := context.Background()
	req := testcontainers.ContainerRequest{
		Image:        "postgres",
		ExposedPorts: []string{"5432/tcp"},
		Env:          map[string]string{"POSTGRES_HOST_AUTH_METHOD": "trust", "POSTGRES_DB": "psql_test"},
		WaitingFor:   wait.ForLog("listening on IPv4 address"),
	}
	postgresC, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("Failed to start container: %v", err)
	}

	actualPort, _ := postgresC.MappedPort(ctx, nat.Port("5432/tcp"))
	ip, _ := postgresC.Host(ctx)
	port := actualPort.Port()

	service = common.NewServiceRunner(pgl.NewPgsqlDataLayer).WithConfigLocation("./resources/layer")
	service = service.WithEnrichConfig(func(config *common.Config) error {
		config.NativeSystemConfig["host"] = "localhost"
		config.NativeSystemConfig["port"] = port
		return nil
	})
	go service.StartAndWait()

	connString := "postgresql://postgres:postgres@" + ip + ":" + port + "/psql_test"
	conn, err = pgx.Connect(context.Background(), connString)
	if err != nil {
		t.Fatalf("Failed to connect to database: %v", err)
	}

	_, err = conn.Exec(context.Background(), `CREATE TABLE IF NOT EXISTS product (
		id INT PRIMARY KEY, product_id INT, productprice INT, date TIMESTAMP,
		reporter VARCHAR(15), timestamp TIMESTAMP, version INT);`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = conn.Exec(context.Background(), `CREATE TABLE IF NOT EXISTS customer (
		id VARCHAR PRIMARY KEY, entity JSONB, last_modified TIMESTAMP);`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = conn.Exec(context.Background(), `CREATE TABLE IF NOT EXISTS orders (
		id VARCHAR PRIMARY KEY, entity JSONB, sequence_no INT);`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// add some insert statements to add some entity objects to the customer table
	_, err = conn.Exec(context.Background(), `INSERT INTO customer (id, entity, last_modified) VALUES
                                                     		('http://data.example.io/customers/1', '{"id": "http://data.example.io/customers/1" }', NOW()),
                                                     		('http://data.example.io/customers/2', '{"id": "http://data.example.io/customers/2" }', NOW()),
                                                     		('http://data.example.io/customers/3', '{"id": "http://data.example.io/customers/3" }', NOW()),
                                                     		('http://data.example.io/customers/4', '{"id": "http://data.example.io/customers/4" }', NOW());
		`)

	_, err = conn.Exec(context.Background(), `INSERT INTO orders (id, entity, sequence_no) VALUES
                                                     		('http://data.example.io/customers/1', '{"id": "http://data.example.io/customers/1" }', 0),
                                                     		('http://data.example.io/customers/2', '{"id": "http://data.example.io/customers/2" }', 1),
                                                     		('http://data.example.io/customers/3', '{"id": "http://data.example.io/customers/3" }', 2),
                                                     		('http://data.example.io/customers/4', '{"id": "http://data.example.io/customers/4" }', 3);
		`)

	if err != nil {
		t.Fatalf("Failed to insert data into customer table: %v", err)
	}

	return postgresC
}

func teardown(t *testing.T, postgresC testcontainers.Container) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	conn.Close(ctx)
	cancel()
	postgresC.Terminate(ctx)
	service.Stop()
}

func TestDatasetEndpoint(t *testing.T) {
	postgresC := setup(t)
	defer teardown(t, postgresC)

	t.Run("Should accept a payload without error", func(t *testing.T) {
		fileBytes, err := os.ReadFile("./resources/test/testdata_1.json")
		if err != nil {
			t.Fatal(err)
		}
		payload := strings.NewReader(string(fileBytes))
		res, err := http.Post(layerUrl+"/entities", "application/json", payload)
		if err != nil || res.StatusCode != http.StatusOK {
			t.Fatalf("Unexpected response: %v", err)
		}
	})

	t.Run("Should return number of rows in table product", func(t *testing.T) {
		var count int
		if err := conn.QueryRow(context.Background(), "SELECT COUNT(*) FROM product").Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 10 {
			t.Fatalf("Expected 10 rows, got %d", count)
		}
	})

	t.Run("Should delete entities where deleted flag is true", func(t *testing.T) {
		var count int
		conn.QueryRow(context.Background(), "SELECT COUNT(*) FROM product WHERE id = 1").Scan(&count)
		if count != 1 {
			t.Fatalf("Expected one row with id=1, got %d", count)
		}

		fileBytes, _ := os.ReadFile("./resources/test/testdata_2.json")
		payload := strings.NewReader(string(fileBytes))
		res, err := http.Post(layerUrl+"/entities", "application/json", payload)
		if err != nil || res.StatusCode != http.StatusOK {
			t.Fatalf("Unexpected response: %v", err)
		}

		conn.QueryRow(context.Background(), "SELECT COUNT(*) FROM product").Scan(&count)
		if count != 9 {
			t.Fatalf("Expected 9 rows after deletion, got %d", count)
		}
	})

	t.Run("Should read changes back from table", func(t *testing.T) {
		fileBytes, _ := os.ReadFile("./resources/test/testdata_2.json")
		payload := strings.NewReader(string(fileBytes))
		http.Post(layerUrl+"/entities", "application/json", payload)

		res, err := http.Get(layerUrl + "/changes")
		if err != nil {
			t.Fatal(err)
		}

		// entity graph data model
		entityParser := egdm.NewEntityParser(egdm.NewNamespaceContext()).WithExpandURIs()
		ec, err := entityParser.LoadEntityCollection(res.Body)

		if err != nil {
			t.Fatal(err)
		}

		if len(ec.Entities) != 9 {
			t.Fatalf("Expected 9 entities, got %d", len(ec.Entities))
		}
	})

	t.Run("Should read changes based on continuation token", func(t *testing.T) {
		fileBytes, _ := os.ReadFile("./resources/test/testdata_1.json")
		payload := strings.NewReader(string(fileBytes))
		http.Post(layerUrl+"/entities", "application/json", payload)

		res, err := http.Get(layerUrl + "/changes")
		if err != nil {
			t.Fatal(err)
		}

		// entity graph data model
		entityParser := egdm.NewEntityParser(egdm.NewNamespaceContext()).WithExpandURIs()
		ec, err := entityParser.LoadEntityCollection(res.Body)

		if err != nil {
			t.Fatal(err)
		}

		if len(ec.Entities) != 10 {
			t.Fatalf("Expected 10 entities, got %d", len(ec.Entities))
		}

		// get the continuation token
		nextToken := ec.Continuation.Token

		// do a get with the continuation token
		res, err = http.Get(layerUrl + "/changes?since=" + nextToken)

		if err != nil {
			t.Fatal(err)
		}

		// entity graph data model
		entityParser = egdm.NewEntityParser(egdm.NewNamespaceContext()).WithExpandURIs()
		ec, err = entityParser.LoadEntityCollection(res.Body)

		if err != nil {
			t.Fatal(err)
		}

		if len(ec.Entities) != 0 {
			t.Fatalf("Expected 0 entities, got %d", len(ec.Entities))
		}

		// now send some updates in the form a delete
		fileBytes, _ = os.ReadFile("./resources/test/testdata_2.json")
		payload = strings.NewReader(string(fileBytes))
		http.Post(layerUrl+"/entities", "application/json", payload)

		// then fetch changes again, there should only be one
		res, err = http.Get(layerUrl + "/changes?since=" + nextToken)
		if err != nil {
			t.Fatal(err)
		}

		// entity graph data model
		entityParser = egdm.NewEntityParser(egdm.NewNamespaceContext()).WithExpandURIs()
		ec, err = entityParser.LoadEntityCollection(res.Body)

		if err != nil {
			t.Fatal(err)
		}

		if len(ec.Entities) != 9 {
			t.Fatalf("Expected 9 entity, got %d", len(ec.Entities))
		}
	})

	t.Run("Should read changes based on continuation token and query", func(t *testing.T) {
		fileBytes, _ := os.ReadFile("./resources/test/testdata_1.json")
		payload := strings.NewReader(string(fileBytes))
		layer2Url := "http://localhost:17777/datasets/products2"

		res, err := http.Post(layerUrl+"/entities", "application/json", payload)
		if err != nil {
			t.Fatal(err)
		}

		res, err = http.Get(layer2Url + "/changes")
		if err != nil {
			t.Fatal(err)
		}

		// entity graph data model
		entityParser := egdm.NewEntityParser(egdm.NewNamespaceContext()).WithExpandURIs()
		ec, err := entityParser.LoadEntityCollection(res.Body)

		if err != nil {
			t.Fatal(err)
		}

		if len(ec.Entities) != 10 {
			t.Fatalf("Expected 10 entities, got %d", len(ec.Entities))
		}

		// get the continuation token
		nextToken := ec.Continuation.Token

		// do a get with the continuation token
		res, err = http.Get(layerUrl + "/changes?since=" + nextToken)

		if err != nil {
			t.Fatal(err)
		}

		// entity graph data model
		entityParser = egdm.NewEntityParser(egdm.NewNamespaceContext()).WithExpandURIs()
		ec, err = entityParser.LoadEntityCollection(res.Body)

		if err != nil {
			t.Fatal(err)
		}

		if len(ec.Entities) != 0 {
			t.Fatalf("Expected 0 entities, got %d", len(ec.Entities))
		}

		// now send some updates in the form a delete
		fileBytes, _ = os.ReadFile("./resources/test/testdata_2.json")
		payload = strings.NewReader(string(fileBytes))
		http.Post(layerUrl+"/entities", "application/json", payload)

		// then fetch changes again, there should only be one
		res, err = http.Get(layerUrl + "/changes?since=" + nextToken)
		if err != nil {
			t.Fatal(err)
		}

		// entity graph data model
		entityParser = egdm.NewEntityParser(egdm.NewNamespaceContext()).WithExpandURIs()
		ec, err = entityParser.LoadEntityCollection(res.Body)

		if err != nil {
			t.Fatal(err)
		}

		if len(ec.Entities) != 9 {
			t.Fatalf("Expected 9 entity, got %d", len(ec.Entities))
		}
	})

	t.Run("Should read changes from entity column", func(t *testing.T) {

		res, err := http.Get(customerLayerUrl + "/changes")
		if err != nil {
			t.Fatal(err)
		}

		// entity graph data model
		entityParser := egdm.NewEntityParser(egdm.NewNamespaceContext()).WithExpandURIs()
		ec, err := entityParser.LoadEntityCollection(res.Body)

		if err != nil {
			t.Fatal(err)
		}

		if len(ec.Entities) != 4 {
			t.Fatalf("Expected 4 entities, got %d", len(ec.Entities))
		}

	})

	t.Run("Should error on bad from entity column", func(t *testing.T) {
		// ignore this test for now
		t.Skip()

		// insert
		_, err := conn.Exec(context.Background(), `INSERT INTO customer (id, entity, last_modified) VALUES
                                                     		('http://data.example.io/customers/7', ' { "id" : "http://data.example.io/customers/7", "refs" : {  "http://data.example.io/customers/worksfor" : null}   }', NOW());
		`)

		_, rerr := http.Get(customerLayerUrl + "/changes")
		if rerr == nil {
			t.Fatal(err)
		}
	})

	t.Run("Should read changes from empty table", func(t *testing.T) {
		// query to delete all rows in product table
		_, err := conn.Exec(context.Background(), "DELETE FROM product")
		if err != nil {
			t.Fatal(err)
		}

		layer2Url := "http://localhost:17777/datasets/products2"

		res, err := http.Get(layer2Url + "/changes")
		if err != nil {
			t.Fatal(err)
		}

		// entity graph data model
		entityParser := egdm.NewEntityParser(egdm.NewNamespaceContext()).WithExpandURIs()
		ec, err := entityParser.LoadEntityCollection(res.Body)

		if err != nil {
			t.Fatal(err)
		}

		if len(ec.Entities) != 0 {
			t.Fatalf("Expected 0 entities, got %d", len(ec.Entities))
		}
	})

	t.Run("Should read changes from entity column with int since", func(t *testing.T) {

		ordersUrl := "http://localhost:17777/datasets/orders"

		res, err := http.Get(ordersUrl + "/changes")
		if err != nil {
			t.Fatal(err)
		}

		// entity graph data model
		entityParser := egdm.NewEntityParser(egdm.NewNamespaceContext()).WithExpandURIs()
		ec, err := entityParser.LoadEntityCollection(res.Body)

		if err != nil {
			t.Fatal(err)
		}

		if len(ec.Entities) != 4 {
			t.Fatalf("Expected 4 entities, got %d", len(ec.Entities))
		}

		// insert new orders
		_, err = conn.Exec(context.Background(), `INSERT INTO orders (id, entity, sequence_no) VALUES
                                                     		('http://data.example.io/customers/5', '{"id": "http://data.example.io/customers/5" }', 5);`)

		token := ec.Continuation.Token

		res, err = http.Get(ordersUrl + "/changes?since=" + token)

		if err != nil {
			t.Fatal(err)
		}

		// entity graph data model
		entityParser = egdm.NewEntityParser(egdm.NewNamespaceContext()).WithExpandURIs()
		ec, err = entityParser.LoadEntityCollection(res.Body)
		if err != nil {
			t.Fatal(err)
		}

		if len(ec.Entities) != 1 {
			t.Fatalf("Expected 1 entity, got %d", len(ec.Entities))
		}

		if ec.Entities[0].ID != "http://data.example.io/customers/5" {
			t.Fatalf("Expected entity id http://data.example.io/customers/5, got %s", ec.Entities[0].ID)
		}

		// try again with since token
		token = ec.Continuation.Token
		res, err = http.Get(ordersUrl + "/changes?since=" + token)

		if err != nil {
			t.Fatal(err)
		}

		// entity graph data model
		entityParser = egdm.NewEntityParser(egdm.NewNamespaceContext()).WithExpandURIs()
		ec, err = entityParser.LoadEntityCollection(res.Body)
		if err != nil {
			t.Fatal(err)
		}

		if len(ec.Entities) != 0 {
			t.Fatalf("Expected 0 entity, got %d", len(ec.Entities))
		}
	})

}
