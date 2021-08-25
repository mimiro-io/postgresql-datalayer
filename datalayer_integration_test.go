// +build integration

package postgres

import (
	"context"
	"github.com/docker/go-connections/nat"
	"github.com/franela/goblin"
	"github.com/jackc/pgx/v4"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.uber.org/fx"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"
)

func TestIntegration(t *testing.T) {
	g := goblin.Goblin(t)

	var app *fx.App
	var conn *pgx.Conn

	layerUrl := "http://localhost:17777/datasets/Product"
	g.Describe("The dataset endpoint", func() {
		var postgresC testcontainers.Container
		g.Before(func() {
			ctx := context.Background()
			req := testcontainers.ContainerRequest{
				Image:        "postgres",
				ExposedPorts: []string{"5432/tcp"},
				Env:          map[string]string{"POSTGRES_HOST_AUTH_METHOD": "trust", "POSTGRES_DB": "psql_test"},
				WaitingFor:   wait.ForLog("listening on IPv4 address"),
			}
			var err error
			postgresC, err = testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
				ContainerRequest: req,
				Started:          true,
			})
			if err != nil {
				t.Error(err)
			}
			actualPort, _ := postgresC.MappedPort(ctx, nat.Port("5432/tcp"))
			ip, _ := postgresC.Host(ctx)
			port := actualPort.Port()

			testConf := replaceTestConf("./resources/test/postgres-local.json", ip, port, t)
			defer os.Remove(testConf.Name())
			os.Setenv("SERVER_PORT", "17777")
			os.Setenv("AUTHORIZATION_MIDDLEWARE", "noop")
			os.Setenv("CONFIG_LOCATION", "file://"+testConf.Name())
			os.Setenv("POSTGRES_DB_USER", "postgres")
			os.Setenv("POSTGRES_DB_PASSWORD", "postgres")
			connString := "postgresql://postgres:postgres@" + ip + ":" + port + "/psql_test"
			conn, err = pgx.Connect(context.Background(), connString)
			if err != nil {
				t.Error(err)
			}
			_, err = conn.Exec(context.Background(), "CREATE TABLE IF NOT EXISTS product "+
				"(id INT PRIMARY KEY, product_id INT, productprice INT, date TIMESTAMP, "+
				"reporter VARCHAR(15), timestamp TIMESTAMP, version INT);")
			if err != nil {
				t.Log(err)
			}
			oldOut := os.Stdout
			oldErr := os.Stderr
			devNull, _ := os.Open("/dev/null")
			os.Stdout = devNull
			os.Stderr = devNull

			app, _ = Start(context.Background())

			os.Stdout = oldOut
			os.Stderr = oldErr
			os.Unsetenv("SERVER_PORT")
		})
		g.After(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			conn.Close(ctx)
			err := app.Stop(ctx)
			cancel()
			g.Assert(err).IsNil()
			postgresC.Terminate(ctx)
		})
		g.It("Should accept a payload without error", func() {
			fileBytes, err := ioutil.ReadFile("./resources/test/testdata_1.json")
			g.Assert(err).IsNil()
			payload := strings.NewReader(string(fileBytes))
			res, err := http.Post(layerUrl+"/entities", "application/json", payload)
			g.Assert(err).IsNil()
			g.Assert(res).IsNotZero()
			g.Assert(res.Status).Eql("200 OK")
		})
		g.It("should return number of rows in table product", func() {
			var count int
			err := conn.QueryRow(context.Background(), "SELECT COUNT(*) FROM product").Scan(&count)
			g.Assert(err).IsNil()
			g.Assert(count).Equal(10)
		})
		g.It("should return values of fields in row", func() {
			rows, err := conn.Query(context.Background(), "SELECT * FROM product WHERE id = 1;")
			g.Assert(err).IsNil()
			defer rows.Close()
			rowSeen := false
			for rows.Next() {
				rowSeen = true
				values, errV := rows.Values()
				g.Assert(errV).IsNil()
				g.Assert(values).IsNotNil()
				g.Assert(values[0]).Equal(int32(1))
				g.Assert(values[1]).Equal(int32(3096))
				g.Assert(values[2]).Equal(int32(183))
				g.Assert(values[3].(time.Time).String()).Equal("2007-02-16 00:00:00 +0000 UTC")
				g.Assert(values[4]).Equal("4")
				g.Assert(values[5].(time.Time).String()).Equal("2007-09-09 00:00:00 +0000 UTC")
				g.Assert(values[6]).Equal(int32(0))
				g.Assert(len(values)).Equal(7)
			}

			g.Assert(rowSeen).IsTrue("Expected to hit at least one row")
		})
		g.It("should delete entities where deleted flag set to true when entity already exist in table", func() {
			//Assert that the one we want to delete is there
			var count int
			err := conn.QueryRow(context.Background(), "SELECT COUNT(*) FROM product WHERE id = 1").Scan(&count)
			g.Assert(err).IsNil()
			g.Assert(count).Equal(1)

			//Assert that there are 10 entities in the table already
			err = conn.QueryRow(context.Background(), "SELECT COUNT(*) FROM product").Scan(&count)
			g.Assert(err).IsNil()
			g.Assert(count).Equal(10)

			//Modify table entries
			fileBytes, err := ioutil.ReadFile("./resources/test/testdata_2.json")
			g.Assert(err).IsNil()
			payload := strings.NewReader(string(fileBytes))
			res, err := http.Post(layerUrl+"/entities", "application/json", payload)
			g.Assert(err).IsNil()
			g.Assert(res).IsNotZero()

			//Assert that one is deleted
			err = conn.QueryRow(context.Background(), "SELECT COUNT(*) FROM product").Scan(&count)
			g.Assert(err).IsNil()
			g.Assert(count).Equal(9)

			//Assert that the first entity with id=1 is the one that has been deleted
			err = conn.QueryRow(context.Background(), "SELECT COUNT(*) FROM product WHERE id = 1").Scan(&count)
			g.Assert(err).IsNil()
			g.Assert(count).Equal(0)
		})

		g.It("should not store entity if flag is set to deleted = true", func() {
			res, err := conn.Exec(context.Background(), "TRUNCATE TABLE product")
			g.Assert(err).IsNil()
			g.Assert(res).IsNotZero()

			fileBytes, err := ioutil.ReadFile("./resources/test/testdata_2.json")
			g.Assert(err).IsNil()
			payload := strings.NewReader(string(fileBytes))
			result, err := http.Post(layerUrl+"/entities", "application/json", payload)
			g.Assert(err).IsNil()
			g.Assert(result).IsNotZero()

			//Assert that one of entity is not stored
			var count int
			err = conn.QueryRow(context.Background(), "SELECT COUNT(*) FROM product").Scan(&count)
			g.Assert(err).IsNil()
			g.Assert(count).Equal(9)

			//Assert that the first entity with id=1 is the one that is not stored
			err = conn.QueryRow(context.Background(), "SELECT COUNT(*) FROM product WHERE id = 1").Scan(&count)
			g.Assert(err).IsNil()
			g.Assert(count).Equal(0)

		})
	})
}

func replaceTestConf(fileTemplate string, host string, port string, t *testing.T) *os.File {
	bts, err := ioutil.ReadFile(fileTemplate)
	if err != nil {
		t.Fatal(err)
	}
	content := strings.ReplaceAll(string(bts), "localhost", host)
	content = strings.ReplaceAll(content, "5432", port)
	tmpfile, err := ioutil.TempFile(".", "integration-test.json")
	if err != nil {
		t.Fatal(err)
	}

	if _, err := tmpfile.Write([]byte(content)); err != nil {
		t.Fatal(err)
	}
	if err := tmpfile.Close(); err != nil {
		t.Fatal(err)
	}
	return tmpfile
}
