package conf

import (
	"fmt"
	"go.uber.org/zap"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestLoadFile(t *testing.T) {

	cmgr := ConfigurationManager{
		logger: zap.NewNop().Sugar(),
	}

	_, err := cmgr.loadFile("file://../../resources/test/test-config.json")
	if err != nil {
		t.FailNow()
	}

}

func TestLoadUrl(t *testing.T) {
	srv := serverMock()
	defer srv.Close()

	cmgr := ConfigurationManager{
		logger:         zap.NewNop().Sugar(),
		TokenProviders: NoOpTokenProviders(),
	}

	_, err := cmgr.loadUrl(fmt.Sprintf("%s/test/config.json", srv.URL))
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
}

func TestParse(t *testing.T) {
	cmgr := ConfigurationManager{
		logger: zap.NewNop().Sugar(),
	}

	file := "file://../../resources/test/test-config.json"

	res, err := cmgr.loadFile(file)
	if err != nil {
		t.FailNow()
	}

	config, err := cmgr.parse(file, res)
	if err != nil {
		t.FailNow()
	}
	if config.Database != "psql_test" {
		t.Errorf("%s != psql_test", config.Database)
	}

}

func serverMock() *httptest.Server {
	handler := http.NewServeMux()
	handler.HandleFunc("/test/config.json", configMock)

	srv := httptest.NewServer(handler)

	return srv
}

func configMock(w http.ResponseWriter, r *http.Request) {
	cmgr := ConfigurationManager{
		logger: zap.NewNop().Sugar(),
	}
	res, _ := cmgr.loadFile("file://../../resources/test/test-config.json")
	_, _ = w.Write(res)
}
