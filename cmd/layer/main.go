package main

import (
	common "github.com/mimiro-io/common-datalayer"
	pgl "github.com/mimiro-io/postgresql-datalayer/internal/layer"
	"os"
)

func main() {
	configFolderLocation := ""
	args := os.Args[1:]
	if len(args) >= 1 {
		configFolderLocation = args[0]
	}

	common.NewServiceRunner(pgl.NewPgsqlDataLayer).WithConfigLocation(configFolderLocation).WithEnrichConfig(pgl.EnrichConfig).StartAndWait()
}
