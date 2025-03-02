build:
	go build -o bin/server cmd/postgres/main.go
	go build -o bin/pgsql-layer-server cmd/layer/main.go

run:
	go run cmd/postgres/main.go

docker:
	docker build . -t postgresql-datalayer

test:
	go vet ./...
	go test ./... -v

integration:
	go test ./... -v -tags=integration
