build:
	go build -o bin/server cmd/postgres/main.go

run:
	go run cmd/postgres/main.go

docker:
	docker build . -t postgresql-datalayer

test:
	go vet ./...
	go test ./... -v

testlocal:
	go vet ./...
	go test ./... -v

integration:
	go test ./... -v -tags=integration
