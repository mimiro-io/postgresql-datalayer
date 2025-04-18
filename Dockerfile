FROM golang:1.23-alpine3.21 AS builder

# Install git + SSL ca certificates.
# Git is required for fetching the dependencies.
# Ca-certificates is required to call HTTPS endpoints.
RUN apk update && apk add --no-cache git gcc musl-dev ca-certificates tzdata && update-ca-certificates

# Set the Current Working Directory inside the container
WORKDIR /app

# Copy go mod and sum files
COPY go.mod go.sum ./

# Download all dependencies. Dependencies will be cached if the go.mod and go.sum files are not changed
RUN go mod download

#####################################################################################################
FROM builder AS build

# Copy the source from the current directory to the Working Directory inside the container
COPY cmd ./cmd
COPY internal ./internal
COPY resources ./resources
COPY *.go ./

# Build the Go app
RUN go build -a -o server cmd/postgres/main.go
RUN go build -a -o pgsql-layer cmd/layer/main.go

# tests
RUN go vet ./...

#####################################################################################################
FROM alpine:latest

WORKDIR /root/

COPY --from=builder /usr/share/zoneinfo /usr/share/zoneinfo
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=build /app/server .
COPY --from=build /app/pgsql-layer .

ADD .env .
ADD resources/default-config.json resources/

# Expose port 8080 to the outside world
EXPOSE 8080

CMD ["./server"]
