FROM golang:1.22.2-alpine3.19 as builder

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
FROM builder as build

# Copy the source from the current directory to the Working Directory inside the container
COPY cmd ./cmd 
COPY internal ./internal 
COPY resources ./resources
COPY *.go ./

# Build the Go app
RUN CGO_ENABLED=0 GOOS=linux go build -a -o server cmd/postgres/main.go

# tests
RUN go vet ./...
RUN go test ./... -v


#####################################################################################################
FROM scratch

WORKDIR /root/

COPY --from=builder /usr/share/zoneinfo /usr/share/zoneinfo
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=build /app/server .

ADD .env .
ADD resources/default-config.json resources/

# Expose port 8080 to the outside world
EXPOSE 8080

# set a non root user
USER 5678

CMD ["./server"]
