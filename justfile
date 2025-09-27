set shell := ["bash", "-cu"]

# Lints and runs all tests
default: lint test

# Ensures that all tools required for local development are installed. Before running, ensure you have go installed as well as protoc
install-tools:
    go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.0.2

    go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.6
    go install github.com/bufbuild/buf/cmd/buf@v1.54.0
    go install connectrpc.com/connect/cmd/protoc-gen-connect-go@v1.18.1

# Stands up local development dependencies in docker
up:
    #!/usr/bin/env bash
    docker compose up -d

    if ! fdbcli -C foundation.cluster --exec status --timeout 1 ; then
        if ! fdbcli -C foundation.cluster --exec "configure new single ssd-redwood-1 ; status" --timeout 10 ; then 
            echo "Unable to configure new FDB cluster."
            exit 1
        fi
    fi

    echo "development environment is ready"

# Tears down the local development dependencies
down:
    docker compose down --remove-orphans

# Lints the code
lint *ARGS="./...":
    golangci-lint run --timeout 1m {{ARGS}}

# Builds and runs the Go executable
r CMD *ARGS:
    go run cmd/{{CMD}}/main.go {{ARGS}}

# Builds and runs the Go executable with the race detector enabled
run CMD *ARGS:
    go run -race cmd/{{CMD}}/main.go {{ARGS}}

# Runs the tests
t *ARGS="./...":
    go test -count=1 -covermode=atomic -coverprofile=test-coverage.out {{ARGS}}

# Runs the tests with the race detector enabled
test *ARGS="./...":
    just t -race {{ARGS}}

# run `just test` first, then run this to view test coverage
cover:
    go tool cover -html coverage.out

# Connects to the local foundationdb developement server
fdbcli:
    fdbcli -C foundation.cluster

# Generates protobuf sources
build-protos:
    #!/usr/bin/env bash
    set +x

    pushd internal/types > /dev/null

    buf lint
    buf generate
