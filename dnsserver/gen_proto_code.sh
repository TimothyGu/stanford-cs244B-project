#!/bin/bash

set -ex

go build google.golang.org/protobuf/cmd/protoc-gen-go
go build google.golang.org/grpc/cmd/protoc-gen-go-grpc

protoc --plugin="$(pwd)/protoc-gen-go" --go_out=. --go_opt=paths=source_relative \
    --plugin="$(pwd)/protoc-gen-go-grpc" --go-grpc_out=. --go-grpc_opt=paths=source_relative \
   internal/pkg/raftcache/raftcache.proto
