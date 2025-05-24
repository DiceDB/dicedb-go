GOLANGCI_LINT_VERSION := 1.60.1

VERSION := $(shell cat VERSION)

lint:
	gofmt -w .
	golangci-lint run ./...

generate:
	protoc --go_out=. --go-grpc_out=. protos/*.proto
	protoc --go_out=. --go-grpc_out=. protos/wal/*.proto

test:
	go test ./...

release:
	git tag -a $(VERSION) -m "release $(VERSION)"
	git push origin $(VERSION)
