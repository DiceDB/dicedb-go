GOLANGCI_LINT_VERSION := 1.60.1

VERSION := $(shell cat VERSION)

lint:
	gofmt -w .
	golangci-lint run ./...

generate:
	protoc --go_out=. --go-grpc_out=. protos/*.proto

test:
	go test -v ./...

release:
	git tag -a $(VERSION) -m "release $(VERSION)"
	git push origin $(VERSION)
