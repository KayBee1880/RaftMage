.PHONY: build test race vet fmt

build:
	go build ./...

test:
	go test ./...

race:
	go test ./... -race

vet:
	go vet ./...

fmt:
	gofmt -l .
