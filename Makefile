SHELL := /bin/bash

build:
	go build -o bin/datahub-server cmd/datahub/main.go

run:
	go run cmd/datahub/main.go

docker:
	docker build . -t datahub

docker-test:
	docker build --target builder . -t datahub-build
	docker run datahub-build go test ./...

license:
	go get -u github.com/google/addlicense; addlicense -c "MIMIRO AS" $(shell find . -iname "*.go")

test:
	go vet ./...
	go test ./... -v
