.PHONY: test
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

bench:
	mkdir -p test
	go test -run=NOTHING -bench=. -count=10 ./internal/server > test/bench.txt
	GOBIN="$$PWD/bin" go get -u golang.org/x/perf/cmd/benchstat
	bin/benchstat last_bench.txt test/bench.txt > test/benchcmp.txt

