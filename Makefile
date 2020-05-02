.PHONY: build docker test lint

build:
	go build -o bin/mongobetween .

docker:
	docker-compose up

test:
	go test -count 1 -race ./...

lint:
	GOGC=75 golangci-lint run --timeout 10m --concurrency 32 -v -E golint ./...
