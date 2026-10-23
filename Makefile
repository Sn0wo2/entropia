BINARY_NAME ?= $(notdir $(CURDIR))

.PHONY: lint lint-fix test build clean security ci release dry-release

lint:
	golangci-lint run

lint-fix:
	golangci-lint run --fix

test:
	go test -race ./...

build:
	go build -trimpath -ldflags "-s -w" -o $(BINARY_NAME) ./cmd/entropia

clean:
	rm -f $(BINARY_NAME) $(BINARY_NAME).exe

security:
	go run golang.org/x/vuln/cmd/govulncheck@latest ./...

ci: lint test security

release:
	goreleaser release --clean

dry-release:
	goreleaser release --snapshot --clean
