BIN      := overlay-bridge
PKG      := ./...
VERSION  ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
IMAGE    ?= ghcr.io/lightwebinc/$(BIN)
LDFLAGS  := -s -w -X main.Version=$(VERSION)

# Every target runs with the Go workspace OFF. This repository is a workspace
# member during development, so an on-workspace build resolves sibling modules
# to whatever is checked out locally, while CI and the Dockerfile resolve them
# from go.mod. Building green here while CI fails is the failure mode that
# costs the most time, so the two are made to agree by default.
export GOWORK = off

.PHONY: all build test race verify lint tidy fmt image clean help ci

all: build

build:                 ## build overlay-bridge on the host
	CGO_ENABLED=0 go build -trimpath -ldflags '$(LDFLAGS)' -o $(BIN) ./cmd/overlay-bridge

test:                  ## go test ./...
	go test -count=1 $(PKG)

race:                  ## go test -race ./...
	go test -race -count=1 $(PKG)

verify: fmt-check vet race licences   ## what CI runs: formatting, vet, tests under -race, licence freshness

fmt-check:             ## fail if anything is unformatted
	@out="$$(gofmt -l .)"; if [ -n "$$out" ]; then echo "unformatted:"; echo "$$out"; exit 1; fi

vet:                   ## go vet ./...
	go vet $(PKG)

lint:                  ## golangci-lint run
	golangci-lint run

tidy:
	go mod tidy

fmt:                   ## gofmt -w
	gofmt -w .

image:                 ## build the container image locally
	docker build --build-arg VERSION=$(VERSION) -t $(IMAGE):$(VERSION) .

clean:
	rm -f $(BIN)

ci: verify             ## alias, so CI and a developer run the same target

help:                  ## list targets
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST) | sort

licences:              ## fail if LICENSE-THIRD-PARTY is stale
	python3 scripts/gen-third-party-licenses.py . --check

licences-update:       ## regenerate LICENSE-THIRD-PARTY from what the binary links
	python3 scripts/gen-third-party-licenses.py .
