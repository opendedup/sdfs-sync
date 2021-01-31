PWD := $(shell pwd)
GOPATH := $(shell go env GOPATH)

GOARCH := $(shell go env GOARCH)
GOOS := $(shell go env GOOS)

VERSION ?= $(shell git describe --tags)
TAG ?= "sdfs/sdfssync:$(VERSION)"

all: build

checks:
	@echo "Checking dependencies"
	@(env bash $(PWD)/buildscripts/checkdeps.sh)

getdeps:
	@mkdir -p ${GOPATH}/bin
	@which golangci-lint 1>/dev/null || (echo "Installing golangci-lint" && curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(GOPATH)/bin v1.27.0)

crosscompile:
	@(env bash $(PWD)/buildscripts/cross-compile.sh)

verifiers: getdeps fmt lint

fmt:
	@echo "Running $@ check"
	@GO111MODULE=on gofmt -d cmd/
	@GO111MODULE=on gofmt -d pkg/

lint:
	@echo "Running $@ check"
	@GO111MODULE=on ${GOPATH}/bin/golangci-lint cache clean
	@GO111MODULE=on ${GOPATH}/bin/golangci-lint run --timeout=5m --config ./.golangci.yml



# Builds sdfssync locally.
build:
	@mkdir -p $(PWD)/build
	@echo "Building sdfssync binary to '$(PWD)/build/sdfssync'"
	@go build -o ./build/sdfssync app/* 

# Builds sdfssync and installs it to $GOPATH/bin.
install: build
	@echo "Installing sdfssync binary to '$(GOPATH)/bin/sdfssync'"
	@mkdir -p $(GOPATH)/bin && cp -f $(PWD)/build/sdfssync $(GOPATH)/bin/sdfssync
	@echo "Installation successful. To learn more, try \"sdfssync\"."

clean:
	@echo "Cleaning up all the generated files"
	@find . -name '*.test' | xargs rm -fv
	@find . -name '*~' | xargs rm -fv
	@rm -rvf sdfssync
	@rm -rvf build
	@rm -rvf release
	@rm -rvf .verify*
