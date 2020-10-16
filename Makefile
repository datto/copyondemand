#  Ensure errors within piped commands are not suppressed
SHELL=/bin/bash -o pipefail

GOCMD=go
GOFMTCMD=gofmt
GOFIXCMD=$(GOCMD) tool fix
GOVETCMD=$(GOCMD) vet
GOBUILD=$(GOCMD) build
GOTEST=$(GOCMD) test
GOGET=$(GOCMD) get
CMD_ENTRY=$(shell find cmd/copyondemand/ -maxdepth 1 -name '*.go' -type f)
BINARY_NAME=copy-on-demand
BINDIR=/usr/bin
GO_FILES := $(shell find . -name '*.go' -type f -print | grep -v /vendor/)

.PHONY: all clean test gofmt gofix

all: help

help: ## Display this help screen
	@echo ""
	@echo "Welcome to the copy-on-demand Makefile!"
	@echo ""
	@echo "The following commands are supported:"
	@echo ""
	@grep -h -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

test: gofmt gofix ## Run gofmt and fix any detected errors

unit: gotestcover goracetest ## Run unit test coverage and basic race detection

gofmt:  ## Run gofmt on all of the go code in this project
	@touch gofmt.patch
	@test -z $(shell printf "%q" "$(shell $(GOFMTCMD) -d $(GO_FILES) | tee gofmt.patch /dev/stderr)")

gofix:  ## Run go fix on all of the go code in this project
	@touch gofix.patch
	@test -z $(shell printf "%q" "$(shell $(GOFIXCMD) -diff $(GO_FILES) | tee gofix.patch /dev/stderr)")

gotestcover: ## Run unit tests with coverage
	$(GOTEST) -cover .

goracetest: ## Run unit tests with race detector enabled
	$(GOTEST) -race .

build: clean  ## build the binary
	$(GOBUILD) $(GOFLAGS) -o .bin/$(BINARY_NAME) $(CMD_ENTRY)

install: .bin  ## install the binary
	install -Dpm 0755 .bin/$(BINARY_NAME) $(DESTDIR)$(BINDIR)/$(BINARY_NAME)

clean: ## Remove previous build
	@rm -rfv .bin
