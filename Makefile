
# This makefile defines the following targets
#
#   - all (default) - formats the code and downloads vendor libs
#   - fmt - formats the code
#   - vendor - download all third party libraries and puts them inside vendor directory
#   - clean-vendor - removes third party libraries from vendor directory

.PHONY: all				##formats the code and downloads vendor libs
all: clean-vendor fmt lint vendor

.PHONY: fmt				##formats the code
fmt:
	@gci -w .
	@go fmt ./...
	@gofumpt -w .

.PHONY: vendor			##download all third party libraries and puts them inside vendor directory
vendor:
	@go mod vendor

.PHONY: clean-vendor			##removes third party libraries from vendor directory
clean-vendor:
	-@rm -rf vendor

.PHONY: lint				##runs code analysis tools
lint: clean-vendor
	go vet ./...
	golint ./...
	golangci-lint run ./...

.PHONY: help				##show this help message
help:
	@echo "usage: make [target]\n"; echo "options:"; \fgrep -h "##" $(MAKEFILE_LIST) | fgrep -v fgrep | sed -e 's/\\$$//' | sed -e 's/##//' | sed 's/.PHONY:*//' | sed -e 's/^/  /'; echo "";