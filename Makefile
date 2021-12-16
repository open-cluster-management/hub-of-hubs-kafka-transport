
# This makefile defines the following targets
#
#   - all (default) - formats the code and downloads vendor libs
#   - fmt - formats the code
#   - vendor - download all third party libraries and puts them inside vendor directory
#   - clean-vendor - removes third party libraries from vendor directory

.PHONY: all				##formats the code and downloads vendor libs
all: vendor fmt lint

.PHONY: fmt				##formats the code
fmt:
	@gci -w ./kafka-client/ ./headers/
	@go fmt ./kafka-client/... ./headers/...
	@gofumpt -w ./kafka-client/ ./headers/

.PHONY: vendor			##download all third party libraries and puts them inside vendor directory
vendor:
	@go mod vendor

.PHONY: clean-vendor			##removes third party libraries from vendor directory
clean-vendor:
	-@rm -rf vendor

.PHONY: lint				##runs code analysis tools
lint:
	go vet ./kafka-client/... ./headers/...
	golint ./kafka-client/... ./headers/...
	golangci-lint run ./kafka-client/... ./headers/...

.PHONY: help				##show this help message
help:
	@echo "usage: make [target]\n"; echo "options:"; \fgrep -h "##" $(MAKEFILE_LIST) | fgrep -v fgrep | sed -e 's/\\$$//' | sed -e 's/##//' | sed 's/.PHONY:*//' | sed -e 's/^/  /'; echo "";