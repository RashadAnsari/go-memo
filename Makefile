.PHONY: format
format:
	find . -type f -name '*.go' -not -path './vendor/*' -exec gofmt -s -w {} +
	find . -type f -name '*.go' -not -path './vendor/*' -exec goimports -w -local github.com/RashadAnsari {} +

.PHONY: lint
lint:
	golangci-lint run ./...
