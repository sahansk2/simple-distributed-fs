.PHONY: querier clean logger all
.DEFAULT_GOAL := all

all: querier logger

clean:
	rm -rf ./build

querier:
	mkdir -p build
	go build -o build/querier querier.go

logger:
	mkdir -p build
	go build -o build/logger logger.go
