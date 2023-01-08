.PHONY: main cli all
.DEFAULT_GOAL := all

main: build/main
cli: build/cli
proto: proto/mp3.pb.go
all: main cli
gziptest: build/gziptest

build/gziptest:
	go build -o build/gziptest ./main/gziptest.go

clean:
	rm -rf build

build:
	mkdir -p build

build/cli: build ./main/cli.go
	go build  -o build/cli ./main/cli.go

build/main: build ./main/main.go
	go build  -o build/main ./main/main.go

proto/mp3.pb.go: proto/mp3.proto
	protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative  proto/mp3.proto
