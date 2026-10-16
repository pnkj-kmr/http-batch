b:
	go build -o httpbatch main.go

r:
	go run main.go

run:
	./httpbatch -f ./input.json -w 10

release:
	goreleaser release --snapshot --clean 

build:
	goreleaser build --single-target --snapshot --clean

