.PHONY: protoc

protoc:
	rm -f ./proto/pb/*.go
	mkdir -p ./proto/pb
	protoc -I ./proto \
	--go_out ./proto/pb --go_opt paths=source_relative \
	--go-grpc_out ./proto/pb --go-grpc_opt paths=source_relative \
	proto/*.proto
