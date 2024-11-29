proto:
	@protoc -I=. --go_out=./master/ --go-grpc_out=./master ./service.proto
	@protoc -I=. --go_out=./worker/ --go-grpc_out=./worker ./service.proto