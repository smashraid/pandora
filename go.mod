module github.com/smashraid/pandora

go 1.26.5

require (
	buf.build/gen/go/bufbuild/protovalidate/protocolbuffers/go v1.36.11-20260709200747-435963d16310.1
	github.com/google/uuid v1.6.0
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.33.0
	google.golang.org/genproto/googleapis/api v0.0.0-20261026165753-2ffab32bce7e
	google.golang.org/grpc v1.84.0
	google.golang.org/protobuf v1.36.12
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	go.uber.org/atomic v1.11.0 // indirect
)

require (
	github.com/kelseyhightower/envconfig v1.4.0
	github.com/lib/pq v1.12.3
	github.com/redis/go-redis/v9 v9.22.0
	golang.org/x/net v0.61.0 // indirect
	golang.org/x/sys v0.49.0 // indirect
	golang.org/x/text v0.43.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20261019180522-ce908d29299b // indirect
)
