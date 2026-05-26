.PHONY: proto
proto:
	@echo "Generating protobuf code..."
	cd api && buf generate
	@echo "Done."

.PHONY: run
run:
	@echo "Starting gRPC server on port 8000..."
	go run cmd/scheduler/main.go

.PHONY: build
build:
	@echo "Building binary..."
	go build -o bin/scheduler cmd/scheduler/main.go
	@echo "Binary created: bin/scheduler"

.PHONY: clean
clean:
	@echo "Cleaning generated files..."
	rm -f api/loanflow/v1/*.pb.go
	rm -f buf.lock
	rm -rf bin/
	@echo "Done."

.PHONY: grpcurl
grpcurl:
	@echo "Calling SubmitApplication with test data..."
	grpcurl -plaintext -d '{"application_id":"test-123"}' \
		localhost:8000 loanflow.v1.LoanService/SubmitApplication

.PHONY: deps
deps:
	@echo "Installing dependencies..."
	go mod tidy
	go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
	go install github.com/fullstorydev/grpcurl/cmd/grpcurl@latest
	@echo "Done."

.PHONY: help
help:
	@echo "Available targets:"
	@echo "  proto      - Generate protobuf code using buf"
	@echo "  run        - Run the gRPC server"
	@echo "  build      - Build a binary into bin/"
	@echo "  clean      - Remove generated files and binaries"
	@echo "  grpcurl    - Call the SubmitApplication endpoint"
	@echo "  deps       - Install required Go tools and dependencies"
	@echo "  help       - Show this help message"