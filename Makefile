.PHONY: all proto lint run build clean grpcurl deps help client

# Default target
all: help

## proto: Update dependencies and generate Protobuf code using Buf v2
proto:
	@echo "Updating dependencies and generating Protobuf code..."
	buf dep update
	buf generate
	@echo "Done."

## lint: Lint Protobuf schemas
lint:
	@echo "Linting Protobuf files..."
	buf lint

## run: Run the gRPC scheduler server locally
run:
	@echo "Starting LoanFlow Scheduler gRPC server..."
	go run cmd/scheduler/main.go

## build: Build binaries into bin/ directory
build:
	@echo "Building binaries..."
	@mkdir -p bin
	go build -o bin/scheduler cmd/scheduler/main.go
	@echo "Binary created: bin/scheduler"

## clean: Remove generated Protobuf code and compiled binaries
clean:
	@echo "Cleaning generated files and binaries..."
	rm -rf gen/
	rm -rf bin/
	@echo "Done."

## grpcurl: Submit a valid test loan application via grpcurl
grpcurl:
	@echo "Calling SubmitApplication endpoint on localhost:50051..."
	grpcurl -plaintext -d '{"application_id":"app-test-999","applicant_email":"jane.doe@example.com","requested_amount_cents":25000000,"priority":"PRIORITY_HIGH","documents":[{"document_id":"123e4567-e89b-12d3-a456-426614174000","type":"PAYSLIP","s3_url":"https://s3.amazonaws.com/bucket/payslip.pdf"}]}' localhost:50051 loanflow.v1.LoanDocumentProcessorService/SubmitApplication

## deps: Install required Go tools andBuf CLI
deps:
	@echo "Installing Go tools and dependencies..."
	go mod tidy
	go install github.com/bufbuild/buf/cmd/buf@latest
	go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
	go install github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-grpc-gateway@latest
	go install github.com/fullstorydev/grpcurl/cmd/grpcurl@latest
	@echo "Done."

## client: Run the example client (when implemented)
client:
	@echo "Running gRPC client..."
	go run cmd/client/main.go

## help: Show available Makefile targets
help:
	@echo "Available targets:"
	@sed -n 's/^##//p' $(MAKEFILE_LIST) | column -t -s ':' |  sed -e 's/^/ /'