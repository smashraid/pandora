package cmd

import (
	"log"

	"github.com/smashraid/pandora/config"
	"github.com/smashraid/pandora/internal/adapters/db"
	"github.com/smashraid/pandora/internal/adapters/grpc"
	"github.com/smashraid/pandora/internal/application/core/api"
)

func main() {
	dbAdapter, err := db.NewAdapter(config.GetDataSourceURL())
	if err != nil {
		log.Fatalf("Failed to connect to database. Error: %v", err)
	}

	application := api.NewApplication(dbAdapter)
	grpcAdapter := grpc.NewAdapter(application, config.GetApplicationPort())
	err = grpcAdapter.Run()
	if err != nil {
		log.Fatalf("Failed to run gRPC. Error: %v", err)
	}
}
