package main

import (
	"flag"
	"log"

	"github.com/danish45007/gofleet/internals/common"
	"github.com/danish45007/gofleet/internals/coordinator"
)

var (
	coordinatorPort = flag.String("coordinator_port", "8082", "Port for the coordinator to listen on")
)

func main() {
	dbConnection := common.GetDBConnectionString()
	coordinatorServicer := coordinator.NewServer(*coordinatorPort, dbConnection)
	if err := coordinatorServicer.Start(); err != nil {
		log.Fatalf("Error starting the coordinator server: %v", err)
	}
}
