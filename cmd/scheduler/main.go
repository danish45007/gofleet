package main

import (
	"flag"
	"log"

	"github.com/danish45007/gofleet/internals/common"
	"github.com/danish45007/gofleet/internals/scheduler"
)

var (
	schedulerPort = flag.String("scheduler_port", "8081", "Port for the scheduler to listen on")
)

func main() {
	dbConnection := common.GetDBConnectionString()
	schedulerServer := scheduler.NewServer(*schedulerPort, dbConnection)
	err := schedulerServer.Start()
	if err != nil {
		log.Fatalf("Error starting the scheduler server: %v", err)
	}
}
