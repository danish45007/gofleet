package main

import (
	"flag"
	"log"

	"github.com/danish45007/gofleet/internals/common"
	"github.com/danish45007/gofleet/internals/scheduler"
)

var (
	schudulerPort = flag.String("schuduler_port", "8081", "Port for the schuduler to listen on")
)

func main() {
	dbConnection := common.GetDBConnectionString()
	schedulerServer := scheduler.NewServer(*schudulerPort, dbConnection)
	err := schedulerServer.Start()
	if err != nil {
		log.Fatalf("Error starting the scheduler server: %v", err)
	}
}
