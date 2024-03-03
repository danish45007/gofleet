package main

import (
	"flag"
	"log"

	"github.com/danish45007/gofleet/internals/worker"
)

var (
	workerPort      = flag.String("worker_port", "", "Port for the worker to listen on")
	coordinatorPort = flag.String("coordinator_port", ":8080", "Port for the coordinator to listen on")
)

func main() {
	// Parse the command-line flags
	flag.Parse()
	workerServicer := worker.NewServer(*workerPort, *coordinatorPort)
	if err := workerServicer.Start(); err != nil {
		log.Fatalf("Error starting the worker server: %v", err)
	}
}
