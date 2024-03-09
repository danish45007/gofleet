package tests

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/danish45007/gofleet/internals/coordinator"
	pb "github.com/danish45007/gofleet/internals/grpcapi"
	"github.com/danish45007/gofleet/internals/scheduler"
	"github.com/danish45007/gofleet/internals/worker"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	postgresUser     = "postgres"
	postgresPassword = "postgres"
	postgresDb       = "scheduler"
	postgresHost     = "localhost"
)

type Cluster struct {
	coordinatorAddress string
	scheduler          *scheduler.SchedulerServer
	coordinator        *coordinator.CoordinatorServer
	workers            []*worker.WorkerServer
	database           testcontainers.Container
}

func getDbConnectionString() string {
	return fmt.Sprintf("postgres://%s:%s@%s:5432/%s",
		postgresUser, postgresPassword, postgresHost, postgresDb)
}

func startService(srv interface {
	Start() error
}) {
	go func() {
		if err := srv.Start(); err != nil {
			log.Fatalf("Error starting service: %v", err)
		}
	}()
}

func (c *Cluster) LaunchCluster(schedulerPort string, coordinatorPort string, workerCount int8) {
	// Launch Database
	if err := c.createDatabase(); err != nil {
		log.Fatalf("Error creating database: %v", err)
	}

	// Launch Coordinator Service
	c.coordinatorAddress = "localhost:" + coordinatorPort
	c.coordinator = coordinator.NewServer(coordinatorPort, getDbConnectionString())
	startService(c.coordinator)

	// Launch Scheduler Service
	c.scheduler = scheduler.NewServer(schedulerPort, getDbConnectionString())
	startService(c.scheduler)

	c.workers = make([]*worker.WorkerServer, workerCount)
	for i := 0; i < int(workerCount); i++ {
		c.workers[i] = worker.NewServer("", c.coordinatorAddress)
		startService(c.workers[i])
	}
	c.waitForWorkers()

}

func (c *Cluster) createDatabase() error {
	ctx := context.Background()

	// define the container request using the custom image
	req := testcontainers.ContainerRequest{
		Image:        "scheduler-postgres", // use the custom image
		ExposedPorts: []string{"5432:5432/tcp"},
		Env: map[string]string{
			"POSTGRES_PASSWORD": postgresPassword,
			"POSTGRES_USER":     postgresUser,
			"POSTGRES_DB":       postgresDb,
		},
		WaitingFor: wait.ForListeningPort("5432/tcp"),
	}
	// start the container
	var err error
	c.database, err = testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	return err
}

func (c *Cluster) StopCluster() {
	// stop each worker server
	for _, worker := range c.workers {
		if err := worker.Stop(); err != nil {
			log.Fatalf("Error stopping worker: %v", err)
		}
	}
	// stop the scheduler server
	if err := c.scheduler.Stop(); err != nil {
		log.Fatalf("Error stopping scheduler: %v", err)
	}
	// stop the coordinator server
	if err := c.coordinator.Stop(); err != nil {
		log.Fatalf("Error stopping coordinator: %v", err)
	}
	// stop the database container
	if err := c.database.Terminate(context.Background()); err != nil {
		log.Fatalf("Error stopping database: %v", err)
	}
}

// waitForWorkers waits for the worker pool to have the required number of workers
func (c *Cluster) waitForWorkers() {
	for {
		// acquire lock on the worker pool
		c.coordinator.WorkerPoolLock.Lock()
		// acquire a read lock on the worker pool keys
		c.coordinator.WorkerPoolKeyLock.RLock()
		// check if the worker pool has the required number of workers
		if len(c.coordinator.WorkerPool) == len(c.workers) {
			// release the read lock on the worker pool keys
			c.coordinator.WorkerPoolKeyLock.RUnlock()
			// release the lock on the worker pool
			c.coordinator.WorkerPoolLock.Unlock()
			break
		}
		// release the read lock on the worker pool keys
		c.coordinator.WorkerPoolKeyLock.RUnlock()
		// release the lock on the worker pool
		c.coordinator.WorkerPoolLock.Unlock()
		// sleep for 1 second
		time.Sleep(1 * time.Second)
	}
}

func (c *Cluster) CreateTestClient(coordinatorAddress string) (*grpc.ClientConn, pb.CoordinatorServiceClient) {
	// create a connection to the coordinator server with insecure credentials and block until the connection is established
	conn, err := grpc.Dial(coordinatorAddress, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		log.Fatalf("Error creating client connection: %v", err)
	}
	// create a client for the coordinator service
	client := pb.NewCoordinatorServiceClient(conn)
	return conn, client
}

// WaitForCondition waits for a condition to be true within a given timeout
func WaitForCondition(condition func() bool, timeout time.Duration, retryInterval time.Duration) error {
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	ticker := time.NewTicker(retryInterval)
	defer ticker.Stop()

	for {
		select {
		case <-timer.C:
			return fmt.Errorf("timeout exceeded")
		case <-ticker.C:
			if condition() {
				return nil
			}
		}
	}
}
