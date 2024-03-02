package coordinator

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/danish45007/gofleet/internals/common"
	pb "github.com/danish45007/gofleet/internals/grpcapi"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	shutDownTimeout           = 5 * time.Second  // the time the server will wait for all goroutines to finish before shutting down
	defaultMaxHeartbeatMisses = 1                // the default number of times a worker can miss the heartbeat before it is removed from the worker pool
	scanIntervalTime          = 10 * time.Second // the interval at which the coordinator will scan db for new tasks that needs to be processed
	executableTimeDuration    = 30 * time.Second // the duration for which the task will be executed
	tnxTimeout                = 30 * time.Second // the timeout for the transaction
)

// ErrUnsupportedTaskStatus is a custom error
var ErrUnsupportedTaskStatus = errors.New("unsupported task status from upstream")

type Worker struct {
	heartbeatMisses     uint8                  // the number of times the worker has missed the heartbeat
	address             string                 // the address of the worker
	grpcConnection      *grpc.ClientConn       // the grpc connection to the worker
	workerServiceClient pb.WorkerServiceClient // the grpc client that will be used to send requests to the worker
}

type CoordinatorServer struct {
	// grpc server
	pb.UnimplementedCoordinatorServiceServer              // implements the grpc CoordinatorServiceServer interface
	serverProt                               string       // the port the server will listen on
	listener                                 net.Listener // the listener listens for incoming connections from clients
	grpcServer                               *grpc.Server // the grpc server that will handle the incoming connection request

	// worker pool
	WorkerPoolLock    sync.Mutex         // a lock that will be used to synchronize access to the worker pool
	WorkerPool        map[uint32]*Worker // the pool of workers that are connected to the coordinator
	WorkerPoolKeyLock sync.RWMutex       // a read write lock that will be used to synchronize access to the worker pool keys
	WorkerPoolKeys    []uint32           // the keys of the worker pool

	maxHeartbeatMisses uint8         // the number of times a worker can miss the heartbeat before it is removed from the worker pool
	heartbeatInterval  time.Duration // the interval at which the coordinator will send heartbeats to the workers
	roundRobinIndex    int           // the index of the worker in the worker pool that will be used to send the next request

	// db connection
	dbConnectionString string             // the connection string to the postgres database
	dbPool             *pgxpool.Pool      // the pool of connections to the postgres database
	ctx                context.Context    // the root context for all goroutines
	cancel             context.CancelFunc // the cancel function that will be used to cancel the root context
	wg                 sync.WaitGroup     // the wait group that will be used to wait for all goroutines to finish
}

// NewServer initializes a new CoordinatorServer and returns a new instance of it
func NewServer(serverPort, dbConnectionString string) *CoordinatorServer {
	ctx, cancel := context.WithCancel(context.Background())
	server := &CoordinatorServer{
		WorkerPool:         make(map[uint32]*Worker),
		maxHeartbeatMisses: defaultMaxHeartbeatMisses,
		heartbeatInterval:  common.DefaultHeartbeatInterval,
		dbConnectionString: dbConnectionString,
		serverProt:         serverPort,
		ctx:                ctx,
		cancel:             cancel,
	}
	return server
}

func (s *CoordinatorServer) removeMissedHeartbeatWorkers() {
	// acquire the lock to the worker pool
	s.WorkerPoolLock.Lock()
	defer s.WorkerPoolLock.Unlock()
	// iterate over the worker pool and remove the workers that have missed the heartbeat
	for workerID, worker := range s.WorkerPool {
		// check if the worker missed heartbeat more than the maxHeartbeatMisses
		if worker.heartbeatMisses > s.maxHeartbeatMisses {
			log.Printf("Removing inactive worker from the worker pool: %d\n", workerID)
			// close the grpc connection to the worker
			worker.grpcConnection.Close()
			delete(s.WorkerPool, workerID)

			// update the worker pool keys
			s.WorkerPoolKeyLock.Lock()
			workerCount := len(s.WorkerPoolKeys)
			s.WorkerPoolKeys = make([]uint32, 0, workerCount)
			for key := range s.WorkerPool {
				s.WorkerPoolKeys = append(s.WorkerPoolKeys, key)
			}
			s.WorkerPoolKeyLock.Unlock()
		} else {
			// increment the heartbeatMisses
			worker.heartbeatMisses++
		}

	}
}

// workerPoolManager is a goroutine that will manage the worker pool
func (s *CoordinatorServer) workerPoolManager() {
	s.wg.Add(1)
	defer s.wg.Done()
	// ticker that will be used to check if any worker has missed the heartbeat
	// ticker will check every maxHeartbeatMisses * heartbeatInterval
	ticker := time.NewTicker(time.Duration(s.maxHeartbeatMisses) * s.heartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			// check if any worker has missed the heartbeat
			// if a worker has missed the heartbeat, remove it from the worker pool
			s.removeMissedHeartbeatWorkers()
		}
	}

}

func (s *CoordinatorServer) startGrpcServer() error {
	var err error
	// listener listens for incoming connections from clients at the serverProt
	s.listener, err = net.Listen("tcp", s.serverProt)
	if err != nil {
		return err
	}
	log.Printf("Starting gRPC service on: %s\n", s.serverProt)
	// creates the new grpc server
	s.grpcServer = grpc.NewServer()
	// register the coordinator service with the grpc server
	pb.RegisterCoordinatorServiceServer(s.grpcServer, s)
	go func() {
		// start the grpc server and map the listener it for incoming connections
		if err := s.grpcServer.Serve(s.listener); err != nil {
			log.Fatalf("Failed to start gRPC server: %v", err)
		}
	}()
	return nil
}

// getAllScheduledTasks will get all the tasks that are scheduled to be executed
// in last intervalDuration seconds
func (s *CoordinatorServer) getAllScheduledTasks(ctx context.Context, txn pgx.Tx, intervalDuration int) (pgx.Rows, error) {
	// get all the tasks that are scheduled to be executed in the next taskScheduledInterval
	query := fmt.Sprintf(`
    SELECT id, command
    FROM tasks
    WHERE scheduled_at < (NOW() + INTERVAL '%d seconds')
        AND picked_at IS NULL
    ORDER BY scheduled_at
    FOR UPDATE SKIP LOCKED
	`, intervalDuration)
	rows, err := txn.Query(ctx, query)
	if err != nil {
		log.Printf("Failed to get the tasks: %v\n", err)
		return nil, err
	}
	return rows, nil
}

func (s *CoordinatorServer) updatePickupTimeForTask(ctx context.Context, txn pgx.Tx, taskId string) error {
	// update the picked_at field of the task
	query := `
		UPDATE tasks
		SET picked_at = NOW()
		WHERE id = $1
	`
	_, err := txn.Exec(ctx, query, taskId)
	if err != nil {
		return err
	}
	return nil
}

// executeAllScheduledTasks will execute all the tasks that are scheduled to be executed
func (s *CoordinatorServer) executeAllScheduledTasks() {
	/*
		txnCtx is the transaction context that will be used to execute the transaction
		with a timeout of 30 seconds
	*/
	txnCtx, cancel := context.WithTimeout(context.Background(), tnxTimeout)
	defer cancel()
	// start a new transaction
	txn, err := s.dbPool.Begin(txnCtx)
	if err != nil {
		log.Printf("Failed to start a new transaction: %v\n", err)
		return
	}
	// defer the rollback of the transaction
	defer func() {
		if err := txn.Rollback(txnCtx); err != nil && err.Error() != "tx is closed" {
			log.Printf("Tnx RollbackError: %v\n", err)
			log.Printf("Failed to rollback the transaction: %v\n", err)
		}
	}()
	// get all the tasks that are scheduled to be executed
	rows, err := s.getAllScheduledTasks(txnCtx, txn, int(executableTimeDuration.Seconds()))
	if err != nil {
		log.Printf("Error executing the query: %v\n", err)
		return
	}
	// defer the closing of the rows
	defer rows.Close()

	// iterate over the rows and execute the tasks

	// tasks holds the tasks that are scheduled to be executed
	var tasks []*pb.TaskRequest
	for rows.Next() {
		var id, command string
		if err := rows.Scan(&id, &command); err != nil {
			log.Printf("Error scanning the row: %v\n", err)
			continue
		}
		tasks = append(tasks, &pb.TaskRequest{
			TaskId: id,
			Data:   command,
		})
	}
	if err := rows.Err(); err != nil {
		log.Printf("Error iterating over the rows: %v\n", err)
		return
	}
	for _, task := range tasks {
		if err := s.submitTaskToWorker(task); err != nil {
			log.Printf("Failed to submit the task %s to the worker: %v\n", task.GetTaskId(), err)
		}
		// update the picked_at field of the task
		if err := s.updatePickupTimeForTask(txnCtx, txn, task.GetTaskId()); err != nil {
			log.Printf("Failed to update the picked_at field of the task: %v\n", err)
			continue
		}
	}
	// commit the transaction
	if err := txn.Commit(txnCtx); err != nil {
		log.Printf("Failed to commit the transaction: %v\n", err)
	}
}

// submitTaskToWorker submits the task to the available worker
func (s *CoordinatorServer) submitTaskToWorker(task *pb.TaskRequest) error {
	worker := s.getNextWorker()
	if worker == nil {
		return errors.New("no worker available to execute the task")
	}
	worker.workerServiceClient.SubmitTask(context.Background(), task)
	return nil
}

// getNextWorker returns the next worker available in the worker pool
func (s *CoordinatorServer) getNextWorker() *Worker {
	// get the next worker in the worker pool
	s.WorkerPoolKeyLock.RLock()
	defer s.WorkerPoolKeyLock.RUnlock()
	workerCount := len(s.WorkerPoolKeys)
	if workerCount == 0 {
		return nil
	}
	// using round robin to get the next worker
	workerID := s.WorkerPoolKeys[s.roundRobinIndex%workerCount]
	s.roundRobinIndex++
	return s.WorkerPool[workerID]
}

func (s *CoordinatorServer) scanDB() {
	// ticker that will be used to scan the db for new tasks at every scanIntervalTime
	ticker := time.NewTicker(scanIntervalTime)
	defer ticker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			log.Println("Shutting down the db scanner")
			return
		case <-ticker.C:
			// scan the db for new tasks
			go s.executeAllScheduledTasks()
		}
	}
}

// Start starts the coordinator server and underlying operations
func (s *CoordinatorServer) Start() error {
	var err error
	// initiates the worker pool manager
	go s.workerPoolManager()
	// start the grpc server
	if err := s.startGrpcServer(); err != nil {
		return fmt.Errorf("failed to start the gRPC server %v", err)
	}
	// connect to the postgres database
	s.dbPool, err = pgxpool.Connect(s.ctx, s.dbConnectionString)
	if err != nil {
		return err
	}
	// start the scan db goroutine
	go s.scanDB()
	return s.awaitShutDown()
}

// awaitShutDown waits for the server to shut down
func (s *CoordinatorServer) awaitShutDown() error {
	stops := make(chan os.Signal, 1)
	signal.Notify(stops, os.Interrupt, syscall.SIGINT)
	<-stops
	log.Println("Shutting down the coordinator server")
	return s.Stop()
}

// Stops gracefully stops the coordinator server
func (s *CoordinatorServer) Stop() error {
	// signal all goroutines to stop
	s.cancel()
	// wait for all goroutines to finish
	s.wg.Wait()

	// close the gRPC worker connections
	s.WorkerPoolLock.Lock()
	defer s.WorkerPoolLock.Unlock()
	for _, worker := range s.WorkerPool {
		// check if the grpc connection to the worker is not nil
		if worker.grpcConnection != nil {
			// close the grpc connection to the worker
			worker.grpcConnection.Close()
		}
	}
	// close the gRPC server
	if s.grpcServer != nil {
		s.grpcServer.GracefulStop()
	}
	// close the coordinator server listener
	if s.listener != nil {
		s.listener.Close()
	}
	// close the db connection
	if s.dbPool != nil {
		s.dbPool.Close()
	}
	return nil
}

// SubmitTask implements the CoordinatorServiceServer.SubmitTask method
// It submits the task to the worker
func (s *CoordinatorServer) SubmitTask(ctx context.Context, request *pb.ClientTaskRequest) (*pb.ClientTaskResponse, error) {
	data := request.GetData()
	taskId := uuid.New().String()
	task := &pb.TaskRequest{
		TaskId: taskId,
		Data:   data,
	}
	if err := s.submitTaskToWorker(task); err != nil {
		return nil, err
	}
	taskResponse := &pb.ClientTaskResponse{
		Message: fmt.Sprintf("Task %s submitted successfully", taskId),
		TaskId:  taskId,
	}
	return taskResponse, nil
}

// SendHeartbeat implements the CoordinatorServiceServer.SendHeartbeat method
func (s *CoordinatorServer) SendHeartbeat(ctx context.Context, request *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	// acquire the lock on the worker pool
	s.WorkerPoolLock.Lock()
	workerId := request.GetWorkerId()
	defer s.WorkerPoolLock.Unlock()
	// get the worker from worker pool using the worker id
	if worker, exists := s.WorkerPool[workerId]; exists {
		// reset the heartbeatMisses
		worker.heartbeatMisses = 0
	} else {
		// if the worker does not exist in the worker pool, add it to the worker pool
		log.Printf("Registering new worker: %d\n", workerId)
		// create a new grpc connection to the worker
		grpcConnection, err := grpc.Dial(request.GetAddress(), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, err
		}
		//update the worker pool with the new worker and its grpc connection
		s.WorkerPool[workerId] = &Worker{
			heartbeatMisses:     0,
			address:             request.GetAddress(),
			grpcConnection:      grpcConnection,
			workerServiceClient: pb.NewWorkerServiceClient(grpcConnection),
		}

		// update the worker pool keys
		s.WorkerPoolKeyLock.Lock()
		defer s.WorkerPoolKeyLock.Unlock()

		workerCount := len(s.WorkerPoolKeys)
		s.WorkerPoolKeys = make([]uint32, 0, workerCount)
		for key := range s.WorkerPool {
			s.WorkerPoolKeys = append(s.WorkerPoolKeys, key)
		}
		log.Printf("Registered new worker: %d\n", workerId)
	}
	return &pb.HeartbeatResponse{
		Acknowledged: true,
	}, nil
}

// UpdateTaskStatus implements the CoordinatorServiceServer.UpdateTaskStatus method
func (s *CoordinatorServer) UpdateTaskStatus(ctx context.Context, request *pb.UpdateTaskStatusRequest) (*pb.UpdateTaskStatusResponse, error) {
	taskId := request.GetTaskId()
	status := request.GetStatus()
	var targetColumn string
	var timestampColumn time.Time
	switch status {
	case pb.TaskStatus_COMPLETED:
		targetColumn = "completed_at"
		timestampColumn = time.Unix(request.GetCompletedAt(), 0)
	case pb.TaskStatus_FAILED:
		targetColumn = "failed_at"
		timestampColumn = time.Unix(request.GetFailedAt(), 0)
	case pb.TaskStatus_STARTED:
		targetColumn = "started_at"
		timestampColumn = time.Unix(request.GetStartedAt(), 0)
	default:
		log.Println("Invalid task status")
		return nil, ErrUnsupportedTaskStatus
	}
	// update the task status in the db
	updateQuery := fmt.Sprintf(`
		UPDATE tasks
		SET %s = $1
		WHERE id = $2
	`, targetColumn)
	_, err := s.dbPool.Exec(ctx, updateQuery, timestampColumn, taskId)

	if err != nil {
		log.Printf("failed to update the task %s status: %v\n", taskId, err)
		return nil, err
	}

	return &pb.UpdateTaskStatusResponse{
		Success: true,
	}, nil
}
