package worker

import (
	"context"
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
	"github.com/danish45007/gofleet/internals/processor"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	taskProcessTime = 5 * time.Second // Simulate time taken to process a task
	workerPoolSize  = 5               // Number of workers in the worker pool
	taskQueueSize   = 100             // Size of the task queue
)

type WorkerServer struct {
	pb.UnimplementedWorkerServiceServer              // interface for the WorkerService that needs to be implemented
	id                                  uint32       // server id
	serverPort                          string       // port on which the worker server listens
	listener                            net.Listener //tcp listener for the worker server

	// coordinator service details
	coordinatorServiceAddress string                      // address of the coordinator service
	gRPCServer                *grpc.Server                // gRPC server for the worker
	coordinatorConnection     *grpc.ClientConn            // gRPC client connection details for the coordinator service
	coordinatorServiceClient  pb.CoordinatorServiceClient // gRPC client for the coordinator service

	heartbeatInterval time.Duration // interval at which the worker sends heart

	// tasks
	taskQueue         chan *pb.TaskRequest       // channel to receive tasks from the coordinator
	ReceivedTasksLock sync.Mutex                 // lock to protect the ReceivedTasks map
	ReceivedTasks     map[string]*pb.TaskRequest // map to keep track of received tasks

	ctx    context.Context    // the root context for all goroutines
	cancel context.CancelFunc // function to cancel the context
	wg     sync.WaitGroup     // waitGroup to wait for all goroutines to finish

}

func NewServer(port string, coordinatorServiceAddress string) *WorkerServer {
	ctx, cancel := context.WithCancel(context.Background())
	return &WorkerServer{
		id:                        uuid.New().ID(),
		serverPort:                port,
		coordinatorServiceAddress: coordinatorServiceAddress,
		heartbeatInterval:         common.DefaultHeartbeatInterval * time.Second,
		taskQueue:                 make(chan *pb.TaskRequest, taskQueueSize), // buffer channel of size taskQueueSize
		ReceivedTasks:             make(map[string]*pb.TaskRequest),
		ctx:                       ctx,
		cancel:                    cancel,
	}
}

// Start initializes the worker server and starts listening for tasks from the coordinator
func (w *WorkerServer) Start() error {
	//
	w.startWorkerPool(workerPoolSize)
	// connect to the coordinator service
	if err := w.connectToCoordinatorService(); err != nil {
		return err
	}
	defer w.coordinatorConnection.Close()
	// send periodic heartbeats
	go w.sendPeriodicHeartBeats()
	// start the gRPC server
	if err := w.startGrpcServer(); err != nil {
		return err
	}
	return w.awaitShutDown()
}

func (w *WorkerServer) connectToCoordinatorService() error {
	log.Println("Connecting to the coordinator service...")
	var err error
	// connect to the coordinator service
	// WithBlock returns a DialOption which makes caller of Dial blocks until the underlying connection is up.
	w.coordinatorConnection, err = grpc.Dial(w.coordinatorServiceAddress, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		log.Printf("Failed to connect to the coordinator service: %v\n", err)
		return err
	}
	// create a gRPC client for the coordinator service
	w.coordinatorServiceClient = pb.NewCoordinatorServiceClient(w.coordinatorConnection)
	log.Println("Connected to the coordinator service")
	return nil
}

func (w *WorkerServer) sendPeriodicHeartBeats() {
	// send periodic heartbeats to the coordinator
	ticker := time.NewTicker(w.heartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-w.ctx.Done():
			log.Println("Stopping periodic heartbeats")
			return
		case <-ticker.C:
			// send a heartbeat to the coordinator
			if err := w.sendHeartbeatToCoordinator(); err != nil {
				log.Printf("Error sending heartbeat to the coordinator: %v\n", err)
				return
			}
		}
	}
}

func (w *WorkerServer) sendHeartbeatToCoordinator() error {
	var err error
	workerAddress := os.Getenv("WORKER_ADDRESS")
	if workerAddress == "" {
		// fallback to listener address if WORKER_ADDRESS is not set
		workerAddress = w.listener.Addr().String()
	} else {
		// add the port to the worker address if not already present
		workerAddress += w.serverPort
	}
	// make a rpc call to the coordinator service to send the heartbeat
	_, err = w.coordinatorServiceClient.SendHeartbeat(w.ctx, &pb.HeartbeatRequest{
		WorkerId: w.id,
		Address:  workerAddress,
	})
	return err
}

func (w *WorkerServer) startGrpcServer() error {
	var err error
	// create a tcp listener

	// when serverPort is not set, find a free port using temporary socket
	if w.serverPort == "" {
		w.listener, err = net.Listen("tcp", ":0")                                // bind to a free port
		w.serverPort = fmt.Sprintf(":%d", w.listener.Addr().(*net.TCPAddr).Port) // update the serverPort with the free port
		if err != nil {
			log.Printf("Failed to find a free port: %v\n", err)
		}
	} else {
		w.listener, err = net.Listen("tcp", w.serverPort)
		if err != nil {
			log.Printf("Failed to listen on port %s: %v\n", w.serverPort, err)
		}
	}
	log.Printf("Starting worker server on port %s\n", w.serverPort)
	w.gRPCServer = grpc.NewServer()                 // create a new gRPC server
	pb.RegisterWorkerServiceServer(w.gRPCServer, w) // register the worker server with the gRPC server
	// start the gRPC server in a separate goroutine
	go func() {
		if err := w.gRPCServer.Serve(w.listener); err != nil {
			log.Printf("Failed to start gRPC server: %v\n", err)
		}
	}()
	return nil
}

// awaitShutDown waits for the shutdown signal and stops all the goroutines
func (w *WorkerServer) awaitShutDown() error {
	stop := make(chan os.Signal, 1)
	// notify the stop channel for the shutdown signals
	signal.Notify(stop, os.Interrupt, syscall.SIGINT)
	// wait for the shutdown signal
	<-stop
	return w.Stop()
}

// Stops gracefully stops the worker server and all the goroutines
func (w *WorkerServer) Stop() error {
	log.Println("Stopping worker server and goroutines...")
	// signal to stop all the goroutines
	w.cancel()

	// wait for all goroutines to finish
	w.wg.Wait()

	// close the gRPC related connections
	w.closeGRPConnection()
	log.Println("Stopped worker server and goroutines")
	return nil
}

func (w *WorkerServer) closeGRPConnection() {
	// close the gRPC server
	if w.gRPCServer != nil {
		w.gRPCServer.GracefulStop()
	}
	// close the listener
	if w.listener != nil {
		if err := w.listener.Close(); err != nil {
			log.Printf("Error while closing the listener: %v", err)
		}
	}
	// close the client connection with the coordinator
	if err := w.coordinatorConnection.Close(); err != nil {
		log.Printf("Error while closing client connection with coordinator: %v", err)
	}
}

// SubmitTask implements the WorkerServiceServer interface
func (w *WorkerServer) SubmitTask(ctx context.Context, request *pb.TaskRequest) (*pb.TaskResponse, error) {
	log.Printf("Received task: %s\n", request)
	w.ReceivedTasksLock.Lock()
	// add the received task to the ReceivedTasks map
	w.ReceivedTasks[request.GetTaskId()] = request
	w.ReceivedTasksLock.Unlock()
	// add the task to the taskQueue to be processed by the worker pool
	w.taskQueue <- request
	return &pb.TaskResponse{
		Message: "Task was submitted successfully",
		Success: true,
		TaskId:  request.GetTaskId(),
	}, nil
}

// startWorkerPool starts a pool of workers to process tasks
func (w *WorkerServer) startWorkerPool(poolSize int) {
	for i := 0; i < poolSize; i++ {
		w.wg.Add(1)
		go w.worker()
	}
}

// worker is a goroutine that processes tasks from the taskQueue
func (w *WorkerServer) worker() {
	defer w.wg.Done() // signal that the worker has finished
	for {
		select {
		case <-w.ctx.Done():
			log.Println("Stopping worker")
			return
		case task := <-w.taskQueue:
			// update the task status to started
			go w.updateTaskStatus(task, pb.TaskStatus_STARTED)
			// process the task
			w.processTask(task)
			// update the task status to completed
			go w.updateTaskStatus(task, pb.TaskStatus_COMPLETED)
		}
	}

}

func (w *WorkerServer) updateTaskStatus(task *pb.TaskRequest, status pb.TaskStatus) error {
	// update the task status to the given status
	// make a rpc call to the coordinator service to update the task status
	_, err := w.coordinatorServiceClient.UpdateTaskStatus(context.Background(), &pb.UpdateTaskStatusRequest{
		TaskId:      task.GetTaskId(),
		Status:      status,
		StartedAt:   time.Now().Unix(),
		CompletedAt: time.Now().Unix(),
	})
	return err
}

func (w *WorkerServer) processTask(task *pb.TaskRequest) {
	log.Printf("Processing task: %s\n", task)
	// simulate processing time
	time.Sleep(taskProcessTime)
	response := processor.Processor()
	log.Printf("Processed task: %s with status %s\n", task, response)
}
