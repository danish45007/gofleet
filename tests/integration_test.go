package tests

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	pb "github.com/danish45007/gofleet/internals/grpcapi"
	"google.golang.org/grpc"
)

const (
	schedularPort                 = ":8081"
	coordinatorPort               = ":50050"
	coordinatorServiceAddress     = "localhost:50050"
	timeoutForWaitCondition       = 20 * time.Second
	retryIntervalForWaitCondition = 1 * time.Second
	workerCount                   = 2
	removedWorkerCount            = 1
	taskSubmitRequestCount        = 4
)

var (
	cluster Cluster
	conn    *grpc.ClientConn
	client  pb.CoordinatorServiceClient
)

// setup launches the test cluster and connects to the coordinator service
func setup(workerCount int8) {
	cluster = Cluster{}
	cluster.LaunchCluster(schedularPort, coordinatorPort, workerCount)
	conn, client = cluster.CreateTestClient(coordinatorServiceAddress)
}

func teardown() {
	cluster.StopCluster()
}

func TestMain(t *testing.T) {
	setup(workerCount)
	defer teardown()
	// client sends POST request to scheduler to create a job
	postData := map[string]interface{}{
		"command":      "echo 'hello world'",
		"scheduled_at": "2024-03-10T22:34:00+05:30",
	}
	// encode postData to JSON
	postDataBytes, _ := json.Marshal(postData)
	// send POST request to scheduler service with postData as body
	res, err := http.Post("http://localhost:8081/schedule", "application/json", strings.NewReader(string(postDataBytes)))
	if err != nil {
		t.Fatalf("Error sending POST request to scheduler service: %v", err)
	}
	// close the response body after the function returns
	defer res.Body.Close()
	// parse response body
	var response map[string]interface{}
	json.NewDecoder(res.Body).Decode(&response)
	// check if the response contains the task_id
	if _, ok := response["task_id"]; !ok {
		t.Fatalf("Response does not contain task_id")
	}
	taskID := response["task_id"].(string)
	// wait for the job to be picked up by a worker
	err = WaitForCondition(func() bool {
		// send GET request to coordinator service to get the status of the job
		getJobStatusURL := "http://localhost:8081/task_status?task_id=" + url.QueryEscape(taskID)
		res, err := http.Get(getJobStatusURL)
		if err != nil {
			t.Fatalf("Error sending GET request to schedular service: %v", err)
		}
		// close the response body after the function returns
		defer res.Body.Close()
		// parse response body
		var response map[string]interface{}
		json.NewDecoder(res.Body).Decode(&response)
		// check if the response contains the status keys "picked_at", "started_at", and "completed_at"
		_, pickedAtExists := response["picked_at"]
		_, startedAtExists := response["started_at"]
		_, completedAtExists := response["completed_at"]
		return pickedAtExists && startedAtExists && completedAtExists
	}, timeoutForWaitCondition, retryIntervalForWaitCondition)
	if err != nil {
		t.Fatalf("Response does not contain picked_at, started_at, and completed_at keys")
	}
}

func TestUnavailableWorker(t *testing.T) {
	setup(workerCount)
	defer teardown()
	// stop the worker servers
	for _, worker := range cluster.workers {
		worker.Stop()
	}
	err := WaitForCondition(func() bool {
		// coordinator client makes a gRPC request to worker service to submit a task
		_, err := client.SubmitTask(context.Background(), &pb.ClientTaskRequest{
			Data: "test data",
		})
		return err != nil && err.Error() == "rpc error: code = Unknown desc = no worker available to execute the task"
	}, timeoutForWaitCondition, retryIntervalForWaitCondition)
	if err != nil {
		t.Fatalf("Coordinator did not clean up the workers within SLO. Error: %s", err.Error())
	}
}

func TestCoordinatorFailOverForInactiveWorkers(t *testing.T) {
	setup(workerCount)
	defer teardown()

	// stop 1 worker server
	cluster.workers[0].Stop()
	err := WaitForCondition(func() bool {
		// acquire lock on the worker pool
		cluster.coordinator.WorkerPoolLock.Lock()
		// check if the worker pool has the required number of workers
		activeWorkerCount := len(cluster.coordinator.WorkerPool)
		// release the lock on the worker pool
		cluster.coordinator.WorkerPoolLock.Unlock()
		return activeWorkerCount == workerCount-removedWorkerCount
	}, timeoutForWaitCondition, retryIntervalForWaitCondition)
	if err != nil {
		t.Fatalf("Coordinator did not clean up the workers within SLO. Error: %s", err.Error())
	}
	// submit 4 tasks
	for i := 0; i < taskSubmitRequestCount; i++ {
		// coordinator client makes a gRPC request to worker service to submit a task
		_, err := client.SubmitTask(context.Background(), &pb.ClientTaskRequest{
			Data: "test data",
		})
		if err != nil {
			t.Fatalf("Error submitting task: %v", err)
		}
	}
	// check the task count received by each worker
	err = WaitForCondition(func() bool {
		worker := cluster.workers[1]
		// acquire lock on the task queue of the worker
		worker.ReceivedTasksLock.Lock()
		// check if the worker has received 4 tasks
		receivedTaskCount := len(worker.ReceivedTasks)
		if receivedTaskCount != taskSubmitRequestCount {
			// release the lock on the task queue of the worker
			worker.ReceivedTasksLock.Unlock()
			return false
		}
		// release the lock on the task queue of the worker
		worker.ReceivedTasksLock.Unlock()
		return true
	}, timeoutForWaitCondition, retryIntervalForWaitCondition)
	if err != nil {
		t.Fatalf("Coordinator not routing requests correctly after failover.")
	}
}

func TestTaskLoadBalancingOverWorkers(t *testing.T) {
	// setup the cluster with 4 workers
	setup(4)
	defer teardown()

	// submit 8 tasks
	for i := 0; i < 8; i++ {
		_, err := client.SubmitTask(context.Background(), &pb.ClientTaskRequest{Data: "test"})
		if err != nil {
			t.Fatalf("Failed to submit task: %v", err)
		}
	}

	err := WaitForCondition(func() bool {
		for _, worker := range cluster.workers {
			worker.ReceivedTasksLock.Lock()
			if len(worker.ReceivedTasks) != 2 {
				worker.ReceivedTasksLock.Unlock()
				return false
			}
			worker.ReceivedTasksLock.Unlock()
		}
		return true
	}, timeoutForWaitCondition, retryIntervalForWaitCondition)

	if err != nil {
		for idx, worker := range cluster.workers {
			worker.ReceivedTasksLock.Lock()
			log.Printf("Worker %d has %d tasks in its log", idx, len(worker.ReceivedTasks))

			worker.ReceivedTasksLock.Unlock()
		}
		t.Fatalf("Coordinator is not using round-robin to execute tasks over worker pool.")
	}
}
