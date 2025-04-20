package main

import (
    "fmt"
    "log"
    "math/rand"
    "os"
    "sync"
    "time"
)

// Task represents a unit of work
type Task struct {
    ID string
}

// DataProcessingSystem manages the task queue and results
type DataProcessingSystem struct {
    taskQueue chan Task
    results   []string
    mu        sync.Mutex
    logger    *log.Logger
    wg        sync.WaitGroup
}

// NewDataProcessingSystem initializes the system
func NewDataProcessingSystem(numWorkers, numTasks int) *DataProcessingSystem {
    logger := log.New(os.Stdout, "INFO: ", log.Ldate|log.Ltime)
    dps := &DataProcessingSystem{
        taskQueue: make(chan Task, 100),
        results:   make([]string, 0),
        logger:    logger,
    }
    dps.initializeTasks(numTasks)
    dps.startWorkers(numWorkers)
    return dps
}

// initializeTasks adds tasks to the queue
func (dps *DataProcessingSystem) initializeTasks(numTasks int) {
    for i := 1; i <= numTasks; i++ {
        task := Task{ID: fmt.Sprintf("Task-%d", i)}
        dps.taskQueue <- task
    }
}

// startWorkers launches worker goroutines
func (dps *DataProcessingSystem) startWorkers(numWorkers int) {
    for i := 1; i <= numWorkers; i++ {
        dps.wg.Add(1)
        go dps.worker(i)
    }
}

// worker processes tasks from the queue
func (dps *DataProcessingSystem) worker(id int) {
    defer dps.wg.Done()
    workerName := fmt.Sprintf("Worker-%d", id)
    dps.logger.Printf("%s started.", workerName)

    for task := range dps.taskQueue {
        result, err := dps.processTask(task)
        if err != nil {
            dps.logger.Printf("%s error processing %s: %v", workerName, task.ID, err)
            continue
        }
        dps.mu.Lock()
        dps.results = append(dps.results, result)
        dps.mu.Unlock()
    }

    dps.logger.Printf("%s completed.", workerName)
}

// processTask simulates task processing
func (dps *DataProcessingSystem) processTask(task Task) (string, error) {
    // Simulate computational work
    time.Sleep(time.Duration(time.Millisecond * time.Duration(rand.Intn(1000))))
    return fmt.Sprintf("Processed: %s", task.ID), nil
}

// Shutdown closes the task queue and waits for workers
func (dps *DataProcessingSystem) Shutdown() {
    close(dps.taskQueue)
    dps.wg.Wait()
}

// GetResults returns the processed results
func (dps *DataProcessingSystem) GetResults() []string {
    dps.mu.Lock()
    defer dps.mu.Unlock()
    return append([]string{}, dps.results...)
}

func main() {
    // Seed the random number generator
    rand.Seed(time.Now().UnixNano()) // Ensures different random numbers each run

    // Configure logger to write to file
    file, err := os.OpenFile("processing.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
    if err != nil {
        log.Fatalf("Failed to open log file: %v", err)
    }
    defer file.Close()
    log.SetOutput(file)

    dps := NewDataProcessingSystem(4, 20)
    dps.Shutdown()
    for _, result := range dps.GetResults() {
        fmt.Println(result)
    }
}