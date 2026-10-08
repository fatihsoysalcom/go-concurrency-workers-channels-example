package main

import (
	"fmt"
	"sync"
	"time"
)

// worker simulates a task that takes some time to complete.
// It processes jobs from the 'jobs' channel and sends results to the 'results' channel.
// A WaitGroup is used to signal completion.
func worker(id int, jobs <-chan int, results chan<- string, wg *sync.WaitGroup) {
	defer wg.Done() // Ensure WaitGroup counter is decremented when worker exits

	for job := range jobs { // Workers continuously pull jobs from the shared 'jobs' channel
		fmt.Printf("Worker %d starting job %d...\n", id, job)
		// Simulate work, e.g., an I/O operation or computation
		time.Sleep(time.Duration(job) * 100 * time.Millisecond)
		result := fmt.Sprintf("Worker %d finished job %d", id, job)
		results <- result // Send the result back to the main goroutine via 'results' channel
		fmt.Printf("Worker %d finished job %d.\n", id, job)
	}
}

func main() {
	const numWorkers = 3 // Number of concurrent workers
	const numJobs = 9    // Total number of jobs to process

	// Create buffered channels for sending jobs to workers and receiving results from them.
	// Channels are Go's primary mechanism for communication and synchronization between goroutines.
	jobs := make(chan int, numJobs)
	results := make(chan string, numJobs)

	var wg sync.WaitGroup // WaitGroup to wait for all goroutines (workers) to complete

	// Launch multiple worker goroutines.
	// Each 'go' keyword starts a new goroutine, enabling concurrent execution.
	for w := 1; w <= numWorkers; w++ {
		wg.Add(1) // Increment WaitGroup counter for each worker launched
		go worker(w, jobs, results, &wg)
	}

	// Send jobs to the 'jobs' channel.
	// The buffered channel allows jobs to be sent without immediately blocking if workers are busy.
	for j := 1; j <= numJobs; j++ {
		jobs <- j
	}
	close(jobs) // Close the 'jobs' channel to signal workers that no more jobs are coming

	wg.Wait() // Block until all workers have called wg.Done(), ensuring all jobs are processed

	// All workers are done, now close the results channel.
	// This is crucial for the range loop below to terminate correctly.
	close(results)

	fmt.Println("\nAll jobs processed. Collecting results:")
	// Collect and print results from the 'results' channel.
	// The range loop over a channel receives values until the channel is closed.
	for r := range results {
		fmt.Println(r)
	}

	fmt.Println("\nDemonstration complete.")
}
