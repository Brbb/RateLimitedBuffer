package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	rlbuffer "github.com/brbb/rlbuffer"
)

func main() {
	// Create a new RLBuffer with a rate limit of 2 events every 1 second

	// Listen for the interrupt signal (Ctrl+C)

	// Create a context with a cancel function
	ctx, cancel := context.WithCancel(context.Background())
	// Handle SIGINT signal
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT)

	go func() {
		<-signalChan
		fmt.Println("\nReceived SIGINT, initiating graceful shutdown...")
		cancel() // Trigger context cancellation
	}()

	rlBuffer := rlbuffer.New(8)

	// Start the RLBuffer processing
	rlBuffer.Start()

	// Receive events from the deliveries channel (this will be rate-limited)
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go consumer(ctx, wg, rlBuffer)
	wg.Add(1)
	go producer(ctx, wg, rlBuffer)

	wg.Wait()
	fmt.Println("Exiting")
}

func producer(ctx context.Context, wg *sync.WaitGroup, rlb *rlbuffer.RLBuffer) {

	ticker := time.NewTicker(300 * time.Millisecond)
	eventCounter := 1
	defer wg.Done()

loop:
	for {
		select {
		case <-ticker.C:
			{
				pubEv := fmt.Sprintf("E%d", eventCounter)
				rlb.Push(pubEv)
				fmt.Println("[PUB]", pubEv)
				eventCounter++
			}
		case <-ctx.Done():
			{
				fmt.Println("Stop producing")
				// close the channel from the sender!
				ticker.Stop()
				rlb.Stop()
				break loop
			}
		}
	}
}

func consumer(ctx context.Context, wg *sync.WaitGroup, rlb *rlbuffer.RLBuffer) {
	defer wg.Done()
loop:
	for {
		select {
		case event := <-rlb.Deliveries():
			{
				fmt.Println(fmt.Sprintf("[RECV] %s", event))
			}
		case <-ctx.Done():
			{
				fmt.Println("Draining last events")

				for event := range rlb.Deliveries() {
					fmt.Println(fmt.Sprintf("[RECV] %s", event))
				}
				fmt.Println("Stop consuming")
				break loop
			}
		}
	}
}
