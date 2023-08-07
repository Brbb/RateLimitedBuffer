package rlbuffer

import (
	"context"
	"time"
)

// RLBuffer is a rate-limited buffer that allows only size events every 1 second to be published to the deliveries channel.
type RLBuffer struct {
	size       int              // Maximum number of events allowed every second
	input      chan interface{} // Input channel to receive events
	buffer     chan interface{} // Buffered channel to store events
	deliveries chan interface{} // Channel to deliver the events after rate-limiting
	ctx        context.Context
	cancel     context.CancelFunc
}

// NewRLBuffer creates a new RLBuffer instance with the specified rate limit.
func New(bufferSize int) *RLBuffer {
	ctx, cancelCtx := context.WithCancel(context.Background())

	return &RLBuffer{
		size:       bufferSize,
		input:      make(chan interface{}),
		buffer:     make(chan interface{}, bufferSize),
		deliveries: make(chan interface{}),
		ctx:        ctx,
		cancel:     cancelCtx,
	}
}

// Start starts the RLBuffer processing.
func (r *RLBuffer) Start() {
	go r.pipe()
}

func (r *RLBuffer) Stop() {
	r.cancel()
}

// AddEvent adds an event to the RLBuffer for processing.
func (r *RLBuffer) Push(event interface{}) {
	r.input <- event
}

func (r *RLBuffer) Deliveries() <-chan interface{} {
	return r.deliveries
}

// process is an internal method that processes events and enforces rate-limiting.
func (r *RLBuffer) pipe() {
	ticker := time.NewTicker(time.Second)

loop:
	for {
		select {
		case event := <-r.input:
			if len(r.buffer) < cap(r.buffer)-1 {
				r.buffer <- event
				continue
			}

			<-ticker.C
			r.buffer <- event
			r.flush()

		case <-ticker.C:
			r.flush()
		case <-r.ctx.Done():
			{
				close(r.input)
				ticker.Stop()
				r.flush()
				close(r.deliveries)
				break loop
			}
		}
	}
}

func (r *RLBuffer) flush() {
	if len(r.buffer) == 0 {
		return
	}
	for len(r.buffer) > 0 {
		r.deliveries <- <-r.buffer
	}
}
