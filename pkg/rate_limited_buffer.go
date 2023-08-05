package rlbuffer

import "golang.org/x/time/rate"

// RateLimitedBuffer is a struct that holds a buffered channel and handles rate-limited value publishing.
type RateLimitedBuffer struct {
	buffer     chan interface{}
	limiter    *rate.Limiter
	deliveries chan interface{}
	stopChan   chan struct{}
}

// New creates a new RateLimitedBuffer instance with size buffer size and rateLimit events per second.
func New(size int, rateLimit float64) *RateLimitedBuffer {
	p := &RateLimitedBuffer{
		buffer:     make(chan interface{}, size),
		limiter:    rate.NewLimiter(rate.Limit(rateLimit), 1),
		deliveries: make(chan interface{}),
		stopChan:   make(chan struct{}),
	}
	return p
}

// Publish adds a value to the buffer.
func (p *RateLimitedBuffer) Publish(value interface{}) {
	p.buffer <- value
}

// pipe publishes all values currently in the buffer at the rate-limited interval.
func (p *RateLimitedBuffer) pipe() {
	for {
		select {
		case value := <-p.buffer:
			p.deliveries <- value
		case <-p.stopChan:
			close(p.deliveries)
			return
		}
	}
}

// startPublisher runs a goroutine to handle rate-limited publishing.
func (p *RateLimitedBuffer) Start() {
	go p.pipe()
}

// Deliveries returns the deliveries channel to expose it for consumption.
func (p *RateLimitedBuffer) Deliveries() <-chan interface{} {
	return p.deliveries
}

// Stop stops the rate-limited publisher and closes the deliveries channel.
func (p *RateLimitedBuffer) Stop() {
	close(p.stopChan)
}
