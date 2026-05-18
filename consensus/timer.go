package consensus

import (
	"sync"
	"time"
)

// Timer provides behavior similar to the Rust Future-based timer.
//
// Rust notes:
//   - tokio::time::Sleep is !Unpin internally and must stay pinned.
//   - Go does not expose pinning directly because heap objects are movable
//     only conceptually; references remain stable from the programmer's view.
//   - time.Timer is managed by the runtime and safe to reference normally.
//
// Memory/concurrency model considerations:
// - Rust's &mut self guarantees exclusive mutable access.
// - Go does NOT provide that guarantee automatically.
// - We use a mutex to preserve safe concurrent access semantics.
type Timer struct {
	duration time.Duration

	mu    sync.Mutex
	timer *time.Timer
}

// New creates a new timer with a millisecond duration.
func NewTimer(duration uint64) *Timer {
	d := time.Duration(duration) * time.Millisecond

	return &Timer{
		duration: d,
		timer:    time.NewTimer(d),
	}
}

// Reset restarts the timer from "now", matching the Rust implementation.
func (t *Timer) Reset() {
	t.mu.Lock()
	defer t.mu.Unlock()

	// Stop safely.
	if !t.timer.Stop() {
		// Drain the channel if already fired.
		select {
		case <-t.timer.C:
		default:
		}
	}

	t.timer.Reset(t.duration)
}

// Wait blocks until the timer fires.
//
// This is the closest semantic equivalent to implementing Future for Timer
// in Rust. In Go, asynchronous waiting is typically expressed with blocking
// operations inside goroutines instead of polling Futures directly.
func (t *Timer) Wait() {
	<-t.timer.C
}
