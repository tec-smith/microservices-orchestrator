// Package circuitbreaker implements the circuit breaker pattern.
//
// A breaker starts Closed and lets every call through. When ReadyToTrip
// decides there have been too many failures, it opens and rejects calls with
// ErrOpen. After Timeout it moves to HalfOpen and lets up to MaxRequests
// trial calls through: if they all succeed it closes again, and if any fails
// it reopens.
package circuitbreaker

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

// State is the state of a circuit breaker.
type State int

const (
	StateClosed State = iota
	StateHalfOpen
	StateOpen
)

func (s State) String() string {
	switch s {
	case StateClosed:
		return "closed"
	case StateHalfOpen:
		return "half-open"
	case StateOpen:
		return "open"
	default:
		return fmt.Sprintf("unknown(%d)", int(s))
	}
}

// MarshalText makes State render as its name in JSON.
func (s State) MarshalText() ([]byte, error) { return []byte(s.String()), nil }

var (
	// ErrOpen is returned while the breaker is open.
	ErrOpen = errors.New("circuitbreaker: circuit is open")
	// ErrTooManyRequests is returned when the half-open trial quota is used up.
	ErrTooManyRequests = errors.New("circuitbreaker: too many requests while half-open")
)

// Counts holds the request counts for the current generation. They reset on
// every state change and, while closed, every Interval.
type Counts struct {
	Requests             uint32
	TotalSuccesses       uint32
	TotalFailures        uint32
	ConsecutiveSuccesses uint32
	ConsecutiveFailures  uint32
}

func (c *Counts) onSuccess() {
	c.TotalSuccesses++
	c.ConsecutiveSuccesses++
	c.ConsecutiveFailures = 0
}

func (c *Counts) onFailure() {
	c.TotalFailures++
	c.ConsecutiveFailures++
	c.ConsecutiveSuccesses = 0
}

// Settings configures a Breaker. Zero values select the defaults.
type Settings struct {
	// Name identifies the breaker in OnStateChange.
	Name string
	// MaxRequests is the number of trial calls allowed while half-open, and
	// the number of consecutive successes needed to close. Default 1.
	MaxRequests uint32
	// Interval clears the counts periodically while closed, so old failures
	// are forgotten. Zero never clears them.
	Interval time.Duration
	// Timeout is how long the breaker stays open before going half-open.
	// Default 30s.
	Timeout time.Duration
	// ReadyToTrip is called after each failure while closed. Returning true
	// opens the breaker. Default: 5 consecutive failures.
	ReadyToTrip func(Counts) bool
	// IsSuccessful decides whether an error returned to Execute counts as a
	// success. Default: only a nil error.
	IsSuccessful func(err error) bool
	// OnStateChange is called after every state change, outside the lock.
	OnStateChange func(name string, from, to State)
	// Now overrides the clock. It is intended for tests.
	Now func() time.Time
}

// ConsecutiveFailures returns a ReadyToTrip func that trips after n
// consecutive failures.
func ConsecutiveFailures(n uint32) func(Counts) bool {
	return func(c Counts) bool { return c.ConsecutiveFailures >= n }
}

// FailureRatio returns a ReadyToTrip func that trips when at least
// minRequests have been made and the failure ratio reaches ratio.
func FailureRatio(ratio float64, minRequests uint32) func(Counts) bool {
	return func(c Counts) bool {
		return c.Requests >= minRequests && float64(c.TotalFailures)/float64(c.Requests) >= ratio
	}
}

type transition struct{ from, to State }

// Breaker is a circuit breaker. It is safe for concurrent use.
type Breaker struct {
	s Settings

	mu         sync.Mutex
	state      State
	generation uint64
	counts     Counts
	expiry     time.Time
	pending    []transition
}

// New returns a closed breaker.
func New(s Settings) *Breaker {
	if s.MaxRequests == 0 {
		s.MaxRequests = 1
	}
	if s.Timeout <= 0 {
		s.Timeout = 30 * time.Second
	}
	if s.ReadyToTrip == nil {
		s.ReadyToTrip = ConsecutiveFailures(5)
	}
	if s.IsSuccessful == nil {
		s.IsSuccessful = func(err error) bool { return err == nil }
	}
	if s.Now == nil {
		s.Now = time.Now
	}
	b := &Breaker{s: s}
	b.newGeneration(s.Now())
	return b
}

// Name returns the breaker's name.
func (b *Breaker) Name() string { return b.s.Name }

// State returns the current state.
func (b *Breaker) State() State {
	b.mu.Lock()
	state, _ := b.currentState(b.s.Now())
	b.unlock()
	return state
}

// Counts returns the counts for the current generation.
func (b *Breaker) Counts() Counts {
	b.mu.Lock()
	b.currentState(b.s.Now())
	c := b.counts
	b.unlock()
	return c
}

// Execute runs fn if the breaker allows it and records the outcome. It
// returns ErrOpen or ErrTooManyRequests without calling fn when the breaker
// rejects the call. A panic in fn counts as a failure and is re-raised.
func (b *Breaker) Execute(fn func() error) error {
	done, err := b.Allow()
	if err != nil {
		return err
	}
	defer func() {
		if r := recover(); r != nil {
			done(false)
			panic(r)
		}
	}()
	err = fn()
	done(b.s.IsSuccessful(err))
	return err
}

// Allow asks permission for one call. On success the caller must call done
// exactly once with the outcome. Use it when the call does not fit in a
// closure, e.g. when the result is only known after reading a response.
func (b *Breaker) Allow() (done func(success bool), err error) {
	b.mu.Lock()
	defer b.unlock()

	state, generation := b.currentState(b.s.Now())
	switch {
	case state == StateOpen:
		return nil, ErrOpen
	case state == StateHalfOpen && b.counts.Requests >= b.s.MaxRequests:
		return nil, ErrTooManyRequests
	}
	b.counts.Requests++

	var once sync.Once
	return func(success bool) {
		once.Do(func() { b.report(generation, success) })
	}, nil
}

func (b *Breaker) report(generation uint64, success bool) {
	b.mu.Lock()
	defer b.unlock()

	now := b.s.Now()
	state, current := b.currentState(now)
	if current != generation {
		// The outcome belongs to an earlier generation; ignore it.
		return
	}
	if success {
		b.counts.onSuccess()
		if state == StateHalfOpen && b.counts.ConsecutiveSuccesses >= b.s.MaxRequests {
			b.setState(StateClosed, now)
		}
		return
	}
	b.counts.onFailure()
	switch state {
	case StateClosed:
		if b.s.ReadyToTrip(b.counts) {
			b.setState(StateOpen, now)
		}
	case StateHalfOpen:
		b.setState(StateOpen, now)
	}
}

// currentState applies time-based transitions. It must be called with b.mu held.
func (b *Breaker) currentState(now time.Time) (State, uint64) {
	switch b.state {
	case StateClosed:
		if !b.expiry.IsZero() && b.expiry.Before(now) {
			b.newGeneration(now)
		}
	case StateOpen:
		if b.expiry.Before(now) {
			b.setState(StateHalfOpen, now)
		}
	}
	return b.state, b.generation
}

// setState must be called with b.mu held.
func (b *Breaker) setState(state State, now time.Time) {
	if b.state == state {
		return
	}
	prev := b.state
	b.state = state
	b.newGeneration(now)
	if b.s.OnStateChange != nil {
		b.pending = append(b.pending, transition{prev, state})
	}
}

// newGeneration must be called with b.mu held.
func (b *Breaker) newGeneration(now time.Time) {
	b.generation++
	b.counts = Counts{}
	switch b.state {
	case StateClosed:
		if b.s.Interval > 0 {
			b.expiry = now.Add(b.s.Interval)
		} else {
			b.expiry = time.Time{}
		}
	case StateOpen:
		b.expiry = now.Add(b.s.Timeout)
	default:
		b.expiry = time.Time{}
	}
}

// unlock releases b.mu and then runs any pending OnStateChange callbacks, so
// callbacks may safely call back into the breaker.
func (b *Breaker) unlock() {
	pending := b.pending
	b.pending = nil
	b.mu.Unlock()
	for _, t := range pending {
		b.s.OnStateChange(b.s.Name, t.from, t.to)
	}
}

/***
  @COPYRIGHT (c) 2026. Thomas EC. Smith (https://www.TECSmith.info). Licensed under the MIT License.
****/
