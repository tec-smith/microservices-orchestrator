package circuitbreaker

import (
	"errors"
	"testing"
	"time"
)

var errBoom = errors.New("boom")

type fakeClock struct{ t time.Time }

func (c *fakeClock) Now() time.Time          { return c.t }
func (c *fakeClock) Advance(d time.Duration) { c.t = c.t.Add(d) }

func newTestBreaker(s Settings) (*Breaker, *fakeClock) {
	clock := &fakeClock{t: time.Unix(0, 0)}
	s.Now = clock.Now
	return New(s), clock
}

func fail(b *Breaker) error    { return b.Execute(func() error { return errBoom }) }
func succeed(b *Breaker) error { return b.Execute(func() error { return nil }) }

func TestTripsAfterConsecutiveFailures(t *testing.T) {
	b, _ := newTestBreaker(Settings{ReadyToTrip: ConsecutiveFailures(3)})

	for i := 0; i < 2; i++ {
		_ = fail(b)
	}
	if got := b.State(); got != StateClosed {
		t.Fatalf("after 2 failures state = %v, want closed", got)
	}
	_ = fail(b)
	if got := b.State(); got != StateOpen {
		t.Fatalf("after 3 failures state = %v, want open", got)
	}

	called := false
	err := b.Execute(func() error { called = true; return nil })
	if !errors.Is(err, ErrOpen) || called {
		t.Fatalf("open breaker: err = %v, called = %v; want ErrOpen and not called", err, called)
	}
}

func TestSuccessResetsConsecutiveFailures(t *testing.T) {
	b, _ := newTestBreaker(Settings{ReadyToTrip: ConsecutiveFailures(2)})
	_ = fail(b)
	_ = succeed(b)
	_ = fail(b)
	if got := b.State(); got != StateClosed {
		t.Fatalf("state = %v, want closed", got)
	}
}

func TestHalfOpenClosesAfterSuccessfulTrials(t *testing.T) {
	b, clock := newTestBreaker(Settings{
		ReadyToTrip: ConsecutiveFailures(1),
		Timeout:     10 * time.Second,
		MaxRequests: 2,
	})
	_ = fail(b)
	clock.Advance(11 * time.Second)
	if got := b.State(); got != StateHalfOpen {
		t.Fatalf("after timeout state = %v, want half-open", got)
	}

	done1, err := b.Allow()
	if err != nil {
		t.Fatal(err)
	}
	done2, err := b.Allow()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.Allow(); !errors.Is(err, ErrTooManyRequests) {
		t.Fatalf("third trial: err = %v, want ErrTooManyRequests", err)
	}
	done1(true)
	if got := b.State(); got != StateHalfOpen {
		t.Fatalf("after one success state = %v, want half-open", got)
	}
	done2(true)
	if got := b.State(); got != StateClosed {
		t.Fatalf("after two successes state = %v, want closed", got)
	}
}

func TestHalfOpenReopensOnFailure(t *testing.T) {
	b, clock := newTestBreaker(Settings{ReadyToTrip: ConsecutiveFailures(1), Timeout: time.Second})
	_ = fail(b)
	clock.Advance(2 * time.Second)
	_ = fail(b)
	if got := b.State(); got != StateOpen {
		t.Fatalf("state = %v, want open", got)
	}
}

func TestIntervalClearsCounts(t *testing.T) {
	b, clock := newTestBreaker(Settings{ReadyToTrip: ConsecutiveFailures(2), Interval: time.Minute})
	_ = fail(b)
	clock.Advance(2 * time.Minute)
	_ = fail(b)
	if got := b.State(); got != StateClosed {
		t.Fatalf("state = %v, want closed (first failure should have been forgotten)", got)
	}
}

func TestStaleOutcomeIgnored(t *testing.T) {
	b, _ := newTestBreaker(Settings{ReadyToTrip: ConsecutiveFailures(1)})
	done, err := b.Allow()
	if err != nil {
		t.Fatal(err)
	}
	_ = fail(b) // opens the breaker, starting a new generation
	done(true)  // belongs to the old generation
	if got := b.State(); got != StateOpen {
		t.Fatalf("state = %v, want open", got)
	}
}

func TestFailureRatio(t *testing.T) {
	trip := FailureRatio(0.5, 4)
	if trip(Counts{Requests: 3, TotalFailures: 3}) {
		t.Error("tripped below minimum requests")
	}
	if !trip(Counts{Requests: 4, TotalFailures: 2}) {
		t.Error("did not trip at 50% of 4 requests")
	}
}

func TestOnStateChangeCanCallBreaker(t *testing.T) {
	var transitions []string
	var b *Breaker
	b, _ = newTestBreaker(Settings{
		Name:        "svc",
		ReadyToTrip: ConsecutiveFailures(1),
		OnStateChange: func(name string, from, to State) {
			// Calling back into the breaker must not deadlock.
			transitions = append(transitions, name+":"+from.String()+"->"+to.String()+":"+b.State().String())
		},
	})
	_ = fail(b)
	want := "svc:closed->open:open"
	if len(transitions) != 1 || transitions[0] != want {
		t.Fatalf("transitions = %v, want [%s]", transitions, want)
	}
}

func TestPanicCountsAsFailure(t *testing.T) {
	b, _ := newTestBreaker(Settings{ReadyToTrip: ConsecutiveFailures(1)})
	func() {
		defer func() { _ = recover() }()
		_ = b.Execute(func() error { panic("oops") })
	}()
	if got := b.State(); got != StateOpen {
		t.Fatalf("state = %v, want open", got)
	}
}

/***
  @COPYRIGHT (c) 2026. Thomas EC. Smith (https://www.TECSmith.info). Licensed under the MIT License.
****/
