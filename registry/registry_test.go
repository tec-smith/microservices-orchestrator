package registry

import (
	"context"
	"errors"
	"net/http/httptest"
	"testing"
	"time"
)

func inst(id string) Instance {
	return Instance{ID: id, Service: "users", Address: "http://" + id + ":8080"}
}

func TestValidate(t *testing.T) {
	cases := map[string]Instance{
		"missing id":       {Service: "s", Address: "http://a"},
		"relative address": {ID: "1", Service: "s", Address: "localhost:8080"},
		"other scheme":     {ID: "1", Service: "s", Address: "ftp://a"},
		"negative weight":  {ID: "1", Service: "s", Address: "http://a", Weight: -1},
	}
	for name, in := range cases {
		if err := in.Validate(); !errors.Is(err, ErrInvalid) {
			t.Errorf("%s: err = %v, want ErrInvalid", name, err)
		}
	}
	if err := inst("a").Validate(); err != nil {
		t.Errorf("valid instance: %v", err)
	}
}

// testRegistry runs the shared behaviour checks against any Registry.
func testRegistry(t *testing.T, reg Registry) {
	ctx := context.Background()

	if err := reg.Register(ctx, inst("b")); err != nil {
		t.Fatal(err)
	}
	if err := reg.Register(ctx, inst("a")); err != nil {
		t.Fatal(err)
	}

	got, err := reg.Instances(ctx, "users")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].ID != "a" || got[1].ID != "b" {
		t.Fatalf("instances = %+v, want a and b in order", got)
	}
	if got[0].Status != StatusPassing {
		t.Fatalf("status = %q, want passing", got[0].Status)
	}

	services, err := reg.Services(ctx)
	if err != nil || len(services) != 1 || services[0] != "users" {
		t.Fatalf("services = %v, %v", services, err)
	}

	if err := reg.SetStatus(ctx, "users", "a", StatusCritical); err != nil {
		t.Fatal(err)
	}
	got, _ = reg.Instances(ctx, "users")
	if healthy := Healthy(got); len(healthy) != 1 || healthy[0].ID != "b" {
		t.Fatalf("healthy = %+v, want only b", healthy)
	}

	if err := reg.SetStatus(ctx, "users", "a", "sleepy"); !errors.Is(err, ErrInvalid) {
		t.Fatalf("bad status: err = %v, want ErrInvalid", err)
	}
	if err := reg.Heartbeat(ctx, "users", "a"); err != nil {
		t.Fatal(err)
	}
	if err := reg.Heartbeat(ctx, "users", "nope"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("heartbeat unknown: err = %v, want ErrNotFound", err)
	}
	if err := reg.Deregister(ctx, "users", "a"); err != nil {
		t.Fatal(err)
	}
	if err := reg.Deregister(ctx, "users", "a"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("second deregister: err = %v, want ErrNotFound", err)
	}
	if err := reg.Register(ctx, Instance{ID: "x"}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("invalid register: err = %v, want ErrInvalid", err)
	}
}

func TestMemory(t *testing.T) {
	reg := NewMemory()
	defer reg.Close()
	testRegistry(t, reg)
}

func TestHTTPRoundTrip(t *testing.T) {
	mem := NewMemory()
	defer mem.Close()
	srv := httptest.NewServer(NewHandler(mem))
	defer srv.Close()
	testRegistry(t, NewHTTPClient(srv.URL))
}

func TestMemoryTTLEviction(t *testing.T) {
	now := time.Unix(0, 0)
	reg := NewMemory(WithClock(func() time.Time { return now }))
	reg.ttl = 30 * time.Second // set directly so no background loop runs
	ctx := context.Background()

	_ = reg.Register(ctx, inst("a"))
	_ = reg.Register(ctx, inst("b"))

	now = now.Add(20 * time.Second)
	_ = reg.Heartbeat(ctx, "users", "a")
	now = now.Add(20 * time.Second)
	reg.reap()

	got, _ := reg.Instances(ctx, "users")
	if len(got) != 1 || got[0].ID != "a" {
		t.Fatalf("instances = %+v, want only a (b missed its heartbeat)", got)
	}
}

func TestMemoryWatch(t *testing.T) {
	reg := NewMemory()
	defer reg.Close()
	ctx, cancel := context.WithCancel(context.Background())

	ch, err := reg.Watch(ctx, "users")
	if err != nil {
		t.Fatal(err)
	}
	if first := <-ch; len(first) != 0 {
		t.Fatalf("initial snapshot = %+v, want empty", first)
	}

	_ = reg.Register(ctx, inst("a"))
	if got := <-ch; len(got) != 1 {
		t.Fatalf("after register = %+v, want one instance", got)
	}

	cancel()
	select {
	case _, ok := <-ch:
		if ok {
			// A final snapshot may still be buffered; the next read must close.
			if _, ok := <-ch; ok {
				t.Fatal("channel not closed after cancel")
			}
		}
	case <-time.After(time.Second):
		t.Fatal("channel not closed after cancel")
	}
}

func TestReregisterKeepsStatus(t *testing.T) {
	reg := NewMemory()
	defer reg.Close()
	ctx := context.Background()

	_ = reg.Register(ctx, inst("a"))
	_ = reg.SetStatus(ctx, "users", "a", StatusCritical)
	updated := inst("a")
	updated.Weight = 3
	_ = reg.Register(ctx, updated)

	got, _ := reg.Instances(ctx, "users")
	if got[0].Status != StatusCritical || got[0].Weight != 3 {
		t.Fatalf("got status %s weight %d, want critical and 3", got[0].Status, got[0].Weight)
	}

	// A fresh registration after deregistering starts as passing.
	_ = reg.Deregister(ctx, "users", "a")
	_ = reg.Register(ctx, inst("a"))
	got, _ = reg.Instances(ctx, "users")
	if got[0].Status != StatusPassing {
		t.Fatalf("status = %s, want passing", got[0].Status)
	}
}

func TestHTTPClientWatch(t *testing.T) {
	mem := NewMemory()
	defer mem.Close()
	srv := httptest.NewServer(NewHandler(mem))
	defer srv.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch, err := NewHTTPClient(srv.URL, WithPollInterval(10*time.Millisecond)).Watch(ctx, "users")
	if err != nil {
		t.Fatal(err)
	}
	<-ch
	_ = mem.Register(ctx, inst("a"))
	select {
	case got := <-ch:
		if len(got) != 1 {
			t.Fatalf("got %+v, want one instance", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no update received")
	}
}

func TestKeepAliveReregistersAndDeregisters(t *testing.T) {
	reg := NewMemory()
	defer reg.Close()
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() { done <- KeepAlive(ctx, reg, inst("a"), 10*time.Millisecond) }()

	waitFor(t, func() bool { got, _ := reg.Instances(ctx, "users"); return len(got) == 1 })

	// Simulate the registry losing the lease; the next heartbeat re-registers.
	_ = reg.Deregister(ctx, "users", "a")
	waitFor(t, func() bool { got, _ := reg.Instances(ctx, "users"); return len(got) == 1 })

	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if got, _ := reg.Instances(context.Background(), "users"); len(got) != 0 {
		t.Fatalf("instances after shutdown = %+v, want none", got)
	}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met in time")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

/***
  @COPYRIGHT (c) 2026. Thomas EC. Smith (https://www.TECSmith.info). Licensed under the MIT License.
****/
