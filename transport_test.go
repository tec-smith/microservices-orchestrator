package orchestrator

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tec-smith/microservices-orchestrator/circuitbreaker"
	"github.com/tec-smith/microservices-orchestrator/registry"
)

type backend struct {
	*httptest.Server
	hits   atomic.Int64
	status atomic.Int64
}

// newBackend starts a server that answers "<id> <path> <body>" with the
// configured status (200 by default).
func newBackend(t *testing.T, id string) *backend {
	t.Helper()
	b := &backend{}
	b.status.Store(http.StatusOK)
	b.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b.hits.Add(1)
		body, _ := io.ReadAll(r.Body)
		w.WriteHeader(int(b.status.Load()))
		io.WriteString(w, id+" "+r.URL.Path+" "+string(body))
	}))
	t.Cleanup(b.Close)
	return b
}

func setup(t *testing.T, backends map[string]*backend) *registry.Memory {
	t.Helper()
	reg := registry.NewMemory()
	t.Cleanup(func() { reg.Close() })
	for id, b := range backends {
		if err := reg.Register(context.Background(), registry.Instance{ID: id, Service: "svc", Address: b.URL}); err != nil {
			t.Fatal(err)
		}
	}
	return reg
}

func get(t *testing.T, c *http.Client, url string) (int, string) {
	t.Helper()
	resp, err := c.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(body)
}

func TestRoutesByServiceName(t *testing.T) {
	a, b := newBackend(t, "a"), newBackend(t, "b")
	client := NewClient(setup(t, map[string]*backend{"a": a, "b": b}))

	seen := map[string]bool{}
	for i := 0; i < 4; i++ {
		status, body := get(t, client, "http://svc/api/items?x=1")
		if status != http.StatusOK || !strings.Contains(body, " /api/items ") {
			t.Fatalf("got %d %q", status, body)
		}
		seen[body[:1]] = true
	}
	if !seen["a"] || !seen["b"] {
		t.Fatalf("requests were not balanced: %v", seen)
	}
}

func TestNoInstances(t *testing.T) {
	client := NewClient(setup(t, nil))
	_, err := client.Get("http://svc/")
	if !errors.Is(err, ErrNoInstances) {
		t.Fatalf("err = %v, want ErrNoInstances", err)
	}
}

func TestSkipsCriticalInstances(t *testing.T) {
	a, b := newBackend(t, "a"), newBackend(t, "b")
	reg := setup(t, map[string]*backend{"a": a, "b": b})
	_ = reg.SetStatus(context.Background(), "svc", "a", registry.StatusCritical)
	client := NewClient(reg, WithCacheTTL(0))

	for i := 0; i < 4; i++ {
		get(t, client, "http://svc/")
	}
	if a.hits.Load() != 0 {
		t.Fatalf("critical instance got %d requests", a.hits.Load())
	}
}

func TestFailsOverOn503(t *testing.T) {
	a, b := newBackend(t, "a"), newBackend(t, "b")
	a.status.Store(http.StatusServiceUnavailable)
	client := NewClient(setup(t, map[string]*backend{"a": a, "b": b}))

	for i := 0; i < 4; i++ {
		status, body := get(t, client, "http://svc/")
		if status != http.StatusOK || !strings.HasPrefix(body, "b") {
			t.Fatalf("request %d: got %d %q, want 200 from b", i, status, body)
		}
	}
}

func TestFailsOverOnConnectionError(t *testing.T) {
	b := newBackend(t, "b")
	dead := newBackend(t, "dead")
	dead.Close()
	client := NewClient(setup(t, map[string]*backend{"dead": dead, "b": b}))

	for i := 0; i < 4; i++ {
		if status, _ := get(t, client, "http://svc/"); status != http.StatusOK {
			t.Fatalf("status = %d, want 200", status)
		}
	}
}

func TestReplaysBodyOnRetry(t *testing.T) {
	a, b := newBackend(t, "a"), newBackend(t, "b")
	a.status.Store(http.StatusBadGateway)
	b.status.Store(http.StatusBadGateway)
	c := newBackend(t, "c")
	client := NewClient(setup(t, map[string]*backend{"a": a, "b": b, "c": c}))

	req, _ := http.NewRequest(http.MethodPut, "http://svc/items/1", strings.NewReader("payload"))
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(body) != "c /items/1 payload" {
		t.Fatalf("body = %q, want the payload replayed to c", body)
	}
}

func TestDoesNotRetryPost(t *testing.T) {
	a, b := newBackend(t, "a"), newBackend(t, "b")
	a.status.Store(http.StatusServiceUnavailable)
	b.status.Store(http.StatusServiceUnavailable)
	client := NewClient(setup(t, map[string]*backend{"a": a, "b": b}))

	resp, err := client.Post("http://svc/orders", "text/plain", strings.NewReader("x"))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if total := a.hits.Load() + b.hits.Load(); total != 1 {
		t.Fatalf("POST was sent %d times, want 1", total)
	}
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want the backend's 503 passed through", resp.StatusCode)
	}
}

func TestCircuitOpensAndRecovers(t *testing.T) {
	a := newBackend(t, "a")
	a.status.Store(http.StatusInternalServerError)
	tr := NewTransport(setup(t, map[string]*backend{"a": a}),
		WithBreakerSettings(circuitbreaker.Settings{
			ReadyToTrip: circuitbreaker.ConsecutiveFailures(2),
			Timeout:     50 * time.Millisecond,
		}),
	)
	client := &http.Client{Transport: tr}

	for i := 0; i < 2; i++ {
		get(t, client, "http://svc/")
	}
	if got := tr.CircuitStates()["svc/a"]; got != circuitbreaker.StateOpen {
		t.Fatalf("state = %v, want open", got)
	}

	_, err := client.Get("http://svc/")
	if !errors.Is(err, ErrAllCircuitsOpen) {
		t.Fatalf("err = %v, want ErrAllCircuitsOpen", err)
	}
	if hits := a.hits.Load(); hits != 2 {
		t.Fatalf("backend got %d requests while open, want 2", hits)
	}

	a.status.Store(http.StatusOK)
	time.Sleep(60 * time.Millisecond)
	if status, _ := get(t, client, "http://svc/"); status != http.StatusOK {
		t.Fatalf("trial request status = %d, want 200", status)
	}
	if got := tr.CircuitStates()["svc/a"]; got != circuitbreaker.StateClosed {
		t.Fatalf("state = %v, want closed", got)
	}
}

func TestGateway(t *testing.T) {
	a := newBackend(t, "a")
	gw := httptest.NewServer(NewGateway(NewTransport(setup(t, map[string]*backend{"a": a})), nil))
	defer gw.Close()

	status, body := get(t, gw.Client(), gw.URL+"/svc/api/hello")
	if status != http.StatusOK || body != "a /api/hello " {
		t.Fatalf("got %d %q", status, body)
	}

	if status, _ := get(t, gw.Client(), gw.URL+"/unknown/x"); status != http.StatusServiceUnavailable {
		t.Fatalf("unknown service status = %d, want 503", status)
	}
	if status, _ := get(t, gw.Client(), gw.URL+"/"); status != http.StatusNotFound {
		t.Fatalf("root status = %d, want 404", status)
	}
}

func TestJoinPath(t *testing.T) {
	cases := []struct{ a, b, want string }{
		{"", "/x", "/x"},
		{"", "", "/"},
		{"/api", "/x", "/api/x"},
		{"/api/", "/x", "/api/x"},
		{"/api", "", "/api"},
	}
	for _, c := range cases {
		if got := joinPath(c.a, c.b); got != c.want {
			t.Errorf("joinPath(%q, %q) = %q, want %q", c.a, c.b, got, c.want)
		}
	}
}

/***
  @COPYRIGHT (c) 2026. Thomas EC. Smith (https://www.TECSmith.info). Licensed under the MIT License.
****/
