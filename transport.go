// Package orchestrator routes HTTP requests to microservices by name.
//
// Transport is an http.RoundTripper that treats the host of each request URL
// as a service name. It looks the service up in a registry, picks a healthy
// instance with a load balancer, guards each instance with its own circuit
// breaker, and retries failed idempotent requests on another instance:
//
//	client := orchestrator.NewClient(reg)
//	resp, err := client.Get("http://users/api/v1/users/42")
//
// Gateway wraps a Transport in a reverse proxy, so clients that know nothing
// about the registry can reach any service at /{service}/{path}.
package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/tec-smith/microservices-orchestrator/circuitbreaker"
	"github.com/tec-smith/microservices-orchestrator/loadbalancer"
	"github.com/tec-smith/microservices-orchestrator/registry"
)

var (
	// ErrNoInstances is returned when a service has no healthy instances.
	ErrNoInstances = errors.New("orchestrator: no healthy instances")
	// ErrAllCircuitsOpen is returned when every healthy instance of a service
	// has an open circuit breaker.
	ErrAllCircuitsOpen = errors.New("orchestrator: all instance circuits are open")
)

// Option configures a Transport.
type Option func(*Transport)

// WithBalancer sets the load balancer. Default round-robin.
func WithBalancer(b loadbalancer.Balancer) Option { return func(t *Transport) { t.balancer = b } }

// WithBaseTransport sets the RoundTripper that sends requests to instances.
// Default http.DefaultTransport.
func WithBaseTransport(rt http.RoundTripper) Option { return func(t *Transport) { t.base = rt } }

// WithMaxRetries sets how many other instances to try after a failure.
// Default 2.
func WithMaxRetries(n int) Option { return func(t *Transport) { t.maxRetries = max(n, 0) } }

// WithCacheTTL sets how long instance lists are cached before asking the
// registry again. Default 2s. If the registry is unreachable, the stale list
// is used.
func WithCacheTTL(d time.Duration) Option { return func(t *Transport) { t.cacheTTL = d } }

// WithBreakerSettings sets the template for per-instance circuit breakers.
// Name is set to "service/instance" for each breaker.
func WithBreakerSettings(s circuitbreaker.Settings) Option {
	return func(t *Transport) { t.breakerSettings = s }
}

// WithLogger sets the logger. Default slog.Default().
func WithLogger(l *slog.Logger) Option { return func(t *Transport) { t.log = l } }

type cacheEntry struct {
	instances []registry.Instance
	expires   time.Time
}

// Transport is a service-aware http.RoundTripper. It is safe for concurrent use.
type Transport struct {
	reg             registry.Registry
	balancer        loadbalancer.Balancer
	base            http.RoundTripper
	maxRetries      int
	cacheTTL        time.Duration
	breakerSettings circuitbreaker.Settings
	log             *slog.Logger

	mu       sync.Mutex
	cache    map[string]cacheEntry
	breakers map[string]map[string]*circuitbreaker.Breaker // service → instance ID
}

// NewTransport returns a Transport that discovers services in reg.
func NewTransport(reg registry.Registry, opts ...Option) *Transport {
	t := &Transport{
		reg:        reg,
		balancer:   &loadbalancer.RoundRobin{},
		base:       http.DefaultTransport,
		maxRetries: 2,
		cacheTTL:   2 * time.Second,
		log:        slog.Default(),
		cache:      make(map[string]cacheEntry),
		breakers:   make(map[string]map[string]*circuitbreaker.Breaker),
	}
	for _, opt := range opts {
		opt(t)
	}
	if t.breakerSettings.OnStateChange == nil {
		t.breakerSettings.OnStateChange = func(name string, from, to circuitbreaker.State) {
			t.log.Info("circuit state changed", "breaker", name, "from", from, "to", to)
		}
	}
	return t
}

// NewClient returns an http.Client that uses a Transport for every request.
func NewClient(reg registry.Registry, opts ...Option) *http.Client {
	return &http.Client{Transport: NewTransport(reg, opts...)}
}

// RoundTrip sends req to an instance of the service named by req.URL's host.
//
// A request fails over to another instance when sending it returns an error
// or a 502, 503 or 504 response, as long as the method is idempotent (or the
// request has an Idempotency-Key header) and the body can be replayed. Any
// error or 5xx response counts as a failure for the instance's breaker.
func (t *Transport) RoundTrip(req *http.Request) (*http.Response, error) {
	service := req.URL.Hostname()
	instances, err := t.resolve(req.Context(), service)
	if err != nil {
		closeBody(req)
		return nil, err
	}
	candidates := registry.Healthy(instances)
	if len(candidates) == 0 {
		closeBody(req)
		return nil, fmt.Errorf("%w for service %q", ErrNoInstances, service)
	}

	maxAttempts := 1
	if retryable(req) {
		maxAttempts += t.maxRetries
	}

	tried := make(map[string]bool, len(candidates))
	var lastErr error
	for attempt := 0; attempt < maxAttempts; {
		available := t.available(service, candidates, tried)
		if len(available) == 0 {
			break
		}
		inst, done, err := t.balancer.Pick(available)
		if err != nil {
			lastErr = err
			break
		}
		tried[inst.ID] = true

		report, err := t.breaker(service, inst.ID).Allow()
		if err != nil {
			// Another request took the half-open trial slot; try elsewhere
			// without using up an attempt.
			done()
			lastErr = err
			continue
		}

		out, err := t.rewrite(req, inst, attempt > 0)
		if err != nil {
			done()
			report(true)
			return nil, err
		}
		attempt++
		resp, err := t.base.RoundTrip(out)

		failed := err != nil || resp.StatusCode >= 500
		if err != nil && req.Context().Err() != nil {
			// The caller gave up; that says nothing about the instance.
			failed = false
		}
		report(!failed)

		if err != nil {
			done()
		} else if resp.StatusCode == http.StatusSwitchingProtocols {
			// The body is the upgraded connection and must not be wrapped.
			done()
		} else {
			resp.Body = &doneBody{ReadCloser: resp.Body, done: done}
		}

		if !shouldFailover(req, resp, err) || attempt >= maxAttempts ||
			len(t.available(service, candidates, tried)) == 0 {
			if err != nil {
				return nil, fmt.Errorf("orchestrator: %s/%s: %w", service, inst.ID, err)
			}
			return resp, nil
		}

		if err != nil {
			lastErr = err
		} else {
			lastErr = fmt.Errorf("instance %s returned %s", inst.ID, resp.Status)
			drain(resp.Body)
		}
		t.log.Debug("failing over", "service", service, "instance", inst.ID, "err", lastErr)
	}

	closeBody(req)
	if lastErr == nil || errors.Is(lastErr, circuitbreaker.ErrOpen) || errors.Is(lastErr, circuitbreaker.ErrTooManyRequests) {
		return nil, fmt.Errorf("%w for service %q", ErrAllCircuitsOpen, service)
	}
	return nil, fmt.Errorf("orchestrator: %s: %w", service, lastErr)
}

// CircuitStates returns the breaker state of every instance seen so far,
// keyed by "service/instance".
func (t *Transport) CircuitStates() map[string]circuitbreaker.State {
	t.mu.Lock()
	breakers := make([]*circuitbreaker.Breaker, 0)
	for _, byID := range t.breakers {
		for _, b := range byID {
			breakers = append(breakers, b)
		}
	}
	t.mu.Unlock()

	states := make(map[string]circuitbreaker.State, len(breakers))
	for _, b := range breakers {
		states[b.Name()] = b.State()
	}
	return states
}

// resolve returns the cached instances of service, refreshing them from the
// registry when the cache has expired.
func (t *Transport) resolve(ctx context.Context, service string) ([]registry.Instance, error) {
	now := time.Now()
	t.mu.Lock()
	entry, ok := t.cache[service]
	t.mu.Unlock()
	if ok && now.Before(entry.expires) {
		return entry.instances, nil
	}

	instances, err := t.reg.Instances(ctx, service)
	if err != nil {
		if ok {
			t.log.Warn("registry unavailable, using stale instances", "service", service, "err", err)
			return entry.instances, nil
		}
		return nil, fmt.Errorf("orchestrator: resolving %q: %w", service, err)
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	t.cache[service] = cacheEntry{instances: instances, expires: now.Add(t.cacheTTL)}
	t.pruneBreakers(service, instances)
	return instances, nil
}

// available returns the candidates not yet tried whose breakers are not open.
func (t *Transport) available(service string, candidates []registry.Instance, tried map[string]bool) []registry.Instance {
	out := make([]registry.Instance, 0, len(candidates))
	for _, inst := range candidates {
		if !tried[inst.ID] && t.breaker(service, inst.ID).State() != circuitbreaker.StateOpen {
			out = append(out, inst)
		}
	}
	return out
}

func (t *Transport) breaker(service, id string) *circuitbreaker.Breaker {
	t.mu.Lock()
	defer t.mu.Unlock()
	byID, ok := t.breakers[service]
	if !ok {
		byID = make(map[string]*circuitbreaker.Breaker)
		t.breakers[service] = byID
	}
	b, ok := byID[id]
	if !ok {
		s := t.breakerSettings
		s.Name = service + "/" + id
		b = circuitbreaker.New(s)
		byID[id] = b
	}
	return b
}

// pruneBreakers drops breakers of instances that have left the registry.
// It must be called with t.mu held.
func (t *Transport) pruneBreakers(service string, instances []registry.Instance) {
	byID := t.breakers[service]
	if len(byID) == 0 {
		return
	}
	live := make(map[string]struct{}, len(instances))
	for _, inst := range instances {
		live[inst.ID] = struct{}{}
	}
	for id := range byID {
		if _, ok := live[id]; !ok {
			delete(byID, id)
		}
	}
}

// rewrite points a copy of req at inst. Retries get a fresh body from GetBody.
func (t *Transport) rewrite(req *http.Request, inst registry.Instance, retry bool) (*http.Request, error) {
	target, err := url.Parse(inst.Address)
	if err != nil {
		return nil, fmt.Errorf("orchestrator: instance %s has invalid address: %w", inst.ID, err)
	}
	out := req.Clone(req.Context())
	out.URL.Scheme = target.Scheme
	out.URL.Host = target.Host
	out.URL.Path = joinPath(target.Path, req.URL.Path)
	out.URL.RawPath = ""
	out.Host = ""
	if retry && req.Body != nil && req.Body != http.NoBody {
		body, err := req.GetBody()
		if err != nil {
			return nil, err
		}
		out.Body = body
	}
	return out, nil
}

// retryable reports whether req may be sent more than once.
func retryable(req *http.Request) bool {
	if req.Body != nil && req.Body != http.NoBody && req.GetBody == nil {
		return false
	}
	switch req.Method {
	case http.MethodGet, http.MethodHead, http.MethodOptions, http.MethodTrace, http.MethodPut, http.MethodDelete:
		return true
	}
	return req.Header.Get("Idempotency-Key") != ""
}

func shouldFailover(req *http.Request, resp *http.Response, err error) bool {
	if err != nil {
		return req.Context().Err() == nil
	}
	switch resp.StatusCode {
	case http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return true
	}
	return false
}

func joinPath(a, b string) string {
	switch {
	case a == "" || a == "/":
		if b == "" {
			return "/"
		}
		return b
	case b == "" || b == "/":
		return a
	}
	return strings.TrimRight(a, "/") + "/" + strings.TrimLeft(b, "/")
}

func closeBody(req *http.Request) {
	if req.Body != nil {
		req.Body.Close()
	}
}

// drain discards a little of body so the connection can be reused, then closes it.
func drain(body io.ReadCloser) {
	_, _ = io.CopyN(io.Discard, body, 4<<10)
	body.Close()
}

// doneBody calls done once the response body is fully read or closed, so
// load balancers that count in-flight requests see the real duration.
type doneBody struct {
	io.ReadCloser
	once sync.Once
	done func()
}

func (b *doneBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if err != nil {
		b.once.Do(b.done)
	}
	return n, err
}

func (b *doneBody) Close() error {
	err := b.ReadCloser.Close()
	b.once.Do(b.done)
	return err
}

/***
  @COPYRIGHT (c) 2026. Thomas EC. Smith (https://www.TECSmith.info). Licensed under the MIT License.
****/
