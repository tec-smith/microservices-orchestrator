// Package health actively probes registered instances and updates their
// status in the registry.
package health

import (
	"context"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/tec-smith/microservices-orchestrator/registry"
)

// MetadataPath is the instance metadata key that overrides the probe path.
const MetadataPath = "health_path"

// Option configures a Checker.
type Option func(*Checker)

// WithPath sets the default path probed on each instance. Default "/health".
func WithPath(path string) Option { return func(c *Checker) { c.path = path } }

// WithInterval sets the time between probe rounds. Default 10s.
func WithInterval(d time.Duration) Option { return func(c *Checker) { c.interval = d } }

// WithTimeout sets the timeout of each probe. Default 2s.
func WithTimeout(d time.Duration) Option { return func(c *Checker) { c.client.Timeout = d } }

// WithThresholds sets how many consecutive failed probes mark an instance
// critical, and how many consecutive passing probes bring it back.
// Defaults 3 and 2.
func WithThresholds(failures, successes int) Option {
	return func(c *Checker) { c.failureThreshold, c.successThreshold = failures, successes }
}

// WithLogger sets the logger. Default slog.Default().
func WithLogger(l *slog.Logger) Option { return func(c *Checker) { c.log = l } }

// Checker probes every instance in a registry over HTTP. A probe passes on
// any 2xx or 3xx response.
type Checker struct {
	reg              registry.Registry
	client           *http.Client
	path             string
	interval         time.Duration
	failureThreshold int
	successThreshold int
	log              *slog.Logger

	mu     sync.Mutex
	streak map[string]int // >0 consecutive passes, <0 consecutive failures
}

// New returns a Checker for reg.
func New(reg registry.Registry, opts ...Option) *Checker {
	c := &Checker{
		reg:              reg,
		client:           &http.Client{Timeout: 2 * time.Second},
		path:             "/health",
		interval:         10 * time.Second,
		failureThreshold: 3,
		successThreshold: 2,
		log:              slog.Default(),
		streak:           make(map[string]int),
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// Run probes all instances every interval until ctx is cancelled.
func (c *Checker) Run(ctx context.Context) {
	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()
	for {
		c.CheckAll(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// CheckAll runs one round of probes against every instance, concurrently.
func (c *Checker) CheckAll(ctx context.Context) {
	services, err := c.reg.Services(ctx)
	if err != nil {
		c.log.Warn("health: listing services failed", "err", err)
		return
	}

	seen := make(map[string]struct{})
	var wg sync.WaitGroup
	for _, service := range services {
		instances, err := c.reg.Instances(ctx, service)
		if err != nil {
			c.log.Warn("health: listing instances failed", "service", service, "err", err)
			continue
		}
		for _, inst := range instances {
			seen[key(inst)] = struct{}{}
			wg.Add(1)
			go func(inst registry.Instance) {
				defer wg.Done()
				c.record(ctx, inst, c.probe(ctx, inst))
			}(inst)
		}
	}
	wg.Wait()

	c.mu.Lock()
	for k := range c.streak {
		if _, ok := seen[k]; !ok {
			delete(c.streak, k)
		}
	}
	c.mu.Unlock()
}

func (c *Checker) probe(ctx context.Context, inst registry.Instance) bool {
	path := c.path
	if p, ok := inst.Metadata[MetadataPath]; ok {
		path = p
	}
	url := strings.TrimRight(inst.Address, "/") + "/" + strings.TrimLeft(path, "/")
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return false
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return false
	}
	resp.Body.Close()
	return resp.StatusCode >= 200 && resp.StatusCode < 400
}

func (c *Checker) record(ctx context.Context, inst registry.Instance, passed bool) {
	c.mu.Lock()
	k := key(inst)
	s := c.streak[k]
	switch {
	case passed && s > 0:
		s++
	case passed:
		s = 1
	case s < 0:
		s--
	default:
		s = -1
	}
	c.streak[k] = s
	c.mu.Unlock()

	var next registry.Status
	switch {
	case inst.Status != registry.StatusCritical && s <= -c.failureThreshold:
		next = registry.StatusCritical
	case inst.Status == registry.StatusCritical && s >= c.successThreshold:
		next = registry.StatusPassing
	default:
		return
	}
	if err := c.reg.SetStatus(ctx, inst.Service, inst.ID, next); err != nil {
		c.log.Warn("health: updating status failed", "service", inst.Service, "instance", inst.ID, "err", err)
		return
	}
	c.log.Info("health: status changed", "service", inst.Service, "instance", inst.ID, "status", next)
}

func key(inst registry.Instance) string { return inst.Service + "/" + inst.ID }

/***
  @COPYRIGHT (c) 2026. Thomas EC. Smith (https://www.TECSmith.info). Licensed under the MIT License.
****/
