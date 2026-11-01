package registry

import (
	"context"
	"sort"
	"sync"
	"time"
)

// MemoryOption configures a Memory registry.
type MemoryOption func(*Memory)

// WithTTL evicts instances that have not sent a heartbeat within ttl.
// A ttl of zero (the default) disables eviction.
func WithTTL(ttl time.Duration) MemoryOption {
	return func(m *Memory) { m.ttl = ttl }
}

// WithClock overrides the time source. It is intended for tests.
func WithClock(now func() time.Time) MemoryOption {
	return func(m *Memory) { m.now = now }
}

// Memory is an in-process Registry. It is safe for concurrent use.
type Memory struct {
	mu       sync.Mutex
	services map[string]map[string]*Instance
	watchers map[string]map[chan []Instance]struct{}

	ttl      time.Duration
	now      func() time.Time
	stop     chan struct{}
	stopOnce sync.Once
}

var _ Registry = (*Memory)(nil)

// NewMemory returns an empty in-memory registry. Call Close to stop the
// background eviction loop.
func NewMemory(opts ...MemoryOption) *Memory {
	m := &Memory{
		services: make(map[string]map[string]*Instance),
		watchers: make(map[string]map[chan []Instance]struct{}),
		now:      time.Now,
		stop:     make(chan struct{}),
	}
	for _, opt := range opts {
		opt(m)
	}
	if m.ttl > 0 {
		go m.reapLoop()
	}
	return m
}

// Close stops background eviction. Watch channels are closed when their
// contexts end.
func (m *Memory) Close() error {
	m.stopOnce.Do(func() { close(m.stop) })
	return nil
}

// Register adds an instance as passing. Registering an instance that is
// already present updates its details but keeps its status, so a restart
// cannot clear a critical status set by the health checker.
func (m *Memory) Register(_ context.Context, inst Instance) error {
	if err := inst.Validate(); err != nil {
		return err
	}
	now := m.now()
	inst = inst.clone()
	inst.Status = StatusPassing
	inst.RegisteredAt = now
	inst.LastHeartbeat = now

	m.mu.Lock()
	defer m.mu.Unlock()
	svc, ok := m.services[inst.Service]
	if !ok {
		svc = make(map[string]*Instance)
		m.services[inst.Service] = svc
	}
	if prev, ok := svc[inst.ID]; ok {
		inst.Status = prev.Status
		inst.RegisteredAt = prev.RegisteredAt
	}
	svc[inst.ID] = &inst
	m.notify(inst.Service)
	return nil
}

func (m *Memory) Deregister(_ context.Context, service, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, err := m.lookup(service, id); err != nil {
		return err
	}
	m.remove(service, id)
	return nil
}

func (m *Memory) Heartbeat(_ context.Context, service, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	inst, err := m.lookup(service, id)
	if err != nil {
		return err
	}
	inst.LastHeartbeat = m.now()
	return nil
}

func (m *Memory) SetStatus(_ context.Context, service, id string, status Status) error {
	if err := validStatus(status); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	inst, err := m.lookup(service, id)
	if err != nil {
		return err
	}
	if inst.Status != status {
		inst.Status = status
		m.notify(service)
	}
	return nil
}

func (m *Memory) Services(context.Context) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	names := make([]string, 0, len(m.services))
	for name := range m.services {
		names = append(names, name)
	}
	sort.Strings(names)
	return names, nil
}

func (m *Memory) Instances(_ context.Context, service string) ([]Instance, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.snapshot(service), nil
}

func (m *Memory) Watch(ctx context.Context, service string) (<-chan []Instance, error) {
	ch := make(chan []Instance, 1)

	m.mu.Lock()
	ws, ok := m.watchers[service]
	if !ok {
		ws = make(map[chan []Instance]struct{})
		m.watchers[service] = ws
	}
	ws[ch] = struct{}{}
	ch <- m.snapshot(service)
	m.mu.Unlock()

	go func() {
		<-ctx.Done()
		m.mu.Lock()
		defer m.mu.Unlock()
		delete(m.watchers[service], ch)
		if len(m.watchers[service]) == 0 {
			delete(m.watchers, service)
		}
		close(ch)
	}()
	return ch, nil
}

// lookup must be called with m.mu held.
func (m *Memory) lookup(service, id string) (*Instance, error) {
	inst, ok := m.services[service][id]
	if !ok {
		return nil, ErrNotFound
	}
	return inst, nil
}

// remove must be called with m.mu held.
func (m *Memory) remove(service, id string) {
	delete(m.services[service], id)
	if len(m.services[service]) == 0 {
		delete(m.services, service)
	}
	m.notify(service)
}

// snapshot must be called with m.mu held.
func (m *Memory) snapshot(service string) []Instance {
	svc := m.services[service]
	out := make([]Instance, 0, len(svc))
	for _, inst := range svc {
		out = append(out, inst.clone())
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// notify sends the latest snapshot to every watcher of service, replacing
// any snapshot the watcher has not yet consumed. It must be called with m.mu held.
func (m *Memory) notify(service string) {
	ws := m.watchers[service]
	if len(ws) == 0 {
		return
	}
	snap := m.snapshot(service)
	for ch := range ws {
		select {
		case <-ch:
		default:
		}
		select {
		case ch <- snap:
		default:
		}
	}
}

func (m *Memory) reapLoop() {
	ticker := time.NewTicker(m.ttl / 2)
	defer ticker.Stop()
	for {
		select {
		case <-m.stop:
			return
		case <-ticker.C:
			m.reap()
		}
	}
}

// reap evicts instances whose lease has expired.
func (m *Memory) reap() {
	m.mu.Lock()
	defer m.mu.Unlock()
	deadline := m.now().Add(-m.ttl)
	for service, svc := range m.services {
		for id, inst := range svc {
			if inst.LastHeartbeat.Before(deadline) {
				m.remove(service, id)
			}
		}
	}
}

/***
  @COPYRIGHT (c) 2026. Thomas EC. Smith (https://www.TECSmith.info). Licensed under the MIT License.
****/
