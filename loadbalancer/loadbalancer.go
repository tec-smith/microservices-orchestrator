// Package loadbalancer chooses which instance of a service handles a request.
package loadbalancer

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"

	"github.com/tec-smith/microservices-orchestrator/registry"
)

// ErrNoInstances is returned by Pick when given an empty list.
var ErrNoInstances = errors.New("loadbalancer: no instances available")

// Done reports that a request sent to the picked instance has finished.
// Callers must call it exactly once; balancers that do not track load
// return a no-op.
type Done func()

// Balancer picks one instance from a list of candidates. Implementations are
// safe for concurrent use, and the list may differ between calls.
type Balancer interface {
	Pick(instances []registry.Instance) (registry.Instance, Done, error)
}

func noop() {}

// Strategy names accepted by New.
const (
	StrategyRoundRobin       = "round-robin"
	StrategyRandom           = "random"
	StrategyWeighted         = "weighted"
	StrategyLeastConnections = "least-connections"
)

// New returns the balancer for a strategy name.
func New(strategy string) (Balancer, error) {
	switch strategy {
	case StrategyRoundRobin, "":
		return &RoundRobin{}, nil
	case StrategyRandom:
		return Random{}, nil
	case StrategyWeighted:
		return &WeightedRoundRobin{}, nil
	case StrategyLeastConnections:
		return &LeastConnections{}, nil
	default:
		return nil, fmt.Errorf("loadbalancer: unknown strategy %q", strategy)
	}
}

// RoundRobin cycles through instances in order.
type RoundRobin struct {
	next atomic.Uint64
}

func (r *RoundRobin) Pick(instances []registry.Instance) (registry.Instance, Done, error) {
	if len(instances) == 0 {
		return registry.Instance{}, nil, ErrNoInstances
	}
	n := r.next.Add(1) - 1
	return instances[n%uint64(len(instances))], noop, nil
}

// Random picks an instance uniformly at random.
type Random struct{}

func (Random) Pick(instances []registry.Instance) (registry.Instance, Done, error) {
	if len(instances) == 0 {
		return registry.Instance{}, nil, ErrNoInstances
	}
	return instances[rand.IntN(len(instances))], noop, nil
}

// WeightedRoundRobin distributes requests in proportion to Instance.Weight
// using smooth weighted round-robin, which interleaves picks rather than
// sending runs of requests to the heaviest instance. A weight of 0 counts as 1.
type WeightedRoundRobin struct {
	mu      sync.Mutex
	current map[string]int
}

func (w *WeightedRoundRobin) Pick(instances []registry.Instance) (registry.Instance, Done, error) {
	if len(instances) == 0 {
		return registry.Instance{}, nil, ErrNoInstances
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.current == nil {
		w.current = make(map[string]int)
	}

	total, best := 0, -1
	for i, inst := range instances {
		weight := max(inst.Weight, 1)
		w.current[inst.ID] += weight
		total += weight
		if best < 0 || w.current[inst.ID] > w.current[instances[best].ID] {
			best = i
		}
	}
	w.current[instances[best].ID] -= total

	if len(w.current) > len(instances) {
		w.prune(instances)
	}
	return instances[best], noop, nil
}

func (w *WeightedRoundRobin) prune(instances []registry.Instance) {
	live := make(map[string]struct{}, len(instances))
	for _, inst := range instances {
		live[inst.ID] = struct{}{}
	}
	for id := range w.current {
		if _, ok := live[id]; !ok {
			delete(w.current, id)
		}
	}
}

// LeastConnections picks the instance with the fewest requests in flight,
// breaking ties at random. Callers must call Done when each request ends.
type LeastConnections struct {
	mu     sync.Mutex
	active map[string]int
}

func (l *LeastConnections) Pick(instances []registry.Instance) (registry.Instance, Done, error) {
	if len(instances) == 0 {
		return registry.Instance{}, nil, ErrNoInstances
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.active == nil {
		l.active = make(map[string]int)
	}

	// Start from a random offset so ties are spread evenly.
	offset := rand.IntN(len(instances))
	best := offset
	for i := 1; i < len(instances); i++ {
		j := (offset + i) % len(instances)
		if l.active[instances[j].ID] < l.active[instances[best].ID] {
			best = j
		}
	}
	id := instances[best].ID
	l.active[id]++

	var once sync.Once
	return instances[best], func() {
		once.Do(func() {
			l.mu.Lock()
			defer l.mu.Unlock()
			if l.active[id]--; l.active[id] <= 0 {
				delete(l.active, id)
			}
		})
	}, nil
}

// Active returns the number of requests in flight to an instance.
func (l *LeastConnections) Active(id string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.active[id]
}

/***
  @COPYRIGHT (c) 2026. Thomas EC. Smith (https://www.TECSmith.info). Licensed under the MIT License.
****/
