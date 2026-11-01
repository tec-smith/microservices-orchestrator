package loadbalancer

import (
	"errors"
	"strings"
	"testing"

	"github.com/tec-smith/microservices-orchestrator/registry"
)

func instances(ids ...string) []registry.Instance {
	out := make([]registry.Instance, len(ids))
	for i, id := range ids {
		out[i] = registry.Instance{ID: id, Service: "svc", Address: "http://" + id}
	}
	return out
}

func pickN(t *testing.T, b Balancer, insts []registry.Instance, n int) []string {
	t.Helper()
	ids := make([]string, n)
	for i := range ids {
		inst, done, err := b.Pick(insts)
		if err != nil {
			t.Fatal(err)
		}
		done()
		ids[i] = inst.ID
	}
	return ids
}

func TestEmptyList(t *testing.T) {
	for _, name := range []string{StrategyRoundRobin, StrategyRandom, StrategyWeighted, StrategyLeastConnections} {
		b, err := New(name)
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := b.Pick(nil); !errors.Is(err, ErrNoInstances) {
			t.Errorf("%s: err = %v, want ErrNoInstances", name, err)
		}
	}
}

func TestUnknownStrategy(t *testing.T) {
	if _, err := New("fastest"); err == nil {
		t.Fatal("expected an error")
	}
}

func TestRoundRobin(t *testing.T) {
	got := strings.Join(pickN(t, &RoundRobin{}, instances("a", "b", "c"), 6), "")
	if got != "abcabc" {
		t.Fatalf("got %s, want abcabc", got)
	}
}

func TestRandomCoversAll(t *testing.T) {
	seen := map[string]bool{}
	for _, id := range pickN(t, Random{}, instances("a", "b", "c"), 300) {
		seen[id] = true
	}
	if len(seen) != 3 {
		t.Fatalf("saw %v, want all three instances", seen)
	}
}

func TestWeightedIsSmoothAndProportional(t *testing.T) {
	insts := instances("a", "b", "c")
	insts[0].Weight = 5
	insts[1].Weight = 1
	insts[2].Weight = 1

	got := strings.Join(pickN(t, &WeightedRoundRobin{}, insts, 7), "")
	// Smooth WRR interleaves the light instances instead of sending five
	// requests in a row to "a".
	if got != "aabacaa" {
		t.Fatalf("got %s, want aabacaa", got)
	}
}

func TestWeightedPrunesRemovedInstances(t *testing.T) {
	w := &WeightedRoundRobin{}
	pickN(t, w, instances("a", "b", "c"), 3)
	pickN(t, w, instances("a"), 1)
	if len(w.current) != 1 {
		t.Fatalf("tracked %d instances, want 1", len(w.current))
	}
}

func TestLeastConnections(t *testing.T) {
	l := &LeastConnections{}
	insts := instances("a", "b")

	first, done1, _ := l.Pick(insts)
	second, done2, _ := l.Pick(insts)
	if first.ID == second.ID {
		t.Fatalf("both picks went to %s while it was busy", first.ID)
	}

	// Release only the first; the next pick must go to it.
	done1()
	third, done3, _ := l.Pick(insts)
	if third.ID != first.ID {
		t.Fatalf("picked %s, want idle instance %s", third.ID, first.ID)
	}

	done2()
	done3()
	done3() // Done is idempotent.
	if l.Active("a") != 0 || l.Active("b") != 0 {
		t.Fatalf("active = a:%d b:%d, want 0", l.Active("a"), l.Active("b"))
	}
}

/***
  @COPYRIGHT (c) 2026. Thomas EC. Smith (https://www.TECSmith.info). Licensed under the MIT License.
****/
