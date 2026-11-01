package health

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/tec-smith/microservices-orchestrator/registry"
)

func TestCheckerMarksCriticalAndRecovers(t *testing.T) {
	var healthy atomic.Bool
	healthy.Store(true)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/health" || !healthy.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
	}))
	defer srv.Close()

	reg := registry.NewMemory()
	defer reg.Close()
	ctx := context.Background()
	_ = reg.Register(ctx, registry.Instance{ID: "a", Service: "svc", Address: srv.URL})

	c := New(reg, WithThresholds(2, 2))
	status := func() registry.Status {
		got, _ := reg.Instances(ctx, "svc")
		return got[0].Status
	}

	healthy.Store(false)
	c.CheckAll(ctx)
	if s := status(); s != registry.StatusPassing {
		t.Fatalf("after 1 failure status = %s, want passing", s)
	}
	c.CheckAll(ctx)
	if s := status(); s != registry.StatusCritical {
		t.Fatalf("after 2 failures status = %s, want critical", s)
	}

	healthy.Store(true)
	c.CheckAll(ctx)
	if s := status(); s != registry.StatusCritical {
		t.Fatalf("after 1 pass status = %s, want critical", s)
	}
	c.CheckAll(ctx)
	if s := status(); s != registry.StatusPassing {
		t.Fatalf("after 2 passes status = %s, want passing", s)
	}
}

func TestCheckerUsesMetadataPath(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/ready" {
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	reg := registry.NewMemory()
	defer reg.Close()
	ctx := context.Background()
	_ = reg.Register(ctx, registry.Instance{
		ID: "a", Service: "svc", Address: srv.URL,
		Metadata: map[string]string{MetadataPath: "/ready"},
	})

	c := New(reg, WithThresholds(1, 1))
	c.CheckAll(ctx)
	got, _ := reg.Instances(ctx, "svc")
	if got[0].Status != registry.StatusPassing {
		t.Fatalf("status = %s, want passing", got[0].Status)
	}
}

/***
  @COPYRIGHT (c) 2026. Thomas EC. Smith (https://www.TECSmith.info). Licensed under the MIT License.
****/
