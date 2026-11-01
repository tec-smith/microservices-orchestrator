// Command echo-service is a demo microservice. It registers itself with the
// orchestrator, keeps its lease alive, and answers every request with its
// own instance ID, so you can watch load balancing and failover happen.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/tec-smith/microservices-orchestrator/registry"
)

func main() {
	hostname, _ := os.Hostname()
	addr := flag.String("addr", ":9000", "listen address")
	name := flag.String("name", "echo", "service name to register")
	id := flag.String("id", "", "instance ID (default hostname-port)")
	advertise := flag.String("advertise", "", "address other services use to reach this one (default http://localhost<addr>)")
	registryURL := flag.String("registry", "http://localhost:8500", "registry base URL")
	weight := flag.Int("weight", 1, "load balancing weight")
	failRate := flag.Float64("fail-rate", 0, "fraction of requests to answer with 500, to demonstrate circuit breaking")
	flag.Parse()

	if *advertise == "" {
		*advertise = "http://localhost" + *addr
	}
	if *id == "" {
		*id = fmt.Sprintf("%s%s", hostname, *addr)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if rand.Float64() < *failRate {
			http.Error(w, "simulated failure", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{
			"service":  *name,
			"instance": *id,
			"method":   r.Method,
			"path":     r.URL.Path,
			"time":     time.Now().UTC().Format(time.RFC3339Nano),
		})
	})

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	srv := &http.Server{Addr: *addr, Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("server failed", "err", err)
			stop()
		}
	}()

	reg := registry.NewHTTPClient(*registryURL)
	inst := registry.Instance{ID: *id, Service: *name, Address: *advertise, Weight: *weight}

	// The registry may start after us (e.g. under Docker Compose), so retry
	// the first registration.
	for {
		err := registry.KeepAlive(ctx, reg, inst, 10*time.Second)
		if err == nil || ctx.Err() != nil {
			break
		}
		slog.Warn("registration failed, retrying", "err", err)
		select {
		case <-ctx.Done():
		case <-time.After(2 * time.Second):
		}
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
}

/***
  @COPYRIGHT (c) 2026. Thomas EC. Smith (https://www.TECSmith.info). Licensed under the MIT License.
****/
