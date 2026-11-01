// Command orchestrator runs a service registry, an active health checker and
// an API gateway in one process.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	orchestrator "github.com/tec-smith/microservices-orchestrator"
	"github.com/tec-smith/microservices-orchestrator/circuitbreaker"
	"github.com/tec-smith/microservices-orchestrator/health"
	"github.com/tec-smith/microservices-orchestrator/loadbalancer"
	"github.com/tec-smith/microservices-orchestrator/registry"
)

type config struct {
	registryAddr    string
	gatewayAddr     string
	ttl             time.Duration
	healthInterval  time.Duration
	healthPath      string
	healthTimeout   time.Duration
	strategy        string
	retries         int
	breakerFailures uint
	breakerTimeout  time.Duration
	logLevel        string
	logFormat       string
}

func main() {
	var cfg config
	flag.StringVar(&cfg.registryAddr, "registry-addr", env("REGISTRY_ADDR", ":8500"), "listen address of the registry API")
	flag.StringVar(&cfg.gatewayAddr, "gateway-addr", env("GATEWAY_ADDR", ":8080"), "listen address of the API gateway; empty disables it")
	flag.DurationVar(&cfg.ttl, "ttl", envDuration("REGISTRY_TTL", 30*time.Second), "evict instances that miss heartbeats for this long; 0 disables")
	flag.DurationVar(&cfg.healthInterval, "health-interval", envDuration("HEALTH_INTERVAL", 10*time.Second), "time between active health checks; 0 disables")
	flag.StringVar(&cfg.healthPath, "health-path", env("HEALTH_PATH", "/health"), "path probed on each instance")
	flag.DurationVar(&cfg.healthTimeout, "health-timeout", envDuration("HEALTH_TIMEOUT", 2*time.Second), "timeout of each health probe")
	flag.StringVar(&cfg.strategy, "lb", env("LB_STRATEGY", loadbalancer.StrategyRoundRobin), "load balancing strategy: round-robin, random, weighted, least-connections")
	flag.IntVar(&cfg.retries, "retries", envInt("RETRIES", 2), "other instances to try after a failed idempotent request")
	flag.UintVar(&cfg.breakerFailures, "breaker-failures", uint(envInt("BREAKER_FAILURES", 5)), "consecutive failures that open an instance's circuit")
	flag.DurationVar(&cfg.breakerTimeout, "breaker-timeout", envDuration("BREAKER_TIMEOUT", 30*time.Second), "how long a circuit stays open before a trial request")
	flag.StringVar(&cfg.logLevel, "log-level", env("LOG_LEVEL", "info"), "debug, info, warn or error")
	flag.StringVar(&cfg.logFormat, "log-format", env("LOG_FORMAT", "text"), "text or json")
	flag.Parse()

	if err := run(cfg); err != nil {
		slog.Error("orchestrator stopped", "err", err)
		os.Exit(1)
	}
}

func run(cfg config) error {
	log, err := newLogger(cfg.logLevel, cfg.logFormat)
	if err != nil {
		return err
	}
	slog.SetDefault(log)

	balancer, err := loadbalancer.New(cfg.strategy)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	reg := registry.NewMemory(registry.WithTTL(cfg.ttl))
	defer reg.Close()

	if cfg.healthInterval > 0 {
		checker := health.New(reg,
			health.WithInterval(cfg.healthInterval),
			health.WithPath(cfg.healthPath),
			health.WithTimeout(cfg.healthTimeout),
			health.WithLogger(log),
		)
		go checker.Run(ctx)
	}

	servers := []*http.Server{newServer(cfg.registryAddr, registry.NewHandler(reg))}
	log.Info("registry listening", "addr", cfg.registryAddr)

	if cfg.gatewayAddr != "" {
		transport := orchestrator.NewTransport(reg,
			orchestrator.WithBalancer(balancer),
			orchestrator.WithMaxRetries(cfg.retries),
			orchestrator.WithBreakerSettings(circuitbreaker.Settings{
				ReadyToTrip: circuitbreaker.ConsecutiveFailures(uint32(cfg.breakerFailures)),
				Timeout:     cfg.breakerTimeout,
			}),
			orchestrator.WithLogger(log),
		)
		mux := http.NewServeMux()
		mux.HandleFunc("GET /_orchestrator/health", func(w http.ResponseWriter, _ *http.Request) {
			writeJSON(w, map[string]string{"status": "ok"})
		})
		mux.HandleFunc("GET /_orchestrator/circuits", func(w http.ResponseWriter, _ *http.Request) {
			writeJSON(w, transport.CircuitStates())
		})
		mux.Handle("/", orchestrator.NewGateway(transport, log))
		servers = append(servers, newServer(cfg.gatewayAddr, mux))
		log.Info("gateway listening", "addr", cfg.gatewayAddr, "lb", cfg.strategy)
	}

	errs := make(chan error, len(servers))
	for _, srv := range servers {
		go func(srv *http.Server) {
			if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				errs <- fmt.Errorf("%s: %w", srv.Addr, err)
			}
		}(srv)
	}

	select {
	case err := <-errs:
		return err
	case <-ctx.Done():
	}

	log.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	for _, srv := range servers {
		if err := srv.Shutdown(shutdownCtx); err != nil {
			log.Warn("shutdown", "addr", srv.Addr, "err", err)
		}
	}
	return nil
}

func newServer(addr string, h http.Handler) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           h,
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
}

func newLogger(level, format string) (*slog.Logger, error) {
	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(level)); err != nil {
		return nil, fmt.Errorf("invalid log level %q", level)
	}
	opts := &slog.HandlerOptions{Level: lvl}
	switch strings.ToLower(format) {
	case "text":
		return slog.New(slog.NewTextHandler(os.Stderr, opts)), nil
	case "json":
		return slog.New(slog.NewJSONHandler(os.Stderr, opts)), nil
	default:
		return nil, fmt.Errorf("invalid log format %q", format)
	}
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func env(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return fallback
}

func envDuration(key string, fallback time.Duration) time.Duration {
	if d, err := time.ParseDuration(os.Getenv(key)); err == nil {
		return d
	}
	return fallback
}

func envInt(key string, fallback int) int {
	var n int
	if _, err := fmt.Sscan(os.Getenv(key), &n); err == nil {
		return n
	}
	return fallback
}

/***
  @COPYRIGHT (c) 2026. Thomas EC. Smith (https://www.TECSmith.info). Licensed under the MIT License.
****/
