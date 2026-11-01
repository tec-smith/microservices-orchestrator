# Changelog

All notable changes to this project are listed here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow
[Semantic Versioning](https://semver.org/). Until 1.0, minor versions may
change the API.

## [Unreleased]

## [0.1.0] - 2026-11-01

First release.

### Added

- Service registry: the `registry.Registry` interface, an in-memory store with
  leases and watches, a JSON REST API, an HTTP client, and `KeepAlive` for
  services to register, renew and deregister themselves.
- Active HTTP health checks with failure and recovery thresholds.
- Load balancers: round-robin, random, smooth weighted round-robin and
  least-connections.
- A circuit breaker with closed, open and half-open states and pluggable trip
  rules.
- `orchestrator.Transport`: a service-aware `http.RoundTripper` with a breaker
  per instance and failover for idempotent requests.
- An API gateway that routes `/{service}/{path}`.
- The `orchestrator` binary, the echo service and client examples, a Docker
  image and a Docker Compose demo.

[Unreleased]: https://github.com/tec-smith/microservices-orchestrator/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/tec-smith/microservices-orchestrator/releases/tag/v0.1.0
