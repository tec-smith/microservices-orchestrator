# Microservices Orchestrator

A lightweight orchestration framework for microservices in Go, with service
discovery, load balancing and circuit breakers. Use it as a library inside your
services, or run it as a standalone registry and API gateway. It depends on
nothing but the Go standard library.

[![CI](https://github.com/tec-smith/microservices-orchestrator/actions/workflows/ci.yml/badge.svg)](https://github.com/tec-smith/microservices-orchestrator/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/tec-smith/microservices-orchestrator.svg)](https://pkg.go.dev/github.com/tec-smith/microservices-orchestrator)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

More about this project: [tecsmith.info/portfolio/repositories/microservices-orchestrator](https://www.tecsmith.info/portfolio/repositories/microservices-orchestrator)

## When to use this

"Orchestration" here means coordinating how services find and call each
other, not scheduling containers. This project doesn't start, scale or place
your services; it helps the ones you already run talk to each other reliably.

**It's a good fit for:**

- Small and medium systems that need discovery and resilience without
  running Consul, etcd or a service mesh.
- Go services that want client-side load balancing, retries and circuit
  breakers behind a plain `http.Client`.
- Local development and test environments that need a registry and gateway
  in one small binary.
- Learning how these patterns work: each one is a short, readable package.

**Use something else when:**

- You need to schedule and run containers. That's Kubernetes or Nomad.
- You already run Kubernetes. Its Services and DNS give you discovery, and a
  mesh such as Linkerd or Istio adds retries and breakers for every language.
- You need a highly available registry. The built-in one is in memory on a
  single node, although you can plug in your own (see
  [Plug in another registry](#using-it-as-a-library)).

## What's included

| Area              | What you get                                                                                                                              |
| ----------------- | ----------------------------------------------------------------------------------------------------------------------------------------- |
| Service discovery | A registry with leases and heartbeats, an in-memory store, a JSON REST API, an HTTP client, and watches that stream changes               |
| Registration      | `registry.KeepAlive`: register on startup, renew the lease, re-register if the registry forgets you, deregister on shutdown               |
| Health checking   | Active HTTP probes with failure and recovery thresholds, so unhealthy instances stop receiving traffic                                    |
| Load balancing    | Round-robin, random, smooth weighted round-robin, and least-connections                                                                   |
| Circuit breaking  | A closed, open and half-open breaker per instance, with pluggable trip rules (consecutive failures or failure ratio)                      |
| Resilience        | Failover to another instance for idempotent requests, request bodies replayed on retry, stale instance lists used if the registry is down |
| Client library    | An `http.RoundTripper`: call `http://users/api/...` with a normal `http.Client` and the service name is resolved for you                  |
| API gateway       | A reverse proxy that routes `/{service}/{path}` to a healthy instance, with a circuit-state endpoint                                      |
| Operations        | One small binary configured by flags or environment variables, structured logs, graceful shutdown, a Docker image, GitHub Actions CI      |

## Quick start

You need Go 1.22 or later. Docker is optional, for the demo stack.

```bash
git clone https://github.com/tec-smith/microservices-orchestrator.git
cd microservices-orchestrator

# Run the whole demo: the orchestrator and three echo services
make demo
```

Then, in another terminal:

```bash
curl localhost:8080/echo/hello                 # through the gateway
curl localhost:8500/v1/services/echo/instances # what the registry knows
curl localhost:8080/_orchestrator/circuits     # breaker state per instance
```

The demo balances by weight, so `echo-2` (weight 2) gets half the traffic.
`echo-3` fails three quarters of its requests on purpose. After three
failures in a row its circuit opens, it gets no traffic for 10 seconds, and
then one trial request decides whether it closes again. Send a burst of
requests to watch it happen:

```bash
for i in $(seq 40); do curl -s localhost:8080/echo/hello; echo; done
curl localhost:8080/_orchestrator/circuits
```

> **No Docker?** Run each piece in its own terminal:
>
> ```bash
> go run ./cmd/orchestrator
> go run ./examples/echo-service -addr :9001 -id echo-1
> go run ./examples/echo-service -addr :9002 -id echo-2
> go run ./examples/client -n 10       # client-side discovery, no gateway
> ```

## Commands

| Command         | What it does                                                     |
| --------------- | ---------------------------------------------------------------- |
| `make build`    | Build the orchestrator and example binaries into `./bin`         |
| `make run`      | Run the orchestrator locally                                     |
| `make demo`     | Run the orchestrator and three echo services with Docker Compose |
| `make validate` | Format check, vet and tests: run before you push                 |
| `make test`     | Tests with the race detector                                     |
| `make coverage` | Tests with a coverage report in `coverage.html`                  |
| `make fmt`      | Format every Go file (`fmt-check` to verify)                     |
| `make docker`   | Build the Docker image                                           |

## Project structure

```text
├── transport.go             # Service-aware http.RoundTripper: discovery, balancing, breakers, failover
├── gateway.go               # Reverse proxy routing /{service}/{path}
├── example_test.go          # Runnable examples, shown on pkg.go.dev
├── registry/
│   ├── registry.go          # Instance, Status and the Registry interface
│   ├── memory.go            # In-memory registry with leases and watches
│   ├── httpapi.go           # REST API for a registry
│   ├── httpclient.go        # Registry client for the REST API
│   └── agent.go             # KeepAlive: register, heartbeat, deregister
├── health/                  # Active HTTP health checker
├── loadbalancer/            # Round-robin, random, weighted, least-connections
├── circuitbreaker/          # Closed, open and half-open breaker
├── cmd/orchestrator/        # Standalone registry, health checker and gateway
├── examples/
│   ├── echo-service/        # A service that registers itself
│   └── client/              # Client-side discovery from another program
├── Dockerfile               # Image with all three binaries
├── docker-compose.yml       # Demo stack
└── .github/                 # CI workflow, Dependabot, issue and PR templates
```

## How it works

```text
                      register / heartbeat
     ┌──────────────┐ ───────────────────────────► ┌──────────────────────┐
     │  service A   │                              │      registry        │
     │  instance 1  │ ◄─────────────────────────── │  (leases, statuses)  │
     └──────────────┘        health probes         └──────────┬───────────┘
            ▲                                                 │ instances
            │                                                 ▼
            │      ┌──────────────────────────────────────────────────────┐
            └──────│ Transport: healthy only → skip open circuits →       │◄── http://service-a/path
                   │ load balancer picks → breaker records → fail over    │    (your code or the gateway)
                   └──────────────────────────────────────────────────────┘
```

- **Registration.** Each instance has an ID, a service name and a base URL
  such as `http://10.0.0.5:8080`. `registry.KeepAlive` registers it and sends
  a heartbeat every interval. If the registry restarts and forgets the
  instance, the next heartbeat gets a not-found and the instance registers
  again. When the context ends, it deregisters.
- **Leases.** The registry evicts an instance that misses heartbeats for the
  TTL (30s by default). This catches crashed processes that never deregister.
- **Health checks.** Heartbeats prove a process is alive, not that it can
  serve. The health checker probes `GET /health` on every instance (or the
  path in the instance's `health_path` metadata). After 3 failed probes in a
  row it marks the instance critical, and after 2 passing probes it marks it
  passing again. Any 2xx or 3xx response passes.
- **Routing.** The transport reads the service name from the request host,
  looks up its instances (cached for 2s), keeps the passing ones, removes any
  whose circuit is open, and asks the load balancer to pick one. If the
  registry can't be reached, it keeps using the last list it saw.
- **Circuit breakers.** Each instance gets its own breaker. A transport error
  or a 5xx response counts as a failure. After 5 failures in a row the
  circuit opens and the instance gets no traffic for 30s. Then one trial
  request goes through: success closes the circuit, failure opens it again.
  A request the caller cancelled doesn't count against the instance.
- **Failover.** When an instance returns a transport error, 502, 503 or 504,
  the request is retried on a different instance, up to 2 more times. This
  only happens for idempotent methods (`GET`, `HEAD`, `OPTIONS`, `PUT`,
  `DELETE`), or requests with an `Idempotency-Key` header, so a `POST` is
  never sent twice by accident. Request bodies are replayed with
  `Request.GetBody`, which `http.NewRequest` sets for in-memory bodies.
  Other 5xx responses are returned to the caller as they are.

### Load balancing strategies

| Strategy            | Picks                                                                                   |
| ------------------- | --------------------------------------------------------------------------------------- |
| `round-robin`       | Each instance in turn (the default)                                                     |
| `random`            | An instance at random                                                                   |
| `weighted`          | In proportion to `Instance.Weight`, interleaved so heavy instances don't get bursts     |
| `least-connections` | The instance with the fewest requests in flight, counted until the response body closes |

## Using it as a library

**Register a service.** Run `KeepAlive` next to your HTTP server:

```go
reg := registry.NewHTTPClient("http://orchestrator:8500")
inst := registry.Instance{
	ID:      "users-1",
	Service: "users",
	Address: "http://10.0.0.5:8080",
}
go registry.KeepAlive(ctx, reg, inst, 10*time.Second)
```

**Call a service.** Use the service name as the host:

```go
client := orchestrator.NewClient(reg,
	orchestrator.WithBalancer(&loadbalancer.LeastConnections{}),
	orchestrator.WithMaxRetries(2),
)
resp, err := client.Get("http://users/api/v1/users/42")
```

`errors.Is(err, orchestrator.ErrNoInstances)` tells you the service has no
healthy instances. `errors.Is(err, orchestrator.ErrAllCircuitsOpen)` tells you
it has some, but all of their circuits are open.

**Tune the breakers.** Pass settings that every per-instance breaker copies:

```go
orchestrator.WithBreakerSettings(circuitbreaker.Settings{
	ReadyToTrip: circuitbreaker.FailureRatio(0.5, 20), // 50% of at least 20 requests
	Interval:    time.Minute,                          // forget old failures
	Timeout:     15 * time.Second,
	MaxRequests: 3,                                    // trial requests when half-open
})
```

**Use a breaker on its own.** It works around any call, not just HTTP:

```go
cb := circuitbreaker.New(circuitbreaker.Settings{Name: "payments"})
err := cb.Execute(func() error { return chargeCard(ctx, order) })
```

**Run everything in one process.** For tests, or a small system without a
separate registry, use `registry.NewMemory()` directly, and add
`health.New(reg).Run(ctx)` if you want active health checks.

**Plug in another registry.** Anything that implements `registry.Registry`
works with the transport, the gateway and the health checker. A Consul, etcd
or Kubernetes adapter only needs those seven methods.

## Registry API

The orchestrator serves this API on the registry address (`:8500` by default).

| Method   | Path                                              | Does                                           |
| -------- | ------------------------------------------------- | ---------------------------------------------- |
| `GET`    | `/v1/services`                                    | Lists service names                            |
| `GET`    | `/v1/services/{service}/instances`                | Lists instances; add `?healthy=true` to filter |
| `POST`   | `/v1/services/{service}/instances`                | Registers an instance (JSON `Instance` body)   |
| `DELETE` | `/v1/services/{service}/instances/{id}`           | Deregisters an instance                        |
| `PUT`    | `/v1/services/{service}/instances/{id}/heartbeat` | Renews the lease; 404 means register again     |
| `PUT`    | `/v1/services/{service}/instances/{id}/status`    | Sets `{"status": "passing"}` or `"critical"`   |
| `GET`    | `/healthz`                                        | Health of the registry itself                  |

Register from any language:

```bash
curl -X POST localhost:8500/v1/services/users/instances \
  -H 'Content-Type: application/json' \
  -d '{"id": "users-1", "address": "http://10.0.0.5:8080", "weight": 2}'
```

The gateway (`:8080` by default) routes `/{service}/{path}` and also serves
`GET /_orchestrator/health` and `GET /_orchestrator/circuits`.

## Testing

```bash
make test       # all tests, with the race detector
make coverage   # and a coverage report
```

- **Unit tests** cover each package on its own: breaker state changes against
  a fake clock, the exact pick order of each balancer, lease expiry and
  watches in the registry, and health thresholds.
- **Examples** (`example_test.go`) are run as tests and checked against
  their expected output, so the usage shown on pkg.go.dev always works.
- **Integration tests** start real HTTP servers with `httptest`: the registry
  API and client run the same test suite as the in-memory registry, and the
  transport tests cover balancing, skipping critical instances, failover on
  errors and 503s, body replay, `POST` not being retried, circuits opening and
  recovering, and the gateway.

CI runs formatting, vet and the tests on each push and pull request, against
both the minimum Go version in `go.mod` and the latest release, and checks
that the Docker image builds.

## Configuration

`orchestrator` reads each setting from a flag, or from the environment
variable shown. Flags win. Run `orchestrator -h` for the full list.

| Flag                | Variable           | Default       | Purpose                                                          |
| ------------------- | ------------------ | ------------- | ---------------------------------------------------------------- |
| `-registry-addr`    | `REGISTRY_ADDR`    | `:8500`       | Listen address of the registry API                               |
| `-gateway-addr`     | `GATEWAY_ADDR`     | `:8080`       | Listen address of the gateway; empty turns it off                |
| `-ttl`              | `REGISTRY_TTL`     | `30s`         | Evict instances that miss heartbeats this long; `0` turns it off |
| `-health-interval`  | `HEALTH_INTERVAL`  | `10s`         | Time between health check rounds; `0` turns them off             |
| `-health-path`      | `HEALTH_PATH`      | `/health`     | Path probed on each instance                                     |
| `-health-timeout`   | `HEALTH_TIMEOUT`   | `2s`          | Timeout of each probe                                            |
| `-lb`               | `LB_STRATEGY`      | `round-robin` | `round-robin`, `random`, `weighted` or `least-connections`       |
| `-retries`          | `RETRIES`          | `2`           | Other instances to try after a failed idempotent request         |
| `-breaker-failures` | `BREAKER_FAILURES` | `5`           | Consecutive failures that open an instance's circuit             |
| `-breaker-timeout`  | `BREAKER_TIMEOUT`  | `30s`         | How long a circuit stays open before a trial request             |
| `-log-level`        | `LOG_LEVEL`        | `info`        | `debug`, `info`, `warn` or `error`                               |
| `-log-format`       | `LOG_FORMAT`       | `text`        | `text` or `json`                                                 |

Keep the heartbeat interval in `KeepAlive` well under the TTL. A third of it
(10s against 30s) lets an instance miss two heartbeats before it's evicted.

## Deployment

### Docker

The `Dockerfile` builds an Alpine image that runs as a non-root user, with a
health check on the registry. It contains `orchestrator`, `echo-service` and
`client`, and runs `orchestrator` by default.

```bash
docker build -t microservices-orchestrator .
docker run -p 8500:8500 -p 8080:8080 microservices-orchestrator
```

Services in other containers register with the address other containers use
to reach them, such as `http://users:8080`, not `localhost`.

### Before going to production

- **The registry is in memory.** If the orchestrator restarts, it starts
  empty, and services re-register on their next heartbeat (within one
  interval). Run one instance per environment. For high availability,
  implement `registry.Registry` on top of Consul, etcd or a database.
- **The registry API has no authentication.** Anyone who can reach it can
  register an instance and receive traffic. Keep it on a private network, or
  put it behind a proxy that checks credentials or mutual TLS.
- **Instances are reached over plain HTTP** unless their address uses
  `https://`. Pass a base transport with your TLS config through
  `orchestrator.WithBaseTransport`.
- **Set timeouts.** Give your `http.Client` a `Timeout`, or each request a
  context deadline. The breaker can only count a hung instance as failing
  once the request ends.
- **Tune the breaker to your traffic.** Five consecutive failures suits low
  traffic. For busy services, `FailureRatio` with an `Interval` reacts to a
  partial failure that consecutive counts can miss.
- **Use JSON logs** (`LOG_FORMAT=json`) so your log pipeline can parse them.

## Security

What the project does by default:

- Request bodies to the registry API are capped at 1 MB, and unknown JSON
  fields are rejected.
- Instance addresses must be absolute `http` or `https` URLs.
- The HTTP servers set header read and idle timeouts, and shut down
  gracefully on `SIGINT` and `SIGTERM`.
- The Docker image runs as a non-root user.
- No third-party dependencies, so there's no supply chain beyond the Go
  toolchain.

See [before going to production](#before-going-to-production) for what you
need to add, in particular authentication on the registry API.

Found a vulnerability? See [SECURITY.md](SECURITY.md).

## Contributing

Contributions are welcome. See [CONTRIBUTING.md](CONTRIBUTING.md), and
[CHANGELOG.md](CHANGELOG.md) for what's changed between releases.

Are you using this project as part of your own solution? [Buy me a cup of coffee to keep me tapping away...](https://www.buymeacoffee.com/tecsmith)

This took me, [Thomas EC. Smith](https://linktr.ee/tecsmith), time and effort to put together. If you're feeling particularly generous, a financial contribution of any amount would be greatly appreciated, and thank you for your consideration!

<table align="center" width="100%" style="border-collapse: collapse;">
  <tr>
    <td align="center" width="33%" style="border: none;"><b>PayPal</b></td>
    <td align="center" width="33%" style="border: none;"><b>Wise</b></td>
    <td align="center" width="34%" style="border: none;"><b>Revolut</b></td>
  </tr>
  <tr>
    <td align="center" style="border: none;"><img src="https://tecsmith.info/assets/image/codes/paypal.webp" alt="PayPal" width="100" height="100"></td>
    <td align="center" style="border: none;"><img src="https://tecsmith.info/assets/image/codes/wise.webp" alt="Wise" width="100" height="100"></td>
    <td align="center" style="border: none;"><img src="https://tecsmith.info/assets/image/codes/revolut.webp" alt="Revolut" width="100" height="100"></td>
  </tr>
</table>

For all enquiries, [please visit my website](https://www.tecsmith.info).

## License

[MIT](LICENSE) 2026 © Thomas EC. Smith
