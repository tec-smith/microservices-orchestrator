# Contributing

Thanks for helping improve the orchestrator. Bug reports, fixes and focused
improvements are all welcome.

## Getting set up

Follow the [quick start](README.md#quick-start) to get the demo running, then
check everything passes:

```bash
make validate   # format check, vet, tests with the race detector
```

The race detector needs cgo, so you need a C compiler such as `gcc` installed.

## Making a change

1. Open an issue first for anything larger than a small fix, so we can agree
   on the approach before you put time in.
2. Branch from `main` and keep the change focused on one thing.
3. Add or update tests for behaviour you change.
4. Add a line under `Unreleased` in [CHANGELOG.md](CHANGELOG.md) for any
   change users will notice.
5. Run `make fmt` and `make validate` before pushing.
6. Open a pull request describing what changed and how you tested it.

## Guidelines

- The project should stay small and use only the standard library. Prefer a
  clear extension point, such as a new `registry.Registry` or
  `loadbalancer.Balancer` implementation, and a note in the README over
  adding a dependency.
- Every exported name needs a doc comment; they're the reference on
  [pkg.go.dev](https://pkg.go.dev/github.com/tec-smith/microservices-orchestrator).
- Everything shared between goroutines must be safe for concurrent use, and
  tested under `-race`.
- Tests that depend on time take a clock (see `circuitbreaker` and
  `registry.WithClock`) rather than sleeping, where they can.
