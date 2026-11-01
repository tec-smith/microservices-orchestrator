// Package registry provides service registration and discovery.
//
// A Registry stores the live instances of every service. Services register
// themselves on startup, send periodic heartbeats, and deregister on shutdown
// (see KeepAlive). Consumers query or watch the registry to find instances.
package registry

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"time"
)

// Status is the health status of an instance.
type Status string

const (
	// StatusPassing marks an instance as able to receive traffic.
	StatusPassing Status = "passing"
	// StatusCritical marks an instance as failing its health checks.
	StatusCritical Status = "critical"
)

var (
	// ErrNotFound is returned when a service or instance does not exist.
	ErrNotFound = errors.New("registry: instance not found")
	// ErrInvalid is returned when an instance or status fails validation.
	ErrInvalid = errors.New("registry: invalid request")
)

// Instance is a single running copy of a service.
type Instance struct {
	// ID uniquely identifies the instance within its service.
	ID string `json:"id"`
	// Service is the logical service name, e.g. "users".
	Service string `json:"service"`
	// Address is the base URL of the instance, e.g. "http://10.0.0.5:8080".
	Address string `json:"address"`
	// Weight is used by weighted load balancers. Zero means 1.
	Weight   int               `json:"weight,omitempty"`
	Tags     []string          `json:"tags,omitempty"`
	Metadata map[string]string `json:"metadata,omitempty"`

	Status        Status    `json:"status"`
	RegisteredAt  time.Time `json:"registered_at"`
	LastHeartbeat time.Time `json:"last_heartbeat"`
}

// Healthy reports whether the instance should receive traffic.
func (i Instance) Healthy() bool { return i.Status != StatusCritical }

// Validate checks that the instance has the fields required for routing.
func (i Instance) Validate() error {
	if i.ID == "" || i.Service == "" || i.Address == "" {
		return fmt.Errorf("%w: id, service and address are required", ErrInvalid)
	}
	u, err := url.Parse(i.Address)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return fmt.Errorf("%w: address %q must be an absolute URL such as http://10.0.0.5:8080", ErrInvalid, i.Address)
	}
	if i.Weight < 0 {
		return fmt.Errorf("%w: weight must not be negative", ErrInvalid)
	}
	return nil
}

func (i Instance) clone() Instance {
	if i.Tags != nil {
		i.Tags = append([]string(nil), i.Tags...)
	}
	if i.Metadata != nil {
		md := make(map[string]string, len(i.Metadata))
		for k, v := range i.Metadata {
			md[k] = v
		}
		i.Metadata = md
	}
	return i
}

// Registry is the interface implemented by service registries.
type Registry interface {
	// Register adds or replaces an instance.
	Register(ctx context.Context, inst Instance) error
	// Deregister removes an instance.
	Deregister(ctx context.Context, service, id string) error
	// Heartbeat renews an instance's lease. It returns ErrNotFound if the
	// instance is not registered, in which case the caller should register again.
	Heartbeat(ctx context.Context, service, id string) error
	// SetStatus updates an instance's health status.
	SetStatus(ctx context.Context, service, id string, status Status) error
	// Services lists the names of all services with at least one instance.
	Services(ctx context.Context) ([]string, error)
	// Instances lists every instance of a service, healthy or not.
	Instances(ctx context.Context, service string) ([]Instance, error)
	// Watch streams the instance list of a service each time it changes,
	// starting with the current list. The channel is closed when ctx ends.
	Watch(ctx context.Context, service string) (<-chan []Instance, error)
}

// Healthy returns the instances that should receive traffic.
func Healthy(instances []Instance) []Instance {
	out := make([]Instance, 0, len(instances))
	for _, inst := range instances {
		if inst.Healthy() {
			out = append(out, inst)
		}
	}
	return out
}

func validStatus(s Status) error {
	if s != StatusPassing && s != StatusCritical {
		return fmt.Errorf("%w: unknown status %q", ErrInvalid, s)
	}
	return nil
}

/***
  @COPYRIGHT (c) 2026. Thomas EC. Smith (https://www.TECSmith.info). Licensed under the MIT License.
****/
