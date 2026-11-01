package registry

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// HTTPClientOption configures an HTTPClient.
type HTTPClientOption func(*HTTPClient)

// WithHTTPClient sets the http.Client used to reach the registry.
func WithHTTPClient(hc *http.Client) HTTPClientOption {
	return func(c *HTTPClient) { c.hc = hc }
}

// WithPollInterval sets how often Watch polls the registry. Default 2s.
func WithPollInterval(d time.Duration) HTTPClientOption {
	return func(c *HTTPClient) { c.pollInterval = d }
}

// HTTPClient is a Registry backed by a remote registry server (see NewHandler).
type HTTPClient struct {
	base         string
	hc           *http.Client
	pollInterval time.Duration
}

var _ Registry = (*HTTPClient)(nil)

// NewHTTPClient returns a client for the registry server at baseURL,
// e.g. "http://localhost:8500".
func NewHTTPClient(baseURL string, opts ...HTTPClientOption) *HTTPClient {
	c := &HTTPClient{
		base:         strings.TrimRight(baseURL, "/"),
		hc:           &http.Client{Timeout: 5 * time.Second},
		pollInterval: 2 * time.Second,
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

func (c *HTTPClient) Register(ctx context.Context, inst Instance) error {
	if err := inst.Validate(); err != nil {
		return err
	}
	return c.do(ctx, http.MethodPost, instancesPath(inst.Service), inst, nil)
}

func (c *HTTPClient) Deregister(ctx context.Context, service, id string) error {
	return c.do(ctx, http.MethodDelete, instancePath(service, id), nil, nil)
}

func (c *HTTPClient) Heartbeat(ctx context.Context, service, id string) error {
	return c.do(ctx, http.MethodPut, instancePath(service, id)+"/heartbeat", nil, nil)
}

func (c *HTTPClient) SetStatus(ctx context.Context, service, id string, status Status) error {
	body := map[string]Status{"status": status}
	return c.do(ctx, http.MethodPut, instancePath(service, id)+"/status", body, nil)
}

func (c *HTTPClient) Services(ctx context.Context) ([]string, error) {
	var names []string
	err := c.do(ctx, http.MethodGet, "/v1/services", nil, &names)
	return names, err
}

func (c *HTTPClient) Instances(ctx context.Context, service string) ([]Instance, error) {
	var instances []Instance
	err := c.do(ctx, http.MethodGet, instancesPath(service), nil, &instances)
	return instances, err
}

// Watch polls the registry and sends the instance list whenever it changes.
// Polling errors are skipped; the last known list stays current.
func (c *HTTPClient) Watch(ctx context.Context, service string) (<-chan []Instance, error) {
	first, err := c.Instances(ctx, service)
	if err != nil {
		return nil, err
	}
	ch := make(chan []Instance, 1)
	ch <- first

	go func() {
		defer close(ch)
		last := fingerprint(first)
		ticker := time.NewTicker(c.pollInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
			instances, err := c.Instances(ctx, service)
			if err != nil {
				continue
			}
			if fp := fingerprint(instances); fp != last {
				last = fp
				select {
				case ch <- instances:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return ch, nil
}

func (c *HTTPClient) do(ctx context.Context, method, path string, in, out any) error {
	var body io.Reader
	if in != nil {
		b, err := json.Marshal(in)
		if err != nil {
			return err
		}
		body = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, body)
	if err != nil {
		return err
	}
	if in != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return fmt.Errorf("registry: %s %s: %w", method, path, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		var e struct {
			Error string `json:"error"`
		}
		_ = json.NewDecoder(io.LimitReader(resp.Body, maxBodyBytes)).Decode(&e)
		switch resp.StatusCode {
		case http.StatusNotFound:
			return ErrNotFound
		case http.StatusBadRequest:
			return fmt.Errorf("%w: %s", ErrInvalid, e.Error)
		default:
			return fmt.Errorf("registry: %s %s: %s: %s", method, path, resp.Status, e.Error)
		}
	}
	if out != nil {
		return json.NewDecoder(resp.Body).Decode(out)
	}
	return nil
}

func instancesPath(service string) string {
	return "/v1/services/" + url.PathEscape(service) + "/instances"
}

func instancePath(service, id string) string {
	return instancesPath(service) + "/" + url.PathEscape(id)
}

// fingerprint identifies the routing-relevant state of an instance list,
// ignoring heartbeat timestamps.
func fingerprint(instances []Instance) string {
	var b strings.Builder
	for _, inst := range instances {
		fmt.Fprintf(&b, "%s|%s|%s|%d;", inst.ID, inst.Address, inst.Status, inst.Weight)
	}
	return b.String()
}

/***
  @COPYRIGHT (c) 2026. Thomas EC. Smith (https://www.TECSmith.info). Licensed under the MIT License.
****/
