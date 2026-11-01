package orchestrator

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httputil"
	"strings"
)

// NewGateway returns a reverse proxy that routes /{service}/{path} to path on
// an instance of service, using rt (normally a *Transport) to pick it.
// It responds 503 when no instance can take the request and 502 when the
// chosen instance fails.
func NewGateway(rt http.RoundTripper, log *slog.Logger) http.Handler {
	if log == nil {
		log = slog.Default()
	}
	proxy := &httputil.ReverseProxy{
		Transport: rt,
		Rewrite: func(pr *httputil.ProxyRequest) {
			service, rest := splitService(pr.In.URL.Path)
			pr.Out.URL.Scheme = "http"
			pr.Out.URL.Host = service
			pr.Out.URL.Path = rest
			pr.Out.URL.RawPath = ""
			pr.SetXForwarded()
		},
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			status := http.StatusBadGateway
			if errors.Is(err, ErrNoInstances) || errors.Is(err, ErrAllCircuitsOpen) {
				status = http.StatusServiceUnavailable
			}
			log.Warn("gateway: request failed", "method", r.Method, "path", r.URL.Path, "status", status, "err", err)
			writeJSONError(w, status, err.Error())
		},
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if service, _ := splitService(r.URL.Path); service == "" {
			writeJSONError(w, http.StatusNotFound, "path must start with a service name: /{service}/...")
			return
		}
		proxy.ServeHTTP(w, r)
	})
}

// splitService splits "/users/api/v1" into "users" and "/api/v1".
func splitService(path string) (service, rest string) {
	path = strings.TrimPrefix(path, "/")
	service, rest, _ = strings.Cut(path, "/")
	return service, "/" + rest
}

func writeJSONError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

/***
  @COPYRIGHT (c) 2026. Thomas EC. Smith (https://www.TECSmith.info). Licensed under the MIT License.
****/
