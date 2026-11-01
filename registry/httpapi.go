package registry

import (
	"encoding/json"
	"errors"
	"net/http"
)

// NewHandler exposes reg over a JSON REST API:
//
//	GET    /v1/services                                  list service names
//	GET    /v1/services/{service}/instances[?healthy=true] list instances
//	POST   /v1/services/{service}/instances              register (body: Instance)
//	DELETE /v1/services/{service}/instances/{id}         deregister
//	PUT    /v1/services/{service}/instances/{id}/heartbeat renew the lease
//	PUT    /v1/services/{service}/instances/{id}/status  set status (body: {"status": "..."})
//	GET    /healthz                                      liveness of the registry itself
func NewHandler(reg Registry) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})

	mux.HandleFunc("GET /v1/services", func(w http.ResponseWriter, r *http.Request) {
		names, err := reg.Services(r.Context())
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, names)
	})

	mux.HandleFunc("GET /v1/services/{service}/instances", func(w http.ResponseWriter, r *http.Request) {
		instances, err := reg.Instances(r.Context(), r.PathValue("service"))
		if err != nil {
			writeError(w, err)
			return
		}
		if r.URL.Query().Get("healthy") == "true" {
			instances = Healthy(instances)
		}
		writeJSON(w, http.StatusOK, instances)
	})

	mux.HandleFunc("POST /v1/services/{service}/instances", func(w http.ResponseWriter, r *http.Request) {
		var inst Instance
		if err := decode(r, &inst); err != nil {
			writeError(w, err)
			return
		}
		inst.Service = r.PathValue("service")
		if err := reg.Register(r.Context(), inst); err != nil {
			writeError(w, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})

	mux.HandleFunc("DELETE /v1/services/{service}/instances/{id}", func(w http.ResponseWriter, r *http.Request) {
		if err := reg.Deregister(r.Context(), r.PathValue("service"), r.PathValue("id")); err != nil {
			writeError(w, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})

	mux.HandleFunc("PUT /v1/services/{service}/instances/{id}/heartbeat", func(w http.ResponseWriter, r *http.Request) {
		if err := reg.Heartbeat(r.Context(), r.PathValue("service"), r.PathValue("id")); err != nil {
			writeError(w, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})

	mux.HandleFunc("PUT /v1/services/{service}/instances/{id}/status", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Status Status `json:"status"`
		}
		if err := decode(r, &body); err != nil {
			writeError(w, err)
			return
		}
		if err := reg.SetStatus(r.Context(), r.PathValue("service"), r.PathValue("id"), body.Status); err != nil {
			writeError(w, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})

	return mux
}

const maxBodyBytes = 1 << 20

func decode(r *http.Request, v any) error {
	dec := json.NewDecoder(http.MaxBytesReader(nil, r.Body, maxBodyBytes))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return errors.Join(ErrInvalid, err)
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, err error) {
	status := http.StatusInternalServerError
	switch {
	case errors.Is(err, ErrNotFound):
		status = http.StatusNotFound
	case errors.Is(err, ErrInvalid):
		status = http.StatusBadRequest
	}
	writeJSON(w, status, map[string]string{"error": err.Error()})
}

/***
  @COPYRIGHT (c) 2026. Thomas EC. Smith (https://www.TECSmith.info). Licensed under the MIT License.
****/
