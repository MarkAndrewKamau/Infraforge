// Command echo is the companion HTTP microservice the worker provisions
// alongside each Postgres. It is deliberately tiny: one binary, two
// endpoints, no external dependencies. It exists so the upcoming Envoy
// xDS control plane has a real Layer 7 workload to dispatch to.
//
//	GET /health   -> {"status":"ok"}
//	GET /whoami   -> {"service": <SERVICE_NAME>, "job": <JOB_ID>, "host": <r.Host>}
package main

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
)

func main() {
	service := envOr("SERVICE_NAME", "unknown")
	jobID := envOr("JOB_ID", "unknown")
	addr := envOr("ECHO_ADDR", ":8080")
	log.Printf("echo starting service=%s job=%s addr=%s", service, jobID, addr)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /whoami", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{
			"service": service,
			"job":     jobID,
			"host":    r.Host,
		})
	})
	log.Fatal(http.ListenAndServe(addr, mux))
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
