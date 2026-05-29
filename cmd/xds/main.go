// Command xds is the Envoy xDS control plane. It speaks two protocols:
//
//   gRPC (default :18000) — Aggregated Discovery Service (ADS) for Envoy
//   HTTP (default :19000) — small admin API the worker uses to register
//                           and unregister service endpoints
//
// Worker → HTTP API → Registry → gRPC ADS → Envoy. The result is that
// adding a new service is a config push, not an Envoy restart.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	discoverygrpcv3 "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	cachev3 "github.com/envoyproxy/go-control-plane/pkg/cache/v3"
	serverv3 "github.com/envoyproxy/go-control-plane/pkg/server/v3"

	xdspkg "github.com/MarkAndrewKamau/infraforge/internal/xds"

	"google.golang.org/grpc"
)

func main() {
	log := slog.New(slog.NewTextHandler(os.Stdout, nil))

	cache := cachev3.NewSnapshotCache(false, cachev3.IDHash{}, cacheLog{l: log})
	reg := xdspkg.NewRegistry(cache, log)

	// Seed an empty snapshot so an Envoy that connects before any
	// service is registered still gets a valid (just empty) listener,
	// rather than the LDS resource being missing.
	if err := reg.PushEmpty(context.Background()); err != nil {
		log.Error("initial snapshot push failed", "err", err)
		os.Exit(1)
	}

	grpcAddr := envOr("XDS_GRPC_ADDR", ":18000")
	httpAddr := envOr("XDS_HTTP_ADDR", ":19000")

	// gRPC ADS server for Envoy.
	lis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		log.Error("grpc listen failed", "addr", grpcAddr, "err", err)
		os.Exit(1)
	}
	grpcSrv := grpc.NewServer()
	xdsSrv := serverv3.NewServer(context.Background(), cache, nil)
	discoverygrpcv3.RegisterAggregatedDiscoveryServiceServer(grpcSrv, xdsSrv)

	// HTTP admin API for the worker.
	httpSrv := &http.Server{Addr: httpAddr, Handler: routes(reg, log)}

	go func() {
		log.Info("xds grpc listening", "addr", grpcAddr)
		if err := grpcSrv.Serve(lis); err != nil {
			log.Error("grpc serve failed", "err", err)
			os.Exit(1)
		}
	}()
	go func() {
		log.Info("xds http listening", "addr", httpAddr)
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("http serve failed", "err", err)
			os.Exit(1)
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()

	log.Info("shutting down")
	grpcSrv.GracefulStop()
	shCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = httpSrv.Shutdown(shCtx)
}

type endpointReq struct {
	Service string `json:"service"`
	Host    string `json:"host"`
	Port    int    `json:"port"`
}

func routes(reg *xdspkg.Registry, log *slog.Logger) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /v1/routes", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, reg.List())
	})
	mux.HandleFunc("POST /v1/register", func(w http.ResponseWriter, r *http.Request) {
		req, err := decode(r)
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		if err := reg.Register(r.Context(), req.Service, xdspkg.Endpoint{Host: req.Host, Port: req.Port}); err != nil {
			log.Error("register failed", "err", err)
			writeError(w, http.StatusInternalServerError, "register failed")
			return
		}
		log.Info("endpoint registered", "service", req.Service, "host", req.Host, "port", req.Port)
		writeJSON(w, http.StatusOK, map[string]string{"status": "registered"})
	})
	mux.HandleFunc("POST /v1/unregister", func(w http.ResponseWriter, r *http.Request) {
		req, err := decode(r)
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		if err := reg.Unregister(r.Context(), req.Service, xdspkg.Endpoint{Host: req.Host, Port: req.Port}); err != nil {
			log.Error("unregister failed", "err", err)
			writeError(w, http.StatusInternalServerError, "unregister failed")
			return
		}
		log.Info("endpoint unregistered", "service", req.Service, "host", req.Host, "port", req.Port)
		writeJSON(w, http.StatusOK, map[string]string{"status": "unregistered"})
	})
	return mux
}

func decode(r *http.Request) (*endpointReq, error) {
	var req endpointReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		return nil, fmt.Errorf("invalid JSON")
	}
	if req.Service == "" {
		return nil, fmt.Errorf("service is required")
	}
	if req.Host == "" {
		return nil, fmt.Errorf("host is required")
	}
	if req.Port <= 0 || req.Port > 65535 {
		return nil, fmt.Errorf("port must be 1..65535")
	}
	return &req, nil
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
func writeError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

// cacheLog adapts slog to go-control-plane's tiny logger interface
// (Debugf/Infof/Warnf/Errorf). The library uses it for snapshot churn
// diagnostics.
type cacheLog struct{ l *slog.Logger }

func (c cacheLog) Debugf(format string, args ...any) { c.l.Debug(fmt.Sprintf(format, args...)) }
func (c cacheLog) Infof(format string, args ...any)  { c.l.Info(fmt.Sprintf(format, args...)) }
func (c cacheLog) Warnf(format string, args ...any)  { c.l.Warn(fmt.Sprintf(format, args...)) }
func (c cacheLog) Errorf(format string, args ...any) { c.l.Error(fmt.Sprintf(format, args...)) }
