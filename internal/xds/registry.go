// Package xds builds the Envoy configuration that routes traffic to
// per-service backends, and serves it to Envoy via a SnapshotCache.
//
// A registered service produces three xDS resources:
//
//   Cluster        one per service, STATIC, with one or more LbEndpoints
//   Route          prefix "/<service>/" -> cluster <service>, rewritten to "/"
//   Listener       a single shared listener on :10000 holding all routes
//
// The listener is constant; the cluster and route sets change as the
// worker registers and unregisters endpoints.
package xds

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	clusterv3 "github.com/envoyproxy/go-control-plane/envoy/config/cluster/v3"
	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	endpointv3 "github.com/envoyproxy/go-control-plane/envoy/config/endpoint/v3"
	listenerv3 "github.com/envoyproxy/go-control-plane/envoy/config/listener/v3"
	routev3 "github.com/envoyproxy/go-control-plane/envoy/config/route/v3"
	routerv3 "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/http/router/v3"
	hcmv3 "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/network/http_connection_manager/v3"
	cachetypes "github.com/envoyproxy/go-control-plane/pkg/cache/types"
	cachev3 "github.com/envoyproxy/go-control-plane/pkg/cache/v3"
	resourcev3 "github.com/envoyproxy/go-control-plane/pkg/resource/v3"

	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/durationpb"
)

const (
	// NodeID is the Envoy node identity the snapshot is keyed under. Must
	// match `node.id` in the Envoy bootstrap config.
	NodeID = "infraforge-edge"

	ListenerName = "infraforge_ingress"
	RouteCfgName = "infraforge_routes"
	ListenerPort = 10000
)

// Endpoint is one routable backend.
type Endpoint struct {
	Host string
	Port int
}

func endpointKey(e Endpoint) string { return fmt.Sprintf("%s:%d", e.Host, e.Port) }

// Registry tracks which services have which endpoints and rebuilds the
// Envoy snapshot whenever the set changes. All public methods are
// concurrency-safe.
type Registry struct {
	mu       sync.Mutex
	services map[string]map[string]Endpoint // service -> "host:port" -> endpoint
	cache    cachev3.SnapshotCache
	version  atomic.Int64
	log      *slog.Logger
}

func NewRegistry(cache cachev3.SnapshotCache, log *slog.Logger) *Registry {
	return &Registry{
		services: make(map[string]map[string]Endpoint),
		cache:    cache,
		log:      log,
	}
}

// Register adds an endpoint to a service and pushes a new snapshot.
// Idempotent: re-registering the same endpoint is a no-op (still pushes
// to be safe, but the snapshot is identical).
func (r *Registry) Register(ctx context.Context, service string, e Endpoint) error {
	r.mu.Lock()
	if r.services[service] == nil {
		r.services[service] = map[string]Endpoint{}
	}
	r.services[service][endpointKey(e)] = e
	snap := r.buildLocked()
	r.mu.Unlock()
	return r.pushSnapshot(ctx, snap)
}

// Unregister removes an endpoint. If the service has no remaining
// endpoints its cluster and route are removed from the snapshot
// entirely. Removing an unknown endpoint is a no-op.
func (r *Registry) Unregister(ctx context.Context, service string, e Endpoint) error {
	r.mu.Lock()
	if endpoints := r.services[service]; endpoints != nil {
		delete(endpoints, endpointKey(e))
		if len(endpoints) == 0 {
			delete(r.services, service)
		}
	}
	snap := r.buildLocked()
	r.mu.Unlock()
	return r.pushSnapshot(ctx, snap)
}

// PushEmpty pushes the current (possibly empty) snapshot. Useful at
// startup so Envoy connecting before any registration still receives a
// valid listener.
func (r *Registry) PushEmpty(ctx context.Context) error {
	r.mu.Lock()
	snap := r.buildLocked()
	r.mu.Unlock()
	return r.pushSnapshot(ctx, snap)
}

// List returns a stable, sorted snapshot of the registry — handy for the
// /v1/routes inspection endpoint.
func (r *Registry) List() map[string][]Endpoint {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make(map[string][]Endpoint, len(r.services))
	for svc, eps := range r.services {
		list := make([]Endpoint, 0, len(eps))
		for _, e := range eps {
			list = append(list, e)
		}
		sort.Slice(list, func(i, j int) bool { return endpointKey(list[i]) < endpointKey(list[j]) })
		out[svc] = list
	}
	return out
}

func (r *Registry) pushSnapshot(ctx context.Context, snap *cachev3.Snapshot) error {
	if err := snap.Consistent(); err != nil {
		return fmt.Errorf("snapshot inconsistent: %w", err)
	}
	if err := r.cache.SetSnapshot(ctx, NodeID, snap); err != nil {
		return fmt.Errorf("set snapshot: %w", err)
	}
	r.log.Info("xds snapshot pushed",
		"version", snap.GetVersion(resourcev3.ListenerType),
		"services", len(r.services))
	return nil
}

// buildLocked must be called with r.mu held.
func (r *Registry) buildLocked() *cachev3.Snapshot {
	services := make([]string, 0, len(r.services))
	for svc := range r.services {
		services = append(services, svc)
	}
	sort.Strings(services) // deterministic snapshot for the same input

	clusters := make([]cachetypes.Resource, 0, len(services))
	routes := make([]*routev3.Route, 0, len(services))

	for _, svc := range services {
		endpoints := r.services[svc]
		keys := make([]string, 0, len(endpoints))
		for k := range endpoints {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		lbEndpoints := make([]*endpointv3.LbEndpoint, 0, len(keys))
		for _, k := range keys {
			e := endpoints[k]
			lbEndpoints = append(lbEndpoints, makeLBEndpoint(e.Host, uint32(e.Port)))
		}
		clusters = append(clusters, makeCluster(svc, lbEndpoints))
		routes = append(routes, makePrefixRoute(svc))
	}

	routeCfg := &routev3.RouteConfiguration{
		Name: RouteCfgName,
		VirtualHosts: []*routev3.VirtualHost{{
			Name:    "all",
			Domains: []string{"*"},
			Routes:  routes,
		}},
	}

	version := fmt.Sprintf("v%d-%d", r.version.Add(1), time.Now().Unix())
	snap, err := cachev3.NewSnapshot(version,
		map[resourcev3.Type][]cachetypes.Resource{
			resourcev3.ListenerType: {makeListener()},
			resourcev3.RouteType:    {routeCfg},
			resourcev3.ClusterType:  clusters,
		})
	if err != nil {
		// Programming bug; the inputs above are always well-formed.
		panic(fmt.Sprintf("build snapshot: %v", err))
	}
	return snap
}

func makeCluster(name string, lbEndpoints []*endpointv3.LbEndpoint) *clusterv3.Cluster {
	return &clusterv3.Cluster{
		Name:                 name,
		ConnectTimeout:       durationpb.New(1 * time.Second),
		ClusterDiscoveryType: &clusterv3.Cluster_Type{Type: clusterv3.Cluster_STATIC},
		LbPolicy:             clusterv3.Cluster_ROUND_ROBIN,
		LoadAssignment: &endpointv3.ClusterLoadAssignment{
			ClusterName: name,
			Endpoints: []*endpointv3.LocalityLbEndpoints{{
				LbEndpoints: lbEndpoints,
			}},
		},
	}
}

func makeLBEndpoint(host string, port uint32) *endpointv3.LbEndpoint {
	return &endpointv3.LbEndpoint{
		HostIdentifier: &endpointv3.LbEndpoint_Endpoint{
			Endpoint: &endpointv3.Endpoint{
				Address: &corev3.Address{
					Address: &corev3.Address_SocketAddress{
						SocketAddress: &corev3.SocketAddress{
							Address:       host,
							PortSpecifier: &corev3.SocketAddress_PortValue{PortValue: port},
						},
					},
				},
			},
		},
	}
}

// makePrefixRoute makes "/<service>/..." match and forward to cluster
// <service>, with the prefix stripped so the backend sees "/...".
func makePrefixRoute(service string) *routev3.Route {
	return &routev3.Route{
		Match: &routev3.RouteMatch{
			PathSpecifier: &routev3.RouteMatch_Prefix{Prefix: "/" + service + "/"},
		},
		Action: &routev3.Route_Route{
			Route: &routev3.RouteAction{
				ClusterSpecifier: &routev3.RouteAction_Cluster{Cluster: service},
				PrefixRewrite:    "/",
			},
		},
	}
}

func makeListener() *listenerv3.Listener {
	routerCfg, err := anypb.New(&routerv3.Router{})
	if err != nil {
		panic(err)
	}

	hcm := &hcmv3.HttpConnectionManager{
		CodecType:  hcmv3.HttpConnectionManager_AUTO,
		StatPrefix: "ingress_http",
		// Route configuration is pulled via RDS over the same ADS stream
		// the listener itself came in on. That means a route change does
		// not require a new listener push.
		RouteSpecifier: &hcmv3.HttpConnectionManager_Rds{
			Rds: &hcmv3.Rds{
				ConfigSource:    makeADSConfigSource(),
				RouteConfigName: RouteCfgName,
			},
		},
		HttpFilters: []*hcmv3.HttpFilter{{
			Name:       "envoy.filters.http.router",
			ConfigType: &hcmv3.HttpFilter_TypedConfig{TypedConfig: routerCfg},
		}},
	}
	hcmAny, err := anypb.New(hcm)
	if err != nil {
		panic(err)
	}

	return &listenerv3.Listener{
		Name: ListenerName,
		Address: &corev3.Address{
			Address: &corev3.Address_SocketAddress{
				SocketAddress: &corev3.SocketAddress{
					Address:       "0.0.0.0",
					PortSpecifier: &corev3.SocketAddress_PortValue{PortValue: ListenerPort},
				},
			},
		},
		FilterChains: []*listenerv3.FilterChain{{
			Filters: []*listenerv3.Filter{{
				Name:       "envoy.filters.network.http_connection_manager",
				ConfigType: &listenerv3.Filter_TypedConfig{TypedConfig: hcmAny},
			}},
		}},
	}
}

func makeADSConfigSource() *corev3.ConfigSource {
	return &corev3.ConfigSource{
		ResourceApiVersion:    corev3.ApiVersion_V3,
		ConfigSourceSpecifier: &corev3.ConfigSource_Ads{Ads: &corev3.AggregatedConfigSource{}},
	}
}
