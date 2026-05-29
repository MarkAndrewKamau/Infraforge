// Package xdsclient is the worker's tiny HTTP client for telling the
// xDS control plane "this service endpoint is now live" or "now gone".
//
// The Noop implementation lets the worker keep working when the xDS
// control plane is not running: provisioning succeeds, the resources
// are real and reachable directly, only the live Envoy routing is
// absent. The choice between Noop and HTTP is made in cmd/worker
// based on the XDS_ADDR env var.
package xdsclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

type Client interface {
	Register(ctx context.Context, service, host string, port int) error
	Unregister(ctx context.Context, service, host string, port int) error
}

// Noop satisfies Client without doing anything.
type Noop struct{}

func (Noop) Register(context.Context, string, string, int) error   { return nil }
func (Noop) Unregister(context.Context, string, string, int) error { return nil }

// HTTP is the real client. It talks to the xDS control plane's admin
// API. Failures are returned to the caller — the worker logs them as
// warnings and proceeds, because the underlying resource is fine, only
// the routing is temporarily wrong.
type HTTP struct {
	base   string
	client *http.Client
}

func NewHTTP(addr string) *HTTP {
	return &HTTP{
		base:   "http://" + addr,
		client: &http.Client{Timeout: 3 * time.Second},
	}
}

type endpointBody struct {
	Service string `json:"service"`
	Host    string `json:"host"`
	Port    int    `json:"port"`
}

func (c *HTTP) Register(ctx context.Context, service, host string, port int) error {
	return c.post(ctx, "/v1/register", endpointBody{Service: service, Host: host, Port: port})
}

func (c *HTTP) Unregister(ctx context.Context, service, host string, port int) error {
	return c.post(ctx, "/v1/unregister", endpointBody{Service: service, Host: host, Port: port})
}

func (c *HTTP) post(ctx context.Context, path string, b endpointBody) error {
	buf, _ := json.Marshal(b)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.base+path, bytes.NewReader(buf))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("xds %s -> %s: %s", path, resp.Status, bytesToString(body))
	}
	return nil
}

func bytesToString(b []byte) string {
	if len(b) > 200 {
		b = b[:200]
	}
	return string(b)
}
