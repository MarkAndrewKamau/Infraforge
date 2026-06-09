// Package brokerclient is a tiny HTTP client for talking to the broker's
// read endpoints. It exists so processes other than the broker itself
// (currently just cmd/xds for reconciliation) can use the broker as the
// authoritative inventory without re-implementing the wire contract.
package brokerclient

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/MarkAndrewKamau/infraforge/internal/model"
)

type Client struct {
	base       string
	httpClient *http.Client
}

func New(addr string) *Client {
	return &Client{
		base:       "http://" + addr,
		httpClient: &http.Client{Timeout: 5 * time.Second},
	}
}

// listResponse mirrors what GET /v1/provision returns.
type listResponse struct {
	Count int         `json:"count"`
	Jobs  []model.Job `json:"jobs"`
}

// List returns every job the broker has on record. The caller is
// responsible for filtering by Status etc.
func (c *Client) List(ctx context.Context) ([]model.Job, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base+"/v1/provision", nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("call broker: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("broker /v1/provision -> %s", resp.Status)
	}
	var body listResponse
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}
	return body.Jobs, nil
}
