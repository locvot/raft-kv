package transport

import (
	"io"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// scrapeMetrics runs srv's MetricsHandler through an httptest.ResponseRecorder
// and returns the scraped Prometheus text body.
func scrapeMetrics(t *testing.T, srv *Server) string {
	t.Helper()
	req := httptest.NewRequest("GET", "/metrics", nil)
	rec := httptest.NewRecorder()
	srv.MetricsHandler().ServeHTTP(rec, req)
	if rec.Code != 200 {
		t.Fatalf("MetricsHandler status = %d, want 200", rec.Code)
	}
	body, err := io.ReadAll(rec.Result().Body)
	if err != nil {
		t.Fatalf("read metrics body: %v", err)
	}
	return string(body)
}

// TestServerMetricsEndpoint exercises M6's observability surface end to
// end: after real Get/Put traffic through a real cluster, every node's
// MetricsHandler must report the request counter/histogram and the live
// key-count gauge doc/raftkv.plan.md's M6 asks for ("latency p50/p99,
// throughput, error rate, số key đang lưu").
func TestServerMetricsEndpoint(t *testing.T) {
	c := newTestCluster(t, 3)
	cli := c.client()
	defer cli.Close()

	if err := cli.Put(ctxTimeout(t, 5*time.Second), "k1", []byte("v1")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if _, _, err := cli.Get(ctxTimeout(t, 5*time.Second), "k1"); err != nil {
		t.Fatalf("Get: %v", err)
	}
	// A Get for a missing key still counts as a served ("ok") request, not
	// an error — Store.Get's ok=false is a normal miss, not a failure.
	if _, _, err := cli.Get(ctxTimeout(t, 5*time.Second), "missing"); err != nil {
		t.Fatalf("Get(missing): %v", err)
	}

	var sawRequestsTotal, sawDuration, sawKeys bool
	for _, srv := range c.servers {
		body := scrapeMetrics(t, srv)
		if strings.Contains(body, "raftkv_requests_total") {
			sawRequestsTotal = true
		}
		if strings.Contains(body, "raftkv_request_duration_seconds") {
			sawDuration = true
		}
		if strings.Contains(body, "raftkv_keys ") {
			sawKeys = true
		}
	}
	if !sawRequestsTotal {
		t.Error("no node's /metrics reported raftkv_requests_total")
	}
	if !sawDuration {
		t.Error("no node's /metrics reported raftkv_request_duration_seconds")
	}
	if !sawKeys {
		t.Error("no node's /metrics reported raftkv_keys")
	}
}
