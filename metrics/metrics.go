// Package metrics is RaftKV's M6 observability surface: a Prometheus
// text-exposition endpoint per node covering exactly what
// doc/raftkv.plan.md's M6 asks for — request latency (p50/p99 via a
// histogram), throughput and error rate (a counter by op/result), and the
// number of keys currently stored.
package metrics

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics is one node's self-contained set of collectors, registered on a
// private prometheus.Registry rather than the global DefaultRegisterer.
//
// Why private: transport/server_test.go builds N-node clusters in-process
// (multiple *transport.Server, and so multiple *Metrics, in one test
// binary). Registering on the global registry would panic on the second
// Server's registration (duplicate metric name) the first time a test —
// not just cmd/server — constructed more than one.
type Metrics struct {
	reg *prometheus.Registry

	requestDuration *prometheus.HistogramVec
	requestsTotal   *prometheus.CounterVec
}

// New creates a fresh, independently-registered Metrics instance. Call
// once per node (transport.NewServer does this).
func New() *Metrics {
	reg := prometheus.NewRegistry()
	m := &Metrics{
		reg: reg,
		requestDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "raftkv_request_duration_seconds",
			Help:    "Latency of client-facing KV RPCs (get/put/delete), by operation. Feeds p50/p99 via histogram_quantile.",
			Buckets: prometheus.DefBuckets,
		}, []string{"op"}),
		requestsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "raftkv_requests_total",
			Help: "Client-facing KV RPCs served, by operation and result (ok/not_leader/error). rate() gives throughput and error rate.",
		}, []string{"op", "result"}),
	}
	reg.MustRegister(m.requestDuration, m.requestsTotal)
	return m
}

// ObserveRequest records one completed Get/Put/Delete RPC. op is
// "get"/"put"/"delete"; result is "ok", "not_leader" (redirect, not a
// server error), or "error"; dur is the handler's total wall-clock time.
func (m *Metrics) ObserveRequest(op, result string, dur time.Duration) {
	m.requestDuration.WithLabelValues(op).Observe(dur.Seconds())
	m.requestsTotal.WithLabelValues(op, result).Inc()
}

// SetKeyCountFunc registers a gauge that calls fn on every scrape to
// report the number of live keys currently stored. Takes a plain
// func() float64 (rather than depending on storage.Store directly) so
// this package stays a leaf with no dependency on storage/transport.
func (m *Metrics) SetKeyCountFunc(fn func() float64) {
	m.reg.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "raftkv_keys",
		Help: "Number of live keys currently stored (memtable + SSTables, tombstones excluded). Recomputed on every scrape.",
	}, fn))
}

// Handler serves this node's metrics in Prometheus text exposition
// format. Mount it at "/metrics" on whatever address the caller chooses.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{})
}
