package usecase

import (
	"github.com/prometheus/client_golang/prometheus"
)

// Metrics collects pipeline-level observability counters and histograms.
// Created once and shared across all use cases.
type Metrics struct {
	Ingested   *prometheus.CounterVec
	Processed  *prometheus.CounterVec
	DLQ        prometheus.Counter
	DLQLocked  prometheus.Counter
	StreamClaims prometheus.Counter
	Duration   *prometheus.HistogramVec
}

// NewMetrics registers pipeline metrics on the given registry and returns
// a Metrics instance. If reg is nil, the default Prometheus registry is used.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	if reg == nil {
		reg = prometheus.DefaultRegisterer
	}
	m := &Metrics{
		Ingested: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "pulseflow_events_ingested_total",
			Help: "Total number of events ingested via the HTTP API.",
		}, []string{"source", "type"}),
		Processed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "pulseflow_events_processed_total",
			Help: "Total number of events successfully processed from the stream.",
		}, []string{"source", "type"}),
		DLQ: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "pulseflow_events_dlq_total",
			Help: "Total number of events routed to the dead-letter queue.",
		}),
		DLQLocked: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "pulseflow_events_dlq_locked_total",
			Help: "Total number of DLQ events that exceeded the retry limit and are permanently locked.",
		}),
		StreamClaims: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "pulseflow_stream_claims_total",
			Help: "Total number of messages claimed via XCLAIM from stale consumers.",
		}),
		Duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "pulseflow_processing_duration_seconds",
			Help:    "Processing duration in seconds.",
			Buckets: prometheus.DefBuckets,
		}, []string{"phase"}),
	}
	reg.MustRegister(m.Ingested, m.Processed, m.DLQ, m.DLQLocked, m.StreamClaims, m.Duration)
	return m
}
