package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	StatusOK    = "ok"
	StatusError = "error"
)

const (
	namespace = "kvdb"
)

var (
	Queries = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name:      "queries",
			Namespace: namespace,
			Help:      "Total number of queries served",
		},
		[]string{"cmd", "status"},
	)

	QueryDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:      "query_duration_seconds",
		Namespace: namespace,
		Help:      "Query duration in seconds",
		Buckets:   prometheus.ExponentialBuckets(0.001, 2, 20),
	}, []string{"cmd", "status"})
)
