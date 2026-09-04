package job

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// 指标（设计文档 16.1；consumer lag 由 kafka-exporter 提供）。
var (
	// pushTotal 投递结果计数。
	pushTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "linkim", Subsystem: "job", Name: "push_total",
		Help: "Push delivery results.",
	}, []string{"result"})
	// storeBatchDuration 落库批次耗时分布。
	storeBatchDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "linkim", Subsystem: "job", Name: "store_batch_duration_seconds",
		Help:    "Store batch flush duration.",
		Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5},
	})
	// storeRowsTotal 落库行数计数。
	storeRowsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "linkim", Subsystem: "job", Name: "store_rows_total",
		Help: "Message rows written to MySQL.",
	})
	// reconcileRemovedTotal 路由对账清理的失效条目数（设计文档 7.2/16.1）。
	reconcileRemovedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "linkim", Subsystem: "reconcile", Name: "removed_total",
		Help: "Stale route entries removed by the reconciler.",
	})
)
