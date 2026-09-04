package logic

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// 指标（设计文档 16.1）。
var (
	// sendMsgDuration SendMsg 处理耗时分布。
	sendMsgDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "linkim", Subsystem: "logic", Name: "sendmsg_duration_seconds",
		Help:    "SendMsg handling duration.",
		Buckets: []float64{.001, .0025, .005, .01, .025, .05, .1, .25, .5, 1},
	})
	// sendMsgTotal SendMsg 结果计数（按业务码）。
	sendMsgTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "linkim", Subsystem: "logic", Name: "sendmsg_total",
		Help: "SendMsg results by business code.",
	}, []string{"code"})
	// idemHitTotal 幂等命中（回放）计数。
	idemHitTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "linkim", Subsystem: "logic", Name: "idem_hit_total",
		Help: "Idempotent replays served from the idem cache.",
	})
)
