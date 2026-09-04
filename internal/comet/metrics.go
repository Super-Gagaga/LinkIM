package comet

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// 指标（设计文档 16.1）。
var (
	// onlineGauge 当前在线连接数（各 bucket 聚合）。
	onlineGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "linkim", Subsystem: "comet", Name: "online",
		Help: "Current online connections on this comet instance.",
	})
	// framesTotal 收发帧计数。
	framesTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "linkim", Subsystem: "comet", Name: "frames_total",
		Help: "Frames sent/received by cmd.",
	}, []string{"cmd", "direction"})
	// authTotal 鉴权结果计数。
	authTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "linkim", Subsystem: "comet", Name: "auth_total",
		Help: "AUTH attempts by result.",
	}, []string{"result"})
	// slowKickTotal 慢连接踢除计数。
	slowKickTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "linkim", Subsystem: "comet", Name: "slow_kick_total",
		Help: "Connections kicked for slow consuming.",
	})
	// reconnectSentTotal drain 广播的 RECONNECT_NOW 帧数。
	reconnectSentTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "linkim", Subsystem: "comet", Name: "reconnect_sent_total",
		Help: "RECONNECT_NOW frames broadcast during drain.",
	})
)

// frameReceived 记一帧上行。
func frameReceived(cmdName string) { framesTotal.WithLabelValues(cmdName, "recv").Inc() }

// frameSent 记一帧下行。
func frameSent(cmdName string) { framesTotal.WithLabelValues(cmdName, "send").Inc() }
