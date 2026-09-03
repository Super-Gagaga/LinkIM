// Command comet 启动 LinkIM WebSocket 接入网关（见设计文档 S5）。
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"go.uber.org/zap"

	"github.com/linkim/linkim/pkg/conf"
	"github.com/linkim/linkim/pkg/logx"
)

// confPath 为配置文件路径，可通过 -conf 参数覆盖。
var confPath = flag.String("conf", "configs/comet.yaml", "配置文件路径")

func main() {
	flag.Parse()

	// 加载 YAML 配置（支持 LINKIM_ 前缀环境变量覆盖）。
	cfg, err := conf.Load(*confPath)
	if err != nil {
		log.Fatalf("comet: 加载配置失败: %v", err)
	}
	// 初始化全局 zap 日志器，进程退出前刷新缓冲。
	logger, err := logx.New(cfg.Log)
	if err != nil {
		log.Fatalf("comet: 初始化日志失败: %v", err)
	}
	defer func() { _ = logger.Sync() }()

	logger.Info("service starting",
		zap.String("service", cfg.Server.Name),
		zap.Int("ws_port", cfg.Server.WSPort),
		zap.Int("grpc_port", cfg.Server.GRPCPort),
	)

	// 监听 SIGINT/SIGTERM，收到退出信号后优雅退出。
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()

	logger.Info("shutdown signal received, exiting")
}
