// Command comet 启动 LinkIM WebSocket 接入层（设计文档 4.5、7 节）。
// 职责：维持长连接、协议编解码、心跳保活、下行推送；
// 不做业务逻辑（鉴权/消息处理下沉 logic）。
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"

	"github.com/linkim/linkim/internal/comet"
	"github.com/linkim/linkim/pkg/conf"
	"github.com/linkim/linkim/pkg/logx"
	"github.com/linkim/linkim/pkg/pb"
	"github.com/linkim/linkim/pkg/redisx"
)

// confPath 为配置文件路径，可通过 -conf 参数覆盖。
var confPath = flag.String("conf", "configs/comet.yaml", "配置文件路径")

func main() {
	flag.Parse()

	cfg, err := conf.Load(*confPath)
	if err != nil {
		log.Fatalf("comet: 加载配置失败: %v", err)
	}
	logger, err := logx.New(cfg.Log)
	if err != nil {
		log.Fatalf("comet: 初始化日志失败: %v", err)
	}
	defer func() { _ = logger.Sync() }()

	if cfg.Server.AdvertiseAddr == "" {
		logger.Fatal("server.advertise_addr 不能为空（写入路由表的本机 gRPC 地址）")
	}

	rdb := redisx.New(cfg.Redis)
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		logger.Fatal("Redis 不可达", zap.Error(err))
	}
	defer func() { _ = rdb.Close() }()

	// logic gRPC 客户端（AUTH 鉴权；S6 起 MSG_SEND 也走此连接）。
	logicConn, err := grpc.NewClient(cfg.Logic.Addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		logger.Fatal("连接 logic 失败", zap.Error(err))
	}
	defer func() { _ = logicConn.Close() }()

	srv := comet.NewServer(cfg.Server.AdvertiseAddr, rdb, pb.NewLogicClient(logicConn),
		cfg.Logic.CallTimeout, logger, nil)

	// 存活注册：comet:alive:{addr} EX 30，10s 续期。
	alive := comet.NewAlive(rdb, cfg.Server.AdvertiseAddr, logger)
	if err := alive.Start(); err != nil {
		logger.Fatal("注册存活 key 失败", zap.Error(err))
	}

	stopScan := srv.StartBackground()

	// WebSocket 服务 :8081/ws。
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", srv.ServeWS)
	wsSrv := &http.Server{
		Addr:              fmt.Sprintf(":%d", cfg.Server.WSPort),
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	// gRPC 服务 :9000（Job 下行 PushFrames / Kick）。
	grpcSrv := grpc.NewServer()
	pb.RegisterCometServer(grpcSrv, comet.NewGRPCService(srv))
	reflection.Register(grpcSrv)
	grpcLis, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.Server.GRPCPort))
	if err != nil {
		logger.Fatal("监听 gRPC 端口失败", zap.Error(err))
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		logger.Info("service starting",
			zap.String("service", cfg.Server.Name),
			zap.Int("ws_port", cfg.Server.WSPort),
			zap.Int("grpc_port", cfg.Server.GRPCPort),
			zap.String("advertise_addr", cfg.Server.AdvertiseAddr),
		)
		if err := wsSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Fatal("WS 服务异常", zap.Error(err))
		}
	}()
	go func() {
		if err := grpcSrv.Serve(grpcLis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			logger.Fatal("gRPC 服务异常", zap.Error(err))
		}
	}()

	<-ctx.Done()
	logger.Info("shutdown signal received, draining")

	// 摘除存活标记 → 停后台扫描 → 关闭监听（连接 drain 由 S10 实现）。
	alive.Stop()
	stopScan()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := wsSrv.Shutdown(shutdownCtx); err != nil {
		logger.Error("WS 优雅关闭失败", zap.Error(err))
	}
	grpcSrv.GracefulStop()
	logger.Info("bye")
}
