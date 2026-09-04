// Command logic 启动 LinkIM 无状态逻辑层 gRPC 服务（设计文档 2.2、5.2）。
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"github.com/linkim/linkim/internal/logic"
	"github.com/linkim/linkim/pkg/conf"
	"github.com/linkim/linkim/pkg/logx"
	"github.com/linkim/linkim/pkg/pb"
	"github.com/linkim/linkim/pkg/redisx"
)

// confPath 为配置文件路径，可通过 -conf 参数覆盖。
var confPath = flag.String("conf", "configs/logic.yaml", "配置文件路径")

func main() {
	flag.Parse()

	cfg, err := conf.Load(*confPath)
	if err != nil {
		log.Fatalf("logic: 加载配置失败: %v", err)
	}
	logger, err := logx.New(cfg.Log)
	if err != nil {
		log.Fatalf("logic: 初始化日志失败: %v", err)
	}
	defer func() { _ = logger.Sync() }()

	rdb := redisx.New(cfg.Redis)
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		logger.Fatal("Redis 不可达", zap.Error(err))
	}
	defer func() { _ = rdb.Close() }()

	srv := grpc.NewServer(grpc.ChainUnaryInterceptor(logic.UnaryInterceptor(logger)))
	pbLogic := logic.NewServer(
		logic.NewRedisCache(rdb),
		logic.NewHTTPVerifier(cfg.Account.Addr, cfg.Account.VerifyTimeout),
		logger,
	)
	pb.RegisterLogicServer(srv, pbLogic)
	// 注册 reflection，供 grpcurl 等工具联调。
	reflection.Register(srv)

	addr := fmt.Sprintf(":%d", cfg.Server.GRPCPort)
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		logger.Fatal("监听 gRPC 端口失败", zap.String("addr", addr), zap.Error(err))
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		logger.Info("service starting",
			zap.String("service", cfg.Server.Name),
			zap.Int("grpc_port", cfg.Server.GRPCPort),
			zap.String("account_addr", cfg.Account.Addr),
		)
		if err := srv.Serve(lis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			logger.Fatal("gRPC serve 异常", zap.Error(err))
		}
	}()

	<-ctx.Done()
	logger.Info("shutdown signal received, draining (10s)")

	// 优雅退出：GracefulStop 等待在途请求；超时 10s 后强制 Stop。
	done := make(chan struct{})
	go func() {
		srv.GracefulStop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		srv.Stop()
	}
	logger.Info("bye")
}
