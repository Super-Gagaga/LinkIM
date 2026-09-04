// Command logic 启动 LinkIM 无状态逻辑层 gRPC 服务（设计文档 2.2、5.1）。
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
	"github.com/linkim/linkim/internal/service"
	"github.com/linkim/linkim/pkg/conf"
	"github.com/linkim/linkim/pkg/kafkax"
	"github.com/linkim/linkim/pkg/logx"
	"github.com/linkim/linkim/pkg/mysqlx"
	"github.com/linkim/linkim/pkg/pb"
	"github.com/linkim/linkim/pkg/redisx"
	"github.com/linkim/linkim/pkg/snowflake"
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

	db, err := mysqlx.New(cfg.MySQL)
	if err != nil {
		logger.Fatal("连接 MySQL 失败", zap.Error(err))
	}
	defer func() { _ = db.Close() }()

	ids, err := snowflake.NewNode(cfg.Server.NodeID)
	if err != nil {
		logger.Fatal("初始化雪花节点失败", zap.Error(err))
	}

	producer := kafkax.NewProducer(cfg.Kafka)

	srv := grpc.NewServer(grpc.ChainUnaryInterceptor(logic.UnaryInterceptor(logger)))
	pbLogic := logic.NewServer(logic.Deps{
		Cache:    logic.NewRedisCache(rdb),
		Verifier: logic.NewHTTPVerifier(cfg.Account.Addr, cfg.Account.VerifyTimeout),
		Friends:  logic.NewFriendCache(rdb, db),
		Seq:      logic.NewRedisSeqGen(rdb),
		Idem:     logic.NewRedisIdemStore(rdb),
		IDs:      ids,
		Producer: producer,
		Sync:     logic.NewMySQLSyncStore(db),
		Members:  service.NewGroupMembers(rdb, db),
		Redis:    rdb,
		Logger:   logger,
	})
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
			zap.Strings("kafka_brokers", cfg.Kafka.Brokers),
		)
		if err := srv.Serve(lis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			logger.Fatal("gRPC serve 异常", zap.Error(err))
		}
	}()

	<-ctx.Done()
	logger.Info("shutdown signal received, draining (10s)")

	// 优雅退出：先停拉取（Serve 返回），再刷写 Kafka 缓冲。
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
	if err := producer.Close(); err != nil {
		logger.Error("关闭 Kafka producer 失败", zap.Error(err))
	}
	logger.Info("bye")
}
