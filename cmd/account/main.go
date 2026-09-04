// Command account 启动 LinkIM 账号服务（HTTP + JWT，设计文档 5.2）。
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.uber.org/zap"

	"github.com/linkim/linkim/internal/account"
	"github.com/linkim/linkim/pkg/conf"
	"github.com/linkim/linkim/pkg/kafkax"
	"github.com/linkim/linkim/pkg/logx"
	"github.com/linkim/linkim/pkg/mysqlx"
	"github.com/linkim/linkim/pkg/redisx"
	"github.com/linkim/linkim/pkg/snowflake"
)

// confPath 为配置文件路径，可通过 -conf 参数覆盖。
var confPath = flag.String("conf", "configs/account.yaml", "配置文件路径")

func main() {
	flag.Parse()

	// 加载 YAML 配置（支持 LINKIM_ 前缀环境变量覆盖）。
	cfg, err := conf.Load(*confPath)
	if err != nil {
		log.Fatalf("account: 加载配置失败: %v", err)
	}
	// 初始化全局 zap 日志器，进程退出前刷新缓冲。
	logger, err := logx.New(cfg.Log)
	if err != nil {
		log.Fatalf("account: 初始化日志失败: %v", err)
	}
	defer func() { _ = logger.Sync() }()

	if cfg.JWT.Secret == "" {
		logger.Fatal("jwt.secret 不能为空")
	}

	// 连接 MySQL（含启动重试）并验证 Redis 可达。
	db, err := mysqlx.New(cfg.MySQL)
	if err != nil {
		logger.Fatal("连接 MySQL 失败", zap.Error(err))
	}
	defer func() { _ = db.Close() }()

	rdb := redisx.New(cfg.Redis)
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		logger.Fatal("Redis 不可达", zap.Error(err))
	}
	defer func() { _ = rdb.Close() }()

	// 雪花节点：uid 分配器，节点 ID 来自配置（集群内唯一）。
	ids, err := snowflake.NewNode(cfg.Server.NodeID)
	if err != nil {
		logger.Fatal("初始化雪花节点失败", zap.Error(err))
	}

	svc := account.NewService(
		account.NewMySQLStore(db),
		account.NewRedisTokenCache(rdb),
		account.NewTokenManager(cfg.JWT.Secret, cfg.JWT.AccessTTL, cfg.JWT.RefreshTTL),
		ids,
	)
	producer := kafkax.NewProducer(cfg.Kafka)
	groupSvc := account.NewGroupService(account.NewMySQLGroupStore(db), ids, producer)
	handler := account.NewHandler(svc, groupSvc, logger)

	srv := &http.Server{
		Addr:              fmt.Sprintf(":%d", cfg.Server.HTTPPort),
		Handler:           handler.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	// 监听 SIGINT/SIGTERM，收到退出信号后优雅退出。
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		logger.Info("service starting",
			zap.String("service", cfg.Server.Name),
			zap.Int("http_port", cfg.Server.HTTPPort),
		)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Fatal("HTTP 服务异常", zap.Error(err))
		}
	}()

	<-ctx.Done()
	logger.Info("shutdown signal received, draining")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("HTTP 优雅关闭失败", zap.Error(err))
	}
	logger.Info("bye")
}
