// Command job 启动 LinkIM Kafka 消费服务（设计文档 8 节）：
// job-push 消费 msg.push 投递给 comet；job-store 消费 msg.store 批量落库。
package main

import (
	"fmt"
	"net/http"

	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"

	"github.com/linkim/linkim/internal/job"
	"github.com/linkim/linkim/internal/service"
	"github.com/linkim/linkim/pkg/conf"
	"github.com/linkim/linkim/pkg/kafkax"
	"github.com/linkim/linkim/pkg/logx"
	"github.com/linkim/linkim/pkg/mysqlx"
	"github.com/linkim/linkim/pkg/redisx"
)

// confPath 为配置文件路径，可通过 -conf 参数覆盖。
var confPath = flag.String("conf", "configs/job.yaml", "配置文件路径")

func main() {
	flag.Parse()

	cfg, err := conf.Load(*confPath)
	if err != nil {
		log.Fatalf("job: 加载配置失败: %v", err)
	}
	logger, err := logx.New(cfg.Log)
	if err != nil {
		log.Fatalf("job: 初始化日志失败: %v", err)
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

	producer := kafkax.NewProducer(cfg.Kafka)

	if cfg.Server.MetricsPort > 0 {
		go func() {
			mux := http.NewServeMux()
			mux.Handle("/metrics", promhttp.Handler())
			if err := http.ListenAndServe(fmt.Sprintf(":%d", cfg.Server.MetricsPort), mux); err != nil {
				logger.Error("metrics serve failed", zap.Error(err))
			}
		}()
	}

	// 路由对账（设计文档 7.2：每 5min 清理失联 comet 的残留路由）。
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	go job.NewReconciler(rdb, logger).Run(ctx)
	defer stop()

	// job-push：在线投递。
	pushReader := job.NewReader(cfg.Kafka.Brokers, cfg.Consumer.PushGroup,
		"msg.push", cfg.Consumer.MinBytes, cfg.Consumer.MaxBytes,
		cfg.Consumer.ReadBackoffMax)
	pushWorker := job.NewPushWorker(rdb, job.NewCometPool(), logger)

	// job-store：批量落库（DLQ 复用 producer）。
	storeReader := job.NewReader(cfg.Kafka.Brokers, cfg.Consumer.StoreGroup,
		"msg.store", cfg.Consumer.MinBytes, cfg.Consumer.MaxBytes,
		cfg.Consumer.ReadBackoffMax)
	commit := func(cctx context.Context, msgs []kafka.Message) error {
		return storeReader.CommitMessages(cctx, msgs...)
	}
	storeWorker := job.NewStoreWorker(db, producer, commit, service.NewGroupMembers(rdb, db), logger)
	kmCh := make(chan kafka.Message, 256)

	errCh := make(chan error, 4)

	// group.event：群成员变更（失效缓存 + 补建会话行）。
	groupReader := job.NewReader(cfg.Kafka.Brokers, "job-group", job.TopicGroupEvent,
		cfg.Consumer.MinBytes, cfg.Consumer.MaxBytes, cfg.Consumer.ReadBackoffMax)
	groupWorker := job.NewGroupEventWorker(rdb, db, logger)
	go func() {
		logger.Info("group event consumer starting")
		errCh <- job.RunLoop(ctx, groupReader, groupWorker.Handle, logger)
	}()
	go func() {
		logger.Info("push consumer starting", zap.String("group", cfg.Consumer.PushGroup))
		errCh <- job.RunLoop(ctx, pushReader, pushWorker.Handle, logger)
	}()
	go func() {
		logger.Info("store consumer starting", zap.String("group", cfg.Consumer.StoreGroup))
		errCh <- job.StoreRunLoop(ctx, storeReader, kmCh, logger)
	}()
	go func() {
		storeWorker.BatchLoop(ctx, kmCh)
		errCh <- nil
	}()

	logger.Info("service starting", zap.String("service", cfg.Server.Name))

	// 任一 worker 出错（非 ctx 取消）则整体退出。
	select {
	case <-ctx.Done():
	case err := <-errCh:
		if err != nil {
			logger.Fatal("worker exited with error", zap.Error(err))
		}
	}

	logger.Info("shutdown signal received, draining (10s)")
	drainCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	<-drainCtx.Done() // 留时间给 BatchLoop 排空在途消息

	if err := pushReader.Close(); err != nil {
		logger.Error("关闭 push reader 失败", zap.Error(err))
	}
	if err := groupReader.Close(); err != nil {
		logger.Error("关闭 group reader 失败", zap.Error(err))
	}
	if err := storeReader.Close(); err != nil {
		logger.Error("关闭 store reader 失败", zap.Error(err))
	}
	if err := producer.Close(); err != nil {
		logger.Error("关闭 producer 失败", zap.Error(err))
	}
	logger.Info("bye")
}
