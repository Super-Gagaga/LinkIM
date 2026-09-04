package job

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"

	"github.com/linkim/linkim/pkg/redisx"
)

// 对账参数（设计文档 7.2：定时扫描路由表中指向失联 comet 的 entry 并清除）。
const (
	reconcileEvery  = 5 * time.Minute
	reconcileScanCT = 1000 // SCAN COUNT 提示值
)

// Reconciler 定时对账路由表：SCAN route:*，对每个 hash entry 检查目标
// comet 的存活 key，不存在则 HDEL（comet 宕机来不及清理的残留条目）。
type Reconciler struct {
	rdb    *redis.Client
	logger *zap.Logger
}

// NewReconciler 构造对账器。
func NewReconciler(rdb *redis.Client, logger *zap.Logger) *Reconciler {
	return &Reconciler{rdb: rdb, logger: logger}
}

// Run 周期执行对账，直到 ctx 取消。
func (r *Reconciler) Run(ctx context.Context) {
	ticker := time.NewTicker(reconcileEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			removed, err := r.ReconcileOnce(ctx)
			if err != nil {
				r.logger.Warn("reconcile pass failed", zap.Error(err))
				continue
			}
			if removed > 0 {
				r.logger.Info("reconcile removed stale route entries", zap.Int64("removed", removed))
			}
		}
	}
}

// ReconcileOnce 执行一轮对账，返回清理的 entry 数。
func (r *Reconciler) ReconcileOnce(ctx context.Context) (int64, error) {
	var removed int64
	var cursor uint64
	for {
		keys, next, err := r.rdb.Scan(ctx, cursor, "route:*", reconcileScanCT).Result()
		if err != nil {
			return removed, err
		}
		for _, key := range keys {
			n, err := r.reconcileHash(ctx, key)
			if err != nil {
				r.logger.Warn("reconcile hash failed", zap.String("key", key), zap.Error(err))
				continue
			}
			removed += n
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}
	reconcileRemovedTotal.Add(float64(removed))
	return removed, nil
}

// reconcileHash 清理单个 route:{uid} hash 中指向失联 comet 的 entry。
func (r *Reconciler) reconcileHash(ctx context.Context, key string) (int64, error) {
	route, err := r.rdb.HGetAll(ctx, key).Result()
	if err != nil {
		return 0, err
	}
	// 按地址批量检查存活，避免逐 entry EXISTS。
	alive := map[string]bool{}
	for _, addr := range route {
		if _, ok := alive[addr]; ok {
			continue
		}
		n, err := r.rdb.Exists(ctx, redisx.CometAliveKey(addr)).Result()
		if err != nil {
			return 0, err
		}
		alive[addr] = n > 0
	}
	var stale []string
	for field, addr := range route {
		if !alive[addr] {
			stale = append(stale, field)
		}
	}
	if len(stale) == 0 {
		return 0, nil
	}
	if err := r.rdb.HDel(ctx, key, stale...).Err(); err != nil {
		return 0, err
	}
	return int64(len(stale)), nil
}
