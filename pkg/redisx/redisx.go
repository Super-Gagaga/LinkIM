// Package redisx 封装 go-redis v9，集中管理设计文档 9.3 的 Redis 键布局，
// 其他包一律不得手拼键字符串。
package redisx

import (
	"github.com/redis/go-redis/v9"

	"github.com/linkim/linkim/pkg/conf"
)

// New 返回 cfg 对应的 Redis 客户端。客户端为惰性连接；
// 调用方需要快速失败时自行用 Ping 验证可用性。
func New(cfg conf.RedisConfig) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:     cfg.Addr,
		Password: cfg.Password,
		DB:       cfg.DB,
	})
}
