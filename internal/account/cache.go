package account

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/linkim/linkim/pkg/redisx"
)

// TokenCache 抽象 token 摘要缓存（Redis token:{uid}，设计文档 9.3），
// 用于单点登出：登录写入当前 access token 摘要，verify 比对一致性。
type TokenCache interface {
	// SetDigest 写入 uid 当前有效 token 摘要，TTL 与 access token 一致。
	SetDigest(ctx context.Context, uid int64, digest string, ttl time.Duration) error
	// GetDigest 读取摘要；不存在返回 ""（不视为错误）。
	GetDigest(ctx context.Context, uid int64) (string, error)
	// DelDigest 删除摘要（登出）。
	DelDigest(ctx context.Context, uid int64) error
}

// redisTokenCache 是 TokenCache 的 go-redis 实现。
type redisTokenCache struct{ rdb *redis.Client }

// NewRedisTokenCache 返回基于 *redis.Client 的 TokenCache。
func NewRedisTokenCache(rdb *redis.Client) TokenCache { return &redisTokenCache{rdb: rdb} }

// SetDigest 实现 TokenCache。
func (c *redisTokenCache) SetDigest(ctx context.Context, uid int64, digest string, ttl time.Duration) error {
	if err := c.rdb.Set(ctx, redisx.TokenKey(uid), digest, ttl).Err(); err != nil {
		return fmt.Errorf("account: set token digest: %w", err)
	}
	return nil
}

// GetDigest 实现 TokenCache。
func (c *redisTokenCache) GetDigest(ctx context.Context, uid int64) (string, error) {
	val, err := c.rdb.Get(ctx, redisx.TokenKey(uid)).Result()
	if errors.Is(err, redis.Nil) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("account: get token digest: %w", err)
	}
	return val, nil
}

// DelDigest 实现 TokenCache。
func (c *redisTokenCache) DelDigest(ctx context.Context, uid int64) error {
	if err := c.rdb.Del(ctx, redisx.TokenKey(uid)).Err(); err != nil {
		return fmt.Errorf("account: del token digest: %w", err)
	}
	return nil
}
