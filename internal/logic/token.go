// Package logic 实现 Logic 服务的无状态业务逻辑：
// token 校验（带 Redis 缓存）、消息处理、序列号生成与 Kafka 生产。
// S4 仅实现 VerifyToken，其余 RPC 返回 Unimplemented。
package logic

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"

	"go.uber.org/zap"
)

// verify 结果缓存（设计文档 5.2：Redis 缓存校验结果，TTL 5 分钟）。
const (
	tokenCacheTTL   = 5 * time.Minute
	cacheValValid   = "1"
	cacheValInvalid = "0"
)

// 业务错误码（沿用全局分段：501xx 存储、502xx 中间件）。
const (
	CodeOK          = 0
	CodeInvalidTok  = 40101 // token 无效或已过期
	CodeAccountDown = 50102 // account 服务不可达
)

// Cache 抽象 verify 结果缓存（Redis 实现），便于单测 mock。
type Cache interface {
	Get(ctx context.Context, key string) (string, error) // 未命中返回 ""
	Set(ctx context.Context, key, val string, ttl time.Duration) error
}

// Verifier 抽象回源校验（account HTTP 实现），便于单测 mock。
// 返回 (valid, err)：err 非空表示 account 不可达（网络/5xx）。
type Verifier interface {
	Verify(ctx context.Context, uid int64, token string) (bool, error)
}

// TokenCacheKey 生成 verify 缓存 key：tokencache:{uid}:{sha256(token) 前 16 字节}。
func TokenCacheKey(uid int64, token string) string {
	sum := sha256.Sum256([]byte(token))
	return fmt.Sprintf("tokencache:%d:%s", uid, hex.EncodeToString(sum[:16]))
}

// verifyTokenFlow 执行校验流程：缓存 → miss 回源 account → 回填缓存。
// account 不可达时返回 50102 且不写缓存（避免负结果污染）。
func (s *Server) verifyTokenFlow(ctx context.Context, uid int64, token string) (bool, int32) {
	key := TokenCacheKey(uid, token)

	if cached, err := s.cache.Get(ctx, key); err != nil {
		// Redis 故障降级：不中断鉴权，直接回源。
		s.logger.Warn("token cache read failed, fallback to account", zap.Error(err))
	} else if cached != "" {
		valid := cached == cacheValValid
		code := int32(CodeOK)
		if !valid {
			code = CodeInvalidTok
		}
		s.logger.Info("verify token cache hit",
			zap.Int64("uid", uid), zap.Bool("valid", valid))
		return valid, code
	}

	valid, err := s.verifier.Verify(ctx, uid, token)
	if err != nil {
		s.logger.Warn("account verify unreachable", zap.Int64("uid", uid), zap.Error(err))
		return false, CodeAccountDown
	}

	code := int32(CodeOK)
	if !valid {
		code = CodeInvalidTok
	}
	// 有效/无效都缓存 5 分钟；不可达不缓存。
	val := cacheValInvalid
	if valid {
		val = cacheValValid
	}
	if err := s.cache.Set(ctx, key, val, tokenCacheTTL); err != nil {
		s.logger.Warn("token cache write failed", zap.Error(err))
	}
	s.logger.Info("verify token via account",
		zap.Int64("uid", uid), zap.Bool("valid", valid))
	return valid, code
}
