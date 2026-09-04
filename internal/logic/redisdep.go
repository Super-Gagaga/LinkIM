package logic

import (
	"context"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/redis/go-redis/v9"

	"github.com/linkim/linkim/pkg/redisx"
)

// friendStatusActive 表示好友关系生效（friend.status：0 待确认 1 生效 2 拉黑）。
const friendStatusActive = 1

// --- IdemStore 的 Redis 实现 ---

type redisIdem struct{ rdb *redis.Client }

// NewRedisIdemStore 返回基于 *redis.Client 的 IdemStore。
func NewRedisIdemStore(rdb *redis.Client) IdemStore { return &redisIdem{rdb: rdb} }

// SetNX 实现 IdemStore。
func (s *redisIdem) SetNX(ctx context.Context, key, val string, ttl time.Duration) (bool, error) {
	return s.rdb.SetNX(ctx, key, val, ttl).Result()
}

// Get 实现 IdemStore；key 不存在返回空串。
func (s *redisIdem) Get(ctx context.Context, key string) (string, error) {
	val, err := s.rdb.Get(ctx, key).Result()
	if err == redis.Nil {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	return val, nil
}

// Set 实现 IdemStore。
func (s *redisIdem) Set(ctx context.Context, key, val string, ttl time.Duration) error {
	return s.rdb.Set(ctx, key, val, ttl).Err()
}

// Del 实现 IdemStore。
func (s *redisIdem) Del(ctx context.Context, key string) error {
	return s.rdb.Del(ctx, key).Err()
}

// --- SeqGen 的 Redis 实现 ---

type redisSeq struct{ rdb *redis.Client }

// NewRedisSeqGen 返回基于 *redis.Client 的 SeqGen（INCR seq:{conv_id}）。
func NewRedisSeqGen(rdb *redis.Client) SeqGen { return &redisSeq{rdb: rdb} }

// Next 实现 SeqGen。
func (s *redisSeq) Next(ctx context.Context, convID string) (int64, error) {
	n, err := s.rdb.Incr(ctx, redisx.SeqKey(convID)).Result()
	if err != nil {
		return 0, fmt.Errorf("logic: incr seq: %w", err)
	}
	return n, nil
}

// --- FriendChecker 的 Redis+MySQL 实现（设计文档 9.3 friend:{uid} ZSet 缓存） ---

// FriendCache 先查 Redis ZSet，miss 时全量回填后判定。
type FriendCache struct {
	rdb *redis.Client
	db  *sqlx.DB
}

// NewFriendCache 构造好友关系缓存校验器。
func NewFriendCache(rdb *redis.Client, db *sqlx.DB) *FriendCache {
	return &FriendCache{rdb: rdb, db: db}
}

// IsFriend 实现 FriendChecker。
func (f *FriendCache) IsFriend(ctx context.Context, uid, friendUID int64) (bool, error) {
	key := redisx.FriendKey(uid)
	member := fmt.Sprintf("%d", friendUID)

	// 缓存命中（ZRANK 非 Nil 即成员存在）。
	if _, err := f.rdb.ZRank(ctx, key, member).Result(); err == nil {
		return true, nil
	} else if !isRedisNil(err) {
		return false, fmt.Errorf("logic: zrank friend: %w", err)
	}

	// miss：查库全量回填 ZSet。
	// 注：friend 表无时间戳列，ZSet score 统一置 0（score 仅作排序淘汰预留，
	// 成员资格判定不依赖 score）。
	rows, err := f.db.QueryxContext(ctx,
		`SELECT friend_uid FROM friend WHERE uid = ? AND status = ?`, uid, friendStatusActive)
	if err != nil {
		return false, fmt.Errorf("logic: load friends: %w", err)
	}
	defer func() { _ = rows.Close() }()

	members := []redis.Z{}
	found := false
	for rows.Next() {
		var fid int64
		if err := rows.Scan(&fid); err != nil {
			return false, fmt.Errorf("logic: scan friend: %w", err)
		}
		if fid == friendUID {
			found = true
		}
		members = append(members, redis.Z{Score: 0, Member: fmt.Sprintf("%d", fid)})
	}
	if err := rows.Err(); err != nil {
		return false, fmt.Errorf("logic: iterate friends: %w", err)
	}
	if len(members) > 0 {
		if err := f.rdb.ZAdd(ctx, key, members...).Err(); err != nil {
			// 回填失败降级：本次直接用查询结果。
			return found, nil
		}
	}
	return found, nil
}

// isRedisNil 判断是否为 redis.Nil（key/成员不存在）。
func isRedisNil(err error) bool {
	return err == redis.Nil
}
