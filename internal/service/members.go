package service

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/redis/go-redis/v9"

	"github.com/linkim/linkim/pkg/redisx"
)

// GroupMembers 提供群成员列表（Redis Set 缓存 + MySQL 回填），
// logic 与 job 共用（设计文档 9.3 conv:members:{gid}，变更时 DEL）。
type GroupMembers struct {
	rdb *redis.Client
	db  *sqlx.DB
}

// NewGroupMembers 构造群成员源。
func NewGroupMembers(rdb *redis.Client, db *sqlx.DB) *GroupMembers {
	return &GroupMembers{rdb: rdb, db: db}
}

// Members 返回 gid 的全部成员 uid（无序）。
// 缓存 miss 时查 group_member 表并回填。
func (g *GroupMembers) Members(ctx context.Context, gid int64) ([]int64, error) {
	key := redisx.ConvMembersKey(gid)

	if vals, err := g.rdb.SMembers(ctx, key).Result(); err == nil && len(vals) > 0 {
		return parseUIDs(vals), nil
	} else if err != nil && err != redis.Nil {
		// Redis 故障降级直查库。
		return g.loadAndBackfill(ctx, gid, false)
	}

	return g.loadAndBackfill(ctx, gid, true)
}

// IsMember 判断 uid 是否为 gid 成员（缓存优先）。
func (g *GroupMembers) IsMember(ctx context.Context, gid, uid int64) (bool, error) {
	members, err := g.Members(ctx, gid)
	if err != nil {
		return false, err
	}
	for _, m := range members {
		if m == uid {
			return true, nil
		}
	}
	return false, nil
}

// loadAndBackfill 查库并在 backfill 时回填缓存。
func (g *GroupMembers) loadAndBackfill(ctx context.Context, gid int64, backfill bool) ([]int64, error) {
	var uids []int64
	err := g.db.SelectContext(ctx, &uids,
		`SELECT uid FROM group_member WHERE group_id = ?`, gid)
	if err != nil {
		return nil, fmt.Errorf("service: load group members: %w", err)
	}
	if backfill && len(uids) > 0 {
		members := make([]any, 0, len(uids))
		for _, uid := range uids {
			members = append(members, strconv.FormatInt(uid, 10))
		}
		if err := g.rdb.SAdd(ctx, redisx.ConvMembersKey(gid), members...).Err(); err != nil {
			// 回填失败降级：本次直接使用查询结果。
			return uids, nil
		}
		g.rdb.Expire(ctx, redisx.ConvMembersKey(gid), 24*time.Hour)
	}
	return uids, nil
}

// Invalidate 失效成员缓存（群成员变更时调用）。
func (g *GroupMembers) Invalidate(ctx context.Context, gid int64) error {
	return g.rdb.Del(ctx, redisx.ConvMembersKey(gid)).Err()
}

func parseUIDs(vals []string) []int64 {
	out := make([]int64, 0, len(vals))
	for _, v := range vals {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			out = append(out, n)
		}
	}
	return out
}
