package job

import (
	"context"
	"encoding/json"

	"github.com/jmoiron/sqlx"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"

	"github.com/linkim/linkim/internal/service"
	"github.com/linkim/linkim/pkg/redisx"
)

// GroupEventWorker 消费 group.event（设计文档 11.3：成员变更顺序消费）：
// join → 失效成员缓存 + 为新成员补建 conversation 行；
// leave/quit → 仅失效缓存（会话行保留历史，unread 不再增长）。
type GroupEventWorker struct {
	rdb    *redis.Client
	db     *sqlx.DB
	logger *zap.Logger
}

// NewGroupEventWorker 构造群事件消费者。
func NewGroupEventWorker(rdb *redis.Client, db *sqlx.DB, logger *zap.Logger) *GroupEventWorker {
	return &GroupEventWorker{rdb: rdb, db: db, logger: logger}
}

// groupEvent 事件体。
type groupEvent struct {
	Event string `json:"event"` // join | leave | quit
	Gid   int64  `json:"gid"`
	UID   int64  `json:"uid"`
}

// Handle 处理一条群事件。
func (w *GroupEventWorker) Handle(ctx context.Context, km kafka.Message) error {
	var ev groupEvent
	if err := json.Unmarshal(km.Value, &ev); err != nil || ev.Gid <= 0 || ev.UID <= 0 {
		w.logger.Warn("group.event payload invalid, skip",
			zap.Int("partition", km.Partition), zap.Int64("offset", km.Offset), zap.Error(err))
		return nil
	}

	// 统一失效成员缓存（下次发送/落库时回填最新成员）。
	if err := w.rdb.Del(ctx, redisx.ConvMembersKey(ev.Gid)).Err(); err != nil {
		w.logger.Warn("invalidate member cache failed", zap.Int64("gid", ev.Gid), zap.Error(err))
	}

	switch ev.Event {
	case "join":
		// 为新成员补建会话行（保留历史游标：已存在则不动）。
		convID := service.ConvIDForGroup(ev.Gid)
		_, err := w.db.ExecContext(ctx, `INSERT IGNORE INTO conversation
			(uid, conv_id, conv_type, target_id, last_seq, read_seq, unread, updated_at)
			VALUES (?, ?, 2, ?, 0, 0, 0, NOW())`, ev.UID, convID, ev.Gid)
		if err != nil {
			w.logger.Warn("create member conversation row failed",
				zap.Int64("gid", ev.Gid), zap.Int64("uid", ev.UID), zap.Error(err))
		}
	case "leave", "quit":
		// 保留会话行与历史游标；缓存已失效，后续发送不再扇出给该成员。
	default:
		w.logger.Warn("unknown group event", zap.String("event", ev.Event))
	}
	w.logger.Info("group event handled",
		zap.String("event", ev.Event), zap.Int64("gid", ev.Gid), zap.Int64("uid", ev.UID))
	return nil
}

// TopicGroupEvent 群成员变更事件 topic。
const TopicGroupEvent = "group.event"
