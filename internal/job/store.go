package job

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/linkim/linkim/internal/service"
	"github.com/linkim/linkim/pkg/mysqlx"
	"github.com/linkim/linkim/pkg/pb"
)

// 存储参数（设计文档 15.3：攒批 ≤50ms 或 100 条）。
const (
	storeBatchSize  = 100
	storeFlushEvery = 50 * time.Millisecond
	storeMaxRetry   = 3
	storeRetryBase  = 50 * time.Millisecond // 指数退避基数
	// TopicDLQStore 是落库失败消息的死信 topic。
	TopicDLQStore = "dlq.msg.store"
)

// SQLExecer 抽象批量写执行（*sqlx.DB 实现，测试可注入）。
type SQLExecer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// Producer 抽象 Kafka 生产者（pkg/kafkax 实现；DLQ 用）。
type Producer interface {
	Send(ctx context.Context, topic string, key, value []byte, headers ...map[string]string) error
}

// CommitFunc 提交一批 Kafka offset（由 Run 循环注入 reader.CommitMessages）。
type CommitFunc func(ctx context.Context, msgs []kafka.Message) error

// StoreWorker 消费 msg.store：解析 PbMsg → 攒批 → INSERT IGNORE 落库 +
// conversation UPSERT（单聊双方行 / 群聊全体成员行分批）→ 手动提交 offset。
type StoreWorker struct {
	exec      SQLExecer
	producer  Producer
	commit    CommitFunc
	memberSrc GroupMemberSource // 群会话 UPSERT 用成员列表
	logger    *zap.Logger
}

// GroupMemberSource 群成员源（internal/service.GroupMembers 实现）。
type GroupMemberSource interface {
	Members(ctx context.Context, gid int64) ([]int64, error)
	IsMember(ctx context.Context, gid, uid int64) (bool, error)
}

// NewStoreWorker 构造 store 消费者。
func NewStoreWorker(exec SQLExecer, producer Producer, commit CommitFunc, memberSrc GroupMemberSource, logger *zap.Logger) *StoreWorker {
	return &StoreWorker{exec: exec, producer: producer, commit: commit, memberSrc: memberSrc, logger: logger}
}

// storeItem 是一条待落库消息（含原始 Kafka 消息引用用于提交 offset）。
type storeItem struct {
	msg *pb.PbMsg // 解析失败为 nil（flush 时跳过但仍提交）
	km  kafka.Message
}

// BatchLoop 批处理主循环：kmCh 收集，50ms ticker 或满 100 条触发 flush。
// ctx 取消时处理完在途消息再返回（优雅退出）。
func (w *StoreWorker) BatchLoop(ctx context.Context, kmCh <-chan kafka.Message) {
	pending := make([]storeItem, 0, storeBatchSize)
	ticker := time.NewTicker(storeFlushEvery)
	defer ticker.Stop()

	for {
		select {
		case km, ok := <-kmCh:
			if !ok {
				w.flush(ctx, pending)
				return
			}
			pending = append(pending, parseStoreItem(km))
			if len(pending) >= storeBatchSize {
				w.flush(ctx, pending)
				pending = pending[:0]
			}
		case <-ticker.C:
			if len(pending) > 0 {
				w.flush(ctx, pending)
				pending = pending[:0]
			}
		case <-ctx.Done():
			// 停止拉取后清空在途。
			for {
				select {
				case km, ok := <-kmCh:
					if !ok {
						w.flush(ctx, pending)
						return
					}
					pending = append(pending, parseStoreItem(km))
				default:
					w.flush(ctx, pending)
					return
				}
			}
		}
	}
}

// parseStoreItem 解析一条 Kafka 消息为 PbMsg（失败则 msg 置 nil）。
func parseStoreItem(km kafka.Message) storeItem {
	var msg pb.PbMsg
	if err := proto.Unmarshal(km.Value, &msg); err != nil {
		return storeItem{km: km}
	}
	return storeItem{msg: &msg, km: km}
}

// flush 把一批消息写入 MySQL（指数退避重试 3 次），失败整批进 DLQ；随后提交 offset。
func (w *StoreWorker) flush(ctx context.Context, items []storeItem) {
	if len(items) == 0 {
		return
	}
	start := time.Now()
	if err := retryExec(ctx, storeMaxRetry, storeRetryBase, func() error {
		return w.writeBatch(ctx, items)
	}); err != nil {
		w.logger.Error("store batch failed after retries, routing leftovers to DLQ",
			zap.Int("count", len(items)), zap.Error(err))
		w.dlq(ctx, items)
	}
	storeBatchDuration.Observe(time.Since(start).Seconds())
	storeRowsTotal.Add(float64(len(items)))
	// 成功或已入 DLQ 均提交 offset，避免毒消息死循环（设计文档 6.1 ④）。
	w.commitBatch(ctx, items)
	w.logger.Debug("store batch flushed", zap.Int("count", len(items)))
}

// commitBatch 提交批次内全部 offset。
func (w *StoreWorker) commitBatch(ctx context.Context, items []storeItem) {
	msgs := make([]kafka.Message, 0, len(items))
	for _, it := range items {
		msgs = append(msgs, it.km)
	}
	if err := w.commit(ctx, msgs); err != nil {
		w.logger.Error("commit offsets failed", zap.Int("count", len(msgs)), zap.Error(err))
	}
}

// retryExec 以指数退避（base*2^attempt）重试 fn，共 max 次。
func retryExec(ctx context.Context, max int, base time.Duration, fn func() error) error {
	var err error
	for attempt := 1; attempt <= max; attempt++ {
		if err = fn(); err == nil {
			return nil
		}
		if attempt < max {
			select {
			case <-time.After(base << uint(attempt-1)):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	return err
}

// writeBatch 单次尝试写库：消息表（分表分组 INSERT IGNORE）+ 会话 UPSERT。
func (w *StoreWorker) writeBatch(ctx context.Context, items []storeItem) error {
	if err := w.insertMessages(ctx, items); err != nil {
		return err
	}
	return w.upsertConversations(ctx, items)
}

// insertMessages 按分表分组批量 INSERT IGNORE INTO message_xx（uk 幂等）。
func (w *StoreWorker) insertMessages(ctx context.Context, items []storeItem) error {
	groups := GroupByShard(items)
	for table, group := range groups {
		query, args, err := BuildMessageInsert(table, group)
		if err != nil {
			// 数据自身非法（如 msgId 非数字）不应重试整批：跳过该组。
			w.logger.Warn("build message insert failed, skip group",
				zap.String("table", table), zap.Error(err))
			continue
		}
		if _, err := w.exec.ExecContext(ctx, query, args...); err != nil {
			return fmt.Errorf("job: insert %s: %w", table, err)
		}
	}
	return nil
}

// GroupByShard 把批次按消息分表分组（表名 = ShardTable(conv_id)）。
func GroupByShard(items []storeItem) map[string][]*pb.PbMsg {
	groups := map[string][]*pb.PbMsg{}
	for _, it := range items {
		if it.msg == nil {
			continue
		}
		table := mysqlx.ShardTable(it.msg.GetConvId())
		groups[table] = append(groups[table], it.msg)
	}
	return groups
}

// BuildMessageInsert 构造多值 INSERT IGNORE 语句。
func BuildMessageInsert(table string, msgs []*pb.PbMsg) (string, []any, error) {
	var sb strings.Builder
	sb.WriteString("INSERT IGNORE INTO ")
	sb.WriteString(table)
	sb.WriteString(" (id, conv_id, seq, sender_id, msg_type, payload, status, created_at) VALUES ")
	args := make([]any, 0, len(msgs)*8)
	for i, m := range msgs {
		id, err := strconv.ParseUint(m.GetMsgId(), 10, 64)
		if err != nil {
			return "", nil, fmt.Errorf("job: bad msg id %q: %w", m.GetMsgId(), err)
		}
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString("(?, ?, ?, ?, ?, ?, ?, ?)")
		args = append(args,
			id,
			m.GetConvId(),
			m.GetSeq(),
			m.GetSenderId(),
			m.GetMsgType(),
			m.GetPayload(),
			0,
			time.UnixMilli(m.GetTimestamp()),
		)
	}
	return sb.String(), args, nil
}

// upsertConversations 批量 UPSERT 会话行：
// 单聊双方各一行同批写入；群聊按成员分批（≤100/批）UPSERT 全体成员。
func (w *StoreWorker) upsertConversations(ctx context.Context, items []storeItem) error {
	var p2p []*pb.PbMsg
	for _, it := range items {
		if it.msg == nil {
			continue
		}
		if it.msg.GetConvType() == 2 {
			if err := w.upsertGroupConversations(ctx, it.msg); err != nil {
				// 单个群的会话行失败不阻塞整批（消息本体已落库，靠重放/对账兜底）。
				w.logger.Warn("group conversation upsert failed",
					zap.String("conv", it.msg.GetConvId()), zap.Error(err))
			}
			continue
		}
		p2p = append(p2p, it.msg)
	}
	if len(p2p) == 0 {
		return nil
	}
	query, args, err := BuildConversationUpsert(p2p)
	if err != nil {
		// 数据问题不阻塞提交：跳过会话行更新（消息本体已落库）。
		w.logger.Warn("build conversation upsert failed", zap.Error(err))
		return nil
	}
	if _, err := w.exec.ExecContext(ctx, query, args...); err != nil {
		return fmt.Errorf("job: upsert conversation: %w", err)
	}
	return nil
}

// upsertGroupConversations 为一条群消息 UPSERT 全体成员的会话行（分批 100/批）。
func (w *StoreWorker) upsertGroupConversations(ctx context.Context, msg *pb.PbMsg) error {
	gid, err := service.ParseGroupConv(msg.GetConvId())
	if err != nil {
		return err
	}
	if w.memberSrc == nil {
		return fmt.Errorf("job: no member source configured")
	}
	members, err := w.memberSrc.Members(ctx, gid)
	if err != nil {
		return err
	}
	for _, stmt := range BuildGroupConversationUpserts(msg, members, groupConvBatch) {
		if _, err := w.exec.ExecContext(ctx, stmt.query, stmt.args...); err != nil {
			return fmt.Errorf("job: upsert group conversation: %w", err)
		}
	}
	return nil
}

// groupConvBatch 群会话行每批上限。
const groupConvBatch = 100

// stmt 是一条待执行的批量语句。
type stmt struct {
	query string
	args  []any
}

// BuildGroupConversationUpserts 生成群会话批量 UPSERT 语句（≤batch 成员/条）。
// 发送者行 unread 增量 0，其余成员 +1；重进群的成员保留历史游标
// （ON DUPLICATE 只推进 last_seq/unread，不重置 read_seq）。
func BuildGroupConversationUpserts(msg *pb.PbMsg, members []int64, batch int) []stmt {
	var out []stmt
	updatedAt := time.UnixMilli(msg.GetTimestamp())
	for start := 0; start < len(members); start += batch {
		end := start + batch
		if end > len(members) {
			end = len(members)
		}
		var sb strings.Builder
		sb.WriteString("INSERT INTO conversation (uid, conv_id, conv_type, target_id, last_seq, read_seq, unread, updated_at) VALUES ")
		args := make([]any, 0, (end-start)*7)
		for i, uid := range members[start:end] {
			delta := int64(1)
			if uid == msg.GetSenderId() {
				delta = 0
			}
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString("(?, ?, ?, ?, ?, 0, ?, ?)")
			args = append(args, uid, msg.GetConvId(), msg.GetConvType(), gidOfConv(msg.GetConvId()), msg.GetSeq(), delta, updatedAt)
		}
		sb.WriteString(" ON DUPLICATE KEY UPDATE last_seq = GREATEST(last_seq, VALUES(last_seq)), unread = unread + VALUES(unread), updated_at = VALUES(updated_at)")
		out = append(out, stmt{query: sb.String(), args: args})
	}
	return out
}

// gidOfConv 从群会话 ID 提取 gid（target_id）；失败返回 0。
func gidOfConv(convID string) int64 {
	gid, err := service.ParseGroupConv(convID)
	if err != nil {
		return 0
	}
	return gid
}

// BuildConversationUpsert 构造双方会话行的多值 UPSERT 语句。
// 主键 (uid, conv_id)：双方各一行，同一批内写入。
func BuildConversationUpsert(msgs []*pb.PbMsg) (string, []any, error) {
	var sb strings.Builder
	sb.WriteString("INSERT INTO conversation (uid, conv_id, conv_type, target_id, last_seq, read_seq, unread, updated_at) VALUES ")
	args := make([]any, 0, len(msgs)*2*7)
	row := 0
	for _, m := range msgs {
		a, b, err := service.ParseP2PConv(m.GetConvId())
		if err != nil {
			return "", nil, err
		}
		recv, err := ReceiverOf(m)
		if err != nil {
			return "", nil, err
		}
		updatedAt := time.UnixMilli(m.GetTimestamp())
		for _, uid := range []int64{a, b} {
			delta := int64(0)
			if uid == recv {
				delta = 1
			}
			target := b
			if uid == b {
				target = a
			}
			if row > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString("(?, ?, ?, ?, ?, 0, ?, ?)")
			args = append(args, uid, m.GetConvId(), m.GetConvType(), target, m.GetSeq(), delta, updatedAt)
			row++
		}
	}
	sb.WriteString(" ON DUPLICATE KEY UPDATE last_seq = GREATEST(last_seq, VALUES(last_seq)), unread = unread + VALUES(unread), updated_at = VALUES(updated_at)")
	return sb.String(), args, nil
}

// dlq 把整批消息 produce 到死信 topic（人工重放工具见 S10）。
func (w *StoreWorker) dlq(ctx context.Context, items []storeItem) {
	for _, it := range items {
		if err := w.producer.Send(ctx, TopicDLQStore, it.km.Key, it.km.Value,
			map[string]string{"origin-topic": it.km.Topic}); err != nil {
			w.logger.Error("produce to DLQ failed", zap.String("topic", TopicDLQStore), zap.Error(err))
		}
	}
}
