package logic

import (
	"context"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
	"go.uber.org/zap"

	"github.com/linkim/linkim/internal/service"
	"github.com/linkim/linkim/pkg/mysqlx"
	"github.com/linkim/linkim/pkg/pb"
	"github.com/linkim/linkim/pkg/redisx"
)

// 同步参数（设计文档 10.1：limit 上限 100，循环由客户端驱动）。
const syncMaxLimit = 100

// SyncStore 抽象离线同步读取（MySQL 实现），便于单测 mock。
type SyncStore interface {
	// PullMessages 返回 conv_id 内 seq > afterSeq 的消息（升序，至多 limit 条）
	// 与会话当前 last_seq（无会话行时为 0）。
	PullMessages(ctx context.Context, uid int64, convID string, afterSeq int64, limit int) ([]*pb.PbMsg, int64, error)
	// PendingConvs 返回 uid 的未读会话列表（last_seq > read_seq）。
	PendingConvs(ctx context.Context, uid int64) ([]*pb.ConvBrief, error)
	// MarkRead 推进 read_seq 游标并扣减未读（防回退、不为负）。
	MarkRead(ctx context.Context, uid int64, convID string, seq int64) error
}

// Deps 注入点见 server.go；sync 字段承载同步存储。

// SyncPull 实现 gRPC Logic.SyncPull（设计文档 10.1：seq 游标 + 增量拉取）。
func (s *Server) SyncPull(ctx context.Context, req *pb.SyncPullReq) (*pb.SyncPullResp, error) {
	uid := req.GetUid()
	if uid <= 0 {
		return &pb.SyncPullResp{Code: CodeBadParam}, nil
	}
	limit := clampLimit(int(req.GetLimit()))

	// 会话归属校验：单聊取双方校验；群聊用成员源校验（不信任客户端）。
	if err := s.checkSyncMember(ctx, uid, req.GetConvId()); err != nil {
		return &pb.SyncPullResp{Code: CodeBadParam}, nil
	}

	msgs, maxSeq, err := s.sync.PullMessages(ctx, uid, req.GetConvId(), req.GetLocalMaxSeq(), limit)
	if err != nil {
		s.logger.Warn("pull messages failed",
			zap.Int64("uid", uid), zap.String("conv", req.GetConvId()), zap.Error(err))
		return &pb.SyncPullResp{Code: CodeRedisErr}, nil
	}
	return &pb.SyncPullResp{Code: 0, Messages: msgs, MaxSeq: maxSeq}, nil
}

// checkSyncMember 校验 uid 是否为会话成员：单聊解析双方，群聊查成员源。
func (s *Server) checkSyncMember(ctx context.Context, uid int64, convID string) error {
	if gid, err := service.ParseGroupConv(convID); err == nil {
		if s.members == nil {
			return fmt.Errorf("logic: no member source")
		}
		isMember, err := s.members.IsMember(ctx, gid, uid)
		if err != nil || !isMember {
			return fmt.Errorf("logic: not group member")
		}
		return nil
	}
	a, b, err := service.ParseP2PConv(convID)
	if err != nil || (uid != a && uid != b) {
		return fmt.Errorf("logic: not conv member")
	}
	return nil
}

// clampLimit 钳制 limit 到 [1,100]（设计文档 10.1：上限 100，循环客户端驱动）。
func clampLimit(n int) int {
	if n < 1 {
		return 1
	}
	if n > syncMaxLimit {
		return syncMaxLimit
	}
	return n
}

// GetPendingConvs 实现 gRPC Logic.GetPendingConvs。
func (s *Server) GetPendingConvs(ctx context.Context, req *pb.PendingReq) (*pb.PendingResp, error) {
	if req.GetUid() <= 0 {
		return &pb.PendingResp{}, nil
	}
	convs, err := s.sync.PendingConvs(ctx, req.GetUid())
	if err != nil {
		s.logger.Warn("pending convs failed", zap.Int64("uid", req.GetUid()), zap.Error(err))
		return &pb.PendingResp{}, nil
	}
	return &pb.PendingResp{Convs: convs}, nil
}

// MarkRead 实现 gRPC Logic.MarkRead（设计文档 10.2：追平后更新 read_seq）。
func (s *Server) MarkRead(ctx context.Context, req *pb.MarkReadReq) (*pb.Empty, error) {
	if req.GetUid() <= 0 || req.GetConvId() == "" || req.GetSeq() < 0 {
		s.logger.Warn("mark read invalid args",
			zap.Int64("uid", req.GetUid()), zap.String("conv", req.GetConvId()), zap.Int64("seq", req.GetSeq()))
		return &pb.Empty{}, nil
	}
	if err := s.sync.MarkRead(ctx, req.GetUid(), req.GetConvId(), req.GetSeq()); err != nil {
		s.logger.Warn("mark read failed",
			zap.Int64("uid", req.GetUid()), zap.String("conv", req.GetConvId()), zap.Error(err))
	}
	return &pb.Empty{}, nil
}

// OnlineEvent 实现 gRPC Logic.OnlineEvent（设计文档 10.2 上线补拉）：
// 补写 presence（Comet 已写则跳过）并返回未读会话列表。
func (s *Server) OnlineEvent(ctx context.Context, req *pb.OnlineEventReq) (*pb.PendingResp, error) {
	if req.GetUid() <= 0 {
		return &pb.PendingResp{}, nil
	}
	if !req.GetOnline() {
		// 下线事件：presence 清理由 comet 负责（判断是否最后一端），此处仅记日志。
		s.logger.Debug("offline event", zap.Int64("uid", req.GetUid()), zap.String("device", req.GetDeviceId()))
		return &pb.PendingResp{}, nil
	}

	// presence：Comet AUTH 时已写则跳过（存在即不覆盖）。
	if s.rdb != nil {
		pKey := redisx.PresenceKey(req.GetUid())
		exists, err := s.rdb.Exists(ctx, pKey).Result()
		if err == nil && exists == 0 {
			if err := s.rdb.Set(ctx, pKey, "online", presenceTTLOnEvent).Err(); err != nil {
				s.logger.Warn("write presence on online event failed", zap.Error(err))
			}
		}
	}

	convs, err := s.sync.PendingConvs(ctx, req.GetUid())
	if err != nil {
		s.logger.Warn("pending convs on online failed", zap.Int64("uid", req.GetUid()), zap.Error(err))
		return &pb.PendingResp{}, nil
	}
	s.logger.Info("online event", zap.Int64("uid", req.GetUid()),
		zap.String("device", req.GetDeviceId()), zap.Int("pending_convs", len(convs)))
	return &pb.PendingResp{Convs: convs}, nil
}

// presenceTTLOnEvent 与 comet 侧 presence TTL 一致（90s，心跳续期）。
const presenceTTLOnEvent = 90 * time.Second

// --- MySQL 实现 ---

// MySQLSyncStore 是 SyncStore 的 sqlx 实现。
type MySQLSyncStore struct {
	db *sqlx.DB
}

// NewMySQLSyncStore 构造同步存储。
func NewMySQLSyncStore(db *sqlx.DB) *MySQLSyncStore { return &MySQLSyncStore{db: db} }

// messageRow 对应 message_xx 表一行。
type messageRow struct {
	ID        uint64 `db:"id"`
	ConvID    string `db:"conv_id"`
	Seq       int64  `db:"seq"`
	SenderID  int64  `db:"sender_id"`
	MsgType   int32  `db:"msg_type"`
	Payload   []byte `db:"payload"`
	Status    int32  `db:"status"`
	CreatedAt int64  `db:"created_at_ms"`
}

// PullMessages 实现 SyncStore（禁止深分页：按 (conv_id, seq) 索引范围扫描）。
func (m *MySQLSyncStore) PullMessages(ctx context.Context, _ int64, convID string, afterSeq int64, limit int) ([]*pb.PbMsg, int64, error) {
	table := mysqlx.ShardTable(convID)

	q := fmt.Sprintf(`SELECT id, conv_id, seq, sender_id, msg_type, payload, status,
		CAST(UNIX_TIMESTAMP(created_at) * 1000 AS SIGNED) AS created_at_ms
		FROM %s WHERE conv_id = ? AND seq > ? ORDER BY seq ASC LIMIT ?`, table)
	var rows []messageRow
	if err := m.db.SelectContext(ctx, &rows, q, convID, afterSeq, limit); err != nil {
		return nil, 0, fmt.Errorf("logic: select messages: %w", err)
	}

	msgs := make([]*pb.PbMsg, 0, len(rows))
	for _, r := range rows {
		msgs = append(msgs, &pb.PbMsg{
			MsgId:     fmt.Sprintf("%d", r.ID),
			ConvId:    r.ConvID,
			ConvType:  1,
			SenderId:  r.SenderID,
			MsgType:   r.MsgType,
			Payload:   r.Payload,
			Seq:       r.Seq,
			Timestamp: r.CreatedAt,
			Status:    r.Status,
		})
	}

	var maxSeq int64
	if err := m.db.GetContext(ctx, &maxSeq,
		`SELECT last_seq FROM conversation WHERE uid = ? AND conv_id = ?`, convMemberUID(convID), convID); err != nil {
		// 用任一成员行取 last_seq（双方一致）；无行视为新会话 max_seq=0。
		maxSeq = 0
	}
	return msgs, maxSeq, nil
}

// convMemberUID 取会话任一成员 uid（P2P 取小端），仅用于读 last_seq。
func convMemberUID(convID string) int64 {
	a, _, err := service.ParseP2PConv(convID)
	if err != nil {
		return 0
	}
	return a
}

// PendingConvs 实现 SyncStore。
func (m *MySQLSyncStore) PendingConvs(ctx context.Context, uid int64) ([]*pb.ConvBrief, error) {
	var rows []struct {
		ConvID   string `db:"conv_id"`
		ConvType int32  `db:"conv_type"`
		LastSeq  int64  `db:"last_seq"`
		Unread   int32  `db:"unread"`
	}
	err := m.db.SelectContext(ctx, &rows,
		`SELECT conv_id, conv_type, last_seq, unread FROM conversation WHERE uid = ? AND last_seq > read_seq`, uid)
	if err != nil {
		return nil, fmt.Errorf("logic: select pending convs: %w", err)
	}
	convs := make([]*pb.ConvBrief, 0, len(rows))
	for _, r := range rows {
		convs = append(convs, &pb.ConvBrief{
			ConvId: r.ConvID, ConvType: r.ConvType, MaxSeq: r.LastSeq, Unread: r.Unread,
		})
	}
	return convs, nil
}

// MarkRead 实现 SyncStore：条件 UPDATE 防游标回退，GREATEST 保证未读不为负。
// SET 顺序关键：先算 unread（引用旧 read_seq），再赋 read_seq。
func (m *MySQLSyncStore) MarkRead(ctx context.Context, uid int64, convID string, seq int64) error {
	res, err := m.db.ExecContext(ctx, `UPDATE conversation
		SET unread = GREATEST(unread - (? - read_seq), 0), read_seq = ?, updated_at = NOW()
		WHERE uid = ? AND conv_id = ? AND read_seq < ?`,
		seq, seq, uid, convID, seq)
	if err != nil {
		return fmt.Errorf("logic: mark read: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		// 无行或游标不落后：幂等无操作。
		return nil
	}
	return nil
}
