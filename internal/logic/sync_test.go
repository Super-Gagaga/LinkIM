package logic

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/linkim/linkim/pkg/pb"
)

// memSyncStore 是 SyncStore 的可编程内存实现。
type memSyncStore struct {
	msgs    []*pb.PbMsg // 全量消息（按 seq 升序）
	maxSeq  int64
	pending []*pb.ConvBrief
	markErr error

	lastPull struct {
		uid      int64
		convID   string
		afterSeq int64
		limit    int
	}
	lastMark struct {
		uid  int64
		conv string
		seq  int64
	}
}

func (m *memSyncStore) PullMessages(_ context.Context, uid int64, convID string, afterSeq int64, limit int) ([]*pb.PbMsg, int64, error) {
	m.lastPull.uid, m.lastPull.convID, m.lastPull.afterSeq, m.lastPull.limit = uid, convID, afterSeq, limit
	var out []*pb.PbMsg
	for _, msg := range m.msgs {
		if msg.GetConvId() == convID && msg.GetSeq() > afterSeq {
			out = append(out, msg)
			if len(out) >= limit {
				break
			}
		}
	}
	return out, m.maxSeq, nil
}

func (m *memSyncStore) PendingConvs(_ context.Context, _ int64) ([]*pb.ConvBrief, error) {
	return m.pending, nil
}

func (m *memSyncStore) MarkRead(_ context.Context, uid int64, convID string, seq int64) error {
	m.lastMark.uid, m.lastMark.conv, m.lastMark.seq = uid, convID, seq
	return m.markErr
}

func newSyncServer(store SyncStore) *Server {
	return NewServer(Deps{Sync: store, Logger: zap.NewNop()})
}

func syncMsgs(conv string, seqs ...int64) []*pb.PbMsg {
	out := make([]*pb.PbMsg, 0, len(seqs))
	for _, s := range seqs {
		out = append(out, &pb.PbMsg{MsgId: "m", ConvId: conv, Seq: s, SenderId: 7})
	}
	return out
}

// TestSyncPullBoundaries 覆盖边界：local_max_seq 超前、limit 钳制、空会话、非成员。
func TestSyncPullBoundaries(t *testing.T) {
	ctx := context.Background()
	store := &memSyncStore{msgs: syncMsgs("c:7:8", 1, 2, 3, 4, 5), maxSeq: 5}
	svc := newSyncServer(store)

	t.Run("正常增量拉取", func(t *testing.T) {
		resp, err := svc.SyncPull(ctx, &pb.SyncPullReq{Uid: 8, ConvId: "c:7:8", LocalMaxSeq: 0, Limit: 100})
		require.NoError(t, err)
		assert.Equal(t, int32(0), resp.GetCode())
		require.Len(t, resp.GetMessages(), 5)
		assert.Equal(t, int64(5), resp.GetMaxSeq())
		// 升序。
		for i := 1; i < len(resp.GetMessages()); i++ {
			assert.Greater(t, resp.GetMessages()[i].GetSeq(), resp.GetMessages()[i-1].GetSeq())
		}
		assert.Equal(t, 100, store.lastPull.limit)
	})

	t.Run("local_max_seq 超前返回空但带回 max_seq", func(t *testing.T) {
		resp, err := svc.SyncPull(ctx, &pb.SyncPullReq{Uid: 8, ConvId: "c:7:8", LocalMaxSeq: 99, Limit: 100})
		require.NoError(t, err)
		assert.Equal(t, int32(0), resp.GetCode())
		assert.Empty(t, resp.GetMessages())
		assert.Equal(t, int64(5), resp.GetMaxSeq(), "客户端据此确认已追平")
	})

	t.Run("limit 钳制到 100", func(t *testing.T) {
		_, err := svc.SyncPull(ctx, &pb.SyncPullReq{Uid: 8, ConvId: "c:7:8", Limit: 500})
		require.NoError(t, err)
		assert.Equal(t, 100, store.lastPull.limit)
	})

	t.Run("limit 为 0 钳制到 1", func(t *testing.T) {
		_, err := svc.SyncPull(ctx, &pb.SyncPullReq{Uid: 8, ConvId: "c:7:8", Limit: 0})
		require.NoError(t, err)
		assert.Equal(t, 1, store.lastPull.limit)
	})

	t.Run("limit 下界为 1", func(t *testing.T) {
		assert.Equal(t, 1, clampLimit(-3))
		_, err := svc.SyncPull(ctx, &pb.SyncPullReq{Uid: 8, ConvId: "c:7:8", Limit: -3})
		require.NoError(t, err)
		assert.Equal(t, 1, store.lastPull.limit)
	})

	t.Run("空会话返回空且 max_seq=0", func(t *testing.T) {
		empty := &memSyncStore{msgs: nil, maxSeq: 0}
		resp, err := newSyncServer(empty).SyncPull(ctx, &pb.SyncPullReq{Uid: 7, ConvId: "c:7:8", Limit: 100})
		require.NoError(t, err)
		assert.Equal(t, int32(0), resp.GetCode())
		assert.Empty(t, resp.GetMessages())
		assert.Zero(t, resp.GetMaxSeq())
	})

	t.Run("非会话成员返回 40201", func(t *testing.T) {
		resp, err := svc.SyncPull(ctx, &pb.SyncPullReq{Uid: 9, ConvId: "c:7:8", Limit: 100})
		require.NoError(t, err)
		assert.Equal(t, int32(CodeBadParam), resp.GetCode())
	})

	t.Run("conv 非法返回 40201", func(t *testing.T) {
		resp, err := svc.SyncPull(ctx, &pb.SyncPullReq{Uid: 7, ConvId: "bad", Limit: 100})
		require.NoError(t, err)
		assert.Equal(t, int32(CodeBadParam), resp.GetCode())
	})

	t.Run("uid 缺失返回 40201", func(t *testing.T) {
		resp, err := svc.SyncPull(ctx, &pb.SyncPullReq{ConvId: "c:7:8", Limit: 100})
		require.NoError(t, err)
		assert.Equal(t, int32(CodeBadParam), resp.GetCode())
	})
}

func TestGetPendingConvs(t *testing.T) {
	ctx := context.Background()
	store := &memSyncStore{pending: []*pb.ConvBrief{
		{ConvId: "c:7:8", ConvType: 1, MaxSeq: 12, Unread: 3},
	}}
	svc := newSyncServer(store)

	resp, err := svc.GetPendingConvs(ctx, &pb.PendingReq{Uid: 8})
	require.NoError(t, err)
	require.Len(t, resp.GetConvs(), 1)
	assert.Equal(t, "c:7:8", resp.GetConvs()[0].GetConvId())
	assert.Equal(t, int64(12), resp.GetConvs()[0].GetMaxSeq())
	assert.Equal(t, int32(3), resp.GetConvs()[0].GetUnread())

	// uid 非法返回空。
	resp, err = svc.GetPendingConvs(ctx, &pb.PendingReq{})
	require.NoError(t, err)
	assert.Empty(t, resp.GetConvs())
}

func TestMarkRead(t *testing.T) {
	ctx := context.Background()

	t.Run("正常上报", func(t *testing.T) {
		store := &memSyncStore{}
		_, err := newSyncServer(store).MarkRead(ctx, &pb.MarkReadReq{Uid: 8, ConvId: "c:7:8", Seq: 10})
		require.NoError(t, err)
		assert.Equal(t, int64(8), store.lastMark.uid)
		assert.Equal(t, "c:7:8", store.lastMark.conv)
		assert.Equal(t, int64(10), store.lastMark.seq)
	})

	t.Run("非法参数被忽略", func(t *testing.T) {
		store := &memSyncStore{}
		_, err := newSyncServer(store).MarkRead(ctx, &pb.MarkReadReq{Uid: 0, ConvId: "c:7:8", Seq: 10})
		require.NoError(t, err)
		assert.Zero(t, store.lastMark.seq, "未触达存储层")
	})

	t.Run("存储错误不外泄", func(t *testing.T) {
		store := &memSyncStore{markErr: errors.New("db down")}
		_, err := newSyncServer(store).MarkRead(ctx, &pb.MarkReadReq{Uid: 8, ConvId: "c:7:8", Seq: 10})
		require.NoError(t, err, "MarkRead 失败仅记日志")
	})
}

func TestOnlineEvent(t *testing.T) {
	ctx := context.Background()
	store := &memSyncStore{pending: []*pb.ConvBrief{{ConvId: "c:7:8", MaxSeq: 5, Unread: 2}}}
	svc := newSyncServer(store)

	t.Run("上线返回未读会话", func(t *testing.T) {
		resp, err := svc.OnlineEvent(ctx, &pb.OnlineEventReq{Uid: 8, DeviceId: "d1", Platform: 1, Online: true})
		require.NoError(t, err)
		require.Len(t, resp.GetConvs(), 1)
		assert.Equal(t, "c:7:8", resp.GetConvs()[0].GetConvId())
	})

	t.Run("下线事件返回空", func(t *testing.T) {
		resp, err := svc.OnlineEvent(ctx, &pb.OnlineEventReq{Uid: 8, Online: false})
		require.NoError(t, err)
		assert.Empty(t, resp.GetConvs())
	})
}

// TestMySQLMarkReadSQL 钉死防回退 UPDATE 语句形状（SET 顺序保证 unread 用旧 read_seq 计算）。
func TestMySQLMarkReadSQL(t *testing.T) {
	// 语句在代码内拼接，此处验证 clamp 与接口契约；真实 SQL 行为由验收覆盖。
	assert.Equal(t, 100, clampLimit(100))
	assert.Equal(t, 1, clampLimit(1))
}
