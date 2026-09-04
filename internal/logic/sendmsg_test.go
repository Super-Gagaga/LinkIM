package logic

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/linkim/linkim/pkg/pb"
	"github.com/linkim/linkim/pkg/redisx"
	"github.com/linkim/linkim/pkg/snowflake"
)

// --- mocks ---

// memIdem 是 IdemStore 的内存实现（含 NX 语义）。
type memIdem struct {
	mu  sync.Mutex
	kv  map[string]string
	err error
}

func newMemIdem() *memIdem { return &memIdem{kv: map[string]string{}} }

func (m *memIdem) SetNX(_ context.Context, key, val string, _ time.Duration) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return false, m.err
	}
	if _, ok := m.kv[key]; ok {
		return false, nil
	}
	m.kv[key] = val
	return true, nil
}

func (m *memIdem) Get(_ context.Context, key string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.kv[key], nil
}

func (m *memIdem) Set(_ context.Context, key, val string, _ time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.kv[key] = val
	return nil
}

func (m *memIdem) Del(_ context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.kv, key)
	return nil
}

func (m *memIdem) raw(key string) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.kv[key]
}

// memSeq 是 SeqGen 的计数器实现。
type memSeq struct {
	mu sync.Mutex
	n  int64
}

func (s *memSeq) Next(_ context.Context, _ string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.n++
	return s.n, nil
}

// memFriends 是 FriendChecker 的可编程实现。
type memFriends struct {
	mu    sync.Mutex
	pairs map[[2]int64]bool
	err   error
	calls int
}

func newMemFriends(pairs ...[2]int64) *memFriends {
	m := &memFriends{pairs: map[[2]int64]bool{}}
	for _, p := range pairs {
		m.pairs[p] = true
	}
	return m
}

func (f *memFriends) IsFriend(_ context.Context, uid, friendUID int64) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.err != nil {
		return false, f.err
	}
	return f.pairs[[2]int64{uid, friendUID}], nil
}

// memProducer 记录全部 Send 调用，可注入错误。
type memProducer struct {
	mu    sync.Mutex
	sends []recordedSend
	errOn map[string]error // topic -> error
}

type recordedSend struct {
	Topic   string
	Key     string
	Value   []byte
	Headers []map[string]string
}

func newMemProducer(errOn map[string]error) *memProducer {
	if errOn == nil {
		errOn = map[string]error{}
	}
	return &memProducer{errOn: errOn}
}

func (p *memProducer) Send(_ context.Context, topic string, key, value []byte, headers ...map[string]string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err, ok := p.errOn[topic]; ok {
		return err
	}
	p.sends = append(p.sends, recordedSend{Topic: topic, Key: string(key), Value: value, Headers: headers})
	return nil
}

func (p *memProducer) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.sends)
}

// --- 测试装配 ---

type sendFixture struct {
	srv      *Server
	idem     *memIdem
	seq      *memSeq
	friends  *memFriends
	producer *memProducer
}

// 7 与 8 互为好友。
var friendPair = [2]int64{7, 8}

func newSendFixture(t *testing.T, producer *memProducer) *sendFixture {
	t.Helper()
	if producer == nil {
		producer = newMemProducer(nil)
	}
	ids, err := snowflake.NewNode(3)
	require.NoError(t, err)
	f := &sendFixture{
		idem:     newMemIdem(),
		seq:      &memSeq{},
		friends:  newMemFriends(friendPair),
		producer: producer,
	}
	f.srv = NewServer(Deps{
		Friends:  f.friends,
		Seq:      f.seq,
		Idem:     f.idem,
		IDs:      ids,
		Producer: producer,
		Logger:   zap.NewNop(),
	})
	return f
}

func validSendReq() *pb.SendMsgReq {
	return &pb.SendMsgReq{
		SenderId:    7,
		ConvId:      "c:7:8",
		ConvType:    1,
		ClientMsgId: "cmid-1",
		DeviceId:    "d1",
		MsgType:     1,
		Payload:     []byte("hello"),
	}
}

// --- 用例 ---

func TestSendMsgHappyPath(t *testing.T) {
	ctx := context.Background()
	f := newSendFixture(t, nil)

	ack, err := f.srv.SendMsg(ctx, validSendReq())
	require.NoError(t, err)
	assert.Equal(t, int32(0), ack.GetCode())
	assert.NotEmpty(t, ack.GetMsgId())
	assert.Equal(t, int64(1), ack.GetSeq())
	assert.Greater(t, ack.GetTimestamp(), int64(0))
	assert.Less(t, ack.GetTimestamp(), time.Now().UnixMilli()+1000, "毫秒时间戳")

	// 双写：msg.push 与 msg.store 各一条，key=conv_id，header 带 trace-id/conv-type。
	require.Equal(t, 2, f.producer.count())
	var push, store *recordedSend
	f.producer.mu.Lock()
	for i := range f.producer.sends {
		s := f.producer.sends[i]
		switch s.Topic {
		case TopicMsgPush:
			push = &s
		case TopicMsgStore:
			store = &s
		}
	}
	f.producer.mu.Unlock()
	require.NotNil(t, push, "必须写 msg.push")
	require.NotNil(t, store, "必须写 msg.store")
	for _, s := range []*recordedSend{push, store} {
		assert.Equal(t, "c:7:8", s.Key)
		h := mergeHeaders(s.Headers)
		assert.Equal(t, "cmid-1", h["trace-id"])
		assert.Equal(t, "1", h["conv-type"])
	}

	// 连发 5 条 seq 严格 +1。
	for i := int64(2); i <= 5; i++ {
		req := validSendReq()
		req.ClientMsgId = fmt.Sprintf("cmid-%d", i)
		ack, err := f.srv.SendMsg(ctx, req)
		require.NoError(t, err)
		assert.Equal(t, int32(0), ack.GetCode())
		assert.Equal(t, i, ack.GetSeq())
	}
	assert.Equal(t, 10, f.producer.count(), "5 条消息 × 2 topic")

	// 成功后幂等键为 JSON 终值。
	val := f.idem.raw(redisx.IdemKey(7, "cmid-1"))
	assert.Contains(t, val, `"msg_id"`)
	assert.Contains(t, val, `"seq":1`)
}

func mergeHeaders(maps []map[string]string) map[string]string {
	out := map[string]string{}
	for _, m := range maps {
		for k, v := range m {
			out[k] = v
		}
	}
	return out
}

// TestSendMsgIdempotentReplay 同 clientMsgID 二次调用：不产生新 producer 调用，ACK 一致。
func TestSendMsgIdempotentReplay(t *testing.T) {
	ctx := context.Background()
	f := newSendFixture(t, nil)

	first, err := f.srv.SendMsg(ctx, validSendReq())
	require.NoError(t, err)
	require.Equal(t, int32(0), first.GetCode())
	sendsAfterFirst := f.producer.count()

	second, err := f.srv.SendMsg(ctx, validSendReq())
	require.NoError(t, err)

	assert.Equal(t, first.GetCode(), second.GetCode())
	assert.Equal(t, first.GetMsgId(), second.GetMsgId(), "回放同一 msgId")
	assert.Equal(t, first.GetSeq(), second.GetSeq(), "回放同一 seq")
	assert.Equal(t, sendsAfterFirst, f.producer.count(), "幂等命中不再写 Kafka")
	assert.Equal(t, int64(1), f.seq.n, "幂等命中不再生成 seq")
}

func TestSendMsgInFlight(t *testing.T) {
	ctx := context.Background()
	f := newSendFixture(t, nil)
	// 预置占位（值为空）模拟同 ID 请求在途。
	require.NoError(t, f.idem.Set(ctx, redisx.IdemKey(7, "cmid-1"), "", time.Minute))

	ack, err := f.srv.SendMsg(ctx, validSendReq())
	require.NoError(t, err)
	assert.Equal(t, int32(CodeInFlight), ack.GetCode())
	assert.Zero(t, f.producer.count())
}

func TestSendMsgNotFriend(t *testing.T) {
	ctx := context.Background()
	f := newSendFixture(t, nil)
	f.friends.pairs = map[[2]int64]bool{} // 清空好友

	ack, err := f.srv.SendMsg(ctx, validSendReq())
	require.NoError(t, err)
	assert.Equal(t, int32(CodeNotFriend), ack.GetCode())
	assert.Zero(t, f.producer.count())
	// 幂等键被回滚（允许修复关系后重发）。
	assert.Empty(t, f.idem.raw(redisx.IdemKey(7, "cmid-1")))
}

func TestSendMsgKafkaFailureRollsBackIdem(t *testing.T) {
	ctx := context.Background()
	producer := newMemProducer(map[string]error{TopicMsgPush: errors.New("kafka down")})
	f := newSendFixture(t, producer)

	ack, err := f.srv.SendMsg(ctx, validSendReq())
	require.NoError(t, err)
	assert.Equal(t, int32(CodeKafkaErr), ack.GetCode())
	// 只写进了 msg.store（push 先失败）。
	assert.Equal(t, 0, producer.count(), "push 先失败，未写任何 topic")
	// 幂等键被删除，客户端可重发。
	assert.Empty(t, f.idem.raw(redisx.IdemKey(7, "cmid-1")))

	// 重发成功（producer 恢复）。
	producer.mu.Lock()
	delete(producer.errOn, TopicMsgPush)
	producer.mu.Unlock()
	ack, err = f.srv.SendMsg(ctx, validSendReq())
	require.NoError(t, err)
	assert.Equal(t, int32(0), ack.GetCode())
	assert.Equal(t, 2, producer.count(), "重发补齐双写")
}

func TestSendMsgBadParams(t *testing.T) {
	ctx := context.Background()
	f := newSendFixture(t, nil)

	tests := []struct {
		name string
		mut  func(*pb.SendMsgReq)
	}{
		{"sender 为 0", func(r *pb.SendMsgReq) { r.SenderId = 0 }},
		{"client_msg_id 为空", func(r *pb.SendMsgReq) { r.ClientMsgId = "" }},
		{"msg_type 非法", func(r *pb.SendMsgReq) { r.MsgType = 0 }},
		{"payload 为空", func(r *pb.SendMsgReq) { r.Payload = nil }},
		{"payload 超限", func(r *pb.SendMsgReq) { r.Payload = make([]byte, msgMaxPayload+1) }},
		{"群聊暂不支持", func(r *pb.SendMsgReq) { r.ConvType = 2 }},
		{"conv 格式非法", func(r *pb.SendMsgReq) { r.ConvId = "c:7" }},
		{"conv 非规范化", func(r *pb.SendMsgReq) { r.ConvId = "c:8:7" }},
		{"发送者不在会话中", func(r *pb.SendMsgReq) { r.SenderId = 9; r.ConvId = "c:7:8" }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := validSendReq()
			tt.mut(req)
			ack, err := f.srv.SendMsg(ctx, req)
			require.NoError(t, err)
			assert.Equal(t, int32(CodeBadParam), ack.GetCode())
		})
	}
	assert.Zero(t, f.producer.count())
	assert.Zero(t, len(f.idem.kv), "参数错误不占用幂等键")
}

func TestSendMsgFriendCheckError(t *testing.T) {
	ctx := context.Background()
	f := newSendFixture(t, nil)
	f.friends.err = errors.New("db down")

	ack, err := f.srv.SendMsg(ctx, validSendReq())
	require.NoError(t, err)
	assert.Equal(t, int32(CodeRedisErr), ack.GetCode())
	assert.Zero(t, f.producer.count())
	assert.Empty(t, f.idem.raw(redisx.IdemKey(7, "cmid-1")), "失败回滚幂等键")
}

func TestSendMsgSeqError(t *testing.T) {
	ctx := context.Background()
	f := newSendFixture(t, nil)
	// 用可注入错误的 seq stub 替换。
	f.srv.seq = errSeqGen{}

	ack, err := f.srv.SendMsg(ctx, validSendReq())
	require.NoError(t, err)
	assert.Equal(t, int32(CodeRedisErr), ack.GetCode())
	assert.Empty(t, f.idem.raw(redisx.IdemKey(7, "cmid-1")))
}

type errSeqGen struct{}

func (errSeqGen) Next(context.Context, string) (int64, error) {
	return 0, errors.New("seq unavailable")
}
