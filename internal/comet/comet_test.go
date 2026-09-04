package comet

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// newTestServer 返回无外部依赖（redis/logic 为 nil）的服务实例，
// 并注入可控假时钟。
func newTestServer() (*Server, *fakeClock) {
	s := NewServer("127.0.0.1:9000", nil, nil, 0, zap.NewNop(), nil)
	fc := &fakeClock{sec: 1_000_000, ms: 1_000_000_000}
	s.nowSec = fc.secNow
	s.nowMs = fc.msNow
	return s, fc
}

// fakeClock 是可手动推进的时钟。
type fakeClock struct {
	sec int64
	ms  int64
}

func (f *fakeClock) secNow() int64 { return f.sec }
func (f *fakeClock) msNow() int64  { return f.ms }

func (f *fakeClock) addSec(n int64) { f.sec += n }

// newTestConn 构造无 ws 的连接（不启动循环）。
func newTestConn(s *Server, uid int64, device string, platform int32) *Conn {
	c := newConn(s, nil)
	if uid != 0 {
		c.setIdentity(uid, device, platform)
		c.markAuthed()
	}
	return c
}

func TestBucketPutGetDel(t *testing.T) {
	b := newBucket()
	c1 := &Conn{send: make(chan []byte, 1)}
	c2 := &Conn{send: make(chan []byte, 1)}

	assert.Nil(t, b.put("7:d1", c1), "首次写入无旧连接")
	assert.Equal(t, 1, b.size())

	got := b.get("7:d1")
	require.NotNil(t, got)
	assert.Same(t, c1, got)
	assert.Nil(t, b.get("7:d2"), "不存在的 key 返回 nil")

	old := b.put("7:d1", c2)
	assert.Same(t, c1, old, "同 key 覆盖时返回旧连接")
	assert.Equal(t, 1, b.size())

	assert.True(t, b.del("7:d1"))
	assert.False(t, b.del("7:d1"), "重复删除返回 false")
	assert.Zero(t, b.size())
}

func TestBucketIndexAndRouteField(t *testing.T) {
	tests := []struct {
		uid       int64
		wantIndex int
	}{
		{uid: 0, wantIndex: 0},
		{uid: 255, wantIndex: 255},
		{uid: 256, wantIndex: 0},
		{uid: 257, wantIndex: 1},
		{uid: 12345, wantIndex: 12345 % 256},
	}
	for _, tt := range tests {
		assert.Equal(t, tt.wantIndex, bucketIndex(tt.uid), "uid=%d", tt.uid)
	}

	assert.Equal(t, "7:d1", DeviceKey(7, "d1"))
	assert.Equal(t, "4:web-1", routeField(4, "web-1"))
}

func TestBucketIdleScan(t *testing.T) {
	s, fc := newTestServer()

	fresh := newTestConn(s, 1, "d1", 1)
	idle1 := newTestConn(s, 2, "d2", 1)
	idle2 := newTestConn(s, 258, "d3", 2) // 与 idle1 不同分片
	unauthed := newConn(s, nil)           // 未鉴权，不参与超时扫描（由 watchdog 管）

	// 全部登记后，把两个连接的活跃时间拨回 100s 前。
	idle1.lastActive = fc.sec - 100
	idle2.lastActive = fc.sec - 100

	b1 := s.bucket(1)
	b1.put(DeviceKey(1, "d1"), fresh)
	b1.put(DeviceKey(2, "d2"), idle1)
	b2 := s.bucket(258)
	b2.put(DeviceKey(258, "d3"), idle2)
	b3 := s.bucket(unauthedUID)
	b3.put(DeviceKey(unauthedUID, ""), unauthed)

	victims := b1.idleConns(fc.sec, int64(idleTimeout.Seconds()))
	assert.Len(t, victims, 1, "分片 1 只有 idle1 超时")
	assert.Same(t, idle1, victims[0])

	victims = b2.idleConns(fc.sec, int64(idleTimeout.Seconds()))
	assert.Len(t, victims, 1, "分片 258 只有 idle2 超时")
	assert.Same(t, idle2, victims[0])

	// 推进时间后 fresh 也超时。
	fc.addSec(200)
	victims = b1.idleConns(fc.sec, int64(idleTimeout.Seconds()))
	assert.Len(t, victims, 2)

	// 未鉴权连接永不进入超时名单。
	assert.Empty(t, b3.idleConns(fc.sec, int64(idleTimeout.Seconds())))
}

const unauthedUID = 3 // 未鉴权连接的 uid（bucket 键使用）

func TestConnPushSlowConsumer(t *testing.T) {
	s, _ := newTestServer()
	c := newTestConn(s, 7, "d1", 1)

	// 填满缓冲。
	for i := 0; i < sendChanSize; i++ {
		require.NoError(t, c.Push([]byte{byte(i)}))
	}
	assert.ErrorIs(t, c.Push([]byte("extra")), ErrSlowConsumer)

	// 排空后恢复。
	for i := 0; i < sendChanSize; i++ {
		<-c.send
	}
	assert.NoError(t, c.Push([]byte("ok")))
}

func TestConnCheckSlow(t *testing.T) {
	s, fc := newTestServer()
	c := newTestConn(s, 7, "d1", 1)

	fill := func() {
		for i := 0; i < sendChanSize; i++ {
			c.send <- []byte{0}
		}
	}
	drain := func(n int) {
		for i := 0; i < n; i++ {
			<-c.send
		}
	}

	// 未打满：不标记。
	assert.False(t, c.checkSlow(fc.ms))

	// 打满后第一次检查：开始计时但不踢（计时起点 = 本次时刻）。
	fill()
	assert.False(t, c.checkSlow(fc.msNow()+1_000))
	// 距起点 3s：未到 5s 阈值。
	assert.False(t, c.checkSlow(fc.msNow()+4_000))
	// 距起点 5.5s：判定慢连接。
	assert.True(t, c.checkSlow(fc.msNow()+6_500))

	// 排空后复位。
	drain(1)
	assert.False(t, c.checkSlow(fc.msNow()+10_000))
	assert.Zero(t, c.fullSince, "排空后 fullSince 复位")
	drain(sendChanSize - 1)
}

func TestConnCloseOnceAndCleanup(t *testing.T) {
	s, _ := newTestServer()
	c := newTestConn(s, 7, "d1", 1)
	s.bucket(7).put(DeviceKey(7, "d1"), c)

	c.Close()
	// 幂等：重复 Close 不 panic。
	c.Close()
	c.Close()

	select {
	case <-c.Closed():
	default:
		t.Fatal("closed 信号应已关闭")
	}
	// bucket 已清理。
	assert.Zero(t, s.bucket(7).size())
	// 关闭后 Push 返回 ErrConnClosed。
	assert.ErrorIs(t, c.Push([]byte("x")), ErrConnClosed)
}

func TestServerBucketRouting(t *testing.T) {
	s, _ := newTestServer()
	c := newTestConn(s, 300, "d1", 1)
	s.bucket(300).put(DeviceKey(300, "d1"), c)
	assert.Same(t, c, s.bucket(300).get(DeviceKey(300, "d1")))
	assert.Same(t, s.bucket(300+256), s.bucket(300), "uid 与 uid+256 同分片")
}

func TestConstantsGuard(t *testing.T) {
	assert.Equal(t, 256, bucketNum)
	assert.Equal(t, 256, sendChanSize)
	assert.Equal(t, 10*time.Second, authTimeout)
	assert.Equal(t, 75*time.Second, idleTimeout)
	assert.Equal(t, 5*time.Second, slowKickAfter)
	assert.Equal(t, 30*time.Second, aliveTTL)
	assert.Equal(t, 10*time.Second, aliveRenewIt)
}

// （原子字段在单测中单协程直接写入即可）
