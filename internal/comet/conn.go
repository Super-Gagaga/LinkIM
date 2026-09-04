// Package comet 实现 WebSocket 长连接接入层：连接管理、读写循环、
// bucket 分片、心跳超时、路由表登记与同端互踢（设计文档 4.5、7、15.1）。
package comet

import (
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

// 连接与协议参数（设计文档 4.5 / 15.1）。
const (
	sendChanSize  = 256              // 下行帧缓冲
	authTimeout   = 10 * time.Second // 鉴权前最大等待
	idleTimeout   = 75 * time.Second // 无帧判定断线（≈2.5 个心跳周期）
	slowKickAfter = 5 * time.Second  // 发送缓冲持续打满判定慢连接
	presenceTTL   = 90 * time.Second
	wsReadLimit   = 128 * 1024 // 防 abusing 大包
)

// 连接级错误。
var (
	// ErrSlowConsumer 表示发送缓冲已满（慢连接），推送方应放弃本帧。
	ErrSlowConsumer = errors.New("comet: slow consumer")
	// ErrConnClosed 表示连接已关闭。
	ErrConnClosed = errors.New("comet: connection closed")
)

// Conn 是一条已升级的 WebSocket 连接。
// 身份字段在 AUTH 成功后写入，authed 原子标志提供 happens-before 保障。
type Conn struct {
	ws     *websocket.Conn
	server *Server

	uid        int64  // atomic；AUTH 后有效
	deviceID   string // AUTH 后有效
	platform   int32  // AUTH 后有效
	authed     atomic.Bool
	send       chan []byte
	lastActive int64 // atomic unix 秒
	fullSince  int64 // atomic 毫秒；发送缓冲连续打满的起点（0=未满）

	closed    chan struct{}
	closeOnce sync.Once
}

// newConn 构造连接对象（不启动循环）。ws 可为 nil（单测场景）。
func newConn(s *Server, ws *websocket.Conn) *Conn {
	if ws != nil {
		ws.SetReadLimit(wsReadLimit)
	}
	c := &Conn{
		ws:         ws,
		server:     s,
		send:       make(chan []byte, sendChanSize),
		lastActive: s.nowSec(),
		closed:     make(chan struct{}),
	}
	return c
}

// DeviceKey 返回 bucket 中的连接键：uid:device_id。
func DeviceKey(uid int64, deviceID string) string {
	return strconv.FormatInt(uid, 10) + ":" + deviceID
}

// UID 返回连接归属 uid（未鉴权为 0）。
func (c *Conn) UID() int64 { return atomic.LoadInt64(&c.uid) }

// Authed 返回是否已通过鉴权。
func (c *Conn) Authed() bool { return c.authed.Load() }

// Touch 刷新活跃时间。
func (c *Conn) Touch() { atomic.StoreInt64(&c.lastActive, c.server.nowSec()) }

// idleSeconds 返回距上次活跃的秒数。
func (c *Conn) idleSeconds(now int64) int64 {
	return now - atomic.LoadInt64(&c.lastActive)
}

// setIdentity 绑定身份并注册到 bucket（由读循环在 AUTH 成功后调用）。
func (c *Conn) setIdentity(uid int64, deviceID string, platform int32) {
	atomic.StoreInt64(&c.uid, uid)
	c.deviceID = deviceID
	c.platform = platform
}

// markAuthed 必须在身份字段写完后调用（原子发布）。
func (c *Conn) markAuthed() { c.authed.Store(true) }

// Push 把一帧放入发送缓冲；缓冲满时非阻塞返回 ErrSlowConsumer。
func (c *Conn) Push(frame []byte) error {
	select {
	case <-c.closed:
		return ErrConnClosed
	default:
	}
	select {
	case c.send <- frame:
		return nil
	default:
		return ErrSlowConsumer
	}
}

// checkSlow 由写循环每秒调用：缓冲持续打满超过 slowKickAfter 判定慢连接。
func (c *Conn) checkSlow(nowMs int64) bool {
	if len(c.send) < cap(c.send) {
		atomic.StoreInt64(&c.fullSince, 0)
		return false
	}
	fs := atomic.LoadInt64(&c.fullSince)
	if fs == 0 {
		atomic.StoreInt64(&c.fullSince, nowMs)
		return false
	}
	return nowMs-fs > slowKickAfter.Milliseconds()
}

// Closed 返回连接关闭信号（用于读写循环退出）。
func (c *Conn) Closed() <-chan struct{} { return c.closed }

// Close 幂等关闭连接并执行清理：从 bucket 摘除、HDEL 路由表、
// 无剩余端时 DEL presence、关闭 ws 与 closed 信号。
// send 通道不关闭——并发 Push 关闭中的通道会 panic，
// 写循环经 closed 信号退出后缓冲由 GC 回收。
func (c *Conn) Close() {
	c.closeOnce.Do(func() {
		s := c.server
		if c.authed.Load() {
			s.removeConn(c)
		}
		close(c.closed)
		if c.ws != nil {
			_ = c.ws.Close()
		}
	})
}
