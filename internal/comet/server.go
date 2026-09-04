package comet

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/linkim/linkim/pkg/pb"
	"github.com/linkim/linkim/pkg/protocol"
	"github.com/linkim/linkim/pkg/redisx"
)

// HandlerFunc 是业务帧分发钩子：S6/S7/S8 注入 MSG_SEND 等真实处理逻辑，
// 本步未注入时回“未实现”业务码。
type HandlerFunc func(s *Server, c *Conn, frame protocol.Frame)

// Server 是 comet 接入层核心。
type Server struct {
	advertiseAddr string // 写入路由表的本机 gRPC 地址
	rdb           *redis.Client
	logic         pb.LogicClient
	logicTimeout  time.Duration
	logger        *zap.Logger
	dispatch      HandlerFunc

	buckets [bucketNum]*Bucket

	cometMu    sync.Mutex
	cometConns map[string]*grpc.ClientConn // 跨机 Kick 的目标 comet 连接缓存

	// nowSec/nowMs 注入时钟，测试用假时钟替换。
	nowSec func() int64
	nowMs  func() int64

	upgrader websocket.Upgrader
}

// NewServer 构造 comet 服务核心。dispatch 可为 nil（未实现的命令回业务码）。
func NewServer(advertiseAddr string, rdb *redis.Client, logic pb.LogicClient,
	logicTimeout time.Duration, logger *zap.Logger, dispatch HandlerFunc) *Server {
	if logicTimeout <= 0 {
		logicTimeout = 2 * time.Second
	}
	s := &Server{
		advertiseAddr: advertiseAddr,
		rdb:           rdb,
		logic:         logic,
		logicTimeout:  logicTimeout,
		logger:        logger,
		dispatch:      dispatch,
		cometConns:    map[string]*grpc.ClientConn{},
		nowSec:        func() int64 { return time.Now().Unix() },
		nowMs:         func() int64 { return time.Now().UnixMilli() },
		upgrader: websocket.Upgrader{
			ReadBufferSize:  4096,
			WriteBufferSize: 8192,
			CheckOrigin:     func(*http.Request) bool { return true }, // 开发环境放开；生产按域名白名单
		},
	}
	for i := range s.buckets {
		s.buckets[i] = newBucket()
	}
	return s
}

// bucket 取 uid 对应分片。
func (s *Server) bucket(uid int64) *Bucket { return s.buckets[bucketIndex(uid)] }

// SetDispatch 注入业务帧处理器（S6+）。
func (s *Server) SetDispatch(h HandlerFunc) { s.dispatch = h }

// StartBackground 启动每 bucket 的超时扫描协程；返回停止函数。
func (s *Server) StartBackground() (stop func()) {
	done := make(chan struct{})
	for i := range s.buckets {
		b := s.buckets[i]
		go func() {
			ticker := time.NewTicker(time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-done:
					return
				case <-ticker.C:
					onlineGauge.Set(float64(s.OnlineCount()))
					for _, c := range b.idleConns(s.nowSec(), int64(idleTimeout.Seconds())) {
						s.logger.Info("kick idle connection",
							zap.Int64("uid", c.UID()), zap.String("device", c.deviceID))
						c.Close()
					}
				}
			}
		}()
	}
	var once sync.Once
	return func() { once.Do(func() { close(done) }) }
}

// ServeWS 处理一次 WebSocket 升级并托管连接生命周期。
func (s *Server) ServeWS(w http.ResponseWriter, r *http.Request) {
	ws, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		s.logger.Warn("ws upgrade failed", zap.Error(err))
		return
	}
	c := newConn(s, ws)
	go c.writeLoop()
	go c.readLoop()

	// 鉴权前 watchdog：10s 内未通过 AUTH 直接关闭。
	time.AfterFunc(authTimeout, func() {
		if !c.authed.Load() {
			s.logger.Info("kick unauthenticated connection", zap.String("remote", ws.RemoteAddr().String()))
			c.Close()
		}
	})
}

// removeConn 断连清理：bucket 摘除 + 路由表清理（Close 内调用）。
func (s *Server) removeConn(c *Conn) {
	key := DeviceKey(c.UID(), c.deviceID)
	s.bucket(c.UID()).del(key)

	if s.rdb == nil { // 测试环境无 redis
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), s.logicTimeout)
	defer cancel()
	routeKey := redisx.RouteKey(c.UID())
	field := routeField(c.platform, c.deviceID)
	if err := s.rdb.HDel(ctx, routeKey, field).Err(); err != nil {
		s.logger.Warn("hdel route failed", zap.Int64("uid", c.UID()), zap.Error(err))
	}
	// 该 uid 无剩余端时清理 presence。
	remain, err := s.rdb.HLen(ctx, routeKey).Result()
	if err == nil && remain == 0 {
		if err := s.rdb.Del(ctx, redisx.PresenceKey(c.UID())).Err(); err != nil {
			s.logger.Warn("del presence failed", zap.Int64("uid", c.UID()), zap.Error(err))
		}
	}
}

// --- 读循环 ---

// readLoop 串行读取并分发帧；退出即关闭连接。
func (c *Conn) readLoop() {
	s := c.server
	sr := protocol.NewStreamReader(&wsMessageReader{ws: c.ws})
	for {
		frame, err := sr.ReadFrame()
		if err != nil {
			s.logger.Debug("read loop exit", zap.Int64("uid", c.UID()), zap.Error(err))
			c.Close()
			return
		}
		c.Touch()
		frameReceived(protocol.CmdString(frame.Cmd))

		if !c.authed.Load() {
			if frame.Cmd != protocol.CmdAuth {
				s.logger.Warn("frame before auth, closing", zap.Uint16("cmd", uint16(frame.Cmd)))
				c.Close()
				return
			}
			s.handleAuth(c, frame)
			continue
		}

		switch frame.Cmd {
		case protocol.CmdHeartbeat:
			c.replyAck(protocol.CmdHeartbeatAck, frame.Seq, nil)
			s.renewPresenceAsync(c.UID())
		case protocol.CmdAuth:
			// 重复 AUTH：忽略（可能是重放），不重复登记路由。
			s.logger.Warn("duplicate auth ignored", zap.Int64("uid", c.UID()))
		case protocol.CmdMsgSend, protocol.CmdMsgReceivedAck, protocol.CmdSyncPull:
			if s.dispatch != nil {
				s.dispatch(s, c, frame)
			} else {
				s.replyNotImplemented(c, frame)
			}
		default:
			s.logger.Warn("unknown or unsupported cmd", zap.Uint16("cmd", uint16(frame.Cmd)))
		}
	}
}

// handleAuth 处理 AUTH 帧：gRPC 校验 → 互踢 → 登记 → 回 ACK。
func (s *Server) handleAuth(c *Conn, frame protocol.Frame) {
	var req pb.AuthReq
	if err := proto.Unmarshal(frame.Body, &req); err != nil {
		s.logger.Warn("auth frame decode failed", zap.Error(err))
		c.replyAck(protocol.CmdAuthAck, frame.Seq, mustMarshal(&pb.AuthAck{Code: CodeAuthFailed, Msg: "auth frame malformed"}))
		c.Close()
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), s.logicTimeout)
	resp, err := s.logic.VerifyToken(ctx, &pb.VerifyTokenReq{Uid: req.GetUid(), Token: req.GetToken()})
	cancel()
	if err != nil || !resp.GetValid() {
		code := int32(CodeAuthFailed)
		if err == nil {
			code = resp.GetCode()
		}
		authTotal.WithLabelValues("fail").Inc()
		s.logger.Info("auth failed", zap.String("device", req.GetDeviceId()), zap.Error(err))
		c.replyAck(protocol.CmdAuthAck, frame.Seq, mustMarshal(&pb.AuthAck{Code: code, Msg: "verify failed"}))
		c.Close()
		return
	}

	authTotal.WithLabelValues("success").Inc()
	uid := resp.GetUid()
	c.setIdentity(uid, req.GetDeviceId(), req.GetPlatform())

	// 同 platform 互踢：先于本连接登记，避免误踢自己。
	if kicked := s.kickSamePlatform(uid, req.GetPlatform(), req.GetDeviceId()); kicked > 0 {
		s.logger.Info("kicked same-platform connections", zap.Int64("uid", uid), zap.Int("count", kicked))
	}

	// 同设备重连：顶替旧连接。
	if old := s.bucket(uid).put(DeviceKey(uid, c.deviceID), c); old != nil && old != c {
		old.Close()
	}

	if s.rdb != nil {
		rctx, rcancel := context.WithTimeout(context.Background(), s.logicTimeout)
		defer rcancel()
		pipe := s.rdb.Pipeline()
		pipe.HSet(rctx, redisx.RouteKey(uid), routeField(c.platform, c.deviceID), s.advertiseAddr)
		pipe.Set(rctx, redisx.PresenceKey(uid), "online", presenceTTL)
		if _, err := pipe.Exec(rctx); err != nil {
			s.logger.Warn("register route failed", zap.Int64("uid", uid), zap.Error(err))
		}
	}

	c.markAuthed()
	c.replyAck(protocol.CmdAuthAck, frame.Seq, mustMarshal(&pb.AuthAck{Code: 0, Msg: "ok", Uid: uid}))
	s.logger.Info("conn authenticated",
		zap.Int64("uid", uid), zap.String("device", c.deviceID), zap.Int32("platform", c.platform))

	// 上线补拉（S8）：异步下发未读会话 SYNC_NOTIFY，不阻塞 AUTH_ACK。
	if s.logic != nil {
		NotifyOnline(s.logic, s.logger, c, c.platform)
	}
}

// kickSamePlatform 踢掉同 uid 同 platform 的其他设备连接。
// 返回踢出的连接数。跨机连接通过 gRPC Kick 目标 comet。
func (s *Server) kickSamePlatform(uid int64, platform int32, curDevice string) int {
	if s.rdb == nil {
		return 0
	}
	ctx, cancel := context.WithTimeout(context.Background(), s.logicTimeout)
	defer cancel()
	route, err := s.rdb.HGetAll(ctx, redisx.RouteKey(uid)).Result()
	if err != nil {
		s.logger.Warn("hgetall route failed for kick", zap.Int64("uid", uid), zap.Error(err))
		return 0
	}

	want := fmt.Sprintf("%d:", platform)
	kicked := 0
	for field, addr := range route {
		if len(field) <= len(want) || field[:len(want)] != want {
			continue
		}
		device := field[len(want):]
		if device == curDevice {
			continue // 同设备重连由 bucket 顶替逻辑处理
		}
		if addr == s.advertiseAddr {
			if old := s.bucket(uid).get(DeviceKey(uid, device)); old != nil {
				s.KickConn(old, "same-platform login")
				kicked++
			}
			continue
		}
		// 跨机：gRPC 调目标 comet；MVP 阶段失败仅告警（旧连接靠超时兜底）。
		if err := s.kickRemote(ctx, addr, uid, device, "same-platform login"); err != nil {
			s.logger.Warn("remote kick failed",
				zap.String("addr", addr), zap.Int64("uid", uid), zap.Error(err))
		} else {
			kicked++
		}
	}
	return kicked
}

// KickConn 向本机连接写 KICK 帧并延迟关闭（留出刷出时间）。
func (s *Server) KickConn(c *Conn, reason string) {
	body := mustMarshal(&pb.AuthAck{Code: CodeKicked, Msg: reason, KickReason: 1})
	if frame, err := protocol.Encode(protocol.Frame{Ver: protocol.Ver, Cmd: protocol.CmdAuthAck, Body: body}); err == nil {
		_ = c.Push(frame)
	}
	time.AfterFunc(200*time.Millisecond, c.Close)
}

// --- 写循环 ---

// writeLoop 串行化写出；慢连接（缓冲持续打满 5s）强制断开。
func (c *Conn) writeLoop() {
	s := c.server
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case frame := <-c.send:
			if err := c.ws.WriteMessage(websocket.BinaryMessage, frame); err != nil {
				s.logger.Debug("write loop exit", zap.Int64("uid", c.UID()), zap.Error(err))
				c.Close()
				return
			}
		case <-ticker.C:
			if c.checkSlow(s.nowMs()) {
				slowKickTotal.Inc()
				s.logger.Warn("kick slow consumer", zap.Int64("uid", c.UID()), zap.String("device", c.deviceID))
				c.Close()
				return
			}
		case <-c.closed:
			return
		}
	}
}

// --- 工具 ---

// replyAck 构造响应帧推入发送缓冲。
func (c *Conn) replyAck(cmd protocol.Cmd, seq uint32, body []byte) {
	frame, err := protocol.Encode(protocol.Frame{Ver: protocol.Ver, Cmd: cmd, Seq: seq, Body: body})
	if err != nil {
		return
	}
	frameSent(protocol.CmdString(cmd))
	_ = c.Push(frame)
}

// replyNotImplemented 对未接入的命令回业务码（S6/S7/S8 替换）。
func (s *Server) replyNotImplemented(c *Conn, frame protocol.Frame) {
	switch frame.Cmd {
	case protocol.CmdMsgSend:
		var req pb.MsgSendReq
		_ = proto.Unmarshal(frame.Body, &req)
		c.replyAck(protocol.CmdMsgSendAck, frame.Seq,
			mustMarshal(&pb.MsgSendAck{Code: CodeNotImplemented, ClientMsgId: req.GetClientMsgId()}))
	case protocol.CmdSyncPull:
		c.replyAck(protocol.CmdSyncResp, frame.Seq, mustMarshal(&pb.SyncResp{Code: CodeNotImplemented}))
	case protocol.CmdMsgReceivedAck:
		// 回执类无需回应，仅记录。
		s.logger.Debug("msg received ack dropped (not implemented)", zap.Int64("uid", c.UID()))
	}
}

// renewPresenceAsync 异步续期 presence（心跳路径不阻塞读循环）。
func (s *Server) renewPresenceAsync(uid int64) {
	if s.rdb == nil {
		return
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := s.rdb.Set(ctx, redisx.PresenceKey(uid), "online", presenceTTL).Err(); err != nil {
			s.logger.Warn("renew presence failed", zap.Int64("uid", uid), zap.Error(err))
		}
	}()
}

// wsMessageReader 把连续的 WebSocket 消息串接成字节流，
// 供 protocol.StreamReader 逐帧读取（帧可跨消息边界）。
type wsMessageReader struct {
	ws  *websocket.Conn
	cur io.Reader
}

// Read 实现 io.Reader。
func (r *wsMessageReader) Read(p []byte) (int, error) {
	for {
		if r.cur != nil {
			n, err := r.cur.Read(p)
			if n > 0 {
				return n, nil // EOF 等到下一次 Read 再处理
			}
			if err != nil && !errors.Is(err, io.EOF) {
				return 0, err
			}
			r.cur = nil // 当前消息耗尽，取下一条
		}
		mt, rd, err := r.ws.NextReader()
		if err != nil {
			return 0, err
		}
		if mt != websocket.BinaryMessage {
			return 0, fmt.Errorf("comet: non-binary ws message type %d", mt)
		}
		r.cur = rd
	}
}

// mustMarshal 序列化 pb 消息；编码失败（理论不可达）返回空体并交由帧层拒绝。
func mustMarshal(m proto.Message) []byte {
	b, err := proto.Marshal(m)
	if err != nil {
		return nil
	}
	return b
}

// OnlineCount 返回本机当前已鉴权在线连接数（全 bucket 聚合）。
func (s *Server) OnlineCount() int {
	n := 0
	for _, b := range s.buckets {
		n += b.size()
	}
	return n
}

// drainGrace 是 drain 广播后等待连接自行断开的最长时间（设计文档 12.2：30s）。
const drainGrace = 30 * time.Second

// Drain 优雅下线：向所有在线连接广播 RECONNECT_NOW（jitter 0~3000ms 随机），
// 等待连接断开（全部断开或超时即止），返回仍在线的连接数。
// 调用方应先摘除 comet:alive 存活标记（Alive.Stop），让 LB/路由停止导入流量。
func (s *Server) Drain() int {
	s.logger.Info("drain: broadcasting RECONNECT_NOW", zap.Int("online", s.OnlineCount()))
	for _, b := range s.buckets {
		b.mu.RLock()
		for _, c := range b.conns {
			jitter := rand.Int31n(3000)
			body := mustMarshal(&pb.ReconnectReq{JitterMs: jitter})
			if frame, err := protocol.Encode(protocol.Frame{
				Ver: protocol.Ver, Cmd: protocol.CmdReconnectNow, Body: body,
			}); err == nil {
				frameSent("RECONNECT_NOW")
				reconnectSentTotal.Inc()
				_ = c.Push(frame)
			}
		}
		b.mu.RUnlock()
	}

	deadline := time.Now().Add(drainGrace)
	for time.Now().Before(deadline) {
		if s.OnlineCount() == 0 {
			s.logger.Info("drain: all connections closed early")
			return 0
		}
		time.Sleep(500 * time.Millisecond)
	}
	remaining := s.OnlineCount()
	s.logger.Warn("drain: grace expired, connections remain", zap.Int("remaining", remaining))
	return remaining
}
