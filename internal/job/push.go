// Package job 实现 Kafka 消费者：job-push（在线投递给 comet）与
// job-store（异步批量落库 MySQL）（设计文档 5.1、6.1 ④⑤、8 节）。
package job

import (
	"context"
	"fmt"
	"sync"

	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"

	"github.com/linkim/linkim/internal/service"
	"github.com/linkim/linkim/pkg/pb"
	"github.com/linkim/linkim/pkg/protocol"
	"github.com/linkim/linkim/pkg/redisx"
)

// PushWorker 消费 msg.push 并投递给接收者所在的 comet（设计文档 5.1 ⑤环节）。
type PushWorker struct {
	rdb    *redis.Client
	pool   *CometPool
	logger *zap.Logger
}

// NewPushWorker 构造 push 消费者。
func NewPushWorker(rdb *redis.Client, pool *CometPool, logger *zap.Logger) *PushWorker {
	return &PushWorker{rdb: rdb, pool: pool, logger: logger}
}

// CometPool 按 comet 地址缓存 gRPC 连接（设计文档 7.3：客户端按需缓存连接）。
type CometPool struct {
	mu    sync.Mutex
	conns map[string]*grpc.ClientConn
}

// NewCometPool 构造连接池。
func NewCometPool() *CometPool { return &CometPool{conns: map[string]*grpc.ClientConn{}} }

// Get 返回目标 comet 的 gRPC 客户端（连接按地址复用）。
func (p *CometPool) Get(addr string) (pb.CometClient, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if c, ok := p.conns[addr]; ok {
		return pb.NewCometClient(c), nil
	}
	c, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("job: dial comet %s: %w", addr, err)
	}
	p.conns[addr] = c
	return pb.NewCometClient(c), nil
}

// Close 关闭池内全部连接。
func (p *CometPool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for addr, c := range p.conns {
		if err := c.Close(); err != nil {
			_ = addr
		}
	}
	p.conns = map[string]*grpc.ClientConn{}
}

// Handle 处理一条 msg.push 消息：解析 Envelope → 查路由 → 按存活 comet 分组投递。
// 返回 nil 表示该消息已处置完毕（可提交 offset）；
// 投递失败也返回 nil——消息仍会落库，靠 S8 拉取兜底（设计文档 6.1 ⑤）。
func (w *PushWorker) Handle(ctx context.Context, km kafka.Message) error {
	// S9 起 msg.push 统一携带 Envelope{recv_uid, msg}（单聊/群聊同构）。
	var env pb.Envelope
	if err := proto.Unmarshal(km.Value, &env); err != nil || env.GetMsg() == nil || env.GetRecvUid() <= 0 {
		// 毒消息或旧格式：跳过并提交，避免阻塞分区。
		w.logger.Warn("msg.push payload not a valid Envelope, skip",
			zap.Int("partition", km.Partition), zap.Int64("offset", km.Offset), zap.Error(err))
		return nil
	}
	recv := env.GetRecvUid()
	msg := env.GetMsg()

	route, err := w.rdb.HGetAll(ctx, redisx.RouteKey(recv)).Result()
	if err != nil {
		// 路由表为缓存性质，读失败按离线处理（设计文档 7.2）。
		w.logger.Warn("route read failed, treat as offline",
			zap.Int64("uid", recv), zap.Error(err))
		return nil
	}
	if len(route) == 0 {
		pushTotal.WithLabelValues("offline").Inc()
		w.logger.Info("receiver offline, rely on sync pull",
			zap.Int64("uid", recv), zap.String("msg_id", msg.GetMsgId()))
		return nil
	}

	frame, err := BuildPushFrame(msg)
	if err != nil {
		w.logger.Error("build push frame failed", zap.Error(err))
		return nil
	}

	// field = platform:device_id（S5 约定），value = comet 地址。
	for field, addr := range route {
		device := deviceOfField(field)
		if exists, err := w.rdb.Exists(ctx, redisx.CometAliveKey(addr)).Result(); err != nil || exists == 0 {
			pushTotal.WithLabelValues("comet_not_alive").Inc()
			w.logger.Warn("target comet not alive, skip group",
				zap.String("addr", addr), zap.Error(err))
			continue
		}
		w.pushToOne(ctx, addr, recv, device, frame, msg)
	}
	return nil
}

// pushToOne 向单个 (comet, device) 投递一帧；传输错误重试 1 次后放弃。
func (w *PushWorker) pushToOne(ctx context.Context, addr string, uid int64, device string, frame []byte, msg *pb.PbMsg) {
	client, err := w.pool.Get(addr)
	if err != nil {
		w.logger.Error("get comet client failed", zap.String("addr", addr), zap.Error(err))
		return
	}
	req := &pb.PushFramesReq{Uid: uid, DeviceId: device, Frames: [][]byte{frame}}
	for attempt := 1; attempt <= 2; attempt++ {
		resp, err := client.PushFrames(ctx, req)
		if err == nil {
			pushTotal.WithLabelValues("ok").Inc()
			if !resp.GetOnline() {
				// 连接刚断开：不重试（设计文档 6.1 ⑤）。
				pushTotal.WithLabelValues("not_online").Inc()
				w.logger.Info("target not online on comet, skip",
					zap.String("addr", addr), zap.Int64("uid", uid), zap.String("device", device))
			}
			return
		}
		if attempt == 1 {
			w.logger.Warn("push rpc failed, retry once",
				zap.String("addr", addr), zap.Error(err))
			continue
		}
		pushTotal.WithLabelValues("failed").Inc()
		w.logger.Error("push failed after retry, rely on sync pull",
			zap.String("addr", addr), zap.String("msg_id", msg.GetMsgId()), zap.Error(err))
	}
}

// ReceiverOf 解析单聊会话的接收者：会话双方中非 sender 的一端。
func ReceiverOf(msg *pb.PbMsg) (int64, error) {
	a, b, err := service.ParseP2PConv(msg.GetConvId())
	if err != nil {
		return 0, err
	}
	switch msg.GetSenderId() {
	case a:
		return b, nil
	case b:
		return a, nil
	default:
		return 0, fmt.Errorf("job: sender %d not in conv %s", msg.GetSenderId(), msg.GetConvId())
	}
}

// BuildPushFrame 构造下行帧：Frame{Ver:1, Cmd:MsgPush, Seq:0, Body=PbMsg}。
func BuildPushFrame(msg *pb.PbMsg) ([]byte, error) {
	body, err := proto.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("job: marshal PbMsg: %w", err)
	}
	return protocol.Encode(protocol.Frame{Ver: protocol.Ver, Cmd: protocol.CmdMsgPush, Seq: 0, Body: body})
}

// deviceOfField 从路由表 field（platform:device_id）提取 device_id。
func deviceOfField(field string) string {
	for i := 0; i < len(field); i++ {
		if field[i] == ':' {
			return field[i+1:]
		}
	}
	return field
}
