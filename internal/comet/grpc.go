package comet

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/linkim/linkim/pkg/pb"
	"github.com/linkim/linkim/pkg/redisx"
)

// GRPCService 实现 pb.CometServer（Job 下行投递与踢下线）。
type GRPCService struct {
	pb.UnimplementedCometServer
	s *Server
}

// NewGRPCService 构造 comet gRPC 服务。
func NewGRPCService(s *Server) *GRPCService { return &GRPCService{s: s} }

// PushFrames 按 (uid, device_id) 定位本机连接并批量写帧。
func (g *GRPCService) PushFrames(_ context.Context, req *pb.PushFramesReq) (*pb.PushFramesResp, error) {
	c := g.s.bucket(req.GetUid()).get(DeviceKey(req.GetUid(), req.GetDeviceId()))
	if c == nil {
		return &pb.PushFramesResp{Online: false}, nil
	}
	for _, frame := range req.GetFrames() {
		if err := c.Push(frame); err != nil {
			g.s.logger.Warn("push frame failed",
				zap.Int64("uid", req.GetUid()), zap.Error(err))
			return &pb.PushFramesResp{Online: false}, nil
		}
	}
	return &pb.PushFramesResp{Online: true}, nil
}

// Kick 定位本机连接，写 KICK 帧后断开。
func (g *GRPCService) Kick(_ context.Context, req *pb.KickConnReq) (*pb.KickConnResp, error) {
	c := g.s.bucket(req.GetUid()).get(DeviceKey(req.GetUid(), req.GetDeviceId()))
	if c == nil {
		return &pb.KickConnResp{}, nil
	}
	g.s.KickConn(c, req.GetReason())
	return &pb.KickConnResp{}, nil
}

// kickRemote 跨机踢下线：按路由表中的 comet 地址 gRPC 调用目标实例。
// 连接按地址缓存（设计文档 7.3）。
func (s *Server) kickRemote(ctx context.Context, addr string, uid int64, deviceID, reason string) error {
	conn, err := s.cometConn(addr)
	if err != nil {
		return err
	}
	_, err = pb.NewCometClient(conn).Kick(ctx, &pb.KickConnReq{
		Uid: uid, DeviceId: deviceID, Reason: reason,
	})
	return err
}

// cometConn 取目标 comet 的缓存 gRPC 连接（进程内复用）。
func (s *Server) cometConn(addr string) (*grpc.ClientConn, error) {
	s.cometMu.Lock()
	defer s.cometMu.Unlock()
	if c, ok := s.cometConns[addr]; ok {
		return c, nil
	}
	c, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	s.cometConns[addr] = c
	return c, nil
}

// Alive 注册与续期（设计文档 7.2）：SET comet:alive:{addr} EX 30，
// 每 10s 续期；Stop 删除 key（SIGTERM 摘除）。
type Alive struct {
	rdb    *redis.Client
	addr   string
	logger *zap.Logger
	stop   chan struct{}
	done   chan struct{}
}

const (
	aliveTTL     = 30 * time.Second
	aliveRenewIt = 10 * time.Second
)

// NewAlive 构造存活注册器。
func NewAlive(rdb *redis.Client, addr string, logger *zap.Logger) *Alive {
	return &Alive{rdb: rdb, addr: addr, logger: logger,
		stop: make(chan struct{}), done: make(chan struct{})}
}

// Start 写入存活 key 并循环续期。
func (a *Alive) Start() error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := a.rdb.Set(ctx, redisx.CometAliveKey(a.addr), "1", aliveTTL).Err(); err != nil {
		return err
	}
	go func() {
		defer close(a.done)
		ticker := time.NewTicker(aliveRenewIt)
		defer ticker.Stop()
		for {
			select {
			case <-a.stop:
				return
			case <-ticker.C:
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				if err := a.rdb.Set(ctx, redisx.CometAliveKey(a.addr), "1", aliveTTL).Err(); err != nil {
					a.logger.Warn("renew alive failed", zap.String("addr", a.addr), zap.Error(err))
				}
				cancel()
			}
		}
	}()
	return nil
}

// Stop 删除存活 key 并停止续期（SIGTERM 摘流）。
func (a *Alive) Stop() {
	close(a.stop)
	<-a.done
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := a.rdb.Del(ctx, redisx.CometAliveKey(a.addr)).Err(); err != nil {
		a.logger.Warn("del alive failed", zap.String("addr", a.addr), zap.Error(err))
	}
}
