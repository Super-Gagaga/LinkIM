package logic

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/linkim/linkim/pkg/pb"
	"github.com/linkim/linkim/pkg/snowflake"
)

// Server 实现 pb.LogicServer。
// S4 实现 VerifyToken；S6 实现 SendMsg；其余 RPC 继承 Unimplemented。
type Server struct {
	pb.UnimplementedLogicServer

	cache    Cache
	verifier Verifier
	friends  FriendChecker
	seq      SeqGen
	idem     IdemStore
	ids      *snowflake.Node
	producer Producer
	logger   *zap.Logger
}

// Deps 聚合 Server 的全部依赖（接口注入，便于单测 mock）。
type Deps struct {
	Cache    Cache
	Verifier Verifier
	Friends  FriendChecker
	Seq      SeqGen
	Idem     IdemStore
	IDs      *snowflake.Node
	Producer Producer
	Logger   *zap.Logger
}

// NewServer 构造 Logic 服务实现。
func NewServer(d Deps) *Server {
	return &Server{
		cache:    d.Cache,
		verifier: d.Verifier,
		friends:  d.Friends,
		seq:      d.Seq,
		idem:     d.Idem,
		ids:      d.IDs,
		producer: d.Producer,
		logger:   d.Logger,
	}
}

// VerifyToken 实现 gRPC Logic.VerifyToken。
func (s *Server) VerifyToken(ctx context.Context, req *pb.VerifyTokenReq) (*pb.VerifyTokenResp, error) {
	if req.GetToken() == "" {
		return &pb.VerifyTokenResp{Valid: false, Code: CodeInvalidTok}, nil
	}
	uid, valid, code := s.verifyTokenFlow(ctx, req.GetUid(), req.GetToken())
	return &pb.VerifyTokenResp{Valid: valid, Code: code, Uid: uid}, nil
}

// redisCache 是 Cache 的 go-redis 实现。
type redisCache struct{ rdb *redis.Client }

// NewRedisCache 返回基于 *redis.Client 的 Cache。
func NewRedisCache(rdb *redis.Client) Cache { return &redisCache{rdb: rdb} }

// Get 实现 Cache；redis.Nil（key 不存在）返回空串。
func (c *redisCache) Get(ctx context.Context, key string) (string, error) {
	val, err := c.rdb.Get(ctx, key).Result()
	if err == redis.Nil {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	return val, nil
}

// Set 实现 Cache。
func (c *redisCache) Set(ctx context.Context, key, val string, ttl time.Duration) error {
	return c.rdb.Set(ctx, key, val, ttl).Err()
}

// UnaryInterceptor 返回 zap 访问日志 + recover 拦截器
// （打印 method/code/耗时；panic 转 Internal 且不外泄细节）。
func UnaryInterceptor(logger *zap.Logger) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (resp any, err error) {
		start := time.Now()
		defer func() {
			if r := recover(); r != nil {
				logger.Error("grpc panic",
					zap.String("method", info.FullMethod),
					zap.Any("panic", r),
					zap.Stack("stack"),
				)
				resp = nil
				err = status.Error(codes.Internal, "internal error")
			}
		}()

		resp, err = handler(ctx, req)

		log := logger.Info
		if err != nil {
			log = logger.Warn
		}
		log("grpc access",
			zap.String("method", info.FullMethod),
			zap.String("code", status.Code(err).String()),
			zap.Duration("cost", time.Since(start)),
		)
		return resp, err
	}
}
