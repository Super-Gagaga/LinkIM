package logic

import (
	"context"
	"time"

	"go.uber.org/zap"

	"github.com/linkim/linkim/pkg/pb"
	"github.com/linkim/linkim/pkg/redisx"
)

// deliveredTTL 已投递观测键有效期（24h，设计文档 S7：仅观测不做强一致）。
const deliveredTTL = 24 * time.Hour

// ReportDelivered 实现 gRPC Logic.ReportDelivered：
// 接收端 MSG_RECEIVED_ACK → comet 上报 → 写 Redis 观测键。
// 观测数据允许丢失：写失败仅记日志，不影响主链路。
func (s *Server) ReportDelivered(ctx context.Context, req *pb.ReportDeliveredReq) (*pb.Empty, error) {
	if req.GetUid() <= 0 || req.GetMsgId() == "" {
		s.logger.Warn("report delivered with invalid args",
			zap.Int64("uid", req.GetUid()), zap.String("msg_id", req.GetMsgId()))
		return &pb.Empty{}, nil
	}
	if err := s.cache.Set(ctx, redisx.DeliveredKey(req.GetUid(), req.GetMsgId()), "1", deliveredTTL); err != nil {
		s.logger.Warn("write delivered key failed",
			zap.Int64("uid", req.GetUid()), zap.String("msg_id", req.GetMsgId()), zap.Error(err))
	}
	return &pb.Empty{}, nil
}
