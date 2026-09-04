package logic

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/linkim/linkim/internal/service"
	"github.com/linkim/linkim/pkg/pb"
	"github.com/linkim/linkim/pkg/protocol"
	"github.com/linkim/linkim/pkg/redisx"
)

// 业务错误码补充（沿用全局分段）。
const (
	CodeBadParam  = 40201 // 请求参数格式错误
	CodeNotFriend = 40301 // 非好友关系，拒绝发送
	CodeKafkaErr  = 50201 // Kafka 写入失败（已回滚幂等键，允许重发）
	CodeRedisErr  = 50202 // Redis 中间件错误
	CodeInFlight  = 50203 // 同一 client_msg_id 仍在处理中，稍后重试
)

// 消息链路参数（设计文档 5.1、6.2）。
const (
	idemTTL       = 10 * time.Minute // 幂等键有效期
	msgMaxPayload = protocol.MaxBodyLen
	convTypeP2P   = 1
)

// Kafka topic（设计文档 8.1）。
const (
	TopicMsgPush  = "msg.push"
	TopicMsgStore = "msg.store"
)

// FriendChecker 校验好友关系（Redis ZSet 缓存 + DB 回填的实现见 friend.go）。
type FriendChecker interface {
	// IsFriend 返回 uid 与 friendUID 是否为生效好友。
	IsFriend(ctx context.Context, uid, friendUID int64) (bool, error)
}

// SeqGen 会话内严格递增序列号（Redis INCR 实现，设计文档 9.4）。
type SeqGen interface {
	Next(ctx context.Context, convID string) (int64, error)
}

// IdemStore 发送幂等存储（SETNX/GET/SET/DEL，设计文档 6.2）。
type IdemStore interface {
	SetNX(ctx context.Context, key, val string, ttl time.Duration) (bool, error)
	Get(ctx context.Context, key string) (string, error)
	Set(ctx context.Context, key, val string, ttl time.Duration) error
	Del(ctx context.Context, key string) error
}

// Producer 抽象 Kafka 生产者（pkg/kafkax 实现）。
type Producer interface {
	Send(ctx context.Context, topic string, key, value []byte, headers ...map[string]string) error
}

// idemValue 是幂等键中存储的 ACK 回放数据。
type idemValue struct {
	MsgID string `json:"msg_id"`
	Seq   int64  `json:"seq"`
}

// SendMsg 实现 gRPC Logic.SendMsg（设计文档 5.1 上行时序）：
// 参数校验 → conv 规范化 → 幂等 → 好友校验 → seq → msgId → 双写 Kafka → ACK。
func (s *Server) SendMsg(ctx context.Context, req *pb.SendMsgReq) (*pb.SendMsgAck, error) {
	// 1. 参数校验。
	if req.GetSenderId() <= 0 || req.GetClientMsgId() == "" || req.GetMsgType() <= 0 {
		return &pb.SendMsgAck{Code: CodeBadParam}, nil
	}
	if len(req.GetPayload()) == 0 || len(req.GetPayload()) > msgMaxPayload {
		return &pb.SendMsgAck{Code: CodeBadParam}, nil
	}
	if req.GetConvType() != convTypeP2P {
		// 群聊（conv_type=2）自 S9 支持。
		return &pb.SendMsgAck{Code: CodeBadParam}, nil
	}

	// 2. conv_id 规范化：服务端重算，不信任客户端传入。
	a, b, err := service.ParseP2PConv(req.GetConvId())
	if err != nil {
		return &pb.SendMsgAck{Code: CodeBadParam}, nil
	}
	sender := req.GetSenderId()
	peer := a
	if sender == a {
		peer = b
	} else if sender != b {
		return &pb.SendMsgAck{Code: CodeBadParam}, nil // 发送者不在会话中
	}
	convID := service.ConvIDForP2P(sender, peer)
	if convID != req.GetConvId() {
		return &pb.SendMsgAck{Code: CodeBadParam}, nil // 非规范化 conv_id
	}

	// 3. 幂等：SETNX 占位，命中则回放或提示在途。
	idemKey := redisx.IdemKey(sender, req.GetClientMsgId())
	ok, err := s.idem.SetNX(ctx, idemKey, "", idemTTL)
	if err != nil {
		s.logger.Warn("idem setnx failed", zap.Error(err))
		return &pb.SendMsgAck{Code: CodeRedisErr}, nil
	}
	if !ok {
		return s.replayOrBusy(ctx, idemKey)
	}

	// 以下失败路径统一回滚幂等键，允许客户端携带同一 client_msg_id 重发。
	rollback := func() {
		if derr := s.idem.Del(ctx, idemKey); derr != nil {
			s.logger.Warn("idem rollback failed", zap.String("key", idemKey), zap.Error(derr))
		}
	}

	// 4. 好友关系校验。
	isFriend, err := s.friends.IsFriend(ctx, sender, peer)
	if err != nil {
		s.logger.Warn("friend check failed", zap.Error(err))
		rollback()
		return &pb.SendMsgAck{Code: CodeRedisErr}, nil
	}
	if !isFriend {
		rollback()
		return &pb.SendMsgAck{Code: CodeNotFriend}, nil
	}

	// 5. 会话内 seq（Redis INCR 严格递增）。
	seq, err := s.seq.Next(ctx, convID)
	if err != nil {
		s.logger.Warn("seq incr failed", zap.Error(err))
		rollback()
		return &pb.SendMsgAck{Code: CodeRedisErr}, nil
	}

	// 6. 全局唯一 msgId（雪花）+ 毫秒时间戳。
	msgID := s.ids.Next()
	ts := time.Now().UnixMilli()

	// 7. 组装 PbMsg 并双写 Kafka（key=conv_id 保序，header 带 trace-id）。
	msg := &pb.PbMsg{
		MsgId:     fmt.Sprintf("%d", msgID),
		ConvId:    convID,
		ConvType:  convTypeP2P,
		SenderId:  sender,
		MsgType:   req.GetMsgType(),
		Payload:   req.GetPayload(),
		Seq:       seq,
		Timestamp: ts,
	}
	value, err := proto.Marshal(msg)
	if err != nil {
		s.logger.Error("pbmsg marshal failed", zap.Error(err))
		rollback()
		return &pb.SendMsgAck{Code: CodeKafkaErr}, nil
	}
	headers := map[string]string{"trace-id": req.GetClientMsgId(), "conv-type": "1"}
	for _, topic := range []string{TopicMsgPush, TopicMsgStore} {
		if err := s.producer.Send(ctx, topic, []byte(convID), value, headers); err != nil {
			s.logger.Warn("kafka produce failed",
				zap.String("topic", topic), zap.Error(err))
			rollback()
			return &pb.SendMsgAck{Code: CodeKafkaErr}, nil
		}
	}

	// 8. 成功：幂等键落终值（回放数据），返回 ACK。
	finalVal, _ := json.Marshal(idemValue{MsgID: msg.MsgId, Seq: seq})
	if err := s.idem.Set(ctx, idemKey, string(finalVal), idemTTL); err != nil {
		// 仅影响重试回放，不影响本条消息正确性。
		s.logger.Warn("idem finalize failed", zap.String("key", idemKey), zap.Error(err))
	}
	s.logger.Info("msg accepted",
		zap.String("msg_id", msg.MsgId), zap.Int64("sender", sender),
		zap.Int64("peer", peer), zap.String("conv", convID), zap.Int64("seq", seq),
		zap.String("trace-id", req.GetClientMsgId()))
	return &pb.SendMsgAck{Code: 0, MsgId: msg.MsgId, Seq: seq, Timestamp: ts}, nil
}

// replayOrBusy 处理幂等键已存在的两种情况：
// 值非空 → 回放上次 ACK；值为空 → 上一次请求仍在途（或已失败待清理）。
func (s *Server) replayOrBusy(ctx context.Context, idemKey string) (*pb.SendMsgAck, error) {
	val, err := s.idem.Get(ctx, idemKey)
	if err != nil {
		s.logger.Warn("idem get failed", zap.String("key", idemKey), zap.Error(err))
		return &pb.SendMsgAck{Code: CodeRedisErr}, nil
	}
	if val == "" {
		return &pb.SendMsgAck{Code: CodeInFlight}, nil
	}
	var iv idemValue
	if err := json.Unmarshal([]byte(val), &iv); err != nil {
		s.logger.Warn("idem value corrupt", zap.String("key", idemKey), zap.Error(err))
		return &pb.SendMsgAck{Code: CodeRedisErr}, nil
	}
	s.logger.Info("msg idempotent replay",
		zap.String("key", idemKey), zap.String("msg_id", iv.MsgID), zap.Int64("seq", iv.Seq))
	return &pb.SendMsgAck{Code: 0, MsgId: iv.MsgID, Seq: iv.Seq, Timestamp: time.Now().UnixMilli()}, nil
}
