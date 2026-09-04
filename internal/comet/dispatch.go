package comet

import (
	"context"
	"time"

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/linkim/linkim/pkg/pb"
	"github.com/linkim/linkim/pkg/protocol"
)

// dispatchTimeout 是上行业务帧调用 logic 的 gRPC 超时（S6：3s）。
const dispatchTimeout = 3 * time.Second

// CodeLogicUnavailable 表示调用 logic 失败（gRPC 错误统一转业务码）。
const CodeLogicUnavailable = 50101

// NewLogicDispatcher 返回业务帧分发器：
// MSG_SEND → gRPC logic.SendMsg 回 MSG_SEND_ACK；
// MSG_RECEIVED_ACK → gRPC logic.ReportDelivered（观测，无回应帧）；
// SYNC_PULL 由 S8 接入，保持原“未实现”行为。
func NewLogicDispatcher(logic pb.LogicClient, logger *zap.Logger) HandlerFunc {
	return func(s *Server, c *Conn, frame protocol.Frame) {
		switch frame.Cmd {
		case protocol.CmdMsgSend:
			handleMsgSend(logic, logger, c, frame)
		case protocol.CmdMsgReceivedAck:
			handleMsgReceivedAck(logic, logger, c, frame)
		case protocol.CmdSyncPull:
			// 留待 S8 接入。
			s.replyNotImplemented(c, frame)
		}
	}
}

// handleMsgReceivedAck 处理 MSG_RECEIVED_ACK：上报 logic 落观测键。
func handleMsgReceivedAck(logic pb.LogicClient, logger *zap.Logger, c *Conn, frame protocol.Frame) {
	var req pb.ReceivedAckReq
	if err := proto.Unmarshal(frame.Body, &req); err != nil {
		logger.Warn("msg_received_ack frame decode failed",
			zap.Int64("uid", c.UID()), zap.Error(err))
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), dispatchTimeout)
	defer cancel()
	if _, err := logic.ReportDelivered(ctx, &pb.ReportDeliveredReq{
		Uid: c.UID(), MsgId: req.GetMsgId(), ConvId: req.GetConvId(),
	}); err != nil {
		// 观测链路失败不回执给客户端、不断连。
		logger.Warn("report delivered rpc failed",
			zap.Int64("uid", c.UID()), zap.String("msg_id", req.GetMsgId()), zap.Error(err))
	}
}

// handleMsgSend 处理 MSG_SEND：透传 logic，回写 MSG_SEND_ACK。
func handleMsgSend(logic pb.LogicClient, logger *zap.Logger, c *Conn, frame protocol.Frame) {
	var req pb.MsgSendReq
	if err := proto.Unmarshal(frame.Body, &req); err != nil {
		logger.Warn("msg_send frame decode failed", zap.Int64("uid", c.UID()), zap.Error(err))
		c.replyAck(protocol.CmdMsgSendAck, frame.Seq,
			mustMarshal(&pb.MsgSendAck{Code: CodeAuthFailed, ClientMsgId: req.GetClientMsgId()}))
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), dispatchTimeout)
	defer cancel()
	ack, err := logic.SendMsg(ctx, &pb.SendMsgReq{
		SenderId:    c.UID(), // 身份以连接绑定为准，不信任客户端
		ConvId:      req.GetConvId(),
		ConvType:    req.GetConvType(),
		ClientMsgId: req.GetClientMsgId(),
		DeviceId:    c.deviceID,
		MsgType:     req.GetMsgType(),
		Payload:     req.GetPayload(),
	})
	if err != nil {
		// gRPC 层错误（超时/不可达）转业务码，不向客户端泄漏内部错误。
		logger.Warn("logic sendmsg rpc failed",
			zap.Int64("uid", c.UID()), zap.Error(err))
		ack = &pb.SendMsgAck{Code: CodeLogicUnavailable}
	}
	// logic 的 SendMsgAck 转客户端协议的 MsgSendAck；帧头 Seq 与请求帧相同。
	c.replyAck(protocol.CmdMsgSendAck, frame.Seq, mustMarshal(&pb.MsgSendAck{
		Code:        ack.GetCode(),
		ClientMsgId: req.GetClientMsgId(),
		MsgId:       ack.GetMsgId(),
		Seq:         ack.GetSeq(),
		Timestamp:   ack.GetTimestamp(),
	}))
}
