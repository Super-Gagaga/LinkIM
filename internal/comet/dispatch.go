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
// MSG_RECEIVED_ACK → gRPC logic.MarkRead（推进已读游标）+ ReportDelivered（观测）；
// SYNC_PULL → gRPC logic.SyncPull 回 SYNC_RESP（帧头 Seq 回带）。
func NewLogicDispatcher(logic pb.LogicClient, logger *zap.Logger) HandlerFunc {
	return func(s *Server, c *Conn, frame protocol.Frame) {
		switch frame.Cmd {
		case protocol.CmdMsgSend:
			handleMsgSend(logic, logger, c, frame)
		case protocol.CmdMsgReceivedAck:
			handleMsgReceivedAck(logic, logger, c, frame)
		case protocol.CmdSyncPull:
			handleSyncPull(logic, logger, c, frame)
		}
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

// handleMsgReceivedAck 处理 MSG_RECEIVED_ACK（设计文档 10.2：追平后更新 read_seq）：
// 上报 logic.MarkRead 推进游标，同时 ReportDelivered 记观测键。
func handleMsgReceivedAck(logic pb.LogicClient, logger *zap.Logger, c *Conn, frame protocol.Frame) {
	var req pb.ReceivedAckReq
	if err := proto.Unmarshal(frame.Body, &req); err != nil {
		logger.Warn("msg_received_ack frame decode failed",
			zap.Int64("uid", c.UID()), zap.Error(err))
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), dispatchTimeout)
	defer cancel()
	if _, err := logic.MarkRead(ctx, &pb.MarkReadReq{
		Uid: c.UID(), ConvId: req.GetConvId(), Seq: req.GetSeq(),
	}); err != nil {
		logger.Warn("mark read rpc failed",
			zap.Int64("uid", c.UID()), zap.String("conv", req.GetConvId()), zap.Error(err))
	}
	if _, err := logic.ReportDelivered(ctx, &pb.ReportDeliveredReq{
		Uid: c.UID(), MsgId: req.GetMsgId(), ConvId: req.GetConvId(),
	}); err != nil {
		// 观测链路失败不影响客户端。
		logger.Warn("report delivered rpc failed",
			zap.Int64("uid", c.UID()), zap.String("msg_id", req.GetMsgId()), zap.Error(err))
	}
}

// handleSyncPull 处理 SYNC_PULL：透传 logic（uid 以连接身份回填），
// PbMsg 列表转客户端协议 MsgPush 后回 SYNC_RESP 帧（帧头 Seq 回带）。
func handleSyncPull(logic pb.LogicClient, logger *zap.Logger, c *Conn, frame protocol.Frame) {
	var req pb.SyncPullReq
	if err := proto.Unmarshal(frame.Body, &req); err != nil {
		logger.Warn("sync_pull frame decode failed", zap.Int64("uid", c.UID()), zap.Error(err))
		c.replyAck(protocol.CmdSyncResp, frame.Seq, mustMarshal(&pb.SyncResp{Code: CodeAuthFailed}))
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), dispatchTimeout)
	defer cancel()
	resp, err := logic.SyncPull(ctx, &pb.SyncPullReq{
		Uid:         c.UID(),
		ConvId:      req.GetConvId(),
		LocalMaxSeq: req.GetLocalMaxSeq(),
		Limit:       req.GetLimit(),
	})
	if err != nil {
		logger.Warn("logic syncpull rpc failed", zap.Int64("uid", c.UID()), zap.Error(err))
		c.replyAck(protocol.CmdSyncResp, frame.Seq,
			mustMarshal(&pb.SyncResp{Code: CodeLogicUnavailable}))
		return
	}

	// PbMsg（跨服务结构）→ MsgPush（客户端帧体）。
	msgs := make([]*pb.MsgPush, 0, len(resp.GetMessages()))
	for _, m := range resp.GetMessages() {
		msgs = append(msgs, &pb.MsgPush{
			MsgId: m.GetMsgId(), ConvId: m.GetConvId(), ConvType: m.GetConvType(),
			SenderId: m.GetSenderId(), MsgType: m.GetMsgType(), Payload: m.GetPayload(),
			Seq: m.GetSeq(), Timestamp: m.GetTimestamp(),
		})
	}
	c.replyAck(protocol.CmdSyncResp, frame.Seq, mustMarshal(&pb.SyncResp{
		ConvId: req.GetConvId(), Messages: msgs, MaxSeq: resp.GetMaxSeq(), Code: resp.GetCode(),
	}))
}

// NotifyOnline 上线补拉（设计文档 10.2）：AUTH 成功后调 logic.OnlineEvent，
// 把返回的未读会话列表组装成 SYNC_NOTIFY 帧推给刚上线的连接。
// 异步执行，不阻塞 AUTH_ACK。
func NotifyOnline(logic pb.LogicClient, logger *zap.Logger, c *Conn, platform int32) {
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), dispatchTimeout)
		defer cancel()
		resp, err := logic.OnlineEvent(ctx, &pb.OnlineEventReq{
			Uid: c.UID(), DeviceId: c.deviceID, Platform: platform,
			CometAddr: c.server.advertiseAddr, Online: true,
		})
		if err != nil {
			logger.Warn("online event rpc failed", zap.Int64("uid", c.UID()), zap.Error(err))
			return
		}
		if len(resp.GetConvs()) == 0 {
			return
		}
		notify := &pb.SyncNotifyReq{Convs: resp.GetConvs()}
		frame, err := protocol.Encode(protocol.Frame{
			Ver: protocol.Ver, Cmd: protocol.CmdSyncNotify, Body: mustMarshal(notify),
		})
		if err != nil {
			logger.Error("encode sync notify failed", zap.Error(err))
			return
		}
		if err := c.Push(frame); err != nil {
			logger.Warn("push sync notify failed", zap.Int64("uid", c.UID()), zap.Error(err))
			return
		}
		logger.Info("sync notify sent",
			zap.Int64("uid", c.UID()), zap.Int("convs", len(resp.GetConvs())))
	}()
}
