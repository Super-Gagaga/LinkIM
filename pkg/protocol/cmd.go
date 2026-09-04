package protocol

import "fmt"

// Cmd 是帧头中承载的命令字（设计文档 4.3）。
type Cmd uint16

// 命令字定义。方向说明：C→S 客户端发往服务端，S→C 服务端发往客户端。
const (
	CmdAuth           Cmd = 0x01 // C→S：登录鉴权（携带 token）
	CmdAuthAck        Cmd = 0x02 // S→C：鉴权结果
	CmdHeartbeat      Cmd = 0x03 // C→S：应用层心跳（30s）
	CmdHeartbeatAck   Cmd = 0x04 // S→C：心跳响应
	CmdMsgSend        Cmd = 0x05 // C→S：发送消息
	CmdMsgSendAck     Cmd = 0x06 // S→C：服务端受理（返回 msgId、seq）
	CmdMsgPush        Cmd = 0x07 // S→C：推送新消息
	CmdMsgReceivedAck Cmd = 0x08 // C→S：客户端已接收确认
	CmdSyncPull       Cmd = 0x09 // C→S：离线消息增量同步
	CmdSyncResp       Cmd = 0x0A // S→C：同步结果
	CmdRecall         Cmd = 0x0B // C→S：消息撤回
	CmdTyping         Cmd = 0x0C // 双向：正在输入/在线状态
	CmdReconnectNow   Cmd = 0x0D // S→C：comet drain，客户端带抖动立即重连（12.2）
	CmdSyncNotify     Cmd = 0x0E // S→C：上线未读会话通知（触发客户端 SYNC_PULL，设计文档 10.2）
)

// cmdNames 将每个命令字映射为稳定可读的名称，用于日志输出。
var cmdNames = map[Cmd]string{
	CmdAuth:           "AUTH",
	CmdAuthAck:        "AUTH_ACK",
	CmdHeartbeat:      "HEARTBEAT",
	CmdHeartbeatAck:   "HEARTBEAT_ACK",
	CmdMsgSend:        "MSG_SEND",
	CmdMsgSendAck:     "MSG_SEND_ACK",
	CmdMsgPush:        "MSG_PUSH",
	CmdMsgReceivedAck: "MSG_RECEIVED_ACK",
	CmdSyncPull:       "SYNC_PULL",
	CmdSyncResp:       "SYNC_RESP",
	CmdRecall:         "RECALL",
	CmdTyping:         "TYPING",
	CmdSyncNotify:     "SYNC_NOTIFY",
	CmdReconnectNow:   "RECONNECT_NOW",
}

// CmdString 返回命令字的可读名称；不属于协议的命令字
// 返回 "UNKNOWN_0x%04x" 形式。
func CmdString(cmd Cmd) string {
	if name, ok := cmdNames[cmd]; ok {
		return name
	}
	return fmt.Sprintf("UNKNOWN_0x%04X", uint16(cmd))
}
