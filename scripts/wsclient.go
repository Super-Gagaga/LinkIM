// wsclient 是 LinkIM 命令行测试客户端：自动 AUTH + 周期心跳 + 打印收到的所有帧。
// 用法示例：
//
//	go run ./scripts/wsclient.go -addr ws://127.0.0.1:8081/ws \
//	  -token <access_token> -uid <uid> -device d1 -platform 1
package main

import (
	"crypto/rand"
	"encoding/hex"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/gorilla/websocket"
	"google.golang.org/protobuf/proto"

	"github.com/linkim/linkim/pkg/pb"
	"github.com/linkim/linkim/pkg/protocol"
)

var (
	addr     = flag.String("addr", "ws://127.0.0.1:8081/ws", "comet WebSocket 地址")
	token    = flag.String("token", "", "access token（必填）")
	uidFlag  = flag.Int64("uid", 0, "登录返回的 uid（必填）")
	device   = flag.String("device", "d1", "设备 ID")
	platform = flag.Int("platform", 1, "平台：1手机 2平板 3桌面 4Web")
	beat     = flag.Duration("interval", 30*time.Second, "心跳发送间隔")
	sendTxt  = flag.String("send", "", "发送一条文本消息后继续收帧（空=不发）")
	peer     = flag.Int64("peer", 0, "消息接收方 uid（与 -send 配合，必填）")
	cmid     = flag.String("cmid", "", "指定 client_msg_id（缺省随机；幂等重发实验用）")
	sendN    = flag.Int("n", 1, "配合 -send 发送的条数")
	sendGap  = flag.Duration("gap", 300*time.Millisecond, "多条发送间隔")
)

func main() {
	flag.Parse()
	if *token == "" || *uidFlag == 0 {
		log.Fatal("必须提供 -token 与 -uid")
	}

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)

	ws, _, err := websocket.DefaultDialer.Dial(*addr, nil)
	if err != nil {
		log.Fatalf("连接 %s 失败: %v", *addr, err)
	}
	defer func() { _ = ws.Close() }()
	ws.SetReadLimit(128 * 1024)

	seq := uint32(0)
	nextSeq := func() uint32 { seq++; return seq }

	// AUTH
	send(ws, protocol.CmdAuth, nextSeq(), &pb.AuthReq{
		Token: *token, DeviceId: *device, Platform: int32(*platform), Uid: *uidFlag,
	})

	// 心跳
	heartbeat := time.NewTicker(*beat)
	defer heartbeat.Stop()

	errCh := make(chan error, 1)
	go func() {
		for {
			mt, data, err := ws.ReadMessage()
			if err != nil {
				errCh <- err
				return
			}
			if mt != websocket.BinaryMessage {
				fmt.Printf("[recv] 非 binary 消息 type=%d\n", mt)
				continue
			}
			frame, err := protocol.Decode(data)
			if err != nil {
				fmt.Printf("[recv] 帧解析失败: %v\n", err)
				continue
			}
			describe(frame)
		}
	}()

	// AUTH_ACK 到达后再发消息（简单等待：固定短暂延迟）。
	if *sendTxt != "" {
		if *peer == 0 {
			log.Fatal("发送消息必须提供 -peer")
		}
		go func() {
			time.Sleep(500 * time.Millisecond) // 等待 AUTH_ACK
			conv := convP2P(*uidFlag, *peer)
			for i := 0; i < *sendN; i++ {
				id := *cmid
				if id == "" {
					id = randomID()
				}
				send(ws, protocol.CmdMsgSend, nextSeq(), &pb.MsgSendReq{
					ClientMsgId: id,
					ConvId:      conv,
					ConvType:    1,
					MsgType:     1,
					Payload:     []byte(*sendTxt),
				})
				if i < *sendN-1 {
					time.Sleep(*sendGap)
				}
			}
		}()
	}

	for {
		select {
		case <-heartbeat.C:
			send(ws, protocol.CmdHeartbeat, nextSeq(), &pb.HeartbeatReq{})
		case err := <-errCh:
			fmt.Printf("[conn] 连接关闭: %v\n", err)
			return
		case <-interrupt:
			fmt.Println("[conn] Ctrl+C，主动断开")
			_ = ws.WriteMessage(websocket.CloseMessage,
				websocket.FormatCloseMessage(websocket.CloseNormalClosure, "bye"))
			return
		}
	}
}

// convP2P 客户端侧计算单聊会话 ID（与服务端 ConvIDForP2P 规则一致）。
func convP2P(a, b int64) string {
	if a > b {
		a, b = b, a
	}
	return fmt.Sprintf("c:%d:%d", a, b)
}

// randomID 生成简易唯一 client_msg_id。
func randomID() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("cm-%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b[:])
}

// send 编码一帧并发送。
func send(ws *websocket.Conn, cmd protocol.Cmd, seq uint32, msg proto.Message) {
	body, err := proto.Marshal(msg)
	if err != nil {
		log.Fatalf("marshal %s 失败: %v", protocol.CmdString(cmd), err)
	}
	frame, err := protocol.Encode(protocol.Frame{Ver: protocol.Ver, Cmd: cmd, Seq: seq, Body: body})
	if err != nil {
		log.Fatalf("encode %s 失败: %v", protocol.CmdString(cmd), err)
	}
	if err := ws.WriteMessage(websocket.BinaryMessage, frame); err != nil {
		log.Fatalf("发送 %s 失败: %v", protocol.CmdString(cmd), err)
	}
	fmt.Printf("[sent] %s seq=%d\n", protocol.CmdString(cmd), seq)
}

// describe 可读化打印收到的帧。
func describe(f protocol.Frame) {
	name := protocol.CmdString(f.Cmd)
	switch f.Cmd {
	case protocol.CmdAuthAck:
		var m pb.AuthAck
		if proto.Unmarshal(f.Body, &m) == nil {
			fmt.Printf("[recv] %s seq=%d code=%d uid=%d kick_reason=%d msg=%q\n",
				name, f.Seq, m.GetCode(), m.GetUid(), m.GetKickReason(), m.GetMsg())
			if m.GetKickReason() != 0 || m.GetCode() == 40103 {
				fmt.Println("[kick] *** KICKED：连接将被服务端关闭 ***")
			}
		}
	case protocol.CmdHeartbeatAck:
		fmt.Printf("[recv] %s seq=%d\n", name, f.Seq)
	case protocol.CmdMsgSendAck:
		var m pb.MsgSendAck
		if proto.Unmarshal(f.Body, &m) == nil {
			fmt.Printf("[recv] %s seq=%d code=%d client_msg_id=%s msg_id=%s seq(msg)=%d ts=%d\n",
				name, f.Seq, m.GetCode(), m.GetClientMsgId(), m.GetMsgId(), m.GetSeq(), m.GetTimestamp())
		}
	case protocol.CmdMsgPush:
		var m pb.MsgPush
		if proto.Unmarshal(f.Body, &m) == nil {
			fmt.Printf("[recv] %s seq(frame)=%d conv=%s msg_seq=%d from=%d payload=%q\n",
				name, f.Seq, m.GetConvId(), m.GetSeq(), m.GetSenderId(), string(m.GetPayload()))
		}
	default:
		fmt.Printf("[recv] %s seq=%d body=%dB\n", name, f.Seq, len(f.Body))
	}
}
