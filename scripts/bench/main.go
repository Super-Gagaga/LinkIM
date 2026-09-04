// bench 是 LinkIM 端到端压测工具：
// 批量注册/登录 → 并发连接 + AUTH → 两两配对互发（A_i ↔ A_(i+1 mod N)）→
// 统计连接成功率、ACK P99、端到端 P99、收发差值（必须为 0，否则退出码非 0）。
//
//	go run ./scripts/bench -clients 200 -duration 120s \
//	  -wsAddrs ws://127.0.0.1:18081/ws,ws://127.0.0.1:18082/ws \
//	  -accountAddr http://127.0.0.1:18080 -dsn "root:linkim123@tcp(127.0.0.1:23306)/linkim"
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/gorilla/websocket"
	"github.com/jmoiron/sqlx"
	"google.golang.org/protobuf/proto"

	"github.com/linkim/linkim/pkg/pb"
	"github.com/linkim/linkim/pkg/protocol"
)

var (
	clients    = flag.Int("clients", 100, "并发客户端数（自动两两配对互发）")
	msgEvery   = flag.Duration("msgInterval", time.Second, "每个客户端发送间隔")
	wsAddrs    = flag.String("wsAddrs", "ws://127.0.0.1:8081/ws", "逗号分隔的 comet WS 地址（轮询分摊）")
	duration   = flag.Duration("duration", 60*time.Second, "压测时长")
	accountAdr = flag.String("accountAddr", "http://127.0.0.1:8080", "account 服务地址")
	dsn        = flag.String("dsn", "root:linkim123@tcp(127.0.0.1:23306)/linkim?charset=utf8mb4&parseTime=true&loc=Local",
		"MySQL DSN（压测前批量建立两两好友关系）")
)

// 统计聚合。
type stats struct {
	mu        sync.Mutex
	ackLat    []float64 // 发送 → MSG_SEND_ACK 毫秒
	e2eLat    []float64 // 发送 → 对端 MSG_PUSH 毫秒
	recvCount int64
	lossMu    sync.Mutex
	sentByDst map[string]int64 // 目标 uid 维度发送计数
	recvByDst map[string]int64 // 收到的消息按发送者计
}

func (s *stats) addACK(ms float64) { s.mu.Lock(); s.ackLat = append(s.ackLat, ms); s.mu.Unlock() }
func (s *stats) addE2E(ms float64) { s.mu.Lock(); s.e2eLat = append(s.e2eLat, ms); s.mu.Unlock() }

func p99(xs []float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	sort.Float64s(xs)
	return xs[(len(xs)-1)*99/100]
}

type benchClient struct {
	idx     int
	uid     int64
	token   string
	peer    int64 // 配对对端（A_i ↔ A_(i+1 mod N)）
	convID  string
	ws      *websocket.Conn
	writeMu sync.Mutex
	// e2e 关联：client_msg_id → 发送时刻（由对端回包时解析 payload 关联）。
	sentAt sync.Map
}

func main() {
	flag.Parse()
	addrs := strings.Split(*wsAddrs, ",")
	for i := range addrs {
		addrs[i] = strings.TrimSpace(addrs[i])
	}
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	fmt.Printf("bench: clients=%d duration=%s addrs=%v interval=%s\n",
		*clients, *duration, addrs, *msgEvery)

	// 1. 批量注册/登录（顺便拿 uid）。
	db, err := sqlx.Connect("mysql", *dsn)
	if err != nil {
		fmt.Printf("连接 MySQL 失败（好友关系无法建立）: %v\n", err)
		osExit(1)
	}
	defer func() { _ = db.Close() }()

	bcs := make([]*benchClient, *clients)
	httpClient := &http.Client{Timeout: 5 * time.Second}
	runID := fmt.Sprintf("b%d", time.Now().UnixMilli()%1e7)
	for i := 0; i < *clients; i++ {
		uid, token, err := registerLogin(httpClient, fmt.Sprintf("%s_u%04d", runID, i))
		if err != nil {
			fmt.Printf("注册/登录 #%d 失败: %v\n", i, err)
			osExit(1)
		}
		bcs[i] = &benchClient{idx: i, uid: uid, token: token}
	}
	fmt.Printf("注册/登录完成：%d 个账号\n", *clients)

	// 2. 两两配对 + 好友关系落库（双向）。
	var friendValues []string
	var friendArgs []any
	for i := 0; i < *clients; i++ {
		a, b := bcs[i], bcs[(i+1)%*clients]
		a.peer, b.peer = b.uid, a.uid
		a.convID = convP2P(a.uid, b.uid)
		b.convID = a.convID
		friendValues = append(friendValues, "(?,?,1,'')", "(?,?,1,'')")
		friendArgs = append(friendArgs, a.uid, b.uid, b.uid, a.uid)
	}
	if _, err := db.Exec(`INSERT IGNORE INTO friend (uid, friend_uid, status, remark) VALUES `+
		strings.Join(friendValues, ","), friendArgs...); err != nil {
		fmt.Printf("写好友关系失败: %v\n", err)
		osExit(1)
	}
	fmt.Println("配对与好友关系就绪")

	// 3. 并发连接 + AUTH。
	st := &stats{sentByDst: map[string]int64{}, recvByDst: map[string]int64{}}
	var connected int64
	var wg sync.WaitGroup
	stopCh := make(chan struct{})
	var sentTotal int64

	for i := 0; i < *clients; i++ {
		bc := bcs[i]
		addr := addrs[bc.idx%len(addrs)]
		wg.Add(1)
		go func() {
			defer wg.Done()
			ws, _, err := websocket.DefaultDialer.Dial(addr, nil)
			if err != nil {
				fmt.Printf("client#%d 连接失败: %v\n", bc.idx, err)
				return
			}
			ws.SetReadLimit(128 * 1024)
			bc.ws = ws
			atomic.AddInt64(&connected, 1)

			// AUTH。
			authBody, _ := proto.Marshal(&pb.AuthReq{
				Token: bc.token, DeviceId: fmt.Sprintf("bench-%d", bc.idx),
				Platform: 4, Uid: bc.uid,
			})
			bc.writeFrame(protocol.CmdAuth, 0, authBody)

			// 读循环：ACK / MSG_PUSH / SYNC。
			go bc.readLoop(st)

			// 心跳。
			go func() {
				t := time.NewTicker(25 * time.Second)
				defer t.Stop()
				for {
					select {
					case <-stopCh:
						return
					case <-t.C:
						bc.writeFrame(protocol.CmdHeartbeat, 0, nil)
					}
				}
			}()

			// 发送循环：向 peer 周期发送。
			t := time.NewTicker(time.Duration(float64(*msgEvery) * (0.8 + 0.4*rng.Float64())))
			defer t.Stop()
			seq := uint32(0)
			for {
				select {
				case <-stopCh:
					return
				case <-t.C:
					seq++
					cmid := fmt.Sprintf("%d-%d", bc.uid, time.Now().UnixNano())
					now := time.Now()
					bc.sentAt.Store(cmid, now)
					payload, _ := json.Marshal([]string{cmid, fmt.Sprintf("%d", now.UnixMilli())})
					body, _ := proto.Marshal(&pb.MsgSendReq{
						ClientMsgId: cmid, ConvId: bc.convID, ConvType: 1,
						MsgType: 1, Payload: payload,
					})
					bc.writeFrame(protocol.CmdMsgSend, seq, body)
					atomic.AddInt64(&sentTotal, 1)
					st.lossMu.Lock()
					st.sentByDst[fmt.Sprintf("%d", bc.uid)]++
					st.lossMu.Unlock()
				}
			}
		}()
	}

	// 4. 运行指定时长。
	time.Sleep(*duration)
	close(stopCh)
	time.Sleep(3 * time.Second) // 留时间收尾在途消息

	// 5. 统计。
	for _, bc := range bcs {
		if bc.ws != nil {
			_ = bc.ws.Close()
		}
	}
	wg.Wait()

	fmt.Println("========== bench 结果 ==========")
	fmt.Printf("连接成功率:      %d/%d (%.1f%%)\n", connected, *clients,
		100*float64(connected)/float64(*clients))
	fmt.Printf("发送条数:        %d\n", atomic.LoadInt64(&sentTotal))
	fmt.Printf("接收条数(MSG_PUSH): %d\n", atomic.LoadInt64(&st.recvCount))
	fmt.Printf("ACK  P99:        %.1f ms（样本 %d）\n", p99(st.ackLat), len(st.ackLat))
	fmt.Printf("端到端 P99:      %.1f ms（样本 %d）\n", p99(st.e2eLat), len(st.e2eLat))

	// 收发对齐：每个目标会话的发送数 == 对端收到数。
	loss := 0
	st.lossMu.Lock()
	sentTotalMap := map[string]int64{}
	// sentByDst 按发送者计；接收按发送者计（recvByDst[senderUID]）。
	for sender, n := range st.sentByDst {
		sentTotalMap[sender] = n
	}
	for sender, n := range sentTotalMap {
		got := st.recvByDst[sender]
		if got != n {
			loss += int(n - got)
			fmt.Printf("  未对齐: sender=%s sent=%d recv=%d\n", sender, n, got)
		}
	}
	st.lossMu.Unlock()
	fmt.Printf("收发差值:        %d（必须为 0）\n", loss)
	if loss != 0 || int(connected) != *clients {
		fmt.Println("结果: FAIL（有丢失或连接不全）")
		osExit(1)
	}
	fmt.Println("结果: PASS（0 丢失）")
}

// readLoop 处理下行帧：ACK 计时 / MSG_PUSH 端到端计时与计数。
func (bc *benchClient) readLoop(st *stats) {
	for {
		mt, data, err := bc.ws.ReadMessage()
		if err != nil {
			return
		}
		if mt != websocket.BinaryMessage {
			continue
		}
		frame, err := protocol.Decode(data)
		if err != nil {
			continue
		}
		switch frame.Cmd {
		case protocol.CmdMsgSendAck:
			var m pb.MsgSendAck
			if proto.Unmarshal(frame.Body, &m) == nil && m.GetCode() == 0 {
				if v, ok := bc.sentAt.LoadAndDelete(m.GetClientMsgId()); ok {
					st.addACK(float64(time.Since(v.(time.Time)).Milliseconds()))
				}
			}
		case protocol.CmdMsgPush:
			var m pb.MsgPush
			if proto.Unmarshal(frame.Body, &m) != nil {
				continue
			}
			bc.countMsg(st, &m)
		case protocol.CmdSyncNotify:
			// 压测客户端简单处理：逐会话拉一次（离线补齐计入接收）。
			var n pb.SyncNotifyReq
			if proto.Unmarshal(frame.Body, &n) == nil {
				for _, cv := range n.GetConvs() {
					body, _ := proto.Marshal(&pb.SyncPullReq{ConvId: cv.GetConvId(), Limit: 100})
					bc.writeFrame(protocol.CmdSyncPull, 0, body)
				}
			}
		case protocol.CmdSyncResp:
			// SYNC_RESP 中补拉的离线消息同样计入接收（关键：补齐即无丢失）。
			var resp pb.SyncResp
			if proto.Unmarshal(frame.Body, &resp) == nil {
				for _, m := range resp.GetMessages() {
					bc.countMsg(st, m)
				}
			}
		case protocol.CmdReconnectNow:
			// 压测期间 drain：记录并退出该读循环（连接由服务端关闭）。
			fmt.Printf("client#%d 收到 RECONNECT_NOW\n", bc.idx)
		}
	}
}

// countMsg 把一条下行消息（实时推或补拉）计入统计。
func (bc *benchClient) countMsg(st *stats, m *pb.MsgPush) {
	atomic.AddInt64(&st.recvCount, 1)
	st.lossMu.Lock()
	st.recvByDst[fmt.Sprintf("%d", m.GetSenderId())]++
	st.lossMu.Unlock()
	// 端到端：payload = [cmid, 发送毫秒时间戳]。
	var parts []string
	if json.Unmarshal(m.GetPayload(), &parts) == nil && len(parts) == 2 {
		var ts int64
		ts, _ = strconv.ParseInt(parts[1], 10, 64)
		st.addE2E(float64(time.Since(time.UnixMilli(ts)).Milliseconds()))
	}
}

// writeFrame 串行写一帧。
func (bc *benchClient) writeFrame(cmd protocol.Cmd, seq uint32, body []byte) {
	frame, err := protocol.Encode(protocol.Frame{Ver: protocol.Ver, Cmd: cmd, Seq: seq, Body: body})
	if err != nil {
		return
	}
	bc.writeMu.Lock()
	defer bc.writeMu.Unlock()
	if bc.ws == nil {
		return
	}
	_ = bc.ws.WriteMessage(websocket.BinaryMessage, frame)
}

// registerLogin 注册并登录，返回 uid 与 access token。
func registerLogin(hc *http.Client, username string) (int64, string, error) {
	body := fmt.Sprintf(`{"username":%q,"password":"bench-pass-8","nickname":%q}`, username, username)
	resp, err := hc.Post(*accountAdr+"/api/v1/register", "application/json", strings.NewReader(body))
	if err != nil {
		return 0, "", err
	}
	var reg struct {
		Code int `json:"code"`
		Data struct {
			UID int64 `json:"uid"`
		} `json:"data"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&reg)
	_ = resp.Body.Close()
	if reg.Code != 0 && reg.Code != 40202 { // 已注册（重跑）也继续登录
		return 0, "", fmt.Errorf("register code=%d", reg.Code)
	}

	resp2, err := hc.Post(*accountAdr+"/api/v1/login", "application/json",
		strings.NewReader(fmt.Sprintf(`{"username":%q,"password":"bench-pass-8"}`, username)))
	if err != nil {
		return 0, "", err
	}
	var login struct {
		Code int `json:"code"`
		Data struct {
			UID         int64  `json:"uid"`
			AccessToken string `json:"access_token"`
		} `json:"data"`
	}
	_ = json.NewDecoder(resp2.Body).Decode(&login)
	_ = resp2.Body.Close()
	if login.Code != 0 || login.Data.AccessToken == "" {
		return 0, "", fmt.Errorf("login code=%d", login.Code)
	}
	return login.Data.UID, login.Data.AccessToken, nil
}

func convP2P(a, b int64) string {
	if a > b {
		a, b = b, a
	}
	return fmt.Sprintf("c:%d:%d", a, b)
}

// osExit 抽象退出（syscall 引用保持工具可裁剪）。
func osExit(code int) { syscall.Exit(code) }
