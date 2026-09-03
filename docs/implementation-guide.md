# LinkIM 分布式即时通讯系统 —— 分步实现指南（附 AI Prompt）

| 项目     | 内容                                                          |
| ------ | ----------------------------------------------------------- |
| 文档版本   | v1.0                                                        |
| 配套设计文档 | [docs/distributed-im-design.md](./distributed-im-design.md) |
| 技术栈    | Go 1.22+ / WebSocket / Kafka / Redis / MySQL                |
| 更新日期   | 2026-09-04                                                  |

---

## 目录

- [使用说明](#使用说明)
- [总体路线图](#总体路线图)
- [统一技术约束与仓库结构](#统一技术约束与仓库结构)
- [S0 工程骨架与本地环境](#s0-工程骨架与本地环境)
- [S1 通信协议库](#s1-通信协议库)
- [S2 存储层与中间件封装](#s2-存储层与中间件封装)
- [S3 Account 账号服务（HTTP + JWT）](#s3-account-账号服务http--jwt)
- [S4 Logic 服务骨架与鉴权](#s4-logic-服务骨架与鉴权)
- [S5 Comet 长连接接入层](#s5-comet-长连接接入层)
- [S6 单聊上行链路（发送 → ACK）](#s6-单聊上行链路发送--ack)
- [S7 Job 推送层（下行投递 + 异步落库）](#s7-job-推送层下行投递--异步落库)
- [S8 离线同步与多端](#s8-离线同步与多端)
- [S9 群聊](#s9-群聊)
- [S10 高可用加固、可观测性与部署](#s10-高可用加固可观测性与部署)
- [进度追踪清单](#进度追踪清单)

---

## 使用说明

本文档把 LinkIM 的 MVP（对应设计文档 P0 阶段 + 部分 P1 能力）拆成 **S0 ~ S10 共 11 个步骤**，每步包含：

- **目标**：这一步结束时系统具备什么能力；
- **前置依赖**：必须已完成的步骤；
- **任务清单**：具体开发内容；
- **验收标准**：可执行命令 / 可观察行为，**必须全部通过才能进入下一步**；
- **AI Prompt**：可直接复制给 AI 编码助手（ZCode / Claude Code / Cursor 等）的完整指令。

**推荐工作流：**

```
1. 打开新 AI 会话 → 粘贴 [全局引导语]（见下）+ 当前步骤的 Prompt
2. AI 完成实现 → 你按"验收标准"逐条验证
3. 验收通过 → git commit（一个步骤至少一个 commit）
4. 进入下一步，重复
```

### 全局引导语（每次新会话先粘贴这段）

```text
你是资深 Go 后端工程师，正在实现 LinkIM 分布式即时通讯系统。
架构与所有设计决策见仓库 docs/distributed-im-design.md（必读，实现必须与该文档一致）。

硬性约束：
1. Go 1.22+，标准库优先；目录结构遵循仓库已有结构，不擅自新建顶层目录。
2. 依赖库固定使用：gorilla/websocket、google/golang.org/grpc + protobuf、
   segmentio/kafka-go、github.com/redis/go-redis/v9、jmoiron/sqlx、
   golang-jwt/jwt/v5、go.uber.org/zap、spf13/viper、
   prometheus/client_golang、stretchr/testify。
3. 所有对外错误返回业务错误码，不向客户端泄漏内部错误。
4. 每个包附带单元测试（table-driven + testify）；热路径避免不必要的内存分配。
5. 完成后运行 `make lint`、`make test`、`make build`，全部通过才算完成。
6. 只实现本步骤要求的内容，不做超前设计；但不得破坏后续步骤的扩展点。
7. 最后输出：变更文件清单 + 每条验收标准的执行结果。
```

> 提示：如果所用 AI 支持读取仓库文件（如 ZCode），全局引导语 + 设计文档即可提供足够上下文；如果不支持，请把设计文档相关章节一并粘贴。

---

## 总体路线图

```
S0 骨架 ──> S1 协议 ──> S2 存储/中间件 ──> S3 Account ──> S4 Logic骨架
                                                              │
   S8 离线同步 <── S7 Job推送/落库 <── S6 单聊上行 <── S5 Comet │
        │                                                    │
        └──────────────> S9 群聊 ──> S10 高可用/可观测/部署 ──┘
```

| 步骤  | 主题     | 交付的服务/模块                                  |
| --- | ------ | ----------------------------------------- |
| S0  | 工程骨架   | Makefile、docker-compose（依赖）、配置、日志         |
| S1  | 通信协议   | `pkg/protocol`（帧编解码）、`pkg/pb`（Protobuf）   |
| S2  | 存储与中间件 | `pkg/mysqlx/redisx/kafkax`、SQL migrations |
| S3  | 账号服务   | `cmd/account`（注册/登录/JWT）                  |
| S4  | 逻辑层骨架  | `cmd/logic`（gRPC：VerifyToken 等）           |
| S5  | 接入层    | `cmd/comet`（WS 连接管理、心跳、鉴权、路由表）            |
| S6  | 上行链路   | Logic SendMsg（幂等/seq/msgId/Kafka）         |
| S7  | 下行链路   | `cmd/job`（投递 + 批量落库）                      |
| S8  | 离线同步   | SYNC_PULL、会话游标、未读数                        |
| S9  | 群聊     | 群 CRUD、成员缓存、扇出推送                          |
| S10 | 加固与部署  | drain、对账、DLQ、Prometheus、K8s               |

---

## 统一技术约束与仓库结构

所有步骤共用的目标仓库结构（S0 建立，后续沿用）：

```
LinkIM/
├── cmd/
│   ├── account/main.go        # HTTP 账号服务 :8080
│   ├── comet/main.go          # WebSocket 接入层 :8081(WS) :9000(gRPC)
│   ├── logic/main.go          # gRPC 逻辑层 :9001
│   └── job/main.go            # Kafka 消费者（推送 + 落库）
├── internal/
│   ├── account/               # 账号业务
│   ├── comet/                 # 连接管理、读写循环、bucket
│   ├── logic/                 # 消息处理、seq、幂等
│   ├── job/                   # push consumer、store consumer
│   └── service/               # 跨服务共享的业务逻辑（路由表操作等）
├── pkg/
│   ├── protocol/              # 二进制帧编解码
│   ├── pb/                    # protobuf 生成代码
│   ├── mysqlx/                # sqlx 封装 + 分表路由
│   ├── redisx/                # go-redis 封装 + key 规范
│   ├── kafkax/                # kafka-go 封装（幂等 producer）
│   ├── snowflake/             # msgId 生成器
│   └── conf/                  # 配置结构体与加载
├── api/                       # .proto 源文件（logic.proto、comet.proto）
├── configs/                   # config.yaml（每服务一份：account.yaml 等）
├── migrations/                # golang-migrate SQL
├── deployments/               # docker-compose.yml、k8s/
├── scripts/                   # 测试客户端、压测、验证脚本
├── docs/                      # 设计文档（已存在）
├── Makefile
└── go.mod
```

**统一约定（各步骤 prompt 中不再重复）：**

- Redis key 规范见设计文档 9.3 节；`comet:alive:{addr}`、`route:{uid}` 为核心。
- Kafka topic：`msg.push`（64 分区）、`msg.store`（128 分区）、`msg.notify`、`presence`、`dlq.msg.push`。
- 帧格式：`ver(1B) cmd(2B) seq(4B) len(4B) body`，body 上限 64KB。
- 业务错误码：`0=成功`，`40x=客户端错误`，`50x=服务端错误`，前两位分段：`4xx01 鉴权`、`4xx02 参数`、`4xx03 关系`、`5xx01 存储`、`5xx02 中间件`。

---

## S0 工程骨架与本地环境

**目标**：建立 Go monorepo、构建体系、本地依赖环境（MySQL/Redis/Kafka），`make build` 可产出四个服务的二进制。

**前置依赖**：无。

**任务清单**：

1. 初始化 go module `github.com/linkim/linkim`（模块名可按实际调整，后续统一）。
2. 建立目录结构（见上），四个 `main.go` 先输出启动日志占位。
3. `pkg/conf`：基于 viper 的配置加载（yaml + 环境变量覆盖 `LINKIM_` 前缀）。
4. 接入 zap（可注入 console / json 两种 encoder），统一初始化入口。
5. `deployments/docker-compose.yml`：MySQL8、Redis7、Kafka（KRaft 模式，单节点即可）+ 初始化 topic 脚本。
6. Makefile：`build / test / lint / migrate-up / migrate-down / compose-up / compose-down`。
7. `.gitignore`、`.golangci.yml`（启用 errcheck、govet、staticcheck、revive）。

**验收标准**：

- [ ] `make compose-up` 后 `mysql/redis/kafka` 全部 healthy；
- [ ] `make build` 生成 `bin/account bin/comet bin/logic bin/job`；
- [ ] `make lint`、`make test` 通过（哪怕暂时无测试）。

**AI Prompt**：

```text
为 LinkIM 项目搭建 Go 工程骨架。参考 docs/distributed-im-design.md 第 17 节部署架构与本文档的仓库结构。

具体要求：
1. go mod init github.com/linkim/linkim；Go 1.22。
2. 创建目录：cmd/{account,comet,logic,job}、internal/{account,comet,logic,job,service}、
   pkg/{protocol,pb,mysqlx,redisx,kafkax,snowflake,conf}、api、configs、migrations、
   deployments、scripts。每个 main.go 仅打印服务名并优雅退出（监听 SIGTERM/SIGINT）。
3. pkg/conf：定义 ServerConfig{Name,HTTPPort,GPRCPort,WSPort}、MySQLConfig、RedisConfig、
   KafkaConfig、LogConfig；Load(path string) 从 yaml 读取，环境变量以 LINKIM_ 前缀覆盖
   （如 LINKIM_MYSQL_DSN）。提供 configs/ 下每个服务的示例 yaml。
4. 日志：pkg 内提供 zap 初始化（dev=console / prod=json），从 LogConfig 读取 level。
5. deployments/docker-compose.yml：
   - mysql:8（库 linkim，root 密码 linkim123，healthcheck）
   - redis:7（appendonly yes）
   - kafka:3.7 KRaft 单节点 + kafka-init 容器，用 kafka-topics 命令创建：
     msg.push(64分区,3副本[单机降为1])、msg.store(128分区)、msg.notify(8)、presence(8)、
     dlq.msg.push(4)
6. Makefile 目标：build（go build -o bin/ ./cmd/...）、test（go test ./... -race -count=1）、
   lint（golangci-lint run）、migrate-up/down（golang-migrate，DSN 从环境变量取）、
   compose-up/down。加 help 目标列注释。
7. .golangci.yml 启用 errcheck/govet/staticcheck/revive；.gitignore 覆盖 bin/、*.log、.idea 等。

验收（逐条执行并在最后报告结果）：
- make compose-up && docker ps 显示三个依赖 healthy
- make build / make lint / make test 通过
```

---

## S1 通信协议库

**目标**：完成 WebSocket 之上的二进制帧编解码（`pkg/protocol`）与 Protobuf 消息定义（`pkg/pb`），带完整单测。

**前置依赖**：S0。

**任务清单**：

1. `pkg/protocol/frame.go`：帧结构 `{Ver uint8, Cmd uint16, Seq uint32, Body []byte}`。
   - `Encode` / `Decode`；`MaxBodyLen = 64KB`，超限返回 `ErrBodyTooLarge`。
   - `Reader`：支持从 `io.Reader` 流式读取（正确处理粘包/半包，用 `io.ReadFull`）。
2. 命令字常量（`cmd.go`）：设计文档 4.3 的 0x01~0x0C 全集，附 `CmdString()` 便于日志。
3. `pkg/pb`：按设计文档 4.4 编写 `api/protocol.proto`（MsgSendReq/MsgSendAck/MsgPush/AuthReq/AuthAck/SyncPullReq/SyncResp/ReceivedAckReq/KickReq 等全部消息），配置 buf 或 protoc 生成脚本（Makefile `proto` 目标，用 buf 优先，回退 protoc 命令写进脚本注释）。
4. 单元测试：编解码回环、半包、粘包（两帧拼接一次读出）、非法长度、最大长度边界。

**验收标准**：

- [ ] `make proto` 生成 `pkg/pb/*.pb.go` 且提交；
- [ ] `go test ./pkg/protocol/ -v` 覆盖上述场景全绿。

**AI Prompt**：

```text
实现 LinkIM 的通信协议库。先读 docs/distributed-im-design.md 第 4 节（协议设计），严格按其帧格式与命令字实现。

任务 1 —— pkg/protocol：
- frame.go：Frame{Ver uint8; Cmd uint16; Seq uint32; Body []byte}。
  Encode(f) ([]byte, error) 与 Decode([]byte) (Frame, error)（整帧解析，长度不符/超限报错）。
  NewStreamReader(r io.Reader) 提供逐帧读取：内部 io.ReadFull 先读 11 字节头得到 bodyLen
  （大端序），校验 MaxBodyLen=64KB，再读 body。所有多字节字段大端序。
- cmd.go：定义常量 CmdAuth=0x01 ... CmdPresence=0x0C（对照设计文档 4.3 表格逐一定义），
  实现 CmdString(Cmd) string。
- errors.go：ErrBodyTooLarge、ErrMalformedFrame。
- 单测（frame_test.go）：编码-解码回环属性测试（含空 body、最大 body=64KB、64KB+1 报错）；
  流式读取：构造两帧拼接的 buffer 一次 ReadFrame 两次得到正确结果；
  构造先给 5 字节再给剩余的半包场景验证阻塞语义（用 pipe 协程喂）。

任务 2 —— pkg/pb：
- api/protocol.proto：syntax proto3，package linkim.pb.v1，go_package 指向 pkg/pb。
  定义设计文档 4.4 中全部消息，并补充：
  AuthReq{token,device_id,platform}、AuthAck{code,msg,uid,kick_reason},
  HeartbeatReq{}, SyncPullReq{conv_id,local_max_seq,limit}, SyncResp{conv_id,messages[],max_seq},
  ReceivedAckReq{msg_id,conv_id,seq}, KickReq{reason}, MsgRecallReq{conv_id,msg_id}。
- Makefile 增加 proto 目标：优先用 buf generate，脚本内注释给出等价 protoc 命令。
  生成到 pkg/pb/。

验收：make proto 成功生成；go test ./pkg/protocol/ -v 全部通过；
go vet ./... 干净。报告每个测试用例名与结果。
```

---

## S2 存储层与中间件封装

**目标**：建库建表（migrations）、封装 MySQL/Redis/Kafka 客户端与分表路由，封装雪花 ID。

**前置依赖**：S0。

**任务清单**：

1. `migrations/`（golang-migrate 格式，up/down 成对）：
   - 用户库：`user` 表（id, username 唯一, password_hash, nickname, created_at）。
   - 关系库：`friend`（uid, friend_uid, status, remark；主键 (uid,friend_uid) 双向冗余插入）、
     `group`、`group_member`（对照设计文档 9.2 DDL）。
   - 会话库：`conversation`（设计文档 9.2 DDL 原样）。
   - 消息库：`message_00`~`message_63`（循环生成 64 张，DDL 见设计文档 9.2）。
2. `pkg/mysqlx`：sqlx 封装（连接池参数可配、ping、重试连接）；`ShardTable(convID string) string` —— 一致性哈希或 `crc32(conv_id)%64` 映射到 `message_00..63`（MVP 单库，分库键留接口）。
3. `pkg/redisx`：go-redis v9 封装；`keys.go` 集中定义设计文档 9.3 全部 key 的构造函数（`RouteKey(uid)`、`CometAliveKey(addr)`、`SeqKey(convID)`、`IdemKey(senderID, clientMsgID)`、`PresenceKey(uid)`、`TokenKey(uid)`、`ConvMembersKey(gid)`、`FriendKey(uid)`）。
4. `pkg/kafkax`：基于 segmentio/kafka-go 的 Producer 封装：Writer 配置 `RequiredAcks: All`、`Balancer: &kafka.Hash{}`、`Compression: LZ4`、`BatchTimeout: 5ms`，`AsyncClose` 支持；提供 `Send(ctx, topic, key, value, headers)`。
5. `pkg/snowflake`：雪花 ID（节点 ID 从配置或环境变量取），`Next()` 线程安全，单测并发唯一性。

**验收标准**：

- [ ] `make compose-up && make migrate-up` 全部表建成（含 64 张 message 表）；
- [ ] `go test ./pkg/... -v` 全绿（重点：snowflake 100 万 ID 并发无重复、分表函数值域正确）。

**AI Prompt**：

```text
实现 LinkIM 的存储与中间件封装层。先读 docs/distributed-im-design.md 第 9 节（存储设计），表结构以 9.2 的 DDL 为准。

任务 1 —— migrations/（golang-migrate，编号 000001 起，up/down 成对）：
- 000001_users.up.sql：user 表（id BIGINT PK, username VARCHAR(64) UNIQUE,
  password_hash VARCHAR(100), nickname VARCHAR(64), created_at DATETIME）。
- 000002_relation.up.sql：friend(uid,friend_uid,status TINYINT,remark VARCHAR(64),
  PRIMARY KEY(uid,friend_uid), KEY idx_friend(friend_uid))；
  group 与 group_member 表按设计文档 9.2 DDL。
- 000003_conversation.up.sql：conversation 表按设计文档 9.2 DDL 原样。
- 000004_messages.up.sql：message_00 到 message_63 共 64 张表，DDL 按设计文档 9.2，
  其中 payload 用 VARBINARY(65535)。
- 每个都有对应 .down.sql（DROP TABLE）。

任务 2 —— pkg/mysqlx：
- New(cfg) 用 sqlx.Connect("mysql", dsn) + SetMaxOpenConns/SetMaxIdleConns/SetConnMaxLifetime
  从配置读；暴露 *sqlx.DB。
- ShardTable.go：func ShardTable(convID string) string = fmt.Sprintf("message_%02d",
  crc32.ChecksumIEEE([]byte(convID))%64)。单测验证输出值域 ∈ [message_00, message_63] 且
  同 convID 稳定。

任务 3 —— pkg/redisx：
- New(cfg) 返回 *redis.Client（go-redis v9）。
- keys.go：按设计文档 9.3 定义全部 key 构造函数：RouteKey(uid int64) string、
  CometAliveKey(addr string)、SeqKey(convID string)、IdemKey(senderID int64, clientMsgID string)、
  PresenceKey(uid int64)、TokenKey(uid int64)、ConvMembersKey(gid int64)、FriendKey(uid int64)。

任务 4 —— pkg/kafkax：
- type Producer 封装 kafka-go Writer（RequiredAcks: kafka.RequiredAcks(),
  即 all；Balancer: &kafka.Hash{}；Compression: kafka.Lz4；BatchTimeout: 5ms；
  AllowAutoTopicCreation: false）。
- Send(ctx, topic, key []byte, value []byte, headers ...map[string]string) error，
  header 里统一附带 trace-id。
- Close() 调 Writer.Close()。

任务 5 —— pkg/snowflake：
- 标准 snowflake（时间戳 41bit + 节点 10bit + 序列 12bit），nodeID 从构造参数注入。
  Next() int64，互斥锁或 CAS 保证并发安全。
- 单测：10 goroutine × 10 万次收集所有 ID，验证总数无重复（用 map 判重）。

验收：make compose-up && make migrate-up 后用 information_schema 查询确认
message 表 64 张、其余表齐全；go test ./pkg/... 全部通过。
```

---

## S3 Account 账号服务（HTTP + JWT）

**目标**：用户注册、登录、签发 JWT；提供内部 token 校验接口（Logic 复用）。

**前置依赖**：S0~S2。

**任务清单**：

1. `cmd/account`：HTTP 服务（标准库 `net/http` + chi 路由可省略，直接 mux 即可）。
2. `internal/account`：
   - `POST /api/v1/register` `{username, password, nickname}` → bcrypt 哈希落库，uid 用雪花分配；
   - `POST /api/v1/login` `{username, password}` → 校验 bcrypt → 签发 JWT（HS256，secret 来自配置；claims: uid、exp=2h；refresh token 30d 简化为同一接口返回两个字段）；
   - `POST /internal/v1/verify` `{token}` → 返回 `{uid, valid}`（供 Logic/Comet 调用）。
3. 登录成功同时写 Redis `token:{uid}`（缓存 token 摘要，TTL 2h）供快速失效。
4. 参数校验（长度、用户名字符集）、统一错误响应 `{code, msg}`（按统一错误码段）。
5. 集成测试：用 sqlmock 或直接连 docker 的 MySQL 跑 register→login→verify 全链路（`make test-int`，可用 build tag `integration` 控制）。

**验收标准**：

- [ ] `make compose-up && ./bin/account` 启动后，curl 注册→登录→verify 三连成功拿到 uid；
- [ ] 错误密码返回 `40101` 类业务码；
- [ ] 伪造 token verify 返回 invalid。

**AI Prompt**：

```text
实现 LinkIM 的账号服务 cmd/account + internal/account。先读 docs/distributed-im-design.md 5.2 与 14 节。

要点：
1. HTTP 端口从配置读（configs/account.yaml：http端口、mysql dsn、redis、jwt_secret、token_ttl）。
   中间件：access log（zap）、recover。
2. POST /api/v1/register {username[4,32], password[8,64], nickname}：
   用户名唯一校验（查库 + 唯一索引兜底）；bcrypt cost=10；uid 用 pkg/snowflake 生成；
   密码错误格式返回 code=40201。
3. POST /api/v1/login {username,password}：
   bcrypt.CompareHashAndPassword；成功签发 golang-jwt/v5 HS256 token：
   claims{uid, exp:now+2h, iat}，同时生成 refresh_token（30d，tokenType=refresh）；
   写 Redis token:{uid} = token 摘要（sha256 后取前 32 字节），TTL=2h。
   响应 {code:0, data:{uid, access_token, refresh_token, expire_at}}。
4. POST /internal/v1/verify {token}：解析 JWT 并校验签名/过期，
   再比对 Redis 中摘要（若存在则要求一致，实现单点登出能力）。
   响应 {uid, valid}。校验失败 code=40101。
5. internal/account 写 service 层 + handler 层分离；错误码集中在 internal/account/errcode.go。
6. 集成测试（build tag integration，连 docker 依赖）：注册→登录→verify→
   登出（DELETE Redis key）→verify 失效。正常单测用 mock（接口抽象 DB/Redis）。

验收：启动后用 curl 完成 注册/登录/verify 三步并贴出响应 JSON；
错误密码与伪造 token 场景各贴一次响应；go test ./internal/account/ 通过。
```

---

## S4 Logic 服务骨架与鉴权

**目标**：Logic gRPC 服务跑起来，实现 `VerifyToken`（带 Redis 缓存），定义后续步骤要用的全部 RPC 接口（本步只实现 VerifyToken，其余返回 Unimplemented）。

**前置依赖**：S0~S3。

**任务清单**：

1. `api/logic.proto`：
   
   ```protobuf
   service Logic {
     rpc VerifyToken(VerifyTokenReq) returns (VerifyTokenResp);     // comet AUTH 时调用
     rpc SendMsg(SendMsgReq) returns (SendMsgAck);                  // S6 实现
     rpc SyncPull(SyncPullReq) returns (SyncPullResp);              // S8 实现
     rpc ReportDelivered(ReportDeliveredReq) returns (Empty);       // S7 实现
     rpc OnlineEvent(OnlineEventReq) returns (Empty);               // 上下线事件
   }
   ```
   
   消息体含：uid/device_id/platform/comet_addr（OnlineEvent）、msgId/convId/seq/clientMsgId 等（SendMsg 相关，字段对照设计文档）。

2. `cmd/logic` + `internal/logic`：gRPC server 启动、zap 中间件（access log + recover）、优雅退出。

3. `VerifyToken` 实现：先查 Redis `token:{uid}` 缓存的校验结果（TTL 5min，命中直接返回），未命中调用 account `/internal/v1/verify`（HTTP client 带超时 1s、重试 1 次），结果回填缓存。

4. proto 生成进 `pkg/pb`，Makefile 更新。

**验收标准**：

- [ ] `grpcurl -plaintext -d '{"uid":1,"token":"xxx"}' 127.0.0.1:9001 logic.Logic/VerifyToken` 返回 valid=false；
- [ ] 用 S3 签发的真 token 返回 valid=true，且第二次请求命中缓存（日志可见 cache hit）。

**AI Prompt**：

```text
实现 LinkIM 的 Logic 服务骨架。先读 docs/distributed-im-design.md 2、5.2 节。

1. api/logic.proto（proto3, package linkim.logic.v1, go_package=pkg/pb）：
   service Logic { VerifyToken / SendMsg / SyncPull / ReportDelivered / OnlineEvent }
   消息定义：
   VerifyTokenReq{uid int64, token string} / VerifyTokenResp{valid bool, code int32}
   SendMsgReq{sender_id, conv_id, conv_type int32, client_msg_id, device_id string,
              msg_type int32, payload bytes}
   SendMsgAck{code int32, msg_id string, seq int64, timestamp int64}
   SyncPullReq{uid int64, conv_id string, local_max_seq int64, limit int32}
   SyncPullResp{code, messages: repeated PbMsg, max_seq int64}
   PbMsg{msg_id, conv_id, conv_type, sender_id, msg_type, payload, seq, timestamp, status}
   ReportDeliveredReq{uid, msg_id string, conv_id string}
   OnlineEventReq{uid, device_id, platform int32, comet_addr, online bool}
   Empty{}
2. cmd/logic：读 configs/logic.yaml（grpc端口、redis、account 内部地址）；启动 gRPC，
   注册 zap unary interceptor（打印 method/code/耗时，recover）；
   监听 SIGTERM 优雅 Stop（等待 10s drain）。
3. internal/logic/token.go 实现 VerifyToken：
   - 先查 Redis 缓存 key: tokencache:{uid}:{sha256(token)前16字节} → "1"/"0"，TTL 5min；
   - miss 则 HTTP POST account /internal/v1/verify（1s 超时），回填缓存；
   - account 不可达返回 code=50102，valid=false。
4. 其余 RPC 返回 codes.Unimplemented。
5. Makefile proto 目标覆盖 logic.proto。

验收：make proto 后编译通过；启动 account+logic，
用 grpcurl 分别验证伪造 token（valid=false）与真实 token（两次调用第二次日志出现 cache hit）。
go test ./internal/logic/ 通过（token 缓存逻辑用 miniredis 或 mock 测）。
```

---

## S5 Comet 长连接接入层

**目标**：Comet 支持 WebSocket 接入：握手 → AUTH（调 Logic VerifyToken）→ 路由表登记 → 心跳保活 → 收发帧；暴露 gRPC `PushMsg`（下行）与 `Kick`；断连清理路由；注册存活心跳。

**前置依赖**：S0~S4。

**任务清单**：

1. `cmd/comet`：启动 WS 端口（`/ws` 路径）+ gRPC 端口；启动时写 `comet:alive:{addr}`（TTL 30s，后台每 10s 续期）；SIGTERM 时清理。
2. `internal/comet/conn.go`：连接对象（uid、deviceID、platform、发送 chan []byte 容量 256、lastActive 原子时间戳）。
3. `internal/comet/server.go`：
   - Upgrade（gorilla/websocket，ReadLimit 128KB，Ping/Pong 交由应用层心跳处理）；
   - **鉴权前限制**：10s 内未收到合法 AUTH 则关闭；鉴权前每 IP 最多 N（默认 10）连接；
   - AUTH 流程：解析帧 → 调 Logic VerifyToken → 成功：`HSET route:{uid} device comet_addr`、`SET presence:{uid} online EX 90`，回 AUTH_ACK；同 platform 旧连接互踢（查路由表，若指向本机直接 Kick 本机连接；跨机通过 Kafka `presence` 或 gRPC 调目标 comet —— MVP 先实现本机踢 + 跨机发 Kafka 事件）；
   - 失败回 AUTH_ACK(code) 并关闭。
4. 读写循环：每连接 2 goroutine。读循环：帧解析 → HEARTBEAT 回 ACK 并刷新活跃时间；MSG_SEND 转发 Logic（S6 接入，本步先打日志）。写循环：从 channel 取数据写出；channel 满且积压超过 5s 判定慢连接强制断开。
5. 心跳超时：时间轮或简化版 ticker 扫描（MVP 允许每秒全量扫描分片 map），75s 无帧判定断开。
6. gRPC `PushFrames(uid, deviceID, frames)`（批量帧下发，job 调用）：查本机连接表直写 channel；连接不存在返回 not-online。
7. 断连清理：`HDEL route:{uid} device`；若该 uid 无剩余端，删 `presence:{uid}`。
8. `scripts/wsclient.go`：命令行测试客户端（可 AUTH、发心跳、打印收到的所有帧），供后续步骤验收用。

**验收标准**：

- [ ] `go run ./scripts/wsclient -token <真token> -device d1` 完成 AUTH 并持续心跳，Redis 中可见 `route:{uid}`；
- [ ] 同 platform 第二个设备登录后，第一个收到 KICKED 帧并被断开；
- [ ] 直接 kill -9 comet 后 30s 内 `comet:alive` 消失，`route` 中该机条目在对账逻辑（S10 完整实现）前先接受短暂残留，本步验证 HDEL 正常路径即可（正常断开立即清理）。

**AI Prompt**：

```text
实现 LinkIM 的 Comet 长连接接入层。先读 docs/distributed-im-design.md 第 4.5、7 节（连接生命周期与路由表）与 15.1（性能实践）。

结构：cmd/comet 只做装配；核心在 internal/comet。

1. Bucket 分片管理：Server 内按 uid%256 分 256 个 bucket（各自读写锁 + map[deviceKey]*Conn），
   避免全局锁。deviceKey = uid+":"+device_id。
2. Conn 结构：ws conn、uid、deviceId、platform、send chan []byte（缓冲 256）、
   lastActive int64（atomic）、closed chan struct{}（close once）。
   提供 Push(frame []byte) error：channel 满时非阻塞返回 ErrSlowConsumer。
3. 读循环 goroutine：
   - 循环 ReadFrame（pkg/protocol StreamReader；websocket ReadLimit 设 128KB）；
   - 收到 AUTH：pb 解析 → gRPC 调 logic.VerifyToken（超时 2s）→
     成功：conn 绑定身份并注册进 bucket；HSET route:{uid} {device} {cometAddr}；
     SET presence:{uid} online EX 90；回 AUTH_ACK{code:0}。
     失败：回 AUTH_ACK{code:40101} 后 Close()。
   - 鉴权前 10s watchdog：未 AUTH 直接关。
   - HEARTBEAT：回 HEARTBEAT_ACK，刷新 lastActive，续期 presence（pipeline 异步）。
   - MSG_SEND / MSG_RECEIVED_ACK / SYNC_PULL：本步仅打日志并回
     "未实现"业务码（S6/S7/S8 替换），接口上先留 dispatch hook（HandlerFunc 注入）。
4. 写循环 goroutine：select { frame := <-send: 写出（WriteMessage Binary）;
   slowCheck ticker 1s: 若 len(send) 持续 ==cap(256) 超过 5s → 强制断开 }。
   写串行化由单 goroutine 保证。
5. 超时扫描：每 bucket 一个 ticker（1s）扫描 lastActive，now-last > 75s 断开清理。
6. 断开清理 Close 逻辑（sync.Once）：从 bucket 删除、HDEL route:{uid} {device}、
   HLEN route:{uid} == 0 时 DEL presence:{uid}、关闭 ws 与 send。
7. gRPC：api/comet.proto 定义 service Comet { rpc PushFrames(PushFramesReq) returns
   (PushFramesResp); rpc Kick(KickReq) returns (Empty); }
   PushFramesReq{uid int64, device_id string, frames repeated bytes}——按 deviceKey 查
   bucket 命中则 Push；未命中返回 online=false。Kick：向连接写 KickReq 帧后断开。
8. 存活注册：启动时 SET comet:alive:{cometAddr} 1 EX 30，后台 10s 续期；SIGTERM 清理并退出。
9. 同 platform 互踢：AUTH 成功后 HGET route:{uid} 找同 platform 旧条目
   （route hash 的 field 编码为 platform:device_id）：
   - 指向本机：直接调本机 Kick；
   - 指向他机：gRPC 调目标 comet Kick（comet 地址解析：从 route 值取 addr 直连，
     客户端按需缓存连接）。MVP 可先只实现本机踢 + 日志告警，跨机留 TODO 但接口已定。
10. scripts/wsclient.go：flag（addr、token、device、platform、发送间隔），
    自动 AUTH + 每 30s 心跳 + 打印所有收到的帧（cmd 名称可读化）。

验收（逐条贴证据）：
- wsclient 用 S3 真 token 上线，redis-cli HGETALL route:{uid} 有值，presence 存在；
- 同 platform 二连，第一个客户端打印出 KICKED 帧且连接关闭；
- 断开 wsclient 后 route/presence 被清理；
- grpcurl 调 PushFrames 向在线设备推一帧，wsclient 打印该帧；
- go test ./internal/comet/ 通过（bucket 增删、慢连接、超时用假时钟或注入 now 函数测试）。
```

---

## S6 单聊上行链路（发送 → ACK）

**目标**：打通 MSG_SEND → Logic 全处理（鉴权、好友校验、幂等、seq、msgId、双写 Kafka）→ MSG_SEND_ACK。

**前置依赖**：S0~S5。

**任务清单**：

1. Comet 的 MSG_SEND dispatch 接入：gRPC 调 Logic.SendMsg，将返回的 msgId/seq 组装 MSG_SEND_ACK 回给发送方（帧 seq 原样回带）。
2. `internal/logic/sendmsg.go`：
   - 参数校验（conv 归一化：单聊 conv_id = 拼接排序 uid，提供 `ConvIDForP2P(a,b)` 工具放 internal/service）；
   - **幂等**：`SET idem:{sender}:{client_msg_id} msgId NX EX 600`，命中则直接回放存储的 ACK（value 存 msgId/seq 的 JSON）；
   - **关系校验**：好友关系（Redis `friend:{uid}` ZSet 缓存，miss 查 MySQL 回填）；
   - **seq**：`INCR seq:{conv_id}`；
   - **msgId**：雪花；
   - 组装 PbMsg 并 produce 到 `msg.push` 与 `msg.store`（key=conv_id，header 带 trace-id=client_msg_id）；
   - 返回 SendMsgAck。
3. 失败路径：Kafka 写入失败 → 返回 50201，**且删除幂等键**（允许客户端重发）。
4. 单测：幂等回放（同 client_msg_id 两次调用 seq 不变）、seq 并发递增（miniredis）、Kafka 用接口 mock 验证 topic/key/header。

**验收标准**：

- [ ] 两个 wsclient，A 发消息后收到 MSG_SEND_ACK（含 msgId 与递增 seq），连发 5 条 seq 严格 +1；
- [ ] kafka-console-consumer 读 `msg.push` 可看到消息（key=conv_id）；
- [ ] 同一 client_msg_id 重发两次，只产生一条 Kafka 消息且两次 ACK 一致。

**AI Prompt**：

```text
实现 LinkIM 单聊上行链路。先读 docs/distributed-im-design.md 5.1（时序图）、6.1~6.3（可靠性三要素）、9.3/9.4（Redis 结构与 seq）。

1. internal/service/conv.go：
   ConvIDForP2P(a, b int64) string —— 小 uid 在前拼接 "c:{min}:{max}"；
   ShardOfConv 调 pkg/mysqlx.ShardTable。
2. internal/logic/sendmsg.go 实现 Logic.SendMsg：
   顺序：参数校验 → conv_id 规范化（服务端重算，不信任客户端传入）→ 幂等检查 →
   好友关系校验 → seq → msgId → 组装 PbMsg → 双写 Kafka → 回 ACK。
   - 幂等：redis SET idem:{sender}:{clientMsgID} 值为
     JSON{msg_id,seq} NX EX 600；命中 → 解析并直接返回相同 SendMsgAck（不再写 Kafka）。
   - 好友校验：ZRANK friend:{sender} friend_uid（缓存 miss 时查 friend 表并
     全量回填 ZSet，score=updated_at）；非好友返回 40301。
   - seq：INCR seq:{conv_id}。
   - msgId：pkg/snowflake。
   - Kafka：pkg/kafkax.Send 到 msg.push 与 msg.store 各一次（key=conv_id，
     header: {"trace-id": clientMsgID, "conv-type": "1"}）。
     任一失败：回滚幂等键（DEL），返回 code=50201。
   - ack.timestamp 毫秒时间戳。
3. Comet 侧（internal/comet）：把 S5 预留的 MSG_SEND dispatch 实现为
   gRPC logic.SendMsg 调用（超时 3s），将结果编码为 MSG_SEND_ACK 帧
   （帧头 Seq 与请求帧相同）写回发送连接。gRPC 错误转业务码 50101。
4. 单测（internal/logic/sendmsg_test.go，miniredis + mock producer）：
   - 幂等：同 clientMsgID 调两次，第二次不产生新的 producer 调用，ACK 完全一致；
   - 关系：非好友返回 40301；
   - Kafka 失败：幂等键被删除，返回 50201。

验收：两个 scripts/wsclient 在线，A 连发 5 条消息，
逐条打印 ACK 中 seq 递增；kafka-console-consumer --topic msg.push 显示 5 条
（重发同一条两次后仍是 5 条）；go test ./internal/logic/ 通过。
```

---

## S7 Job 推送层（下行投递 + 异步落库）

**目标**：消费 `msg.push` 把消息实时推给接收者；消费 `msg.store` 批量落库。两个 wsclient 完成端到端互发。

**前置依赖**：S0~S6。

**任务清单**：

1. `cmd/job` + `internal/job`：两个 consumer group 并行启动（`job-push`、`job-store`），手动 commit，优雅退出（先停拉取、处理完在途消息、提交 offset、关闭）。
2. `internal/job/push.go`：
   - 解析 PbMsg；
   - `HMGET route:{recv_uid}` 拿所有在线端（含 device 与 comet 地址）；
   - 目标 comet 存活检查（`EXISTS comet:alive:{addr}`，不在线跳过——靠 S8 拉取兜底）；
   - 按 comet 地址分组，gRPC `PushFrames` 批量下发（每帧 = MSG_PUSH 的 PbMsg 编码 + 帧头）；
   - 投递失败（连接刚断）重试 1 次后放弃并记日志，**不回滚 offset**。
3. 接收端 ACK：Comet 收到 MSG_RECEIVED_ACK → gRPC `ReportDelivered` → Logic 写 Redis `delivered:{uid}:{msg_id}`（TTL 24h，仅作观测，MVP 不做强一致）。
4. `internal/job/store.go`：
   - 攒批：≤50ms 或 100 条触发 flush；
   - `INSERT IGNORE INTO message_xx ... VALUES(...)...`（多值批量，表名来自 conv_id 分表）；
   - 同时 UPSERT 会话双方 `conversation`（`last_seq = GREATEST(last_seq, ?)`、未读 +1 仅接收方）；
   - 唯一键冲突忽略；失败整批重试 3 次后进 `dlq.msg.store`。
5. 接收客户端去重：wsclient 按msg_id 记忆最近 1024 条（LRU）打印去重结果（模拟真实客户端行为，为验收提供证据）。

**验收标准**：

- [ ] A↔B 双向实时互发，双方均收到 MSG_PUSH 且 seq 有序；
- [ ] B 离线时 A 发 10 条：`message` 表 10 行、B 的 conversation.unread=10、无推送报错；
- [ ] 重启 job 进程，Kafka 无重复落库（INSERT IGNORE + 唯一键验证）。

**AI Prompt**：

```text
实现 LinkIM 的 Job 推送与落库层。先读 docs/distributed-im-design.md 5.1、6.1（④⑤环节）、8 节（Kafka 设计）与 9.2（表结构）。

1. cmd/job：读 configs/job.yaml（kafka brokers、consumer 组、mysql、redis、comet 发现方式）。
   启动两个 consumer：job-push（订阅 msg.push）、job-store（订阅 msg.store）。
   kafka-go Reader：CommitInterval=0（手动 FetchMessage/CommitMessages）、
   MinBytes/MaxBytes 合理、ReadBackoffMax 2s。SIGTERM：停止 Fetch → 处理完在途 → Commit → 退出。
2. internal/job/push.go：
   - 消息体为 PbMsg（pkg/pb）。
   - 接收者 = 会话中对端（单聊：conv_id 解析出双方，取非 sender 的一端）。
   - HGETALL route:{recv_uid}：无 → 记 offline 日志（无操作，S8 兜底）；有 →
     按 comet_addr 分组，逐组调 gRPC Comet.PushFrames（连接池按 addr 缓存 grpc.ClientConn）。
   - EXISTS comet:alive:{addr} 不存在 → 跳过该组。
   - PushFrames 返回 online=false → 不重试；gRPC 传输错误 → 重试 1 次 →
     放弃记 error（消息仍在 MySQL，S8 补拉）。
   - 下行帧：Frame{Ver:1, Cmd:MsgPush, Seq:0, Body: pb.Marshal(PbMsg)}。
3. Comet 侧实现 MSG_RECEIVED_ACK dispatch：gRPC logic.ReportDelivered；
   Logic 写 redis SET delivered:{uid}:{msgId} 1 EX 86400（观测用）。
4. internal/job/store.go：
   - 批处理器：chan 收集 + ticker 50ms + 满 100 条触发；
   - 批量 INSERT IGNORE INTO message_%02d (id,conv_id,seq,sender_id,msg_type,payload,
     status,created_at) VALUES ...（表名 = ShardTable(conv_id)，同批按表名再分组）；
   - conversation UPSERT：INSERT ... ON DUPLICATE KEY UPDATE
     last_seq=GREATEST(last_seq,VALUES(last_seq)), unread=unread+VALUES(unread_delta),
     updated_at=VALUES(updated_at)；发送方 unread_delta=0，接收方=1（接收方 uid 从
     conv 解析）。注意 conversation 表主键是 (uid, conv_id)，双方各一行，两行都在本批内写入；
   - 失败：重试 3 次（指数退避）→ 剩余写入 produce 到 dlq.msg.store 并提交 offset。
5. scripts/wsclient.go 扩展：-send "text" 参数发一条消息；接收循环按 msg_id LRU(1024)
   去重后打印 [conv][seq][from] payload。

验收（贴输出）：
- A、B 双端在线互发各 5 条，双方打印 seq 连续；
- B 下线后 A 发 10 条：SQL 查询 message 分表 count=10、
  SELECT unread FROM conversation WHERE uid=B AND conv_id=... 为 10；
- 手动向 msg.store 重放一条已落库消息（kcat -P 或测试代码），count 不变（幂等验证）；
- go test ./internal/job/ 通过（批处理器时序、表名分组、重试逻辑）。
```

---

## S8 离线同步与多端

**目标**：实现 SYNC_PULL 增量同步、上线自动补拉、未读数接口；断线重连消息不丢。

**前置依赖**：S0~S7。

**任务清单**：

1. `Logic.SyncPull`：查 `message_分表` `WHERE conv_id=? AND seq>? ORDER BY seq LIMIT ?`（limit 上限 100，循环客户端驱动）；返回消息 + 会话 max_seq。
2. 上线补拉：Comet AUTH 成功 → gRPC `OnlineEvent` → Logic 查 `conversation WHERE uid=? AND last_seq>read_seq` 返回未读会话列表（新增 RPC `GetPendingConvs`）→ Comet 向客户端推 `SYNC_NOTIFY` 帧（未读会话 + max_seq）；客户端逐会话 SYNC_PULL。
3. Comet 的 SYNC_PULL dispatch：透传 Logic，回 SYNC_RESP 帧。
4. 已读上报：新增 RPC `MarkRead(conv_id, seq)` → UPDATE conversation SET read_seq=?, unread=GREATEST(unread-(seq-read_seq),0)。wsclient 拉取完成后自动上报。
5. 多端游标：conversation 表 uid 维度天然多端共享（同一账号各端各自本地游标，服务端 read_seq 取最新）。

**验收标准**：

- [ ] B 离线时 A 发 10 条，B 上线后 1s 内自动收齐 10 条（顺序正确）；
- [ ] B 的未读数在拉取 + MarkRead 后归零；
- [ ] 杀掉 B 的连接 3 秒内 A 发 3 条，B 重连后补拉到 3 条（推拉结合验证）。

**AI Prompt**：

```text
实现 LinkIM 离线同步。先读 docs/distributed-im-design.md 第 10 节（seq 游标同步模型）。

1. api/logic.proto 扩展：
   rpc GetPendingConvs(PendingReq{uid}) returns (PendingResp{repeated ConvBrief})；
   ConvBrief{conv_id, conv_type, max_seq, unread}；
   rpc MarkRead(MarkReadReq{uid, conv_id, seq}) returns (Empty)；
   pb 增加 SYNC_NOTIFY 帧体：SyncNotifyReq{repeated ConvBrief}。
2. Logic.SyncPull 实现：
   SELECT ... FROM %s WHERE conv_id=? AND seq>? ORDER BY seq ASC LIMIT ?（limit 钳制 [1,100]），
   max_seq 取 conversation.last_seq；payload 直接回传。
3. OnlineEvent 处理（internal/logic/online.go）：
   - 写 presence（Comet 已写则跳过）；
   - SELECT conv_id,conv_type,last_seq,unread FROM conversation
     WHERE uid=? AND last_seq>read_seq；
   - 返回列表给 Comet，Comet 组装 SYNC_NOTIFY 帧推给刚上线的连接（该连接 AUTH 完成后触发）。
4. Comet dispatch：SYNC_PULL → gRPC logic.SyncPull → SYNC_RESP 帧（帧 seq 回带）。
5. Logic.MarkRead：UPDATE conversation SET read_seq=?, unread=unread-?,
   updated_at=NOW() WHERE uid=? AND conv_id=? AND read_seq<?（防回退），unread 减量
   = seq - 旧 read_seq，事务内先 SELECT 读旧值或用条件 UPDATE 保证不为负。
6. scripts/wsclient.go：收到 SYNC_NOTIFY 后自动逐会话 SYNC_PULL(100) 直到追平 max_seq，
   全部完成后调 MarkRead，打印 [sync] conv=... got=N。

验收（贴输出）：
- B 离线，A 发 10 条 → B 启动 wsclient，自动打印收齐 10 条且顺序 seq=1..10；
- 之后 SQL 查 B 的 conversation：read_seq=10, unread=0；
- B 在线时 Ctrl+C 断开，A 立刻发 3 条，B 重启 wsclient 自动补拉 3 条；
- go test ./internal/logic/ 覆盖 SyncPull 边界（local_max_seq 超前、limit 钳制、空会话）。
```

---

## S9 群聊

**目标**：群 CRUD、群成员缓存、群消息扇出推送与落库、群会话游标。

**前置依赖**：S0~S8。

**任务清单**：

1. 群管理 HTTP（挂在 account 服务或独立 `cmd/account` 路由组，`/api/v1/groups`）：建群、拉成员、加人、退群（鉴权用 JWT 中间件）。
2. `conv_id` 群聊规范：`g:{group_id}`；Logic SendMsg 支持 conv_type=2：成员校验（`SMEMBERS conv:members:{gid}` 缓存，miss 查 group_member 回填，变更时 DEL）。
3. Job push 扇出：群消息接收者 = 全体成员；遍历成员 `HMGET route`（pipeline 批量），按 comet 分组批量 PushFrames；群消息 Kafka key 改为 `recv_uid`（由 Logic 在写 `msg.push` 时按成员数判断：≤200 成员逐成员写一条副本 key=uid；>200 改写通知信令）。
   - MVP 简化：统一"Logic 层扇出写多份（key=recv_uid）"，`msg.store` 仍单份（key=conv_id）。
4. store 侧：群消息落 message 表一份（读扩散）；conversation UPSERT 所有成员行（批量，分批 100）。
5. 群成员变更事件：Kafka `group.event` topic（加/退群），消费者失效成员缓存 + 给新成员补建 conversation 行。

**验收标准**：

- [ ] 3 人群（A、B、C）：A 发 1 条，B、C 均收到，各未读 +1；
- [ ] C 离线时 A 发 5 条，C 上线自动补齐，B 实时收到；
- [ ] C 退群后 A 发消息 C 不再收到、无 C 的新 conversation 更新。

**AI Prompt**：

```text
实现 LinkIM 群聊。先读 docs/distributed-im-design.md 第 11 节（扩散设计与热点处理）。

1. 群管理 HTTP（internal/account/group.go，路由 /api/v1/groups，JWT 中间件解析 uid）：
   POST / 创建群{name, member_uids[]}（创建者即群主，上限 500 人）；
   GET /:id/members；POST /:id/members {uid}（管理员以上）；DELETE /:id/members/{uid}。
   写 group/group_member 表，同时 DEL conv:members:{gid} 缓存，并向 Kafka topic
   group.event produce {event: join/leave/quit, gid, uid}。
2. internal/service/conv.go 增加 ConvIDForGroup(gid) = "g:{gid}"。
3. Logic.SendMsg 支持 conv_type=2：
   校验发送者是成员（SMEMBER conv:members:{gid}，miss 时查表回填）；
   seq/幂等同单聊；
   写 Kafka：msg.store 一条（key=conv_id）；msg.push 逐成员一条副本
   （key=fmt uid 字符串，保证同一接收者有序；发送者本人跳过），
   成员来自 SMEMBERS。副本 header 附 {"conv-type":"2"}。
4. job push：逻辑天然复用（每条副本接收者唯一，从 header/PbMsg 取 recv_uid——
   因此 PbMsg 或外层信封需增加 recv_uid 字段：封装 Envelope{recv_uid, pbmsg} 作为
   msg.push 的 value，单聊也统一用 Envelope，改造 S6/S7 相关序列化并保持兼容测试通过）。
5. job store：群消息 INSERT message 一份；conversation 批量 UPSERT 全体成员
   （分批 100/批；发送者行 unread+0，其余 +1；用 INSERT ... ON DUPLICATE KEY）。
6. 新增 group.event 消费者（internal/job/group.go）：失效成员缓存；
   join → 为新成员 UPSERT conversation 行；leave/quit → 不删除行（保留历史），
   但标记 muted=1（可选）或仅失效缓存。
7. wsclient 支持指定 conv_id 发送群消息（-conv 参数）。

验收（贴输出）：
- 建 3 人群（脚本或 curl），A 发 1 条：B、C 同时打印收到，SQL 查两行 conversation unread=1；
- C 下线后 A 发 5 条 → C 重启 wsclient 自动补齐 5 条；
- C 退群 → A 再发 1 条 → C 无任何新帧，C 的 conversation unread 不再增长；
- go test：群扇出副本数 = 成员数-1；store 批量 UPSERT 分批正确。
```

---

## S10 高可用加固、可观测性与部署

**目标**：优雅重启 drain、路由对账、死信处理、Prometheus 指标、全链路 docker-compose 一键起、基础 K8s 清单与压测脚本。

**前置依赖**：S0~S9。

**任务清单**：

1. **Comet drain**：SIGTERM → 反注册 alive key → 向所有连接发 `RECONNECT_NOW` 控制帧（新增 cmd 0x0D）→ 30s 后强制关闭退出。wsclient 收到该帧立即重连（抖动 0~3s）。
2. **路由对账**（`internal/job/reconcile.go`，每 5min）：扫描 `route:{uid}` 全量 entry（SCAN），目标 comet 无 alive key 则 HDEL；补偿 Prometheus counter。
3. **DLQ 消费工具**：`scripts/dlqreplay.go` 读 `dlq.*` 人工重放。
4. **Prometheus 指标**（各服务 /metrics）：
   - comet：在线连接数（gauge，分 bucket 汇总）、帧收发速率、AUTH 成功率、慢连接踢除数；
   - logic：SendMsg QPS/耗时、幂等命中率、seq QPS；
   - job：consumer lag（kafka-go client group lag 或 exporter）、投递成功/失败、落库批大小/耗时；
   - 提供 `deployments/grafana/` 基础看板 JSON（在线数、消息 P99、lag 三个核心图）。
5. **docker-compose 全链路**：`deployments/docker-compose-all.yml` 一键起依赖 + 四服务 + prometheus + grafana；Makefile `run-all`。
6. **K8s 清单**（`deployments/k8s/`）：四服务 Deployment/Service + comet 的 preStop sleep 10 + terminationGracePeriodSeconds 40；HPA（logic/job 按 CPU）；ConfigMap 挂配置。
7. **压测脚本** `scripts/bench/main.go`：可配 N 客户端并发连接、每客户端发送速率，输出 ACK P99、端到端 P99（发送→对端收到打点，经 Prometheus 或本地汇总）。

**验收标准**：

- [ ] 双 comet 部署，A、B 分属不同 comet 互发正常（跨网关投递）；
- [ ] 优雅重启 comet-1：客户端 3s 内自动重连到 comet-2，期间 A 发的 5 条消息 B 最终全部收到（补拉）；
- [ ] `curl :9090/metrics` 四服务指标齐全；grafana 看板有数；
- [ ] bench 1000 连接 × 1msg/s 跑 5 分钟：无消息丢失（收发计数对齐）、内存平稳。

**AI Prompt**：

```text
实现 LinkIM 的高可用加固、可观测性与部署产物。先读 docs/distributed-im-design.md 12.2、12.3、16、17 节。

1. Comet drain（internal/comet）：
   - 新增 CmdReconnectNow=0x0D，pb 帧体 ReconnectReq{jitter_ms}；
   - SIGTERM 流程：DEL comet:alive:{addr}（先摘除存活标记）→ 遍历 bucket 向所有连接
     Push 该帧（jitter 0~3000ms 随机）→ 等 30s（或全部断开即止）→ 关闭 listener 退出；
   - wsclient 收到该帧：随机 sleep jitter 后重连（复用同 token/device）。
2. internal/job/reconcile.go：每 5min，SCAN route:*（COUNT 1000），
   对每个 hash 的每个 entry 检查 comet:alive:{addr} 是否存在，不存在则 HDEL；
   记 prometheus counter linkim_reconcile_removed_total。
3. scripts/dlqreplay.go：读 dlq.msg.store / dlq.msg.push，--confirm 参数后逐条重新
   produce 回原 topic。
4. Prometheus 接入（各服务 main 挂 /metrics）：
   - comet：linkim_comet_online（Gauge，bucket 聚合）、linkim_comet_frames_total{cmd,direction}、
     linkim_comet_auth_total{result}、linkim_comet_slow_kick_total；
   - logic：linkim_logic_sendmsg_duration（Histogram）、linkim_logic_sendmsg_total{code}、
     linkim_logic_idem_hit_total；
   - job：linkim_job_push_total{result}、linkim_job_store_batch_duration（Histogram）、
     linkim_job_store_rows_total、lag 通过 prometheus-kafka-exporter（compose 加）；
   - deployments/grafana/linkim.json：三张核心图（在线连接数、SendMsg P99、consumer lag）。
5. deployments/docker-compose-all.yml：依赖（mysql/redis/kafka/kafka-exporter/prometheus/
   grafana）+ account/comet×2/logic×2/job；服务健康检查；Makefile run-all/down-all。
6. deployments/k8s/：namespace linkim；account/logic/job Deployment+Service（replicas 可配，
   logic/job 挂 HPA CPU 70%）；comet Deployment（terminationGracePeriodSeconds: 40，
   preStop: ["sleep","10"]）+ Service；ConfigMap 从 configs/ 生成（kustomize 可选）。
7. scripts/bench/main.go：flag{clients, msgInterval, wsAddrs[]（轮询分摊）,
   duration, accountAddr}；流程：批量注册/登录拿 token → 并发连接+AUTH →
   两两配对互发（A_i 发给 A_(i+1 mod N)）→ 统计：连接成功率、ACK P99、
   端到端（发送时间戳→收到 MSG_PUSH 时间戳）P99、收发条数差值（必须为 0）。
   退出码非 0 表示有丢失。

验收（逐条贴证据）：
- docker-compose-all 起双 comet：A 连 comet-1、B 连 comet-2，互发正常；
- docker restart comet-1 期间 A 发 5 条：B 最终全部收到（推 + 补拉），打印对齐计数；
- prometheus targets 全绿，grafana 三图有数据；
- go run ./scripts/bench -clients 200 -duration 120s：输出 0 丢失、
  ACK P99 与端到端 P99 数值；
- go test ./... 全绿，make lint 通过。
```

---

## 进度追踪清单

| 步骤  | 主题             | 状态  | commit |
| --- | -------------- | --- | ------ |
| S0  | 工程骨架与本地环境      | ☑   |        |
| S1  | 通信协议库          | ☑   |        |
| S2  | 存储层与中间件封装      | ☑   |        |
| S3  | Account 账号服务   | ☑   |        |
| S4  | Logic 骨架与鉴权    | ☐   |        |
| S5  | Comet 长连接接入层   | ☐   |        |
| S6  | 单聊上行链路         | ☐   |        |
| S7  | Job 推送层（投递+落库） | ☐   |        |
| S8  | 离线同步与多端        | ☐   |        |
| S9  | 群聊             | ☐   |        |
| S10 | 高可用/可观测/部署     | ☐   |        |

**完成 S10 后**，系统即达到设计文档 P0（MVP）+ P1 部分能力：单聊、群聊（≤500 人）、离线同步、多端登录、优雅重启、基础可观测。后续 P1/P2 项（已读回执细化、消息撤回、ES 搜索、冷热分层、异地多活）建议按设计文档 18.2 演进路线单独拆步，拆步方式与本文档相同：目标 → 任务 → 验收 → prompt。
