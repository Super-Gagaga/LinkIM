# LinkIM 分布式即时通讯系统

LinkIM 是一个基于 **Go + WebSocket + Kafka + Redis + MySQL** 的分布式即时通讯系统，采用「接入层 comet / 逻辑层 logic / 推送层 job」三层架构。当前已完成 S0–S10 全部里程碑，MVP（P0）+ 部分 P1 能力已落地：支持单聊、群聊、离线同步、多端登录、优雅重启与基础可观测。

## 功能特性

- **单聊 / 群聊**：好友与群成员关系校验、群成员扇出推送（读写扩散混合）
- **消息可靠性**：端到端「不丢、不重、会话内有序」——全链路 ACK、`client_msg_id` 幂等、Kafka 分区有序、消息表唯一键兜底
- **离线同步**：基于会话 `seq` 游标的增量拉取模型（SYNC_PULL），推拉结合，天然支持多端
- **多端登录**：同端互踢、多端独立设备会话、未读数与已读游标
- **运维能力**：comet drain 优雅重启、路由表对账、DLQ 死信重放、Prometheus 指标 + Grafana 看板
- **部署形态**：本地 docker-compose 一键全链路、K8s 清单、压测客户端 `scripts/bench`

## 整体架构

```
客户端(WebSocket WSS) ──► comet 接入网关 ──► Kafka 消息总线 ──► job 推送/落库
      AUTH/收发帧          (WS :8081)        msg.push / msg.store     │
                              │                  ▲                     │
                              ▼                  │                     ▼
                    Redis 路由表/在线状态/seq   logic 无状态业务层   MySQL 分库分表
                     (route/seq/幂等/缓存)      (gRPC :9001)       (message×64 等)
                              ▲                  │
                              └──── account 账号服务(HTTP :8080, JWT) ─┘
```

| 服务 | 职责 | 监听端口 |
| --- | --- | --- |
| account | 注册、登录、JWT 签发与校验、群管理 | HTTP :8080 |
| logic | 无状态业务层：鉴权、幂等、seq 生成、消息校验、双写 Kafka、离线同步 | gRPC :9001 |
| comet | WebSocket 接入、连接管理、心跳、互踢、下行推送 | WS :8081/ws · gRPC :9000 |
| job | Kafka 消费：在线投递（comet 扇出）与异步批量落库、路由对账 | 无监听（Kafka 消费组） |

**中间件**：MySQL 8（持久化）、Redis 7（路由表 / 在线状态 / seq 生成器 / 缓存）、Kafka 3.7（KRaft 单节点，消息通道）。

> 详细设计依据见 [docs/distributed-im-design.md](./docs/distributed-im-design.md)；分步实现过程见 [docs/implementation-guide.md](./docs/implementation-guide.md)。

## 技术栈

| 类别 | 选型 |
| --- | --- |
| 语言 / 构建 | Go 1.26+（go.mod 声明）、Makefile |
| 长连接 | gorilla/websocket + 自定义二进制帧协议（Protobuf 载荷） |
| 服务间通信 | gRPC（buf 生成） |
| 消息队列 | segmentio/kafka-go（幂等生产者、手动提交消费者） |
| 缓存 | redis/go-redis/v9 |
| 存储 | MySQL 8 + jmoiron/sqlx（按 conv_id CRC32 分 64 表） |
| 鉴权 | golang-jwt/v5（HS256，双 token） |
| 其他 | zap 日志、spf13/viper 配置、snowflake 分布式 ID、Prometheus 指标 |

## 目录结构

```
├── api/              # Protobuf 定义（protocol / logic / comet）与 buf 生成配置
├── cmd/              # 四个服务入口：account / logic / comet / job
├── internal/         # 各服务内部实现 + service（conv_id 规范、成员缓存）
│   ├── account/      # HTTP 路由、JWT、群管理 HTTP API
│   ├── comet/        # bucket 连接管理、读写循环、dispatch、gRPC PushFrames/Kick、drain
│   ├── logic/        # VerifyToken / SendMsg / SyncPull / MarkRead 等 gRPC 实现
│   ├── job/          # push & store 消费者、群事件消费、路由对账、DLQ
│   └── service/      # 跨服务共享逻辑
├── pkg/              # 可复用库：protocol/pb/conf/logx/mysqlx/redisx/kafkax/snowflake
├── configs/          # 各服务 YAML 配置（LINKIM_ 前缀环境变量覆盖）
├── deployments/      # docker-compose（依赖 / 全链路）、Dockerfile、prometheus、grafana、k8s/
├── migrations/       # golang-migrate 迁移脚本（含 64 张 message 分表）
├── scripts/          # wsclient 联调客户端、bench 压测、dlqreplay 死信重放
└── docs/             # 架构设计方案 + 分步实现指南
```

## 快速开始

环境要求：Go ≥ 1.26、Docker、golangci-lint、golang-migrate（执行 `make migrate-up` 时需要）。

### 方式一：手动启动（本地调试）

```bash
# 1. 启动依赖中间件（MySQL :23306 / Redis :16379 / Kafka :9092，含 topic 初始化）
make compose-up

# 2. 应用数据库迁移（建表 + 64 张 message 分表）
make migrate-up

# 3. 编译四个服务到 bin/
make build

# 4. 起服务（建议开 4 个终端，按依赖顺序；Windows 下二进制为 .exe）
./bin/account -conf configs/account.yaml   # HTTP :8080
./bin/logic   -conf configs/logic.yaml     # gRPC :9001
./bin/comet   -conf configs/comet.yaml     # WS :8081/ws + gRPC :9000
./bin/job     -conf configs/job.yaml       # 消费 Kafka，无监听
```

> 本机 3306 / 6379 若被占用：compose 将 MySQL 映射到 23306、Redis 映射到 16379（容器内仍为默认端口），配置文件已指向映射端口。

### 方式二：一键启动全链路（含监控栈）

```bash
make run-all   # 构建镜像并启动：依赖 + 四服务(comet×2/logic×2) + migrate + prometheus + grafana
```

对外端口：account :18080、comet WS :18081/:18082、kafka-exporter :19308、Prometheus :19090、Grafana :13000。

### 账号与消息联调

```bash
# 注册两个用户，登录拿 token（复制 access_token / uid 备用）
curl -s -X POST http://127.0.0.1:8080/api/v1/register \
  -H 'Content-Type: application/json' \
  -d '{"username":"alice","password":"pass1234","nickname":"Alice"}'
curl -s -X POST http://127.0.0.1:8080/api/v1/login \
  -H 'Content-Type: application/json' \
  -d '{"username":"alice","password":"pass1234"}'
# → {"code":0,"data":{"uid":..,"access_token":"..","refresh_token":"..","expire_at":..}}

# 开两个终端分别模拟 alice / bob，alice 给 bob 发一条单聊消息
go run ./scripts/wsclient.go -addr ws://127.0.0.1:8081/ws -token <alice_token> -uid <alice_uid> \
  -device a1 -send "你好 bob" -peer <bob_uid>
go run ./scripts/wsclient.go -addr ws://127.0.0.1:8081/ws -token <bob_token>  -uid <bob_uid> -device b1
```

wsclient 会自动完成 AUTH + 周期心跳 + 打印收到的全部帧（含 MSG_PUSH、SYNC_NOTIFY 等），收到 comet 的 RECONNECT_NOW（drain）后按抖动自动重连。常用参数：

| 参数 | 说明 |
| --- | --- |
| `-send <text>` | 发送一条文本消息后继续收帧；配合 `-peer`（单聊）或 `-conv`（群聊 `g:{gid}`） |
| `-n` / `-gap` | 配合 `-send` 控制条数与间隔（压 seq 连续性与幂等用） |
| `-cmid` | 指定 client_msg_id（默认随机，可复现幂等重发实验） |
| `-platform` | 1 手机 / 2 平板 / 3 桌面 / 4 Web（同平台二连触发互踢） |

## HTTP API 一览

账号与群管理接口均挂在 account 服务（:8080），响应统一为 `{code, msg, data?}`。

| 方法 | 路径 | 说明 | 鉴权 |
| --- | --- | --- | --- |
| POST | /api/v1/register | 注册 `{username,password,nickname}` | 无 |
| POST | /api/v1/login | 登录签发 JWT | 无 |
| POST | /internal/v1/verify | 内部 token 校验（logic/comet 调用） | 无 |
| POST | /api/v1/groups | 建群 `{name, member_uids[]}` | JWT |
| GET | /api/v1/groups/{gid}/members | 群成员列表 | JWT |
| POST | /api/v1/groups/{gid}/members | 加人 `{uid}` | JWT |
| DELETE | /api/v1/groups/{gid}/members/{uid} | 移除成员 | JWT |

WebSocket 私有二进制协议（帧 = 1B ver + 2B cmd + 4B seq + 4B len + Protobuf body）定义见 `api/protocol.proto`，实现见 `pkg/protocol`。

## 可靠性要点（实现落地）

- **上行**：logic 做 `client_msg_id` 幂等（Redis `SET NX`）→ 关系校验 → Redis `INCR` 会话 seq → 雪花 msgId → 双写 Kafka `msg.push` / `msg.store`（key = conv_id，分区内有序）→ 回 MSG_SEND_ACK
- **下行**：job 消费 `msg.push` 查 `route:{uid}` 定位 comet，跨网关 gRPC 批量 PushFrames；comet 掉线回退离线，由上线补拉兜底
- **落库**：job 消费 `msg.store` 攒批 `INSERT IGNORE`（`(conv_id, seq)` 唯一键幂等），失败重试后进 DLQ
- **同步**：客户端持有各会话本地 seq，SYNC_PULL 增量拉取追平；上线时 logic 下发未读会话清单（SYNC_NOTIFY）
- **维护**：job 定时对账清理指向失联 comet 的残留路由；`scripts/dlqreplay` 重放死信

## 配置说明

- 配置位于 `configs/{account,logic,comet,job}.yaml`；启动参数 `-conf <path>` 指定。
- 所有键可用 `LINKIM_` 前缀环境变量覆盖，路径 `.` 映射为 `_`，如 `LINKIM_MYSQL_DSN`、`LINKIM_SERVER_ADVERTISE_ADDR`、`LINKIM_JWT_SECRET`。
- 未提供的键回退到 `pkg/conf` 注册的默认值。**生产环境务必用环境变量注入 `jwt.secret`**，并在多实例时保证各服务 `node_id`（snowflake 节点）互异、comet 的 `advertise_addr` 为对端可达地址。

## 测试与开发约定

```bash
make test        # 全量单元测试（-race）
make test-int    # account 集成测试（需 compose-up + migrate-up，连 docker 依赖）
make lint        # golangci-lint（errcheck/govet/staticcheck/revive）
make proto       # 由 api/*.proto 重新生成 pkg/pb（buf 优先，回退 protoc）
make clean       # 清理 bin/
```

- 代码注释使用中文；导出符号的文档注释以英文标识符开头，符合 gofmt/doc 规范
- 设计与实现分别以 `docs/distributed-im-design.md`、`docs/implementation-guide.md` 为权威依据

## 可观测性

- 各服务暴露 `/metrics`：comet（在线连接数、帧收发、AUTH 成功率）、logic（SendMsg QPS/耗时、幂等命中）、job（投递结果、落库批大小/耗时）
- `make run-all` 附带 Prometheus（:19090）+ Grafana（:13000），内置看板 `deployments/grafana/linkim.json`（在线连接数 / SendMsg P99 / consumer lag）
- 全链路日志携带 `trace-id`（= client_msg_id），可按消息检索

## 当前进度

| 步骤 | 主题 | 状态 |
| --- | --- | --- |
| S0 | 工程骨架与本地环境 | ✅ |
| S1 | 通信协议库（protocol / pb） | ✅ |
| S2 | 存储层与中间件封装（mysqlx / redisx / kafkax / snowflake） | ✅ |
| S3 | Account 账号服务（HTTP + JWT） | ✅ |
| S4 | Logic 骨架与鉴权（gRPC） | ✅ |
| S5 | Comet 长连接接入层（WS + 互踢 + 路由表） | ✅ |
| S6 | 单聊上行链路（幂等 / seq / 双写 Kafka） | ✅ |
| S7 | Job 推送层（投递 + 批量落库 + DLQ） | ✅ |
| S8 | 离线同步与多端（seq 游标 / 已读 / 上线补拉） | ✅ |
| S9 | 群聊（群管理 / 成员扇出 / 群事件） | ✅ |
| S10 | 高可用 / 可观测 / 部署（metrics + drain + 对账 + k8s） | ✅ |

**MVP（P0）+ 部分 P1 已完成**，达到设计文档 P0 验收线：单聊、群聊（≤500 人）、离线同步、多端登录、优雅重启、基础可观测。

## 演进路线（P1 余项 / P2）

已读回执细化、消息撤回、输入状态、消息搜索（ES 接入）、历史消息冷热分层、大群优化、异地多活、E2EE 等，详细拆分见 [docs/distributed-im-design.md](./docs/distributed-im-design.md) 第 18.2 节演进路线。
