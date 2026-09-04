# LinkIM 分布式即时通讯系统

LinkIM 是一个参考企业级架构设计的分布式 IM（即时通讯）系统，使用 Go 实现。当前处于工程骨架阶段（实施指南 S0 已完成），后续按里程碑逐步补齐各层实现。

## 整体架构

```
客户端(WebSocket) ──► comet 接入网关 ──► Kafka ──► job 消费落库
                        │                  ▲
                        ▼                  │
        Redis 路由表/在线状态          logic 无状态业务层 ◄── account 账号服务(HTTP+JWT)
                        │                  │
                        └───── MySQL(分库分表) ─────┘
```

| 服务 | 职责 | 监听端口 |
| --- | --- | --- |
| account | 注册、登录、JWT 签发与校验 | HTTP :8080 |
| logic | 无状态业务逻辑：消息校验、seq 生成、投递 Kafka | gRPC :9001 |
| comet | WebSocket 接入、连接管理、消息推送 | WS :8081 / gRPC :9000 |
| job | Kafka 消费：推送到 comet、异步批量落库 | 无监听 |

依赖中间件：MySQL 8（存储）、Redis 7（路由表/在线状态）、Kafka 3.7（消息通道，KRaft 单节点）。

## 目录结构

```
├── api/              # Protobuf 协议定义（protocol.proto）与 buf 代码生成配置
├── cmd/              # 各服务启动入口（account / logic / comet / job）
├── configs/          # 各服务 YAML 配置（支持 LINKIM_ 前缀环境变量覆盖）
├── deployments/      # docker-compose：本地依赖环境（MySQL/Redis/Kafka + topic 初始化）
├── docs/             # 设计方案与实施指南（开发的权威依据）
├── internal/         # 各服务内部实现（account / comet / job / logic / service）
├── migrations/       # 数据库迁移脚本（S2 起提供）
├── pkg/              # 可复用库：conf/logx/protocol/pb/mysqlx/redisx/kafkax/snowflake
└── scripts/          # 辅助脚本
```

## 快速开始

环境要求：Go ≥ 1.22、Docker、golangci-lint、golang-migrate（后续迁移用）。

```bash
# 1. 启动本地依赖（MySQL :23306 / Redis :16379 / Kafka :9092，含 topic 初始化）
make compose-up

# 2. 编译四个服务到 bin/
make build

# 3. 运行单元测试（含 race 检测）
make test

# 4. 运行集成测试（连接 docker 依赖，需先 make compose-up && make migrate-up）
make test-int

# 5. 启动单个服务（示例：account）
./bin/account.exe -conf configs/account.yaml

# 6. WebSocket 联调客户端（自动 AUTH + 周期心跳，打印收到的所有帧）
go run ./scripts/wsclient.go -addr ws://127.0.0.1:8081/ws -token <access_token> -uid <uid>
```

> 端口说明：本机 3306/3307/6379 已被占用，compose 将 MySQL 映射到 23306、Redis 映射到 16379（容器内仍为默认端口）。

## 配置说明

- 配置文件位于 `configs/`，按服务区分；
- 所有键都可用 `LINKIM_` 前缀环境变量覆盖，键路径中的 `.` 映射为 `_`，例如 `LINKIM_MYSQL_DSN`、`LINKIM_SERVER_HTTP_PORT`；
- 文件与环境变量均未提供的键回退到 `pkg/conf` 中注册的默认值。

## 开发约定

- 代码注释使用中文，方便后续协作与维护；导出符号的文档注释以 `Package X` / 函数名等英文标识符开头，符合 gofmt/doc 规范；
- 提交前执行 `gofmt`、`go vet ./...` 与 `make lint`；
- 设计与实施步骤以 `docs/distributed-im-design.md`（架构）与 `docs/implementation-guide.md`（分阶段实施清单）为准。

## 当前进度

| 步骤 | 主题 | 状态 |
| --- | --- | --- |
| S0 | 工程骨架与本地环境 | ✅ |
| S1 | 通信协议库（protocol / pb） | ✅ |
| S2 | 存储层与中间件封装（mysqlx / redisx / kafkax） | ✅ |
| S3 | Account 账号服务（HTTP + JWT） | ✅ |
| S4 | Logic 骨架与鉴权（gRPC） | ✅ |
| S5 | Comet 长连接接入层（WS + 互踢 + 路由表） | ✅ |
| S6 | 单聊上行链路（幂等 / seq / 双写 Kafka） | ✅ |
| S7+ | Job 推送层等服务实现 | ⬜ |

详细里程碑见 `docs/implementation-guide.md`。
