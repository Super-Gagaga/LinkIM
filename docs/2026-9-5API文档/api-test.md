# LinkIM Account API 测试文档

> 依据 [openapi.yaml](./openapi.yaml)(OpenAPI 3.0.3)与 `internal/account` 实际实现编写。
> 覆盖注册、登录、内部校验、群管理共 7 个端点,共 26 个用例,含正向与异常分支。

## 1. 环境准备

```bash
# 启动依赖(MySQL :23306 / Redis :16379 / Kafka :9092)并应用迁移
make compose-up
make migrate-up

# 启动 account 服务(另开终端)
./bin/account.exe -conf configs/account.yaml   # HTTP :8080
```

- **Base URL**:`http://127.0.0.1:8080`(docker 全链路方式为 `:18080`)
- **Content-Type**:`application/json`(除 DELETE 外均需请求体)
- **鉴权**:群管理接口需请求头 `Authorization: Bearer <access_token>`

## 2. 通用约定

### 2.1 响应信封

所有接口统一返回:

```json
{"code": 0, "msg": "ok", "data": {}}
```

### 2.2 业务错误码

| 业务码 | 含义 | HTTP 状态 |
| --- | --- | --- |
| 0 | 成功 | 200 |
| 40101 | 鉴权失败(密码错误 / token 无效、过期、被顶替) | 401 |
| 40201 | 请求参数格式错误 | 400 |
| 40202 | 用户名已被注册 | 400 |
| 40203 | 群名非法(长度须 1~64) | 400 |
| 40204 | 群不存在 | 404 |
| 40302 | 无权限操作(非管理员/非本人) | 403 |
| 40303 | 群成员超过上限 500 | 400 |
| 50101 | 数据库存储错误 | 500 |
| 50201 | Redis 中间件错误 | 503 |

### 2.3 示例测试数据

| 角色 | 用户名 | 密码 | UID(雪花,运行时生成) |
| --- | --- | --- | --- |
| 用户 A | `alice` | `pass1234` | `7345000000000001001` |
| 用户 B | `bob` | `pass1234` | `7345000000000001002` |
| 用户 C | `carol` | `pass1234` | `7345000000000001003` |

> 下文示例中的 UID / gid / token 均为示意值,实测时以接口实际返回为准。
> access token 有效期 2h(`jwt.access_ttl`),refresh token 30d;`expire_at` 为 Unix 秒。

## 3. 用例明细

### 3.1 POST /api/v1/register — 注册

| 用例 | 场景 | 预期 |
| --- | --- | --- |
| TC-REG-01 | 正常注册 | 200,`code=0`,`data.uid` > 0 |
| TC-REG-02 | 用户名过短(`abc`,<4 字符) | 400,`code=40201` |
| TC-REG-03 | 用户名含非法字符(`al-ice`) | 400,`code=40201` |
| TC-REG-04 | 密码过短(`1234567`,<8 字符) | 400,`code=40201` |
| TC-REG-05 | 重复注册同名用户 | 400,`code=40202` |
| TC-REG-06 | 请求体非合法 JSON | 400,`code=40201` |

```bash
# TC-REG-01 正常注册
curl -s -X POST http://127.0.0.1:8080/api/v1/register \
  -H 'Content-Type: application/json' \
  -d '{"username":"alice","password":"pass1234","nickname":"Alice"}'
# → {"code":0,"msg":"ok","data":{"uid":7345000000000001001}}

# TC-REG-02 用户名过短
curl -s -X POST http://127.0.0.1:8080/api/v1/register \
  -H 'Content-Type: application/json' \
  -d '{"username":"abc","password":"pass1234"}'
# → {"code":40201,"msg":"用户名长度须为 4~32 个字符"}

# TC-REG-03 用户名含非法字符
curl -s -X POST http://127.0.0.1:8080/api/v1/register \
  -H 'Content-Type: application/json' \
  -d '{"username":"al-ice","password":"pass1234"}'
# → {"code":40201,"msg":"用户名仅允许字母、数字与下划线"}

# TC-REG-05 重复注册(先按 TC-REG-01 注册一次再执行)
curl -s -X POST http://127.0.0.1:8080/api/v1/register \
  -H 'Content-Type: application/json' \
  -d '{"username":"alice","password":"pass1234"}'
# → HTTP 400 {"code":40202,"msg":"用户名已被注册"}
```

### 3.2 POST /api/v1/login — 登录

| 用例 | 场景 | 预期 |
| --- | --- | --- |
| TC-LOG-01 | 正确用户名密码 | 200,`code=0`,`data` 含 `uid/access_token/refresh_token/expire_at` |
| TC-LOG-02 | 密码错误 | 401,`code=40101` |
| TC-LOG-03 | 用户不存在(防枚举,与密码错误同文案) | 401,`code=40101` |
| TC-LOG-04 | 用户名或密码为空 | 400,`code=40201` |
| TC-LOG-05 | 二次登录顶替:alice 连续登录两次后,用旧 token 调群接口 | 401,`code=40101`,msg=`token 已失效` |

```bash
# TC-LOG-01 正常登录 → 记下 access_token(下称 ALICE_TOKEN)与 uid
curl -s -X POST http://127.0.0.1:8080/api/v1/login \
  -H 'Content-Type: application/json' \
  -d '{"username":"alice","password":"pass1234"}'
# → {"code":0,"msg":"ok","data":{"uid":7345000000000001001,
#      "access_token":"eyJhbGciOi...","refresh_token":"eyJhbGciOi...",
#      "expire_at":1787653200}}

# TC-LOG-02 密码错误
curl -s -X POST http://127.0.0.1:8080/api/v1/login \
  -H 'Content-Type: application/json' -d '{"username":"alice","password":"wrongpass"}'
# → HTTP 401 {"code":40101,"msg":"用户名或密码错误"}

# TC-LOG-03 用户不存在
curl -s -X POST http://127.0.0.1:8080/api/v1/login \
  -H 'Content-Type: application/json' -d '{"username":"nouser","password":"pass1234"}'
# → HTTP 401 {"code":40101,"msg":"用户名或密码错误"}
```

同理注册并登录 bob、carol,得到 `BOB_TOKEN`(uid `...1002`)、`CAROL_TOKEN`(uid `...1003`)。

### 3.3 POST /internal/v1/verify — 内部 token 校验

| 用例 | 场景 | 预期 |
| --- | --- | --- |
| TC-VER-01 | 有效 access token | 200,`code=0`,`data.valid=true`,`data.uid` 正确 |
| TC-VER-02 | 篡改签名的 token | 200,`code=40101`,`data.valid=false` |
| TC-VER-03 | 过期 token(可把 `LINKIM_JWT_ACCESS_TTL=1ns` 重启后测) | 200,`code=40101`,`data.valid=false` |
| TC-VER-04 | 误用 refresh token | 200,`code=40101`,`data.valid=false`(类型不符) |

```bash
# TC-VER-01 有效 token
curl -s -X POST http://127.0.0.1:8080/internal/v1/verify \
  -H 'Content-Type: application/json' -d "{\"token\":\"$ALICE_TOKEN\"}"
# → {"code":0,"msg":"ok","data":{"uid":7345000000000001001,"valid":true}}

# TC-VER-02 篡改签名
curl -s -X POST http://127.0.0.1:8080/internal/v1/verify \
  -H 'Content-Type: application/json' \
  -d '{"token":"eyJhbGciOiJIUzI1NiJ9.x.y"}'
# → HTTP 200 {"code":40101,"msg":"token 无效或已过期","data":{"uid":0,"valid":false}}
```

> 注意:校验失败也返回 HTTP 200,调用方须按 body 的 `code`/`valid` 判断。

### 3.4 POST /api/v1/groups — 建群

| 用例 | 场景 | 预期 |
| --- | --- | --- |
| TC-GRP-01 | alice 建群并带 2 个初始成员 | 200,`code=0`,`data.gid` > 0 |
| TC-GRP-02 | 群名为空或超 64 字符 | 400,`code=40203` |
| TC-GRP-03 | member_uids 含创建者本人 / 重复 uid | 200,建群成功(自动去重) |
| TC-GRP-04 | 未带 token | 401,`code=40101`,msg=`缺少 Bearer token` |
| TC-GRP-05 | 带无效 token | 401,`code=40101` |
| TC-GRP-06 | member_uids 超 499 个(总成员 > 500) | 400,`code=40303` |

```bash
# TC-GRP-01 建群 → 记下 gid
curl -s -X POST http://127.0.0.1:8080/api/v1/groups \
  -H "Authorization: Bearer $ALICE_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"name":"项目交流群","member_uids":[7345000000000001002,7345000000000001003]}'
# → {"code":0,"msg":"ok","data":{"gid":7345000000000002001}}

# TC-GRP-02 群名为空
curl -s -X POST http://127.0.0.1:8080/api/v1/groups \
  -H "Authorization: Bearer $ALICE_TOKEN" \
  -H 'Content-Type: application/json' -d '{"name":"  ","member_uids":[]}'
# → HTTP 400 {"code":40203,"msg":"群名长度须为 1~64"}

# TC-GRP-04 缺少 token
curl -s -X POST http://127.0.0.1:8080/api/v1/groups \
  -H 'Content-Type: application/json' -d '{"name":"项目交流群"}'
# → HTTP 401 {"code":40101,"msg":"缺少 Bearer token"}
```

### 3.5 GET /api/v1/groups/{gid}/members — 成员列表

| 用例 | 场景 | 预期 |
| --- | --- | --- |
| TC-LST-01 | 查询存在的群 | 200,`members` 含群主(role=2)与初始成员(role=0),按 uid 升序 |
| TC-LST-02 | 查询不存在的 gid | 404,`code=40204` |
| TC-LST-03 | gid 非数字(`abc`) | 400,`code=40201` |

```bash
# TC-LST-01 成员列表
curl -s http://127.0.0.1:8080/api/v1/groups/7345000000000002001/members \
  -H "Authorization: Bearer $ALICE_TOKEN"
# → {"code":0,"msg":"ok","data":{"members":[
#      {"uid":7345000000000001001,"role":2,"join_at":"2026-09-05T01:20:30+08:00"},
#      {"uid":7345000000000001002,"role":0,"join_at":"2026-09-05T01:20:30+08:00"},
#      {"uid":7345000000000001003,"role":0,"join_at":"2026-09-05T01:20:30+08:00"}]}}

# TC-LST-02 群不存在
curl -s http://127.0.0.1:8080/api/v1/groups/999999/members \
  -H "Authorization: Bearer $ALICE_TOKEN"
# → HTTP 404 {"code":40204,"msg":"群不存在"}
```

### 3.6 POST /api/v1/groups/{gid}/members — 添加成员

| 用例 | 场景 | 预期 |
| --- | --- | --- |
| TC-ADD-01 | 群主(bob 被设为管理员时)拉新成员 dave | 200,`code=0` |
| TC-ADD-02 | 重复添加已在群成员 | 200,幂等成功 |
| TC-ADD-03 | 普通成员拉人 | 403,`code=40302`,msg=`仅管理员可拉人` |
| TC-ADD-04 | uid ≤ 0 或缺失 | 400,`code=40201`,msg=`uid 非法` |
| TC-ADD-05 | 群不存在 | 404,`code=40204` |

```bash
# TC-ADD-03 普通成员(bob,role=0)拉人
curl -s -X POST http://127.0.0.1:8080/api/v1/groups/7345000000000002001/members \
  -H "Authorization: Bearer $BOB_TOKEN" \
  -H 'Content-Type: application/json' -d '{"uid":7345000000000001004}'
# → HTTP 403 {"code":40302,"msg":"仅管理员可拉人"}

# TC-ADD-01 群主 alice 拉人
curl -s -X POST http://127.0.0.1:8080/api/v1/groups/7345000000000002001/members \
  -H "Authorization: Bearer $ALICE_TOKEN" \
  -H 'Content-Type: application/json' -d '{"uid":7345000000000001004}'
# → {"code":0,"msg":"ok"}
```

### 3.7 DELETE /api/v1/groups/{gid}/members/{uid} — 移除成员 / 退群

| 用例 | 场景 | 预期 |
| --- | --- | --- |
| TC-DEL-01 | 群主移除普通成员 | 200,`code=0`(发 leave 事件) |
| TC-DEL-02 | 成员本人退群 | 200,`code=0`(发 quit 事件) |
| TC-DEL-03 | 普通成员移除他人 | 403,`code=40302`,msg=`仅管理员可移除成员` |
| TC-DEL-04 | 移除不在群内的 uid | 200,幂等成功 |
| TC-DEL-05 | 群不存在 | 404,`code=40204` |

```bash
# TC-DEL-03 普通成员 bob 尝试移除 carol
curl -s -X DELETE http://127.0.0.1:8080/api/v1/groups/7345000000000002001/members/7345000000000001003 \
  -H "Authorization: Bearer $BOB_TOKEN"
# → HTTP 403 {"code":40302,"msg":"仅管理员可移除成员"}

# TC-DEL-02 carol 本人退群
curl -s -X DELETE http://127.0.0.1:8080/api/v1/groups/7345000000000002001/members/7345000000000001003 \
  -H "Authorization: Bearer $CAROL_TOKEN"
# → {"code":0,"msg":"ok"}
```

## 4. 端到端场景(冒烟)

按顺序串起全部接口,任何一步失败即中断:

```bash
set -e
BASE=http://127.0.0.1:8080

# 1) 注册 alice / bob(carol 同理)
curl -sf -X POST $BASE/api/v1/register -H 'Content-Type: application/json' \
  -d '{"username":"alice","password":"pass1234","nickname":"Alice"}'

# 2) 登录取 token
ALICE_TOKEN=$(curl -sf -X POST $BASE/api/v1/login -H 'Content-Type: application/json' \
  -d '{"username":"alice","password":"pass1234"}' | python -c "import sys,json;print(json.load(sys.stdin)['data']['access_token'])")
BOB_UID=$(curl -sf -X POST $BASE/api/v1/login -H 'Content-Type: application/json' \
  -d '{"username":"bob","password":"pass1234"}' | python -c "import sys,json;print(json.load(sys.stdin)['data']['uid'])")

# 3) 建群(alice + bob)
GID=$(curl -sf -X POST $BASE/api/v1/groups -H "Authorization: Bearer $ALICE_TOKEN" \
  -H 'Content-Type: application/json' \
  -d "{\"name\":\"e2e群\",\"member_uids\":[$BOB_UID]}" | python -c "import sys,json;print(json.load(sys.stdin)['data']['gid'])")

# 4) 查成员(应 2 人,alice role=2)
curl -sf $BASE/api/v1/groups/$GID/members -H "Authorization: Bearer $ALICE_TOKEN"

# 5) bob 退群
curl -sf -X DELETE $BASE/api/v1/groups/$GID/members/$BOB_UID \
  -H "Authorization: Bearer $(curl -sf -X POST $BASE/api/v1/login -H 'Content-Type: application/json' \
        -d '{"username":"bob","password":"pass1234"}' | python -c "import sys,json;print(json.load(sys.stdin)['data']['access_token'])")"

echo "E2E OK"
```

## 5. 注意事项

- **重复执行**:注册类用例重复跑会命中 40202,建议每次换用户名后缀(如 `alice_$(date +%s)`)或用 `make compose-down -v` 后重建。
- **token 顶替**:同一用户重复登录会使旧 access token 失效(Redis 摘要单点顶替),脚本里并行用多个 token 时注意重新登录。
- **集成测试**:项目自带 `make test-int`(account 集成测试,依赖 compose 环境),可作为本文档用例的自动化回归。
- 所有 `code`/`msg` 以 `internal/account/errcode.go`、`group.go` 为准;本文示例值在默认配置(`configs/account.yaml`)下成立。
