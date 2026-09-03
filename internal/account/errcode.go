// Package account 实现账号服务业务：注册、登录、JWT 签发与校验。
package account

import "fmt"

// 业务错误码分段（统一约定）：
//
//	0        成功
//	401xx    鉴权类（密码错误 / token 无效 / 已登出）
//	402xx    参数类（格式非法 / 用户名冲突）
//	403xx    关系类（预留给消息链路）
//	501xx    存储类
//	502xx    中间件类（Redis/Kafka）
const (
	CodeOK             = 0
	CodeUnauthorized   = 40101 // 鉴权失败：密码错误、token 无效或过期、已被登出
	CodeBadParam       = 40201 // 请求参数格式错误
	CodeUsernameExists = 40202 // 用户名已被注册
	CodeStorageError   = 50101 // 数据库存储错误
	CodeRedisError     = 50201 // Redis 中间件错误
)

// BizError 是携带业务错误码的错误，handler 层据此生成对外响应，
// 保证不向客户端泄漏内部错误细节。
type BizError struct {
	Code int    `json:"code"`
	Msg  string `json:"msg"`
}

// Error 实现 error 接口。
func (e *BizError) Error() string { return fmt.Sprintf("account: code=%d msg=%s", e.Code, e.Msg) }

// bizErr 构造 *BizError。
func bizErr(code int, msg string) *BizError { return &BizError{Code: code, Msg: msg} }
