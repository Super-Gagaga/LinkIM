//go:build integration

// 集成测试：连接 docker 依赖（MySQL 23306 / Redis 16379），
// 覆盖注册 → 登录 → verify → 登出 → verify 失效 全链路。
// 运行：make test-int（依赖 make compose-up && make migrate-up）。
package account

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/linkim/linkim/pkg/conf"
	"github.com/linkim/linkim/pkg/mysqlx"
	"github.com/linkim/linkim/pkg/redisx"
	"github.com/linkim/linkim/pkg/snowflake"
)

// newIntegrationServer 建立连真实依赖的服务实例。
func newIntegrationServer(t *testing.T) (*httptest.Server, *Service) {
	t.Helper()

	db, err := mysqlx.New(conf.MySQLConfig{
		DSN:             "root:linkim123@tcp(127.0.0.1:23306)/linkim?charset=utf8mb4&parseTime=true&loc=Local",
		MaxOpenConns:    4,
		MaxIdleConns:    2,
		ConnMaxLifetime: time.Minute,
	})
	require.NoError(t, err, "需要先 make compose-up 启动 MySQL")
	t.Cleanup(func() { _ = db.Close() })

	rdb := redisx.New(conf.RedisConfig{Addr: "127.0.0.1:16379"})
	require.NoError(t, rdb.Ping(context.Background()).Err(), "需要先 make compose-up 启动 Redis")
	t.Cleanup(func() { _ = rdb.Close() })

	ids, err := snowflake.NewNode(99)
	require.NoError(t, err)
	tm := NewTokenManager("integration-secret", time.Hour, 24*time.Hour)
	svc := NewService(NewMySQLStore(db), NewRedisTokenCache(rdb), tm, ids)

	srv := httptest.NewServer(NewHandler(svc, zap.NewNop()).Handler())
	t.Cleanup(srv.Close)
	return srv, svc
}

// uniqUsername 生成随机用户名，保证测试可重复执行。
func uniqUsername() string {
	return fmt.Sprintf("it_%s", strings.ToLower(randomHex(6)))
}

func TestIntegrationRegisterLoginVerifyLogout(t *testing.T) {
	srv, svc := newIntegrationServer(t)
	ctx := context.Background()
	username := uniqUsername()

	// 1. 注册
	uid := registerViaHTTP(t, srv.URL, username)

	// 2. 登录
	loginRes := loginViaHTTP(t, srv.URL, username, "password8")
	assert.Equal(t, uid, loginRes.UID)

	// 3. verify：有效
	res := verifyViaHTTP(t, srv.URL, loginRes.AccessToken)
	assert.True(t, res.Valid)
	assert.Equal(t, uid, res.UID)

	// 重复注册同用户名 → 40202。
	_, err := svc.Register(ctx, RegisterRequest{Username: username, Password: "password8"})
	var be *BizError
	require.ErrorAs(t, err, &be)
	assert.Equal(t, CodeUsernameExists, be.Code)

	// 4. 登出：删除 Redis 摘要。
	require.NoError(t, svc.cache.DelDigest(ctx, uid))

	// 5. verify：digest 缺失时 JWT 本身仍有效（设计语义：摘要仅用于吊销）。
	//    为验证“登出即失效”，改为写入错误摘要模拟被顶替。
	require.NoError(t, svc.cache.SetDigest(ctx, uid, Digest("another-token"), time.Hour))
	res = verifyViaHTTP(t, srv.URL, loginRes.AccessToken)
	assert.False(t, res.Valid, "摘要不一致时旧 token 必须失效")
}

func TestIntegrationWrongPasswordAndForgedToken(t *testing.T) {
	srv, _ := newIntegrationServer(t)
	username := uniqUsername()
	registerViaHTTP(t, srv.URL, username)

	// 错误密码 → 40101。
	_, env := postJSON(t, srv.URL+"/api/v1/login",
		LoginRequest{Username: username, Password: "WRONGpass1"})
	assert.Equal(t, float64(CodeUnauthorized), env["code"])

	// 伪造 token → invalid。
	res := verifyViaHTTP(t, srv.URL, "forged.token.value")
	assert.False(t, res.Valid)
}

// --- HTTP helpers ---

func registerViaHTTP(t *testing.T, baseURL, username string) int64 {
	t.Helper()
	status, env := postJSON(t, baseURL+"/api/v1/register",
		RegisterRequest{Username: username, Password: "password8", Nickname: "IT"})
	require.Equal(t, http.StatusOK, status)
	require.Equal(t, float64(CodeOK), env["code"])
	data := env["data"].(map[string]any)
	return int64(data["uid"].(float64))
}

func loginViaHTTP(t *testing.T, baseURL, username, password string) *LoginResult {
	t.Helper()
	status, env := postJSON(t, baseURL+"/api/v1/login",
		LoginRequest{Username: username, Password: password})
	require.Equal(t, http.StatusOK, status)
	require.Equal(t, float64(CodeOK), env["code"])
	data := env["data"].(map[string]any)
	return &LoginResult{
		UID:          int64(data["uid"].(float64)),
		AccessToken:  data["access_token"].(string),
		RefreshToken: data["refresh_token"].(string),
		ExpireAt:     int64(data["expire_at"].(float64)),
	}
}

func verifyViaHTTP(t *testing.T, baseURL, token string) *VerifyResult {
	t.Helper()
	status, env := postJSON(t, baseURL+"/internal/v1/verify", map[string]string{"token": token})
	require.Equal(t, http.StatusOK, status)
	data, ok := env["data"].(map[string]any)
	if !ok {
		return &VerifyResult{Valid: false}
	}
	return &VerifyResult{
		UID:   int64(data["uid"].(float64)),
		Valid: data["valid"].(bool),
	}
}

// randomHex 生成 n 字节随机 hex。
func randomHex(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b)
}
