package account

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/linkim/linkim/pkg/snowflake"
)

// newTestServer 返回挂好中间件的测试服务器与 mock store。
func newTestServer(t *testing.T) (*httptest.Server, *mockStore) {
	t.Helper()
	store := &mockStore{}
	cache := newMemTokenCache()
	ids, err := snowflake.NewNode(2)
	require.NoError(t, err)
	tm := NewTokenManager("test-secret", time.Hour, 24*time.Hour)
	svc := NewService(store, cache, tm, ids)
	srv := httptest.NewServer(NewHandler(svc, nil, zap.NewNop()).Handler())
	t.Cleanup(srv.Close)
	return srv, store
}

// postJSON 发送 POST 请求并解析 envelope 响应。
func postJSON(t *testing.T, url string, body any) (int, map[string]any) {
	t.Helper()
	b, err := json.Marshal(body)
	require.NoError(t, err)
	resp, err := http.Post(url, "application/json", strings.NewReader(string(b)))
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	var env map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&env))
	return resp.StatusCode, env
}

func TestHandlerEndpoints(t *testing.T) {
	hashed := mustHash(t, "password8")
	user := &User{ID: 42, Username: "alice01", PasswordHash: hashed, Nickname: "Alice"}

	t.Run("register invalid param returns 40201", func(t *testing.T) {
		srv, _ := newTestServer(t)
		status, env := postJSON(t, srv.URL+"/api/v1/register",
			RegisterRequest{Username: "ab", Password: "password8"})
		assert.Equal(t, http.StatusBadRequest, status)
		assert.Equal(t, float64(CodeBadParam), env["code"])
	})

	t.Run("register success returns uid", func(t *testing.T) {
		srv, store := newTestServer(t)
		store.On("GetUserByUsername", mock.Anything, "alice01").Return(nil, ErrUserNotFound).Once()
		store.On("CreateUser", mock.Anything, mock.AnythingOfType("*account.User")).Return(nil).Once()

		status, env := postJSON(t, srv.URL+"/api/v1/register",
			RegisterRequest{Username: "alice01", Password: "password8", Nickname: "Alice"})
		assert.Equal(t, http.StatusOK, status)
		assert.Equal(t, float64(CodeOK), env["code"])
		data := env["data"].(map[string]any)
		assert.NotZero(t, data["uid"])
	})

	t.Run("register malformed json returns 40201", func(t *testing.T) {
		srv, _ := newTestServer(t)
		resp, err := http.Post(srv.URL+"/api/v1/register", "application/json", strings.NewReader("{bad"))
		require.NoError(t, err)
		defer func() { _ = resp.Body.Close() }()
		assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
		var env map[string]any
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&env))
		assert.Equal(t, float64(CodeBadParam), env["code"])
	})

	t.Run("login wrong password returns 40101", func(t *testing.T) {
		srv, store := newTestServer(t)
		store.On("GetUserByUsername", mock.Anything, "alice01").Return(user, nil).Once()

		status, env := postJSON(t, srv.URL+"/api/v1/login",
			LoginRequest{Username: "alice01", Password: "WRONGpass"})
		assert.Equal(t, http.StatusUnauthorized, status)
		assert.Equal(t, float64(CodeUnauthorized), env["code"])
	})

	t.Run("login and verify round trip", func(t *testing.T) {
		srv, store := newTestServer(t)
		store.On("GetUserByUsername", mock.Anything, "alice01").Return(user, nil).Twice()

		status, env := postJSON(t, srv.URL+"/api/v1/login",
			LoginRequest{Username: "alice01", Password: "password8"})
		require.Equal(t, http.StatusOK, status)
		data := env["data"].(map[string]any)
		access := data["access_token"].(string)
		refresh := data["refresh_token"].(string)
		assert.NotEmpty(t, access)
		assert.NotEmpty(t, refresh)

		status, env = postJSON(t, srv.URL+"/internal/v1/verify", map[string]string{"token": access})
		assert.Equal(t, http.StatusOK, status)
		assert.Equal(t, float64(CodeOK), env["code"])
		vdata := env["data"].(map[string]any)
		assert.Equal(t, true, vdata["valid"])
		assert.Equal(t, float64(42), vdata["uid"])
	})

	t.Run("verify forged token returns 40101 with valid=false", func(t *testing.T) {
		srv, _ := newTestServer(t)
		status, env := postJSON(t, srv.URL+"/internal/v1/verify", map[string]string{"token": "forged"})
		assert.Equal(t, http.StatusOK, status)
		assert.Equal(t, float64(CodeUnauthorized), env["code"])
		vdata, ok := env["data"].(map[string]any)
		require.True(t, ok, "verify 失败也应返回 data.valid=false")
		assert.Equal(t, false, vdata["valid"])
	})

	t.Run("unknown path returns 404", func(t *testing.T) {
		srv, _ := newTestServer(t)
		resp, err := http.Post(srv.URL+"/api/v1/nope", "application/json", strings.NewReader("{}"))
		require.NoError(t, err)
		defer func() { _ = resp.Body.Close() }()
		assert.Equal(t, http.StatusNotFound, resp.StatusCode)
	})
}

// TestRecoverMiddleware 验证 panic 会被转换为 500 业务响应。
func TestRecoverMiddleware(t *testing.T) {
	boom := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		panic("boom")
	})
	h := NewHandler(nil, nil, zap.NewNop())
	srv := httptest.NewServer(h.recoverMiddleware(boom))
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/x", "application/json", strings.NewReader("{}"))
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
	var env map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&env))
	assert.Equal(t, float64(CodeStorageError), env["code"])
}
