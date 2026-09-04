package logic

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/linkim/linkim/pkg/pb"
)

// memCache 是 Cache 的内存实现。
type memCache struct {
	mu     sync.Mutex
	kv     map[string]string
	getErr error
}

func newMemCache() *memCache { return &memCache{kv: map[string]string{}} }

func (c *memCache) Get(_ context.Context, key string) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.getErr != nil {
		return "", c.getErr
	}
	return c.kv[key], nil
}

func (c *memCache) Set(_ context.Context, key, val string, _ time.Duration) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.kv[key] = val
	return nil
}

// stubVerifier 是 Verifier 的可编程 stub（uid 为回源返回的归属 uid）。
type stubVerifier struct {
	mu    sync.Mutex
	uid   int64
	valid bool
	err   error
	calls int
}

func (v *stubVerifier) Verify(_ context.Context, _ int64, _ string) (int64, bool, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.calls++
	return v.uid, v.valid, v.err
}

func (v *stubVerifier) callCount() int {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.calls
}

func newTestServer(cache Cache, verifier Verifier) *Server {
	return NewServer(Deps{Cache: cache, Verifier: verifier, Logger: zap.NewNop()})
}

// TestTokenCacheKey 钉死缓存 key 格式。
func TestTokenCacheKey(t *testing.T) {
	k1 := TokenCacheKey(7, "token")
	k2 := TokenCacheKey(7, "other")
	k3 := TokenCacheKey(8, "token")

	assert.Contains(t, k1, "tokencache:7:")
	assert.NotEqual(t, k1, k2, "不同 token 摘要不同")
	assert.NotEqual(t, k1, k3, "不同 uid 不同 key")
	// 摘要是 sha256 前 16 字节的 hex（32 字符）。
	parts := strings.SplitN(k1, ":", 3)
	require.Len(t, parts, 3)
	assert.Len(t, parts[2], 32)
}

// TestVerifyTokenFlow 覆盖缓存命中/回源/回填/不可达/降级/uid 一致性。
func TestVerifyTokenFlow(t *testing.T) {
	ctx := context.Background()

	t.Run("cache miss falls through to account and backfills", func(t *testing.T) {
		cache := newMemCache()
		verifier := &stubVerifier{uid: 7, valid: true}
		svc := newTestServer(cache, verifier)

		uid, valid, code := svc.verifyTokenFlow(ctx, 7, "tok")
		assert.Equal(t, int64(7), uid)
		assert.True(t, valid)
		assert.Equal(t, int32(CodeOK), code)
		assert.Equal(t, 1, verifier.callCount())
		assert.Equal(t, "1", cache.kv[TokenCacheKey(7, "tok")])

		// 第二次命中缓存，不再回源。
		_, valid, code = svc.verifyTokenFlow(ctx, 7, "tok")
		assert.True(t, valid)
		assert.Equal(t, int32(CodeOK), code)
		assert.Equal(t, 1, verifier.callCount())
	})

	t.Run("uid zero is resolved from account", func(t *testing.T) {
		cache := newMemCache()
		verifier := &stubVerifier{uid: 42, valid: true}
		svc := newTestServer(cache, verifier)

		// 调用方未声明 uid（comet 场景），回源解析出真实 uid。
		// 缓存 key 退化为 uid=0 前缀，comet 后续重连会再次回源。
		uid, valid, code := svc.verifyTokenFlow(ctx, 0, "tok")
		assert.Equal(t, int64(42), uid)
		assert.True(t, valid)
		assert.Equal(t, int32(CodeOK), code)
	})

	t.Run("declared uid mismatching token owner is rejected", func(t *testing.T) {
		cache := newMemCache()
		verifier := &stubVerifier{uid: 42, valid: true}
		svc := newTestServer(cache, verifier)

		_, valid, code := svc.verifyTokenFlow(ctx, 7, "tok")
		assert.False(t, valid)
		assert.Equal(t, int32(CodeInvalidTok), code)
		// 不一致按无效缓存，防止枚举探测。
		assert.Equal(t, "0", cache.kv[TokenCacheKey(7, "tok")])
	})

	t.Run("invalid token cached as 0", func(t *testing.T) {
		cache := newMemCache()
		verifier := &stubVerifier{uid: 7, valid: false}
		svc := newTestServer(cache, verifier)

		_, valid, code := svc.verifyTokenFlow(ctx, 7, "bad")
		assert.False(t, valid)
		assert.Equal(t, int32(CodeInvalidTok), code)
		assert.Equal(t, "0", cache.kv[TokenCacheKey(7, "bad")])

		// 命中负缓存。
		_, _, code = svc.verifyTokenFlow(ctx, 7, "bad")
		assert.Equal(t, int32(CodeInvalidTok), code)
		assert.Equal(t, 1, verifier.callCount())
	})

	t.Run("account unreachable returns 50102 without caching", func(t *testing.T) {
		cache := newMemCache()
		verifier := &stubVerifier{err: errors.New("dial tcp: refused")}
		svc := newTestServer(cache, verifier)

		_, valid, code := svc.verifyTokenFlow(ctx, 7, "tok")
		assert.False(t, valid)
		assert.Equal(t, int32(CodeAccountDown), code)
		assert.Empty(t, cache.kv, "不可达不应写缓存")

		// 恢复后可正常回源（未污染）。
		verifier.mu.Lock()
		verifier.err = nil
		verifier.valid = true
		verifier.uid = 7
		verifier.mu.Unlock()
		_, valid, code = svc.verifyTokenFlow(ctx, 7, "tok")
		assert.True(t, valid)
		assert.Equal(t, int32(CodeOK), code)
	})

	t.Run("redis read error degrades to account", func(t *testing.T) {
		cache := newMemCache()
		cache.getErr = errors.New("redis down")
		verifier := &stubVerifier{uid: 7, valid: true}
		svc := newTestServer(cache, verifier)

		_, valid, code := svc.verifyTokenFlow(ctx, 7, "tok")
		assert.True(t, valid)
		assert.Equal(t, int32(CodeOK), code)
		assert.Equal(t, 1, verifier.callCount())
	})

	t.Run("different uid does not share cache", func(t *testing.T) {
		cache := newMemCache()
		verifier := &stubVerifier{uid: 7, valid: true}
		svc := newTestServer(cache, verifier)

		_, _, _ = svc.verifyTokenFlow(ctx, 7, "tok")
		_, _, _ = svc.verifyTokenFlow(ctx, 8, "tok")
		assert.Equal(t, 2, verifier.callCount())
	})
}

// TestVerifyTokenRPC 覆盖 gRPC 入口行为。
func TestVerifyTokenRPC(t *testing.T) {
	ctx := context.Background()

	t.Run("empty token rejected directly", func(t *testing.T) {
		svc := newTestServer(newMemCache(), &stubVerifier{})
		resp, err := svc.VerifyToken(ctx, &pb.VerifyTokenReq{Uid: 1, Token: ""})
		require.NoError(t, err)
		assert.False(t, resp.GetValid())
		assert.Equal(t, int32(CodeInvalidTok), resp.GetCode())
	})

	t.Run("valid token resolved with uid", func(t *testing.T) {
		svc := newTestServer(newMemCache(), &stubVerifier{uid: 42, valid: true})
		resp, err := svc.VerifyToken(ctx, &pb.VerifyTokenReq{Token: "real"})
		require.NoError(t, err)
		assert.True(t, resp.GetValid())
		assert.Equal(t, int32(CodeOK), resp.GetCode())
		assert.Equal(t, int64(42), resp.GetUid())
	})

	t.Run("unimplemented RPCs return codes.Unimplemented", func(t *testing.T) {
		svc := newTestServer(newMemCache(), &stubVerifier{})
		// SendMsg 自 S6 起已实现，不再断言其 Unimplemented。

		// SendMsg（S6）与 ReportDelivered（S7）已实现。

		_, err := svc.SyncPull(ctx, &pb.SyncPullReq{})
		assert.Equal(t, codes.Unimplemented, status.Code(err))

		_, err = svc.OnlineEvent(ctx, &pb.OnlineEventReq{})
		assert.Equal(t, codes.Unimplemented, status.Code(err))
	})
}

// TestHTTPVerifierAgainstStubServer 用 httptest 模拟 account。
func TestHTTPVerifierAgainstStubServer(t *testing.T) {
	t.Run("valid response parsed with uid", func(t *testing.T) {
		srv := stubAccountServer(t, `{"code":0,"msg":"ok","data":{"uid":7,"valid":true}}`, 200)
		v := NewHTTPVerifier(srv.URL, time.Second)
		uid, valid, err := v.Verify(context.Background(), 7, "tok")
		require.NoError(t, err)
		assert.True(t, valid)
		assert.Equal(t, int64(7), uid)
	})

	t.Run("invalid response parsed", func(t *testing.T) {
		srv := stubAccountServer(t, `{"code":40101,"msg":"x","data":{"uid":0,"valid":false}}`, 200)
		v := NewHTTPVerifier(srv.URL, time.Second)
		_, valid, err := v.Verify(context.Background(), 7, "tok")
		require.NoError(t, err)
		assert.False(t, valid)
	})

	t.Run("non-2xx treated as unreachable", func(t *testing.T) {
		srv := stubAccountServer(t, `boom`, 503)
		v := NewHTTPVerifier(srv.URL, time.Second)
		_, _, err := v.Verify(context.Background(), 7, "tok")
		assert.Error(t, err)
	})

	t.Run("connection refused treated as unreachable", func(t *testing.T) {
		v := NewHTTPVerifier("http://127.0.0.1:1", 200*time.Millisecond)
		_, _, err := v.Verify(context.Background(), 7, "tok")
		assert.Error(t, err)
	})
}

// stubAccountServer 启动一个返回固定 body/status 的 account 桩服务。
func stubAccountServer(t *testing.T, body string, status int) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	return srv
}
