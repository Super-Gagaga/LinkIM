package account

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/bcrypt"

	"github.com/linkim/linkim/pkg/snowflake"
)

// newTestService 组装使用 mock 依赖的 Service。
func newTestService(t *testing.T) (*Service, *mockStore, *memTokenCache) {
	t.Helper()
	store := &mockStore{}
	cache := newMemTokenCache()
	ids, err := snowflake.NewNode(1)
	require.NoError(t, err)
	tm := NewTokenManager("test-secret", time.Hour, 30*24*time.Hour)
	return NewService(store, cache, tm, ids), store, cache
}

func TestRegister(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name     string
		req      RegisterRequest
		setup    func(*mockStore)
		wantCode int
	}{
		{
			name:     "username too short",
			req:      RegisterRequest{Username: "abc", Password: "password8"},
			wantCode: CodeBadParam,
		},
		{
			name:     "username too long",
			req:      RegisterRequest{Username: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Password: "password8"},
			wantCode: CodeBadParam,
		},
		{
			name:     "username bad charset",
			req:      RegisterRequest{Username: "bad name!", Password: "password8"},
			wantCode: CodeBadParam,
		},
		{
			name:     "password too short",
			req:      RegisterRequest{Username: "alice01", Password: "short"},
			wantCode: CodeBadParam,
		},
		{
			name:     "password too long",
			req:      RegisterRequest{Username: "alice01", Password: string(make([]byte, 65))},
			wantCode: CodeBadParam,
		},
		{
			name:     "nickname too long",
			req:      RegisterRequest{Username: "alice01", Password: "password8", Nickname: string(make([]byte, 65))},
			wantCode: CodeBadParam,
		},
		{
			name: "username already exists (precheck)",
			req:  RegisterRequest{Username: "alice01", Password: "password8"},
			setup: func(m *mockStore) {
				m.On("GetUserByUsername", mock.Anything, "alice01").
					Return(&User{ID: 1, Username: "alice01"}, nil).Once()
			},
			wantCode: CodeUsernameExists,
		},
		{
			name: "username already exists (unique index fallback)",
			req:  RegisterRequest{Username: "alice01", Password: "password8"},
			setup: func(m *mockStore) {
				m.On("GetUserByUsername", mock.Anything, "alice01").
					Return(nil, ErrUserNotFound).Once()
				m.On("CreateUser", mock.Anything, mock.AnythingOfType("*account.User")).
					Return(ErrUsernameExists).Once()
			},
			wantCode: CodeUsernameExists,
		},
		{
			name: "storage failure on insert",
			req:  RegisterRequest{Username: "alice01", Password: "password8"},
			setup: func(m *mockStore) {
				m.On("GetUserByUsername", mock.Anything, "alice01").
					Return(nil, ErrUserNotFound).Once()
				m.On("CreateUser", mock.Anything, mock.AnythingOfType("*account.User")).
					Return(errors.New("db down")).Once()
			},
			wantCode: CodeStorageError,
		},
		{
			name: "success",
			req:  RegisterRequest{Username: "alice01", Password: "password8", Nickname: "Alice"},
			setup: func(m *mockStore) {
				m.On("GetUserByUsername", mock.Anything, "alice01").
					Return(nil, ErrUserNotFound).Once()
				m.On("CreateUser", mock.Anything, mock.AnythingOfType("*account.User")).
					Run(func(args mock.Arguments) {
						u := args.Get(1).(*User)
						assert.NotZero(t, u.ID)
						assert.Equal(t, "alice01", u.Username)
						// bcrypt 哈希，cost 10。
						assert.Len(t, u.PasswordHash, 60)
						assert.Equal(t, "Alice", u.Nickname)
					}).
					Return(nil).Once()
			},
			wantCode: CodeOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc, store, _ := newTestService(t)
			if tt.setup != nil {
				tt.setup(store)
			}
			uid, err := svc.Register(ctx, tt.req)
			if tt.wantCode != CodeOK {
				var be *BizError
				require.ErrorAs(t, err, &be)
				assert.Equal(t, tt.wantCode, be.Code)
				return
			}
			require.NoError(t, err)
			assert.NotZero(t, uid)
			store.AssertExpectations(t)
		})
	}
}

func TestLogin(t *testing.T) {
	ctx := context.Background()
	hashed := mustHash(t, "password8")
	user := &User{ID: 42, Username: "alice01", PasswordHash: hashed}

	t.Run("empty credentials", func(t *testing.T) {
		svc, store, _ := newTestService(t)
		_, err := svc.Login(ctx, LoginRequest{})
		assertBizCode(t, err, CodeBadParam)
		store.AssertNotCalled(t, "GetUserByUsername")
	})

	t.Run("unknown user", func(t *testing.T) {
		svc, store, _ := newTestService(t)
		store.On("GetUserByUsername", mock.Anything, "nobody1").Return(nil, ErrUserNotFound).Once()
		_, err := svc.Login(ctx, LoginRequest{Username: "nobody1", Password: "password8"})
		assertBizCode(t, err, CodeUnauthorized)
	})

	t.Run("wrong password", func(t *testing.T) {
		svc, store, _ := newTestService(t)
		store.On("GetUserByUsername", mock.Anything, "alice01").Return(user, nil).Once()
		_, err := svc.Login(ctx, LoginRequest{Username: "alice01", Password: "WRONGpass"})
		assertBizCode(t, err, CodeUnauthorized)
	})

	t.Run("redis failure", func(t *testing.T) {
		svc, store, cache := newTestService(t)
		store.On("GetUserByUsername", mock.Anything, "alice01").Return(user, nil).Once()
		cache.err = errors.New("redis down")
		_, err := svc.Login(ctx, LoginRequest{Username: "alice01", Password: "password8"})
		assertBizCode(t, err, CodeRedisError)
	})

	t.Run("success writes digest and issues tokens", func(t *testing.T) {
		svc, store, cache := newTestService(t)
		store.On("GetUserByUsername", mock.Anything, "alice01").Return(user, nil).Once()

		res, err := svc.Login(ctx, LoginRequest{Username: "alice01", Password: "password8"})
		require.NoError(t, err)
		assert.Equal(t, int64(42), res.UID)
		assert.NotEmpty(t, res.AccessToken)
		assert.NotEmpty(t, res.RefreshToken)
		assert.NotEqual(t, res.AccessToken, res.RefreshToken)
		assert.InDelta(t, time.Now().Add(time.Hour).Unix(), res.ExpireAt, 5)

		// Redis 中保存了 access token 的摘要。
		assert.Equal(t, Digest(res.AccessToken), cache.digests[42])

		// access token 可被解析为 access 类型。
		claims, err := svc.tm.Parse(res.AccessToken)
		require.NoError(t, err)
		assert.Equal(t, int64(42), claims.UID)
		assert.Equal(t, TokenTypeAccess, claims.TokenType)

		// refresh token 是 refresh 类型。
		rclaims, err := svc.tm.Parse(res.RefreshToken)
		require.NoError(t, err)
		assert.Equal(t, TokenTypeRefresh, rclaims.TokenType)
	})

	t.Run("expired token fails to parse", func(t *testing.T) {
		tm := NewTokenManager("s", -time.Minute, time.Hour)
		tok, _, err := tm.Issue(42, true)
		require.NoError(t, err)
		_, err = tm.Parse(tok)
		assert.Error(t, err)
	})

	t.Run("token signed with different secret fails", func(t *testing.T) {
		other := NewTokenManager("other-secret", time.Hour, time.Hour)
		tok, _, err := other.Issue(42, true)
		require.NoError(t, err)
		svc, _, _ := newTestService(t)
		_, err = svc.tm.Parse(tok)
		assert.Error(t, err)
	})
}

func TestVerify(t *testing.T) {
	ctx := context.Background()
	hashed := mustHash(t, "password8")
	user := &User{ID: 42, Username: "alice01", PasswordHash: hashed}

	login := func(t *testing.T) (*Service, string) {
		svc, store, _ := newTestService(t)
		store.On("GetUserByUsername", mock.Anything, "alice01").Return(user, nil).Once()
		res, err := svc.Login(ctx, LoginRequest{Username: "alice01", Password: "password8"})
		require.NoError(t, err)
		return svc, res.AccessToken
	}

	t.Run("valid token passes", func(t *testing.T) {
		svc, token := login(t)
		res, err := svc.Verify(ctx, token)
		require.NoError(t, err)
		assert.True(t, res.Valid)
		assert.Equal(t, int64(42), res.UID)
	})

	t.Run("forged token rejected", func(t *testing.T) {
		svc, _ := login(t)
		res, err := svc.Verify(ctx, "forged.token.value")
		assertBizCode(t, err, CodeUnauthorized)
		assert.False(t, res.Valid)
	})

	t.Run("refresh token rejected as access", func(t *testing.T) {
		svc, store, _ := newTestService(t)
		store.On("GetUserByUsername", mock.Anything, "alice01").Return(user, nil).Once()
		res, err := svc.Login(ctx, LoginRequest{Username: "alice01", Password: "password8"})
		require.NoError(t, err)
		vres, verr := svc.Verify(ctx, res.RefreshToken)
		assertBizCode(t, verr, CodeUnauthorized)
		assert.False(t, vres.Valid)
	})

	t.Run("revoked by digest mismatch (old token after re-login)", func(t *testing.T) {
		svc, token := login(t)
		// 模拟重新登录覆盖了摘要：直接写入另一会话 token 的摘要。
		// （不采用真实二次登录，因为同秒内签发的 token 可能完全相同，
		// 使测试退化为依赖时间精度的偶发用例。）
		cache := svc.cache.(*memTokenCache)
		cache.mu.Lock()
		cache.digests[42] = Digest("another-session-token")
		cache.mu.Unlock()

		res, err := svc.Verify(ctx, token)
		assertBizCode(t, err, CodeUnauthorized)
		assert.False(t, res.Valid)
	})

	t.Run("digest absent (expired from redis) still valid", func(t *testing.T) {
		svc, token := login(t)
		cache := svc.cache.(*memTokenCache)
		require.NoError(t, cache.DelDigest(ctx, 42))
		res, err := svc.Verify(ctx, token)
		require.NoError(t, err)
		assert.True(t, res.Valid)
	})

	t.Run("redis error surfaces 50201", func(t *testing.T) {
		svc, token := login(t)
		svc.cache.(*memTokenCache).err = errors.New("redis down")
		res, err := svc.Verify(ctx, token)
		assertBizCode(t, err, CodeRedisError)
		assert.False(t, res.Valid)
	})
}

func TestDigest(t *testing.T) {
	d1 := Digest("token-a")
	d2 := Digest("token-b")
	assert.Len(t, d1, 64) // 32 字节 hex
	assert.NotEqual(t, d1, d2)
	assert.Equal(t, d1, Digest("token-a"))
}

// --- helpers ---

func assertBizCode(t *testing.T, err error, want int) {
	t.Helper()
	var be *BizError
	require.ErrorAs(t, err, &be)
	assert.Equal(t, want, be.Code)
}

func mustHash(t *testing.T, password string) string {
	t.Helper()
	hashed, err := bcrypt.GenerateFromPassword([]byte(password), bcryptCost)
	require.NoError(t, err)
	return string(hashed)
}
