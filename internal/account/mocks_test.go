package account

import (
	"context"
	"sync"
	"time"

	"github.com/stretchr/testify/mock"
)

// mockStore 是 Store 的内存 mock。
type mockStore struct {
	mock.Mock
}

func (m *mockStore) CreateUser(ctx context.Context, u *User) error {
	args := m.Called(ctx, u)
	return args.Error(0)
}

func (m *mockStore) GetUserByUsername(ctx context.Context, username string) (*User, error) {
	args := m.Called(ctx, username)
	u, _ := args.Get(0).(*User)
	return u, args.Error(1)
}

// memTokenCache 是 TokenCache 的内存 mock。
type memTokenCache struct {
	mu      sync.Mutex
	digests map[int64]string
	err     error
}

func newMemTokenCache() *memTokenCache { return &memTokenCache{digests: map[int64]string{}} }

func (c *memTokenCache) SetDigest(_ context.Context, uid int64, digest string, _ time.Duration) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.err != nil {
		return c.err
	}
	c.digests[uid] = digest
	return nil
}

func (c *memTokenCache) GetDigest(_ context.Context, uid int64) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.err != nil {
		return "", c.err
	}
	return c.digests[uid], nil
}

func (c *memTokenCache) DelDigest(_ context.Context, uid int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.digests, uid)
	return nil
}
