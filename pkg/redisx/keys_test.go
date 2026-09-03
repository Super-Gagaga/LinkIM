package redisx

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestKeyConstructors 固定设计文档 9.3 的键布局。
func TestKeyConstructors(t *testing.T) {
	tests := []struct {
		name string
		got  string
		want string
	}{
		{name: "route key", got: RouteKey(1001), want: "route:1001"},
		{name: "route key negative uid", got: RouteKey(-1), want: "route:-1"},
		{name: "comet alive key", got: CometAliveKey("10.0.1.12:9000"), want: "comet:alive:10.0.1.12:9000"},
		{name: "seq key p2p", got: SeqKey("c:1:2"), want: "seq:c:1:2"},
		{name: "seq key group", got: SeqKey("g:5"), want: "seq:g:5"},
		{name: "idem key", got: IdemKey(7, "uuid-abc"), want: "idem:7:uuid-abc"},
		{name: "presence key", got: PresenceKey(7), want: "presence:7"},
		{name: "token key", got: TokenKey(7), want: "token:7"},
		{name: "conv members key", got: ConvMembersKey(42), want: "conv:members:42"},
		{name: "friend key", got: FriendKey(42), want: "friend:42"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.got)
		})
	}
}

// TestNewClientConfig 确保 New 按配置构建客户端且不 panic
// （go-redis 为惰性连接）。
func TestNewClientConfig(t *testing.T) {
	c := New(redisTestCfg())
	assert.NotNil(t, c)
	assert.NoError(t, c.Close())
}
