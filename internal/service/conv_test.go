package service

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/linkim/linkim/pkg/mysqlx"
)

func TestConvIDForP2P(t *testing.T) {
	tests := []struct {
		name string
		a, b int64
		want string
	}{
		{"a<b", 1, 2, "c:1:2"},
		{"a>b 归一化为小 uid 在前", 2, 1, "c:1:2"},
		{"大 uid", 354167017353252864, 354167017353252865, "c:354167017353252864:354167017353252865"},
		{"反向同值", -5, 10, "c:-5:10"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, ConvIDForP2P(tt.a, tt.b))
			// 交换参数结果一致（双向同会话）。
			assert.Equal(t, ConvIDForP2P(tt.a, tt.b), ConvIDForP2P(tt.b, tt.a))
		})
	}
}

func TestParseP2PConv(t *testing.T) {
	tests := []struct {
		convID       string
		wantA, wantB int64
		wantErr      bool
	}{
		{convID: "c:1:2", wantA: 1, wantB: 2},
		{convID: "c:100:200", wantA: 100, wantB: 200},
		{convID: "g:5", wantErr: true},
		{convID: "c:1", wantErr: true},
		{convID: "c:1:2:3", wantErr: true},
		{convID: "c:x:2", wantErr: true},
		{convID: "c:2:1", wantErr: true}, // min>=max
		{convID: "c:1:1", wantErr: true},
		{convID: "", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.convID, func(t *testing.T) {
			a, b, err := ParseP2PConv(tt.convID)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantA, a)
			assert.Equal(t, tt.wantB, b)
			// 解析与构造互逆。
			assert.Equal(t, tt.convID, ConvIDForP2P(a, b))
		})
	}
}

func TestShardOfConv(t *testing.T) {
	// 与 pkg/mysqlx.ShardTable 一致且值域合法。
	for _, convID := range []string{"c:1:2", "c:3:4", "g:9"} {
		shard := ShardOfConv(convID)
		assert.Equal(t, mysqlx.ShardTable(convID), shard)
		assert.Regexp(t, `^message_[0-5][0-9]|message_6[0-3]$`, shard)
	}
}
