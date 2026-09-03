package mysqlx

import (
	"fmt"
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestShardTablePinnedValues 固定 crc32/IEEE 回归值：
// 分表映射属于数据放置契约，绝不允许静默变更。
func TestShardTablePinnedValues(t *testing.T) {
	tests := []struct {
		convID string
		want   string
	}{
		{convID: "c:1:2", want: "message_27"},
		{convID: "c:10:20", want: "message_01"},
		{convID: "g:5", want: "message_45"},
		{convID: "c:100:200", want: "message_19"},
	}
	for _, tt := range tests {
		t.Run(tt.convID, func(t *testing.T) {
			assert.Equal(t, tt.want, ShardTable(tt.convID))
		})
	}
}

func TestShardTableRange(t *testing.T) {
	// 所有合法表名为 message_00..message_63，
	// 正则同时校验两位数字的零填充。
	pattern := regexp.MustCompile(`^message_([0-5][0-9]|6[0-3])$`)
	for i := 0; i < 5000; i++ {
		name := ShardTable(fmt.Sprintf("c:%d:%d", i, i*7919))
		assert.Regexp(t, pattern, name)
	}
}

func TestShardTableStable(t *testing.T) {
	convIDs := []string{"c:1:2", "g:9", "c:88:99", "some-very-long-conversation-id-值"}
	for _, id := range convIDs {
		assert.Equal(t, ShardTable(id), ShardTable(id), id)
	}
	// 同一会话 ID 映射恒定，分表结果一致。
	assert.Equal(t, ShardTable("c:1:2"), ShardTable("c:1:2"))
}

func TestShardTableCoversAllShards(t *testing.T) {
	seen := make(map[string]struct{})
	for i := 0; i < 20000; i++ {
		seen[ShardTable(fmt.Sprintf("conv-%d", i))] = struct{}{}
	}
	assert.Len(t, seen, 64, "expected the sample to hit all 64 shards")
}
