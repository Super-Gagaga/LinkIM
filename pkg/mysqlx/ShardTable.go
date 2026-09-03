package mysqlx

import (
	"fmt"
	"hash/crc32"
)

// shardCount 是消息分表数量（message_00..message_63）；
// 对应设计文档 9.1 的 8库×64表 布局（MVP 阶段单库）。
const shardCount = 64

// ShardTable 按 crc32(conv_id) % 64 将会话 ID 映射到其消息表表名。
// 对同一 conv_id 映射结果恒定不变。
func ShardTable(convID string) string {
	return fmt.Sprintf("message_%02d", crc32.ChecksumIEEE([]byte(convID))%shardCount)
}
