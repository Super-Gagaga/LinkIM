// Package service 包含跨服务共享的业务逻辑：
// 会话 ID 派生、分表路由等纯函数工具。
package service

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/linkim/linkim/pkg/mysqlx"
)

// ConvIDForP2P 返回单聊会话 ID："c:{min}:{max}"（小 uid 在前）。
// A→B 与 B→A 因此得到同一会话、同一 Kafka 分区（设计文档 6.3）。
func ConvIDForP2P(a, b int64) string {
	if a > b {
		a, b = b, a
	}
	return fmt.Sprintf("c:%d:%d", a, b)
}

// ParseP2PConv 解析单聊会话 ID，返回其中的两个 uid（升序）。
// 格式非法（非 c:min:max、非数字、min>max）返回错误。
func ParseP2PConv(convID string) (int64, int64, error) {
	parts := strings.Split(convID, ":")
	if len(parts) != 3 || parts[0] != "c" {
		return 0, 0, fmt.Errorf("service: invalid p2p conv id %q", convID)
	}
	a, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("service: invalid p2p conv id %q: %w", convID, err)
	}
	b, err := strconv.ParseInt(parts[2], 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("service: invalid p2p conv id %q: %w", convID, err)
	}
	if a >= b {
		return 0, 0, fmt.Errorf("service: invalid p2p conv id %q: min>=max", convID)
	}
	return a, b, nil
}

// ShardOfConv 返回会话对应的消息分表名（crc32 % 64）。
func ShardOfConv(convID string) string {
	return mysqlx.ShardTable(convID)
}

// ConvType 常量。
const (
	ConvTypeP2P   = int32(1)
	ConvTypeGroup = int32(2)
)

// ConvIDForGroup 返回群聊会话 ID："g:{gid}"。
func ConvIDForGroup(gid int64) string {
	return fmt.Sprintf("g:%d", gid)
}

// ParseGroupConv 解析群聊会话 ID，返回 gid；格式非法返回错误。
func ParseGroupConv(convID string) (int64, error) {
	parts := strings.Split(convID, ":")
	if len(parts) != 2 || parts[0] != "g" {
		return 0, fmt.Errorf("service: invalid group conv id %q", convID)
	}
	gid, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil || gid <= 0 {
		return 0, fmt.Errorf("service: invalid group conv id %q: %w", convID, err)
	}
	return gid, nil
}
