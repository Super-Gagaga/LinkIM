// Package snowflake 实现雪花算法的全局唯一 ID 生成器：
// 41 位毫秒时间戳 + 10 位节点 ID + 12 位序列号，
// 产出单调递增的 int64 ID（设计文档 5.1）。
package snowflake

import (
	"errors"
	"sync"
	"time"
)

// 位布局与纪元。纪元固定为 2024-01-01T00:00:00Z，
// 保证 ID 在服务生命周期内远不会溢出正数 int64。
const (
	timestampBits = 41
	nodeBits      = 10
	sequenceBits  = 12

	maxNode     = -1 ^ (-1 << nodeBits) // 1023
	maxSequence = -1 ^ (-1 << sequenceBits)

	nodeShift      = sequenceBits
	timestampShift = nodeBits + sequenceBits

	// epochMs 为纪元的 Unix 毫秒时间戳。
	epochMs = int64(1704067200000)
)

// ErrInvalidNode 当节点 ID 超出 10 位范围时返回。
var ErrInvalidNode = errors.New("snowflake: node id out of range [0,1023]")

// Node 为单个进程实例生成雪花 ID。
type Node struct {
	mu       sync.Mutex
	node     int64
	lastMs   int64
	sequence int64
}

// NewNode 返回 nodeID（0..1023）对应的 Node。
func NewNode(nodeID int64) (*Node, error) {
	if nodeID < 0 || nodeID > maxNode {
		return nil, ErrInvalidNode
	}
	return &Node{node: nodeID}, nil
}

// Next 返回下一个唯一 ID。当前毫秒的序列号耗尽时自旋等待到下一毫秒。
// 并发调用安全。
func (n *Node) Next() int64 {
	n.mu.Lock()
	defer n.mu.Unlock()

	now := time.Now().UnixMilli()
	switch {
	case now < n.lastMs:
		// 时钟回拨：保持最后已知时间，使 ID 单调而不碰撞
		// （接受单节点内的等待风险）。
		now = n.lastMs
	case now == n.lastMs:
		n.sequence = (n.sequence + 1) & maxSequence
		if n.sequence == 0 {
			for now <= n.lastMs {
				time.Sleep(50 * time.Microsecond)
				now = time.Now().UnixMilli()
			}
		}
	default:
		n.sequence = 0
	}
	n.lastMs = now

	return ((now - epochMs) << timestampShift) | (n.node << nodeShift) | n.sequence
}
