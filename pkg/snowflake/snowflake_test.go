package snowflake

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNewNodeValidation 覆盖 10 位节点 ID 的取值范围校验。
func TestNewNodeValidation(t *testing.T) {
	tests := []struct {
		node    int64
		wantErr bool
	}{
		{node: 0, wantErr: false},
		{node: 1, wantErr: false},
		{node: 1023, wantErr: false},
		{node: -1, wantErr: true},
		{node: 1024, wantErr: true},
		{node: 1 << 40, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("node=%d", tt.node), func(t *testing.T) {
			n, err := NewNode(tt.node)
			if tt.wantErr {
				assert.ErrorIs(t, err, ErrInvalidNode)
			} else {
				require.NoError(t, err)
				assert.NotNil(t, n)
			}
		})
	}
}

// TestNextBitLayout 验证 41/10/12 位布局可正确解码出节点 ID，
// 且时间戳与序列号结构单调。
func TestNextBitLayout(t *testing.T) {
	const nodeID = 555
	n, err := NewNode(nodeID)
	require.NoError(t, err)

	prev := int64(-1)
	for i := 0; i < 5000; i++ {
		id := n.Next()
		assert.Greater(t, id, prev, "IDs must be strictly increasing on one node")
		prev = id

		gotNode := (id >> nodeShift) & maxNode
		assert.Equal(t, int64(nodeID), gotNode)

		gotSeq := id & maxSequence
		assert.LessOrEqual(t, gotSeq, int64(maxSequence))

		ts := (id >> timestampShift) + epochMs
		assert.Greater(t, ts, int64(0))
	}
}

// TestNextConcurrentUnique 是核心性质：10 个协程各取 10 万次，
// 共 100 万个 ID 不得出现重复。
func TestNextConcurrentUnique(t *testing.T) {
	const (
		goroutines = 10
		perG       = 100_000
	)

	var mu sync.Mutex
	ids := make(map[int64]struct{}, goroutines*perG)

	n, err := NewNode(1)
	require.NoError(t, err)

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			local := make([]int64, 0, perG)
			for i := 0; i < perG; i++ {
				local = append(local, n.Next())
			}
			mu.Lock()
			defer mu.Unlock()
			for _, id := range local {
				ids[id] = struct{}{}
			}
		}()
	}
	wg.Wait()

	assert.Len(t, ids, goroutines*perG, "every generated ID must be unique")
}

// TestDifferentNodesProduceDistinctIDs 对节点间交叉生成做基本校验。
func TestDifferentNodesProduceDistinctIDs(t *testing.T) {
	n1, err := NewNode(1)
	require.NoError(t, err)
	n2, err := NewNode(2)
	require.NoError(t, err)

	seen := make(map[int64]struct{})
	for i := 0; i < 1000; i++ {
		seen[n1.Next()] = struct{}{}
		seen[n2.Next()] = struct{}{}
	}
	assert.Len(t, seen, 2000)
}
