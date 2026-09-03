package kafkax

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/linkim/linkim/pkg/conf"
)

// TestBuildHeaders 覆盖 header 合并、trace-id 透传与自动生成。
func TestBuildHeaders(t *testing.T) {
	t.Run("no headers gets a generated trace-id", func(t *testing.T) {
		hs := buildHeaders(nil)
		require.Len(t, hs, 1)
		assert.Equal(t, TraceIDHeader, hs[0].Key)
		assert.NotEmpty(t, hs[0].Value)
		assert.Len(t, hs[0].Value, 32, "128-bit hex")
	})

	t.Run("caller trace-id is preserved", func(t *testing.T) {
		hs := buildHeaders([]map[string]string{{"trace-id": "my-trace", "conv-type": "1"}})
		byKey := map[string]string{}
		for _, h := range hs {
			byKey[h.Key] = string(h.Value)
		}
		assert.Equal(t, "my-trace", byKey["trace-id"])
		assert.Equal(t, "1", byKey["conv-type"])
		assert.Len(t, hs, 2, "no duplicate trace-id header added")
	})

	t.Run("multiple maps are merged with later maps winning", func(t *testing.T) {
		hs := buildHeaders([]map[string]string{
			{"a": "1", "b": "1"},
			{"b": "2", "trace-id": "t2"},
		})
		byKey := map[string]string{}
		for _, h := range hs {
			byKey[h.Key] = string(h.Value)
		}
		assert.Equal(t, "1", byKey["a"])
		assert.Equal(t, "2", byKey["b"])
		assert.Equal(t, "t2", byKey["trace-id"])
	})

	t.Run("trace-id matching is case-insensitive", func(t *testing.T) {
		hs := buildHeaders([]map[string]string{{"Trace-Id": "upper"}})
		byKey := map[string]string{}
		for _, h := range hs {
			byKey[h.Key] = string(h.Value)
		}
		assert.Equal(t, "upper", byKey["Trace-Id"])
		assert.Len(t, hs, 1)
	})
}

// TestSendRequiresBroker 验证 Send 会把不可达 broker 的传输错误
// 向上抛出，而不是静默成功。
func TestSendRequiresBroker(t *testing.T) {
	p := NewProducer(conf.KafkaConfig{Brokers: []string{"127.0.0.1:1"}, ClientID: "test"})
	require.NotNil(t, p)
	err := p.Send(context.Background(), "msg.push", []byte("k"), []byte("v"))
	require.Error(t, err)
	assert.NoError(t, p.Close())
}

// TestCloseFreshProducer 确保新建 Producer 的 Close 正常返回。
func TestCloseFreshProducer(t *testing.T) {
	p := NewProducer(conf.KafkaConfig{Brokers: []string{"127.0.0.1:9092"}})
	assert.NoError(t, p.Close())
}
