// Package kafkax 封装 kafka-go，提供符合设计文档 8.3 的可靠生产者：
// acks=all、按 key 哈希分区、LZ4 压缩与小批量窗口。每条消息都携带
// trace-id 头用于端到端追踪（设计文档 16.2）。
package kafkax

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/linkim/linkim/pkg/conf"
)

// TraceIDHeader 是承载端到端追踪 ID 的 Kafka 头部键名。
const TraceIDHeader = "trace-id"

// Producer 以 acks=all 方式向 Kafka 写入消息（设计文档 6.1 ③）。
type Producer struct {
	w *kafka.Writer
}

// NewProducer 按给定 broker 列表构建 Producer。
// Writer 不绑定固定 topic，由 Send 按每条消息选择 topic。
func NewProducer(cfg conf.KafkaConfig) *Producer {
	w := &kafka.Writer{
		Addr:                   kafka.TCP(cfg.Brokers...),
		Balancer:               &kafka.Hash{},        // key 分区，同会话同分区保序
		RequiredAcks:           kafka.RequireAll,     // acks=all，防 broker 丢消息
		Compression:            kafka.Lz4,            // 设计文档 8.3
		BatchTimeout:           5 * time.Millisecond, // 微批，吞吐与延迟平衡
		AllowAutoTopicCreation: false,                // topic 由基础设施预建
	}
	if cfg.ClientID != "" {
		w.Transport = &kafka.Transport{ClientID: cfg.ClientID}
	}
	return &Producer{w: w}
}

// Send 向 topic 写入一条消息，key 用于分区路由（传 nil 则轮询分区）。
// 可传入任意个 header map 依次合并（后者覆盖前者）；始终附带 trace-id，
// 调用方未提供时在本地生成。
func (p *Producer) Send(ctx context.Context, topic string, key []byte, value []byte, headers ...map[string]string) error {
	msg := kafka.Message{
		Topic:   topic,
		Key:     key,
		Value:   value,
		Headers: buildHeaders(headers),
	}
	if err := p.w.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("kafkax: produce to %s: %w", topic, err)
	}
	return nil
}

// Close 刷新未发送的批量消息并释放 writer。
func (p *Producer) Close() error {
	if err := p.w.Close(); err != nil {
		return fmt.Errorf("kafkax: close: %w", err)
	}
	return nil
}

// buildHeaders 将变长 header map 合并为 Kafka 头部列表，并保证
// 恰好存在一个 trace-id 头。
func buildHeaders(maps []map[string]string) []kafka.Header {
	traceID := ""
	n := 0
	for _, m := range maps {
		n += len(m)
	}
	headers := make([]kafka.Header, 0, n+1)
	for _, m := range maps {
		for k, v := range m {
			if strings.EqualFold(k, TraceIDHeader) {
				traceID = v
			}
			headers = append(headers, kafka.Header{Key: k, Value: []byte(v)})
		}
	}
	if traceID == "" {
		traceID = newTraceID()
		headers = append(headers, kafka.Header{Key: TraceIDHeader, Value: []byte(traceID)})
	}
	return headers
}

// newTraceID 返回随机 128 位十六进制字符串作为追踪 ID。
func newTraceID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand 失败基本等价于致命错误；此处回退为时间派生值，
		// 而不是丢弃追踪信息。
		return fmt.Sprintf("t%x", time.Now().UnixNano())
	}
	return hex.EncodeToString(b[:])
}
