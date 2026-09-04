package job

import (
	"context"
	"time"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

// RunLoop 通用消费循环：手动 FetchMessage → handler 处置 → CommitMessages。
// handler 返回 nil 即提交；SIGTERM 时 ctx 取消，FetchMessage 返回错误退出循环
// （在途消息已在 handler 内处置完），符合“处理完在途 → Commit → 退出”。
func RunLoop(
	ctx context.Context,
	reader *kafka.Reader,
	handler func(ctx context.Context, km kafka.Message) error,
	logger *zap.Logger,
) error {
	for {
		km, err := reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				// 优雅退出：不再拉取新消息。
				return nil
			}
			return err
		}
		if err := handler(ctx, km); err != nil {
			logger.Error("handle message failed, skip commit",
				zap.String("topic", km.Topic), zap.Int("partition", km.Partition),
				zap.Int64("offset", km.Offset), zap.Error(err))
			continue // 不提交，重启后重新消费（至少一次）
		}
		if err := reader.CommitMessages(ctx, km); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
	}
}

// StoreRunLoop store 专用循环：Fetch 后交给 channel（BatchLoop 攒批写库并提交）。
func StoreRunLoop(
	ctx context.Context,
	reader *kafka.Reader,
	kmCh chan<- kafka.Message,
	logger *zap.Logger,
) error {
	for {
		km, err := reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		select {
		case kmCh <- km:
		case <-ctx.Done():
			// 已 Fetch 未入库的消息留在 channel 由 BatchLoop 排空；
			// 此条未送达的不再投递（重启后 offset 未提交会重新消费）。
			return nil
		}
	}
}

// NewReader 构造手动提交的 kafka Reader（CommitInterval=0）。
func NewReader(brokers []string, groupID, topic string, minBytes, maxBytes int, readBackoffMax time.Duration) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		GroupID:        groupID,
		Topic:          topic,
		MinBytes:       minBytes,
		MaxBytes:       maxBytes,
		CommitInterval: 0, // 手动提交（设计文档 8.3）
		StartOffset:    kafka.LastOffset,
		ReadBackoffMax: readBackoffMax,
	})
}
