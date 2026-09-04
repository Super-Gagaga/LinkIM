// dlqreplay 读取死信 topic 并可选重放回原 topic（人工处理死信的工具）。
// 用法：
//
//	go run ./scripts/dlqreplay.go -topic dlq.msg.store            # 只读列出
//	go run ./scripts/dlqreplay.go -topic dlq.msg.store -confirm  # 逐条重放回原 topic
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/linkim/linkim/pkg/conf"
	"github.com/linkim/linkim/pkg/kafkax"
)

var (
	topic   = flag.String("topic", "dlq.msg.store", "死信 topic（dlq.msg.store / dlq.msg.push）")
	confirm = flag.Bool("confirm", false, "确认后逐条重新 produce 回原 topic")
	limit   = flag.Int("limit", 1000, "最多处理条数")
)

func main() {
	flag.Parse()

	// 原.topic 映射：dlq.msg.store → msg.store；dlq.msg.push → msg.push。
	origin := ""
	switch *topic {
	case "dlq.msg.store":
		origin = "msg.store"
	case "dlq.msg.push":
		origin = "msg.push"
	default:
		log.Fatalf("未知死信 topic %q（支持 dlq.msg.store / dlq.msg.push）", *topic)
	}

	cfg := conf.KafkaConfig{Brokers: []string{"127.0.0.1:9092"}, ClientID: "dlqreplay"}
	if v := os.Getenv("LINKIM_KAFKA_BROKERS"); v != "" {
		cfg.Brokers = []string{v}
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  cfg.Brokers,
		GroupID:  fmt.Sprintf("dlqreplay-%d", time.Now().UnixMilli()),
		Topic:    *topic,
		MinBytes: 1, MaxBytes: 1 << 20,
		CommitInterval: 0,
		StartOffset:    kafka.FirstOffset,
	})
	defer func() { _ = reader.Close() }()

	var producer *kafkax.Producer
	if *confirm {
		producer = kafkax.NewProducer(cfg)
		defer func() { _ = producer.Close() }()
		fmt.Printf("重放 %s → %s（最多 %d 条）\n", *topic, origin, *limit)
	} else {
		fmt.Printf("预览 %s（最多 %d 条，加 -confirm 执行重放）\n", *topic, *limit)
	}

	count := 0
	for count < *limit {
		km, err := reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			log.Fatalf("fetch 失败: %v", err)
		}
		count++
		fmt.Printf("#%d partition=%d offset=%d key=%s value=%dB\n",
			count, km.Partition, km.Offset, string(km.Key), len(km.Value))
		if *confirm {
			if err := producer.Send(ctx, origin, km.Key, km.Value,
				map[string]string{"replayed-from": *topic}); err != nil {
				log.Fatalf("重放失败: %v", err)
			}
		}
		if err := reader.CommitMessages(ctx, km); err != nil {
			log.Fatalf("commit 失败: %v", err)
		}
	}
	fmt.Printf("完成：处理 %d 条%s\n", count, map[bool]string{true: "（已重放）", false: ""}[*confirm])
}
