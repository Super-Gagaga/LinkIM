// Package logx 提供所有 LinkIM 服务统一的 zap 日志器初始化，
// 由 conf.LogConfig 驱动。
package logx

import (
	"fmt"
	"io"
	"os"
	"strings"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/linkim/linkim/pkg/conf"
)

// 支持的日志输出格式。
const (
	FormatConsole = "console" // 人类可读，用于开发环境
	FormatJSON    = "json"    // 机器可读，用于生产环境
)

// New 构建一个输出到 stderr 的 zap.Logger：
// 开发环境使用 console 编码器，生产环境使用 JSON 编码器。
func New(cfg conf.LogConfig) (*zap.Logger, error) {
	return NewWithWriter(cfg, os.Stderr)
}

// NewWithWriter 是 New 的自定义输出版本（供测试注入 writer）。
func NewWithWriter(cfg conf.LogConfig, out io.Writer) (*zap.Logger, error) {
	// 解析日志级别，未配置时默认 info。
	level := zapcore.InfoLevel
	if cfg.Level != "" {
		if err := level.UnmarshalText([]byte(cfg.Level)); err != nil {
			return nil, fmt.Errorf("logx: invalid log level %q: %w", cfg.Level, err)
		}
	}

	// 解析输出格式，未配置时默认 console。
	format := strings.ToLower(strings.TrimSpace(cfg.Format))
	if format == "" {
		format = FormatConsole
	}

	var encoder zapcore.Encoder
	switch format {
	case FormatConsole:
		encCfg := zap.NewDevelopmentEncoderConfig()
		encCfg.EncodeTime = zapcore.ISO8601TimeEncoder
		encoder = zapcore.NewConsoleEncoder(encCfg)
	case FormatJSON:
		encCfg := zap.NewProductionEncoderConfig()
		encCfg.EncodeTime = zapcore.EpochMillisTimeEncoder
		encoder = zapcore.NewJSONEncoder(encCfg)
	default:
		return nil, fmt.Errorf("logx: unknown log format %q (want %q or %q)", cfg.Format, FormatConsole, FormatJSON)
	}

	core := zapcore.NewCore(encoder, zapcore.AddSync(out), level)
	return zap.New(core, zap.AddCaller()), nil
}
