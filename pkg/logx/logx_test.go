package logx

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/linkim/linkim/pkg/conf"
)

func TestNew(t *testing.T) {
	tests := []struct {
		name       string
		cfg        conf.LogConfig
		log        func(l *zap.Logger)
		wantOut    []string // 输出中应包含的子串
		notWantOut []string // 输出中必须不出现的子串
		wantErr    bool
	}{
		{
			name: "json format emits structured records",
			cfg:  conf.LogConfig{Level: "info", Format: "json"},
			log:  func(l *zap.Logger) { l.Info("hello", zap.String("key", "value")) },
			wantOut: []string{
				`"level":"info"`,
				`"msg":"hello"`,
				`"key":"value"`,
			},
		},
		{
			name: "console format emits readable records",
			cfg:  conf.LogConfig{Level: "info", Format: "console"},
			log:  func(l *zap.Logger) { l.Info("hello", zap.String("key", "value")) },
			wantOut: []string{
				"INFO",
				"hello",
				"key",
			},
			notWantOut: []string{`"msg":"hello"`},
		},
		{
			name: "empty config falls back to info level and console format",
			cfg:  conf.LogConfig{},
			log:  func(l *zap.Logger) { l.Info("fallback") },
			wantOut: []string{
				"INFO",
				"fallback",
			},
		},
		{
			name: "warn level filters out info logs",
			cfg:  conf.LogConfig{Level: "warn", Format: "json"},
			log: func(l *zap.Logger) {
				l.Info("dropped")
				l.Warn("kept")
			},
			wantOut:    []string{`"msg":"kept"`},
			notWantOut: []string{`"msg":"dropped"`},
		},
		{
			name:    "debug level keeps debug logs",
			cfg:     conf.LogConfig{Level: "debug", Format: "json"},
			log:     func(l *zap.Logger) { l.Debug("dbg") },
			wantOut: []string{`"msg":"dbg"`},
		},
		{
			name:    "uppercase level is accepted",
			cfg:     conf.LogConfig{Level: "ERROR", Format: "json"},
			log:     func(l *zap.Logger) { l.Error("boom") },
			wantOut: []string{`"msg":"boom"`},
		},
		{
			name:    "invalid level is rejected",
			cfg:     conf.LogConfig{Level: "verbose", Format: "json"},
			wantErr: true,
		},
		{
			name:    "unknown format is rejected",
			cfg:     conf.LogConfig{Level: "info", Format: "xml"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			logger, err := NewWithWriter(tt.cfg, &buf)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, logger)

			tt.log(logger)
			require.NoError(t, logger.Sync())

			out := buf.String()
			for _, want := range tt.wantOut {
				assert.Contains(t, out, want)
			}
			for _, notWant := range tt.notWantOut {
				assert.NotContains(t, out, notWant)
			}
		})
	}
}
