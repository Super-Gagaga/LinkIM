package conf

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// writeConfig 将一段 YAML 配置写入测试临时目录并返回路径。
func writeConfig(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
	return path
}

func TestLoad(t *testing.T) {
	full := `
server:
  name: logic
  http_port: 8080
  grpc_port: 9001
  ws_port: 8081
mysql:
  dsn: "root:linkim123@tcp(127.0.0.1:3306)/linkim"
  max_open_conns: 32
  max_idle_conns: 8
  conn_max_lifetime: 3m
redis:
  addr: 10.0.0.1:6379
  password: secret
  db: 2
kafka:
  brokers:
    - 10.0.0.2:9092
    - 10.0.0.3:9092
  client_id: logic-1
log:
  level: debug
  format: json
`
	minimal := `
server:
  name: job
`

	tests := []struct {
		name    string
		content string
		env     map[string]string
		check   func(t *testing.T, cfg *Config)
	}{
		{
			name:    "full config loads every section",
			content: full,
			check: func(t *testing.T, cfg *Config) {
				assert.Equal(t, "logic", cfg.Server.Name)
				assert.Equal(t, 8080, cfg.Server.HTTPPort)
				assert.Equal(t, 9001, cfg.Server.GRPCPort)
				assert.Equal(t, 8081, cfg.Server.WSPort)
				assert.Equal(t, "root:linkim123@tcp(127.0.0.1:3306)/linkim", cfg.MySQL.DSN)
				assert.Equal(t, 32, cfg.MySQL.MaxOpenConns)
				assert.Equal(t, 8, cfg.MySQL.MaxIdleConns)
				assert.Equal(t, 3*time.Minute, cfg.MySQL.ConnMaxLifetime)
				assert.Equal(t, "10.0.0.1:6379", cfg.Redis.Addr)
				assert.Equal(t, "secret", cfg.Redis.Password)
				assert.Equal(t, 2, cfg.Redis.DB)
				assert.Equal(t, []string{"10.0.0.2:9092", "10.0.0.3:9092"}, cfg.Kafka.Brokers)
				assert.Equal(t, "logic-1", cfg.Kafka.ClientID)
				assert.Equal(t, "debug", cfg.Log.Level)
				assert.Equal(t, "json", cfg.Log.Format)
			},
		},
		{
			name:    "missing sections fall back to defaults",
			content: minimal,
			check: func(t *testing.T, cfg *Config) {
				assert.Equal(t, "job", cfg.Server.Name)
				assert.Equal(t, 0, cfg.Server.HTTPPort)
				assert.Equal(t, 64, cfg.MySQL.MaxOpenConns)
				assert.Equal(t, 16, cfg.MySQL.MaxIdleConns)
				assert.Equal(t, 5*time.Minute, cfg.MySQL.ConnMaxLifetime)
				assert.Equal(t, "127.0.0.1:16379", cfg.Redis.Addr)
				assert.Equal(t, []string{"127.0.0.1:9092"}, cfg.Kafka.Brokers)
				assert.Equal(t, "info", cfg.Log.Level)
				assert.Equal(t, "console", cfg.Log.Format)
			},
		},
		{
			name:    "env vars override file values",
			content: full,
			env: map[string]string{
				"LINKIM_MYSQL_DSN":               "env-dsn",
				"LINKIM_SERVER_HTTP_PORT":        "9999",
				"LINKIM_REDIS_ADDR":              "10.9.9.9:6379",
				"LINKIM_MYSQL_CONN_MAX_LIFETIME": "1m",
				"LINKIM_LOG_LEVEL":               "warn",
			},
			check: func(t *testing.T, cfg *Config) {
				assert.Equal(t, "env-dsn", cfg.MySQL.DSN)
				assert.Equal(t, 9999, cfg.Server.HTTPPort)
				assert.Equal(t, "10.9.9.9:6379", cfg.Redis.Addr)
				assert.Equal(t, time.Minute, cfg.MySQL.ConnMaxLifetime)
				assert.Equal(t, "warn", cfg.Log.Level)
				// 未被覆盖的键保留文件中的值。
				assert.Equal(t, "logic", cfg.Server.Name)
				assert.Equal(t, 9001, cfg.Server.GRPCPort)
			},
		},
		{
			name:    "env var overrides a key absent from the file",
			content: minimal,
			env:     map[string]string{"LINKIM_LOG_FORMAT": "json"},
			check: func(t *testing.T, cfg *Config) {
				assert.Equal(t, "json", cfg.Log.Format)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for k, val := range tt.env {
				t.Setenv(k, val)
			}
			cfg, err := Load(writeConfig(t, tt.content))
			require.NoError(t, err)
			require.NotNil(t, cfg)
			tt.check(t, cfg)
		})
	}
}

func TestLoadErrors(t *testing.T) {
	tests := []struct {
		name string
		path string
	}{
		{name: "empty path", path: ""},
		{name: "missing file", path: filepath.Join(t.TempDir(), "nope.yaml")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Load(tt.path)
			assert.Error(t, err)
		})
	}
}
