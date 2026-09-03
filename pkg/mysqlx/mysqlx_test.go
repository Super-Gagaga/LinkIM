package mysqlx

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/linkim/linkim/pkg/conf"
)

// mysqlTestCfg 构造一个连接池参数较小的测试用 MySQLConfig。
func mysqlTestCfg(dsn string) conf.MySQLConfig {
	return conf.MySQLConfig{
		DSN:             dsn,
		MaxOpenConns:    2,
		MaxIdleConns:    1,
		ConnMaxLifetime: time.Minute,
	}
}

func TestNewConfigApplied(t *testing.T) {
	// 对不可达地址调用 New 必须在有界重试后失败。
	_, err := New(mysqlTestCfg("root:x@tcp(127.0.0.1:1)/linkim"))
	require.Error(t, err)
}
