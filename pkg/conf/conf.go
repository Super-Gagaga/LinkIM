// Package conf 定义所有 LinkIM 服务共享的配置结构，
// 从 YAML 文件加载，并支持 LINKIM_ 前缀的环境变量覆盖。
package conf

import (
	"fmt"
	"strings"
	"time"

	"github.com/spf13/viper"
)

// ServerConfig 定义单个服务的标识与监听端口。
// 并非每个服务都使用所有端口（例如 job 不监听任何端口）。
type ServerConfig struct {
	Name          string `mapstructure:"name"`
	HTTPPort      int    `mapstructure:"http_port"`
	GRPCPort      int    `mapstructure:"grpc_port"`
	WSPort        int    `mapstructure:"ws_port"`
	NodeID        int64  `mapstructure:"node_id"`        // snowflake 节点 ID，集群内唯一
	AdvertiseAddr string `mapstructure:"advertise_addr"` // 对外可达的 gRPC 地址（写入路由表），如 10.0.1.12:9000
}

// MySQLConfig 定义 MySQL DSN 与连接池调优参数。
type MySQLConfig struct {
	DSN             string        `mapstructure:"dsn"`
	MaxOpenConns    int           `mapstructure:"max_open_conns"`
	MaxIdleConns    int           `mapstructure:"max_idle_conns"`
	ConnMaxLifetime time.Duration `mapstructure:"conn_max_lifetime"`
}

// RedisConfig 定义 Redis 连接参数。
type RedisConfig struct {
	Addr     string `mapstructure:"addr"`
	Password string `mapstructure:"password"`
	DB       int    `mapstructure:"db"`
}

// KafkaConfig 定义 Kafka 接入参数。
type KafkaConfig struct {
	Brokers  []string `mapstructure:"brokers"`
	ClientID string   `mapstructure:"client_id"`
}

// LogConfig 选择日志行为：级别（debug/info/warn/error）与
// 格式（console 用于开发环境，json 用于生产环境）。
type LogConfig struct {
	Level  string `mapstructure:"level"`
	Format string `mapstructure:"format"`
}

// JWTConfig 定义 token 签名密钥与有效期（设计文档 5.2：access 2h + refresh 30d）。
type JWTConfig struct {
	Secret     string        `mapstructure:"secret"`
	AccessTTL  time.Duration `mapstructure:"access_ttl"`
	RefreshTTL time.Duration `mapstructure:"refresh_ttl"`
}

// AccountConfig 定义上游账号服务地址（logic 调用 /internal/v1/verify）。
type AccountConfig struct {
	Addr          string        `mapstructure:"addr"`
	VerifyTimeout time.Duration `mapstructure:"verify_timeout"`
}

// LogicConfig 定义上游 logic 服务地址（comet 调用 VerifyToken/SendMsg 等）。
type LogicConfig struct {
	Addr        string        `mapstructure:"addr"`
	CallTimeout time.Duration `mapstructure:"call_timeout"`
}

// Config 是单个 LinkIM 服务的完整配置。
type Config struct {
	Server  ServerConfig  `mapstructure:"server"`
	MySQL   MySQLConfig   `mapstructure:"mysql"`
	Redis   RedisConfig   `mapstructure:"redis"`
	Kafka   KafkaConfig   `mapstructure:"kafka"`
	Log     LogConfig     `mapstructure:"log"`
	JWT     JWTConfig     `mapstructure:"jwt"`
	Account AccountConfig `mapstructure:"account"`
	Logic   LogicConfig   `mapstructure:"logic"`
}

// EnvPrefix 是用于覆盖文件配置值的环境变量前缀，
// 例如 LINKIM_MYSQL_DSN 覆盖 mysql.dsn。
const EnvPrefix = "LINKIM"

// Load 读取 path 指向的 YAML 配置并返回解析后的 Config。
// 优先级：LINKIM_ 前缀环境变量 > 文件值（键路径中的点映射为下划线，
// 例如 LINKIM_SERVER_HTTP_PORT）。两者均未提供的键回退到下方注册的
// 默认值——因此每个键对 viper 都是已知可覆盖的。
func Load(path string) (*Config, error) {
	if path == "" {
		return nil, fmt.Errorf("conf: config path is empty")
	}
	v := viper.New()
	v.SetConfigFile(path)
	v.SetEnvPrefix(EnvPrefix)
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	setDefaults(v)

	if err := v.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("conf: read %s: %w", path, err)
	}

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("conf: unmarshal %s: %w", path, err)
	}
	return &cfg, nil
}

// setDefaults 注册所有配置键的默认值，
// 保证未在文件与环境变量中出现的键也有确定取值。
func setDefaults(v *viper.Viper) {
	v.SetDefault("server.name", "")
	v.SetDefault("server.http_port", 0)
	v.SetDefault("server.grpc_port", 0)
	v.SetDefault("server.ws_port", 0)
	v.SetDefault("server.node_id", 0)
	v.SetDefault("server.advertise_addr", "")

	v.SetDefault("mysql.dsn", "")
	v.SetDefault("mysql.max_open_conns", 64)
	v.SetDefault("mysql.max_idle_conns", 16)
	v.SetDefault("mysql.conn_max_lifetime", 5*time.Minute)

	v.SetDefault("redis.addr", "127.0.0.1:16379")
	v.SetDefault("redis.password", "")
	v.SetDefault("redis.db", 0)

	v.SetDefault("kafka.brokers", []string{"127.0.0.1:9092"})
	v.SetDefault("kafka.client_id", "")

	v.SetDefault("log.level", "info")
	v.SetDefault("log.format", "console")

	v.SetDefault("jwt.secret", "")
	v.SetDefault("jwt.access_ttl", 2*time.Hour)
	v.SetDefault("jwt.refresh_ttl", 720*time.Hour)

	v.SetDefault("account.addr", "http://127.0.0.1:8080")
	v.SetDefault("account.verify_timeout", time.Second)

	v.SetDefault("logic.addr", "127.0.0.1:9001")
	v.SetDefault("logic.call_timeout", 2*time.Second)
}
