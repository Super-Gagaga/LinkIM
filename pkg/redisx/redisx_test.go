package redisx

import "github.com/linkim/linkim/pkg/conf"

func redisTestCfg() conf.RedisConfig {
	return conf.RedisConfig{Addr: "127.0.0.1:16379", Password: "", DB: 15}
}
