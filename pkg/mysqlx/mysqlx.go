// Package mysqlx 封装 sqlx，按配置调优连接池，并提供按会话 ID 的
// 消息表分表路由（设计文档 9.1）。
package mysqlx

import (
	"fmt"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"

	"github.com/linkim/linkim/pkg/conf"
)

// connectAttempts / connectInterval：服务可能先于 MySQL 就绪启动
// （例如共用的 docker-compose 开发环境），因此初始连接做短重试。
const (
	connectAttempts = 5
	connectInterval = time.Second
)

// New 打开 MySQL 连接池并用 ping 验证可用性，失败时按上限重试。
// 连接池参数来自 cfg。
func New(cfg conf.MySQLConfig) (*sqlx.DB, error) {
	var db *sqlx.DB
	var err error
	for attempt := 1; attempt <= connectAttempts; attempt++ {
		// sqlx.Connect = sqlx.Open + sqlx.Ping。
		db, err = sqlx.Connect("mysql", cfg.DSN)
		if err == nil {
			break
		}
		if attempt < connectAttempts {
			time.Sleep(connectInterval)
		}
	}
	if err != nil {
		return nil, fmt.Errorf("mysqlx: connect: %w", err)
	}

	db.SetMaxOpenConns(cfg.MaxOpenConns)
	db.SetMaxIdleConns(cfg.MaxIdleConns)
	db.SetConnMaxLifetime(cfg.ConnMaxLifetime)
	return db, nil
}
