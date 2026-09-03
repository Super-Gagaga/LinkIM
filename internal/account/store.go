package account

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

// 存储层哨兵错误。
var (
	// ErrUsernameExists 表示用户名已被唯一索引拒绝（查库预检或 INSERT 兜底）。
	ErrUsernameExists = errors.New("account: username already exists")
	// ErrUserNotFound 表示用户名不存在。
	ErrUserNotFound = errors.New("account: user not found")
)

// Store 抽象用户数据访问，便于单测 mock。
type Store interface {
	// CreateUser 插入一个新用户；用户名冲突返回 ErrUsernameExists。
	CreateUser(ctx context.Context, u *User) error
	// GetUserByUsername 按用户名查询；不存在返回 ErrUserNotFound。
	GetUserByUsername(ctx context.Context, username string) (*User, error)
}

// mysqlStore 是 Store 的 MySQL 实现（user 表）。
type mysqlStore struct{ db *sqlx.DB }

// NewMySQLStore 返回基于 *sqlx.DB 的 Store。
func NewMySQLStore(db *sqlx.DB) Store { return &mysqlStore{db: db} }

// CreateUser 实现 Store。
func (s *mysqlStore) CreateUser(ctx context.Context, u *User) error {
	const q = `INSERT INTO user (id, username, password_hash, nickname, created_at) VALUES (?, ?, ?, ?, ?)`
	_, err := s.db.ExecContext(ctx, q, u.ID, u.Username, u.PasswordHash, u.Nickname, u.CreatedAt)
	if isDuplicateKey(err) {
		return ErrUsernameExists
	}
	if err != nil {
		return fmt.Errorf("account: insert user: %w", err)
	}
	return nil
}

// GetUserByUsername 实现 Store。
func (s *mysqlStore) GetUserByUsername(ctx context.Context, username string) (*User, error) {
	const q = `SELECT id, username, password_hash, nickname, created_at FROM user WHERE username = ?`
	var u User
	err := s.db.GetContext(ctx, &u, q, username)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrUserNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("account: select user: %w", err)
	}
	return &u, nil
}

// isDuplicateKey 判断是否为 MySQL 唯一键冲突（errno 1062）。
func isDuplicateKey(err error) bool {
	var me *mysql.MySQLError
	return errors.As(err, &me) && me.Number == 1062
}
