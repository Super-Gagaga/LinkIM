package account

import "time"

// User 对应 user 表一行（设计文档 9.2）。
type User struct {
	ID           int64     `db:"id" json:"uid"`
	Username     string    `db:"username" json:"username"`
	PasswordHash string    `db:"password_hash" json:"-"`
	Nickname     string    `db:"nickname" json:"nickname"`
	CreatedAt    time.Time `db:"created_at" json:"created_at"`
}
