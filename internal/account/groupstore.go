package account

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/jmoiron/sqlx"
)

// GroupStore 抽象群/群成员数据访问，便于单测 mock。
type GroupStore interface {
	// CreateGroup 建群并写入群主与初始成员。
	CreateGroup(ctx context.Context, gid int64, name string, ownerUID int64, members []groupMemberRow) error
	// GetGroup 返回群主 uid；群不存在返回 sql.ErrNoRows。
	GetGroup(ctx context.Context, gid int64) (int64, error)
	// ListMembers 返回全部成员行。
	ListMembers(ctx context.Context, gid int64) ([]groupMemberRow, error)
	// MemberRole 返回 uid 在群内角色；非成员返回 -1。
	MemberRole(ctx context.Context, gid, uid int64) (int32, error)
	// AddMember 添加成员（幂等：已存在则忽略）。
	AddMember(ctx context.Context, gid, uid int64) error
	// RemoveMember 移除成员。
	RemoveMember(ctx context.Context, gid, uid int64) error
}

// mysqlGroupStore 是 GroupStore 的 MySQL 实现。
type mysqlGroupStore struct{ db *sqlx.DB }

// NewMySQLGroupStore 构造群存储。
func NewMySQLGroupStore(db *sqlx.DB) GroupStore { return &mysqlGroupStore{db: db} }

// CreateGroup 实现 GroupStore（事务写 group + group_member）。
func (s *mysqlGroupStore) CreateGroup(ctx context.Context, gid int64, name string, ownerUID int64, members []groupMemberRow) error {
	tx, err := s.db.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("account: begin create group: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.ExecContext(ctx,
		`INSERT INTO `+"`group`"+` (id, name, owner_uid, max_members, created_at) VALUES (?, ?, ?, ?, NOW())`,
		gid, name, ownerUID, groupMaxMembers); err != nil {
		return fmt.Errorf("account: insert group: %w", err)
	}
	for _, m := range members {
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO group_member (group_id, uid, role, join_at) VALUES (?, ?, ?, ?)`,
			gid, m.UID, m.Role, m.JoinAt); err != nil {
			return fmt.Errorf("account: insert group member: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("account: commit create group: %w", err)
	}
	return nil
}

// GetGroup 实现 GroupStore。
func (s *mysqlGroupStore) GetGroup(ctx context.Context, gid int64) (int64, error) {
	var owner int64
	err := s.db.GetContext(ctx, &owner, `SELECT owner_uid FROM `+"`group`"+` WHERE id = ?`, gid)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, sql.ErrNoRows
	}
	if err != nil {
		return 0, fmt.Errorf("account: select group: %w", err)
	}
	return owner, nil
}

// ListMembers 实现 GroupStore。
func (s *mysqlGroupStore) ListMembers(ctx context.Context, gid int64) ([]groupMemberRow, error) {
	var rows []groupMemberRow
	err := s.db.SelectContext(ctx, &rows,
		`SELECT uid, role, join_at FROM group_member WHERE group_id = ? ORDER BY uid`, gid)
	if err != nil {
		return nil, fmt.Errorf("account: list group members: %w", err)
	}
	return rows, nil
}

// MemberRole 实现 GroupStore（非成员返回 -1）。
func (s *mysqlGroupStore) MemberRole(ctx context.Context, gid, uid int64) (int32, error) {
	var role int32
	err := s.db.GetContext(ctx, &role,
		`SELECT role FROM group_member WHERE group_id = ? AND uid = ?`, gid, uid)
	if errors.Is(err, sql.ErrNoRows) {
		return -1, nil
	}
	if err != nil {
		return -1, fmt.Errorf("account: select member role: %w", err)
	}
	return role, nil
}

// AddMember 实现 GroupStore（INSERT IGNORE 幂等）。
func (s *mysqlGroupStore) AddMember(ctx context.Context, gid, uid int64) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT IGNORE INTO group_member (group_id, uid, role, join_at) VALUES (?, ?, 0, NOW())`, gid, uid)
	if err != nil {
		return fmt.Errorf("account: add group member: %w", err)
	}
	return nil
}

// RemoveMember 实现 GroupStore。
func (s *mysqlGroupStore) RemoveMember(ctx context.Context, gid, uid int64) error {
	_, err := s.db.ExecContext(ctx,
		`DELETE FROM group_member WHERE group_id = ? AND uid = ?`, gid, uid)
	if err != nil {
		return fmt.Errorf("account: remove group member: %w", err)
	}
	return nil
}
