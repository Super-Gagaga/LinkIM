package account

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/linkim/linkim/pkg/snowflake"
	"go.uber.org/zap"
)

// 群管理业务码（沿用全局分段：402xx 参数、403xx 关系/权限）。
const (
	CodeGroupNameBad   = 40203 // 群名非法
	CodeGroupNotFound  = 40204 // 群不存在
	CodeGroupForbidden = 40302 // 无权限操作（非管理员/非本人）
	CodeGroupFull      = 40303 // 群成员达到上限
)

// TopicGroupEvent 群成员变更事件 topic（设计文档 11.3）。
const TopicGroupEvent = "group.event"

// groupMaxMembers 群成员上限（设计文档 9.2 group.max_members 默认 500）。
const groupMaxMembers = 500

// 群角色。
const (
	roleMember = 0
	roleAdmin  = 1
	roleOwner  = 2
)

// GroupService 群管理业务（写 group/group_member + 失效缓存 + 发事件）。
type GroupService struct {
	store    GroupStore
	ids      *snowflake.Node
	producer GroupProducer
}

// GroupProducer 抽象事件生产（kafkax 实现）。
type GroupProducer interface {
	Send(ctx context.Context, topic string, key, value []byte, headers ...map[string]string) error
}

// NewGroupService 构造群管理服务。
func NewGroupService(store GroupStore, ids *snowflake.Node, producer GroupProducer) *GroupService {
	return &GroupService{store: store, ids: ids, producer: producer}
}

// --- 请求/响应结构 ---

type createGroupRequest struct {
	Name       string  `json:"name"`
	MemberUIDs []int64 `json:"member_uids"`
}

type groupMemberRow struct {
	UID    int64     `db:"uid" json:"uid"`
	Role   int32     `db:"role" json:"role"`
	JoinAt time.Time `db:"join_at" json:"join_at"`
}

// --- JWT 中间件 ---

// jwtAuth 从 Authorization: Bearer 解析 token 并校验，注入 uid。
func (h *Handler) jwtAuth(next func(w http.ResponseWriter, r *http.Request, uid int64)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		token := bearerToken(r)
		if token == "" {
			writeJSON(w, http.StatusUnauthorized, envelope{Code: CodeUnauthorized, Msg: "缺少 Bearer token"})
			return
		}
		claims, err := h.svc.tm.Parse(token)
		if err != nil || claims.TokenType != TokenTypeAccess {
			writeJSON(w, http.StatusUnauthorized, envelope{Code: CodeUnauthorized, Msg: "token 无效或已过期"})
			return
		}
		// 摘要吊销检查（单点登出）。
		cached, err := h.svc.cache.GetDigest(r.Context(), claims.UID)
		if err == nil && cached != "" && cached != Digest(token) {
			writeJSON(w, http.StatusUnauthorized, envelope{Code: CodeUnauthorized, Msg: "token 已失效"})
			return
		}
		next(w, r, claims.UID)
	}
}

func bearerToken(r *http.Request) string {
	h := r.Header.Get("Authorization")
	if len(h) > 7 && strings.EqualFold(h[:7], "Bearer ") {
		return h[7:]
	}
	return ""
}

// --- 端点 ---

// handleCreateGroup POST /api/v1/groups：建群，创建者即群主。
func (h *Handler) handleCreateGroup(w http.ResponseWriter, r *http.Request, uid int64) {
	var req createGroupRequest
	if !h.decodeBody(w, r, &req) {
		return
	}
	name := strings.TrimSpace(req.Name)
	if l := len(name); l < 1 || l > 64 {
		writeJSON(w, http.StatusBadRequest, envelope{Code: CodeGroupNameBad, Msg: "群名长度须为 1~64"})
		return
	}
	// 去重成员并排除创建者本人。
	seen := map[int64]bool{uid: true}
	members := make([]int64, 0, len(req.MemberUIDs))
	for _, m := range req.MemberUIDs {
		if m <= 0 || seen[m] {
			continue
		}
		seen[m] = true
		members = append(members, m)
	}
	if len(members)+1 > groupMaxMembers {
		writeJSON(w, http.StatusBadRequest, envelope{Code: CodeGroupFull, Msg: "群成员超过上限 500"})
		return
	}

	gid := h.groups.ids.Next()
	ctx := r.Context()
	rows := make([]groupMemberRow, 0, len(members)+1)
	now := time.Now()
	rows = append(rows, groupMemberRow{UID: uid, Role: roleOwner, JoinAt: now})
	for _, m := range members {
		rows = append(rows, groupMemberRow{UID: m, Role: roleMember, JoinAt: now})
	}
	if err := h.groups.store.CreateGroup(ctx, gid, name, uid, rows); err != nil {
		h.writeBizError(w, wrapStorage(err))
		return
	}
	// 逐成员发 join 事件（消费端补建 conversation 行）。
	for _, m := range members {
		h.emitGroupEvent(ctx, "join", gid, m)
	}
	writeJSON(w, http.StatusOK, envelope{Code: CodeOK, Msg: "ok", Data: map[string]any{"gid": gid}})
}

// handleListMembers GET /api/v1/groups/{id}/members。
func (h *Handler) handleListMembers(w http.ResponseWriter, r *http.Request, uid int64) {
	gid, ok := pathID(w, r, "gid")
	if !ok {
		return
	}
	ctx := r.Context()
	gs := h.groups.store
	if _, err := gs.GetGroup(ctx, gid); err != nil {
		writeGroupErr(w, err)
		return
	}
	members, err := gs.ListMembers(ctx, gid)
	if err != nil {
		h.writeBizError(w, wrapStorage(err))
		return
	}
	_ = uid
	writeJSON(w, http.StatusOK, envelope{Code: CodeOK, Msg: "ok", Data: map[string]any{"members": members}})
}

// handleAddMember POST /api/v1/groups/{id}/members {uid}（管理员以上）。
func (h *Handler) handleAddMember(w http.ResponseWriter, r *http.Request, uid int64) {
	gid, ok := pathID(w, r, "gid")
	if !ok {
		return
	}
	var req struct {
		UID int64 `json:"uid"`
	}
	if !h.decodeBody(w, r, &req) || req.UID <= 0 {
		writeJSON(w, http.StatusBadRequest, envelope{Code: CodeBadParam, Msg: "uid 非法"})
		return
	}
	ctx := r.Context()
	gs := h.groups.store
	if _, err := gs.GetGroup(ctx, gid); err != nil {
		writeGroupErr(w, err)
		return
	}
	role, err := gs.MemberRole(ctx, gid, uid)
	if err != nil {
		h.writeBizError(w, wrapStorage(err))
		return
	}
	if role < roleAdmin {
		writeJSON(w, http.StatusForbidden, envelope{Code: CodeGroupForbidden, Msg: "仅管理员可拉人"})
		return
	}
	if err := gs.AddMember(ctx, gid, req.UID); err != nil {
		h.writeBizError(w, wrapStorage(err))
		return
	}
	h.emitGroupEvent(ctx, "join", gid, req.UID)
	writeJSON(w, http.StatusOK, envelope{Code: CodeOK, Msg: "ok"})
}

// handleRemoveMember DELETE /api/v1/groups/{id}/members/{uid}
// （管理员以上移除他人；成员本人可退出）。
func (h *Handler) handleRemoveMember(w http.ResponseWriter, r *http.Request, uid int64) {
	gid, ok := pathID(w, r, "gid")
	if !ok {
		return
	}
	target, ok := pathID(w, r, "uid")
	if !ok {
		return
	}
	ctx := r.Context()
	gs := h.groups.store
	role, err := gs.MemberRole(ctx, gid, uid)
	if err != nil {
		h.writeBizError(w, wrapStorage(err))
		return
	}
	self := uid == target
	if role < roleAdmin && !self {
		writeJSON(w, http.StatusForbidden, envelope{Code: CodeGroupForbidden, Msg: "仅管理员可移除成员"})
		return
	}
	if err := gs.RemoveMember(ctx, gid, target); err != nil {
		h.writeBizError(w, wrapStorage(err))
		return
	}
	event := "leave"
	if self {
		event = "quit"
	}
	h.emitGroupEvent(ctx, event, gid, target)
	writeJSON(w, http.StatusOK, envelope{Code: CodeOK, Msg: "ok"})
}

// emitGroupEvent produce 群成员变更事件（异步不阻塞响应）。
func (h *Handler) emitGroupEvent(ctx context.Context, event string, gid, uid int64) {
	if h.groups.producer == nil {
		return
	}
	val, _ := json.Marshal(map[string]any{"event": event, "gid": gid, "uid": uid})
	go func() {
		gctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		key := []byte(fmt.Sprintf("%d", gid))
		if err := h.groups.producer.Send(gctx, TopicGroupEvent, key, val,
			map[string]string{"event": event}); err != nil {
			h.logger.Warn("emit group event failed",
				zap.String("event", event), zap.Int64("gid", gid), zap.Error(err))
		}
	}()
}

// pathID 解析路径参数。
func pathID(w http.ResponseWriter, r *http.Request, name string) (int64, bool) {
	val := r.PathValue(name)
	id, err := strconv.ParseInt(val, 10, 64)
	if err != nil || id <= 0 {
		writeJSON(w, http.StatusBadRequest, envelope{Code: CodeBadParam, Msg: "路径参数非法"})
		return 0, false
	}
	return id, true
}

func writeGroupErr(w http.ResponseWriter, err error) {
	if errors.Is(err, sql.ErrNoRows) {
		writeJSON(w, http.StatusNotFound, envelope{Code: CodeGroupNotFound, Msg: "群不存在"})
		return
	}
	writeJSON(w, http.StatusInternalServerError, envelope{Code: CodeStorageError, Msg: "服务暂时不可用"})
}
