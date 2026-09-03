package account

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"time"

	"golang.org/x/crypto/bcrypt"

	"github.com/linkim/linkim/pkg/snowflake"
)

// bcryptCost 是口令哈希成本（实施指南 S3：cost=10）。
const bcryptCost = 10

// usernamePattern 限定用户名字符集：字母、数字、下划线。
var usernamePattern = regexp.MustCompile(`^[a-zA-Z0-9_]+$`)

// RegisterRequest 是注册入参。
type RegisterRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
	Nickname string `json:"nickname"`
}

// LoginRequest 是登录入参。
type LoginRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

// LoginResult 是登录成功返回的数据。
type LoginResult struct {
	UID          int64  `json:"uid"`
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	ExpireAt     int64  `json:"expire_at"` // access token 过期 unix 秒
}

// VerifyResult 是 token 校验结果。
type VerifyResult struct {
	UID   int64 `json:"uid"`
	Valid bool  `json:"valid"`
}

// Service 是账号业务逻辑层。依赖以接口注入（Store/TokenCache），
// 便于单元测试 mock。
type Service struct {
	store Store
	cache TokenCache
	tm    *TokenManager
	ids   *snowflake.Node
}

// NewService 构造账号服务。
func NewService(store Store, cache TokenCache, tm *TokenManager, ids *snowflake.Node) *Service {
	return &Service{store: store, cache: cache, tm: tm, ids: ids}
}

// Register 校验参数、哈希口令并以雪花 uid 落库（设计文档 5.2）。
func (s *Service) Register(ctx context.Context, req RegisterRequest) (int64, error) {
	if err := validateRegister(req); err != nil {
		return 0, err
	}

	// 预检给出友好错误；并发窗口由唯一索引兜底。
	if _, err := s.store.GetUserByUsername(ctx, req.Username); err == nil {
		return 0, bizErr(CodeUsernameExists, "用户名已被注册")
	} else if !errors.Is(err, ErrUserNotFound) {
		return 0, wrapStorage(err)
	}

	hash, err := bcrypt.GenerateFromPassword([]byte(req.Password), bcryptCost)
	if err != nil {
		return 0, wrapStorage(err)
	}
	uid := s.ids.Next()
	u := &User{
		ID:           uid,
		Username:     req.Username,
		PasswordHash: string(hash),
		Nickname:     req.Nickname,
		CreatedAt:    time.Now(),
	}
	if err := s.store.CreateUser(ctx, u); err != nil {
		if errors.Is(err, ErrUsernameExists) {
			return 0, bizErr(CodeUsernameExists, "用户名已被注册")
		}
		return 0, wrapStorage(err)
	}
	return uid, nil
}

// Login 校验口令并签发 access/refresh 双 token，同时写 Redis 摘要。
func (s *Service) Login(ctx context.Context, req LoginRequest) (*LoginResult, error) {
	if req.Username == "" || req.Password == "" {
		return nil, bizErr(CodeBadParam, "用户名或密码不能为空")
	}
	u, err := s.store.GetUserByUsername(ctx, req.Username)
	if errors.Is(err, ErrUserNotFound) {
		// 不区分“用户不存在/密码错误”，统一 40101，避免账号枚举。
		return nil, bizErr(CodeUnauthorized, "用户名或密码错误")
	}
	if err != nil {
		return nil, wrapStorage(err)
	}
	if bcrypt.CompareHashAndPassword([]byte(u.PasswordHash), []byte(req.Password)) != nil {
		return nil, bizErr(CodeUnauthorized, "用户名或密码错误")
	}

	access, exp, err := s.tm.Issue(u.ID, true)
	if err != nil {
		return nil, wrapStorage(err)
	}
	refresh, _, err := s.tm.Issue(u.ID, false)
	if err != nil {
		return nil, wrapStorage(err)
	}

	// 摘要 TTL 与 access token 对齐；新登录覆盖旧摘要即实现单端踢出旧 token。
	if err := s.cache.SetDigest(ctx, u.ID, Digest(access), s.tm.accessTTL); err != nil {
		return nil, bizErr(CodeRedisError, "服务暂时不可用")
	}
	return &LoginResult{
		UID:          u.ID,
		AccessToken:  access,
		RefreshToken: refresh,
		ExpireAt:     exp.Unix(),
	}, nil
}

// Verify 校验 token：签名/过期 → token 类型 → Redis 摘要比对（若存在）。
func (s *Service) Verify(ctx context.Context, token string) (*VerifyResult, error) {
	claims, err := s.tm.Parse(token)
	if err != nil || claims.TokenType != TokenTypeAccess {
		return &VerifyResult{Valid: false}, bizErr(CodeUnauthorized, "token 无效或已过期")
	}
	cached, err := s.cache.GetDigest(ctx, claims.UID)
	if err != nil {
		return &VerifyResult{Valid: false}, bizErr(CodeRedisError, "服务暂时不可用")
	}
	// 摘要存在则必须一致（单点登出/被新登录顶替后旧 token 失效）。
	if cached != "" && cached != Digest(token) {
		return &VerifyResult{Valid: false}, bizErr(CodeUnauthorized, "token 已失效")
	}
	return &VerifyResult{UID: claims.UID, Valid: true}, nil
}

// validateRegister 校验注册参数：username[4,32] 限字符集，password[8,64]，nickname≤64。
func validateRegister(req RegisterRequest) error {
	if n := len(req.Username); n < 4 || n > 32 {
		return bizErr(CodeBadParam, "用户名长度须为 4~32 个字符")
	}
	if !usernamePattern.MatchString(req.Username) {
		return bizErr(CodeBadParam, "用户名仅允许字母、数字与下划线")
	}
	if n := len(req.Password); n < 8 || n > 64 {
		return bizErr(CodeBadParam, "密码长度须为 8~64 个字符")
	}
	if len(req.Nickname) > 64 {
		return bizErr(CodeBadParam, "昵称最长 64 个字符")
	}
	return nil
}

// wrapStorage 把内部存储错误统一转成业务码，不泄漏细节。
func wrapStorage(err error) error {
	return fmt.Errorf("%w: %v", bizErr(CodeStorageError, "服务暂时不可用"), err)
}
