package account

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// token 类型标记，防止 refresh token 被当作 access token 使用。
const (
	TokenTypeAccess  = "access"
	TokenTypeRefresh = "refresh"
)

// Claims 是账号服务签发的 JWT 载荷。
type Claims struct {
	UID       int64  `json:"uid"`
	TokenType string `json:"typ"` // access | refresh
	jwt.RegisteredClaims
}

// TokenManager 负责 HS256 签发与解析校验（设计文档 5.2、14 节）。
type TokenManager struct {
	secret     []byte
	accessTTL  time.Duration
	refreshTTL time.Duration
}

// NewTokenManager 用签名密钥与两类 token 有效期构造 TokenManager。
func NewTokenManager(secret string, accessTTL, refreshTTL time.Duration) *TokenManager {
	return &TokenManager{
		secret:     []byte(secret),
		accessTTL:  accessTTL,
		refreshTTL: refreshTTL,
	}
}

// Issue 为 uid 签发一张 token（accessToken=true 签发 access，否则 refresh），
// 返回 token 字符串与过期时间。
func (m *TokenManager) Issue(uid int64, accessToken bool) (string, time.Time, error) {
	typ := TokenTypeAccess
	ttl := m.accessTTL
	if !accessToken {
		typ = TokenTypeRefresh
		ttl = m.refreshTTL
	}
	now := time.Now()
	exp := now.Add(ttl)
	claims := Claims{
		UID:       uid,
		TokenType: typ,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(exp),
			IssuedAt:  jwt.NewNumericDate(now),
			Issuer:    "linkim-account",
		},
	}
	tok := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	s, err := tok.SignedString(m.secret)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("account: sign token: %w", err)
	}
	return s, exp, nil
}

// Parse 解析并校验签名与过期时间；token 无效返回错误。
func (m *TokenManager) Parse(token string) (*Claims, error) {
	var claims Claims
	_, err := jwt.ParseWithClaims(token, &claims, func(t *jwt.Token) (any, error) {
		if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("account: unexpected signing method %v", t.Header["alg"])
		}
		return m.secret, nil
	}, jwt.WithValidMethods([]string{"HS256"}))
	if err != nil {
		return nil, fmt.Errorf("account: parse token: %w", err)
	}
	return &claims, nil
}

// Digest 计算 token 摘要：sha256 后取前 32 字节的 hex（设计文档 5.2）。
func Digest(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:32])
}
