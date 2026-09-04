package account

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

// maxRequestBody 限制请求体大小，防滥用。
const maxRequestBody = 1 << 20 // 1MB

// Handler 是账号服务 HTTP 层。
type Handler struct {
	svc    *Service
	groups *GroupService
	logger *zap.Logger
}

// NewHandler 构造 HTTP handler（groups 可为 nil：不启用群管理端点）。
func NewHandler(svc *Service, groups *GroupService, logger *zap.Logger) *Handler {
	return &Handler{svc: svc, groups: groups, logger: logger}
}

// NewRouter 组装路由与中间件（access log + recover）。
func NewRouter(h *Handler) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/register", h.handleRegister)
	mux.HandleFunc("POST /api/v1/login", h.handleLogin)
	mux.HandleFunc("POST /internal/v1/verify", h.handleVerify)

	// 群管理（S9）：JWT 鉴权后注入 uid。
	mux.HandleFunc("POST /api/v1/groups", h.jwtAuth(h.handleCreateGroup))
	mux.HandleFunc("GET /api/v1/groups/{gid}/members", h.jwtAuth(h.handleListMembers))
	mux.HandleFunc("POST /api/v1/groups/{gid}/members", h.jwtAuth(h.handleAddMember))
	mux.HandleFunc("DELETE /api/v1/groups/{gid}/members/{uid}", h.jwtAuth(h.handleRemoveMember))
	mux.Handle("/metrics", promhttp.Handler())
	return mux
}

// Handler 返回挂好中间件的根 handler。
func (h *Handler) Handler() http.Handler {
	return h.recoverMiddleware(h.accessLogMiddleware(NewRouter(h)))
}

// --- 中间件 ---

// statusRecorder 记录响应状态码供访问日志使用。
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

func (h *Handler) accessLogMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)
		h.logger.Info("http access",
			zap.String("method", r.Method),
			zap.String("path", r.URL.Path),
			zap.Int("status", rec.status),
			zap.Duration("cost", time.Since(start)),
			zap.String("remote", r.RemoteAddr),
		)
	})
}

func (h *Handler) recoverMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				h.logger.Error("http panic",
					zap.Any("panic", rec),
					zap.String("path", r.URL.Path),
					zap.Stack("stack"),
				)
				writeJSON(w, http.StatusInternalServerError,
					envelope{Code: CodeStorageError, Msg: "服务内部错误"})
			}
		}()
		next.ServeHTTP(w, r)
	})
}

// --- 业务端点 ---

func (h *Handler) handleRegister(w http.ResponseWriter, r *http.Request) {
	var req RegisterRequest
	if !h.decodeBody(w, r, &req) {
		return
	}
	uid, err := h.svc.Register(r.Context(), req)
	if err != nil {
		h.writeBizError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, envelope{Code: CodeOK, Msg: "ok", Data: map[string]int64{"uid": uid}})
}

func (h *Handler) handleLogin(w http.ResponseWriter, r *http.Request) {
	var req LoginRequest
	if !h.decodeBody(w, r, &req) {
		return
	}
	res, err := h.svc.Login(r.Context(), req)
	if err != nil {
		h.writeBizError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, envelope{Code: CodeOK, Msg: "ok", Data: res})
}

func (h *Handler) handleVerify(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Token string `json:"token"`
	}
	if !h.decodeBody(w, r, &req) {
		return
	}
	res, err := h.svc.Verify(r.Context(), req.Token)
	if err != nil {
		// 校验失败仍返回 200 + 业务码，body 内 valid=false 便于调用方判断。
		var be *BizError
		if errors.As(err, &be) && be.Code == CodeUnauthorized {
			writeJSON(w, http.StatusOK, envelope{Code: be.Code, Msg: be.Msg, Data: res})
			return
		}
		h.writeBizError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, envelope{Code: CodeOK, Msg: "ok", Data: res})
}

// --- 工具 ---

// decodeBody 解析 JSON 请求体；失败时已写出响应并返回 false。
func (h *Handler) decodeBody(w http.ResponseWriter, r *http.Request, v any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBody)
	if err := json.NewDecoder(r.Body).Decode(v); err != nil {
		writeJSON(w, http.StatusBadRequest,
			envelope{Code: CodeBadParam, Msg: "请求体不是合法 JSON"})
		return false
	}
	return true
}

// writeBizError 把业务错误映射为 HTTP 状态码 + 统一响应。
func (h *Handler) writeBizError(w http.ResponseWriter, err error) {
	var be *BizError
	if !errors.As(err, &be) {
		be = &BizError{Code: CodeStorageError, Msg: "服务内部错误"}
	}
	h.logger.Warn("biz error", zap.Int("code", be.Code), zap.Error(err))

	status := http.StatusInternalServerError
	switch {
	case be.Code >= 40100 && be.Code < 40200:
		status = http.StatusUnauthorized
	case be.Code >= 40200 && be.Code < 40300:
		status = http.StatusBadRequest
	case be.Code >= 50200 && be.Code < 50300:
		status = http.StatusServiceUnavailable
	}
	writeJSON(w, status, envelope{Code: be.Code, Msg: be.Msg})
}

// envelope 是统一响应包装。
type envelope struct {
	Code int    `json:"code"`
	Msg  string `json:"msg"`
	Data any    `json:"data,omitempty"`
}

func writeJSON(w http.ResponseWriter, status int, e envelope) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(e)
}
