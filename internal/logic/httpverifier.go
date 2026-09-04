package logic

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// accountVerifyResponse 对应 account /internal/v1/verify 的响应体。
type accountVerifyResponse struct {
	Code int    `json:"code"`
	Msg  string `json:"msg"`
	Data struct {
		UID   int64 `json:"uid"`
		Valid bool  `json:"valid"`
	} `json:"data"`
}

// HTTPVerifier 通过 account 的内部 HTTP 接口校验 token。
type HTTPVerifier struct {
	addr   string
	client *http.Client
}

// NewHTTPVerifier 构造 account 校验客户端；timeout 为单次请求超时（1s）。
func NewHTTPVerifier(addr string, timeout time.Duration) *HTTPVerifier {
	return &HTTPVerifier{
		addr: addr,
		client: &http.Client{
			Timeout: timeout,
		},
	}
}

// Verify 实现 Verifier：POST /internal/v1/verify。
// 返回 account 侧解析出的真实 uid 与校验结果；
// 网络错误与非 2xx 均视为 account 不可达（返回错误）。
func (v *HTTPVerifier) Verify(ctx context.Context, uid int64, token string) (int64, bool, error) {
	body, err := json.Marshal(map[string]any{"token": token})
	if err != nil {
		return 0, false, fmt.Errorf("logic: marshal verify req: %w", err)
	}
	url := v.addr + "/internal/v1/verify"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return 0, false, fmt.Errorf("logic: build verify req: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := v.client.Do(req)
	if err != nil {
		return 0, false, fmt.Errorf("logic: post %s: %w", url, err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return 0, false, fmt.Errorf("logic: %s returned status %d", url, resp.StatusCode)
	}

	var out accountVerifyResponse
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxVerifyBody)).Decode(&out); err != nil {
		return 0, false, fmt.Errorf("logic: decode verify resp: %w", err)
	}
	return out.Data.UID, out.Data.Valid, nil
}

// maxVerifyBody 限制响应体读取，防御异常上游。
const maxVerifyBody = 1 << 20
