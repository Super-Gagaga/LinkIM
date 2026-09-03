package protocol

import "errors"

// 帧编解码错误。这些哨兵错误可安全地映射为 comet 层的业务错误码，
// 不会向客户端泄露内部细节。
var (
	// ErrBodyTooLarge 当 body 超过 MaxBodyLen 时返回。
	ErrBodyTooLarge = errors.New("protocol: body exceeds maximum length")

	// ErrMalformedFrame 当缓冲区无法解析为完整合法的帧时返回
	// （帧头过短、长度字段不匹配、版本非法等）。
	ErrMalformedFrame = errors.New("protocol: malformed frame")
)
