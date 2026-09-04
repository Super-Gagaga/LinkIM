package comet

// Comet 层业务错误码（沿用全局分段）。
const (
	// CodeAuthFailed 鉴权失败（透传 logic 的 40101/50102）。
	CodeAuthFailed = 40101
	// CodeKicked 同端互踢被顶替下线。
	CodeKicked = 40103
	// CodeNotImplemented 该命令字尚未实现（S6/S7/S8 将替换 dispatch）。
	CodeNotImplemented = 50104
)
