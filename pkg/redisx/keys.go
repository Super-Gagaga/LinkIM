package redisx

import "strconv"

// Redis 键构造器（设计文档 9.3）。花括号为占位符，由参数替换。
const (
	routePrefix       = "route:"        // + uid        Hash：device -> comet 地址
	cometAlivePrefix  = "comet:alive:"  // + addr       String，TTL 30s
	seqPrefix         = "seq:"          // + convID     String INCR
	idemPrefix        = "idem:"         // + sender:clientMsgID，String NX EX 600
	presencePrefix    = "presence:"     // + uid        String，TTL 90s
	tokenPrefix       = "token:"        // + uid        String，TTL 2h
	convMembersPrefix = "conv:members:" // + gid        Set
	friendPrefix      = "friend:"       // + uid        ZSet（score=updated_at）
	deliveredPrefix   = "delivered:"    // + uid:msgId  String，TTL 24h（投递观测）
)

// RouteKey 返回 uid 的连接路由表键（Hash：field 为设备，value 为 comet gRPC 地址）。
func RouteKey(uid int64) string { return routePrefix + strconv.FormatInt(uid, 10) }

// CometAliveKey 返回 comet 实例的存活键（TTL 30s，由实例心跳续期）。
func CometAliveKey(addr string) string { return cometAlivePrefix + addr }

// SeqKey 返回会话级序列号生成器键（INCR）。
func SeqKey(convID string) string { return seqPrefix + convID }

// IdemKey 返回（发送方， clientMsgID）维度的发送幂等键。
func IdemKey(senderID int64, clientMsgID string) string {
	return idemPrefix + strconv.FormatInt(senderID, 10) + ":" + clientMsgID
}

// PresenceKey 返回 uid 的在线状态键。
func PresenceKey(uid int64) string { return presencePrefix + strconv.FormatInt(uid, 10) }

// TokenKey 返回 uid 的登录 token 缓存键。
func TokenKey(uid int64) string { return tokenPrefix + strconv.FormatInt(uid, 10) }

// ConvMembersKey 返回 gid 的群成员缓存键。
func ConvMembersKey(gid int64) string { return convMembersPrefix + strconv.FormatInt(gid, 10) }

// FriendKey 返回 uid 的好友列表缓存键（ZSet）。
func FriendKey(uid int64) string { return friendPrefix + strconv.FormatInt(uid, 10) }

// DeliveredKey 返回（uid, msgId）维度的已投递观测键，TTL 24h。
func DeliveredKey(uid int64, msgID string) string {
	return deliveredPrefix + strconv.FormatInt(uid, 10) + ":" + msgID
}
