package pb

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// TestMarshalRoundTrip 对协议核心消息做序列化→反序列化往返，
// 固定生成代码的可用性。
func TestMarshalRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		msg  proto.Message
	}{
		{
			name: "MsgSendReq",
			msg: &MsgSendReq{
				ClientMsgId: "c-uuid-1",
				ConvId:      "c:1:2",
				ConvType:    1,
				MsgType:     1,
				Payload:     []byte("hello"),
			},
		},
		{
			name: "MsgSendAck",
			msg: &MsgSendAck{
				Code:        0,
				ClientMsgId: "c-uuid-1",
				MsgId:       "123456789",
				Seq:         42,
				Timestamp:   1700000000,
			},
		},
		{
			name: "MsgPush",
			msg: &MsgPush{
				MsgId:     "123456789",
				ConvId:    "c:1:2",
				ConvType:  1,
				SenderId:  1,
				MsgType:   1,
				Payload:   []byte("push-body"),
				Seq:       43,
				Timestamp: 1700000001,
			},
		},
		{
			name: "AuthReq",
			msg:  &AuthReq{Token: "jwt-token", DeviceId: "d1", Platform: 4},
		},
		{
			name: "AuthAck",
			msg:  &AuthAck{Code: 0, Msg: "ok", Uid: 7, KickReason: 0},
		},
		{
			name: "HeartbeatReq",
			msg:  &HeartbeatReq{},
		},
		{
			name: "SyncPullReq",
			msg:  &SyncPullReq{ConvId: "c:1:2", LocalMaxSeq: 10, Limit: 100},
		},
		{
			name: "SyncResp",
			msg: &SyncResp{
				ConvId: "c:1:2",
				Messages: []*MsgPush{
					{MsgId: "1", ConvId: "c:1:2", Seq: 11},
					{MsgId: "2", ConvId: "c:1:2", Seq: 12},
				},
				MaxSeq: 12,
			},
		},
		{
			name: "ReceivedAckReq",
			msg:  &ReceivedAckReq{MsgId: "123456789", ConvId: "c:1:2", Seq: 43},
		},
		{
			name: "KickReq",
			msg:  &KickReq{Reason: "kicked by same-platform login"},
		},
		{
			name: "MsgRecallReq",
			msg:  &MsgRecallReq{ConvId: "c:1:2", MsgId: "123456789"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := proto.Marshal(tt.msg)
			require.NoError(t, err)

			got := tt.msg.ProtoReflect().New().Interface()
			require.NoError(t, proto.Unmarshal(data, got))
			assert.True(t, proto.Equal(tt.msg, got))
		})
	}
}
