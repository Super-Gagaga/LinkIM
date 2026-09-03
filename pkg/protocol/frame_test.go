package protocol

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestEncodeDecodeRoundTrip 覆盖编码→解码的往返一致性，
// 包含空 body 与 64KB 边界情况。
func TestEncodeDecodeRoundTrip(t *testing.T) {
	maxBody := bytes.Repeat([]byte{0xAB}, MaxBodyLen)

	tests := []struct {
		name  string
		frame Frame
	}{
		{
			name:  "typical frame with body",
			frame: Frame{Ver: Ver, Cmd: CmdMsgSend, Seq: 42, Body: []byte("hello linkim")},
		},
		{
			name:  "empty body (heartbeat-like)",
			frame: Frame{Ver: Ver, Cmd: CmdHeartbeat, Seq: 1, Body: nil},
		},
		{
			name:  "empty body explicit empty slice",
			frame: Frame{Ver: Ver, Cmd: CmdHeartbeatAck, Seq: 7, Body: []byte{}},
		},
		{
			name:  "maximum body exactly 64KB",
			frame: Frame{Ver: Ver, Cmd: CmdMsgPush, Seq: 0xFFFFFFFF, Body: maxBody},
		},
		{
			name:  "maximum command word and version byte",
			frame: Frame{Ver: 0xFF, Cmd: Cmd(0xFFFF), Seq: 1, Body: []byte("x")},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf, err := Encode(tt.frame)
			require.NoError(t, err)
			require.Len(t, buf, HeaderLen+len(tt.frame.Body))

			got, err := Decode(buf)
			require.NoError(t, err)
			assert.Equal(t, tt.frame.Ver, got.Ver)
			assert.Equal(t, tt.frame.Cmd, got.Cmd)
			assert.Equal(t, tt.frame.Seq, got.Seq)
			// Decode 会把 body 拷贝到独立切片，nil 与空切片解码后均为空
			// body，因此按字节比较语义内容。
			if len(tt.frame.Body) == 0 {
				assert.Empty(t, got.Body)
			} else {
				assert.Equal(t, tt.frame.Body, got.Body)
			}
			// 解码后的 body 不得与输入缓冲共享底层内存：事后篡改编码
			// 缓冲，解码出的 body 必须不受影响。
			if len(buf) > HeaderLen {
				buf[HeaderLen] ^= 0xFF
				assert.Equal(t, tt.frame.Body, got.Body, "body must be a copy, not an alias")
			}
		})
	}
}

// TestEncodeBodyTooLarge 验证超过 MaxBodyLen 的 body（64KB+1）
// 在编码阶段即被拒绝。
func TestEncodeBodyTooLarge(t *testing.T) {
	f := Frame{Ver: Ver, Cmd: CmdMsgSend, Seq: 1, Body: make([]byte, MaxBodyLen+1)}
	_, err := Encode(f)
	assert.ErrorIs(t, err, ErrBodyTooLarge)
}

// TestDecodeErrors 覆盖畸形与超长的输入。
func TestDecodeErrors(t *testing.T) {
	valid, err := Encode(Frame{Ver: Ver, Cmd: CmdAuth, Seq: 9, Body: []byte("token")})
	require.NoError(t, err)

	tests := []struct {
		name    string
		input   []byte
		wantErr error
	}{
		{name: "empty buffer", input: nil, wantErr: ErrMalformedFrame},
		{name: "truncated header", input: valid[:HeaderLen-1], wantErr: ErrMalformedFrame},
		{name: "missing body bytes", input: valid[:len(valid)-1], wantErr: ErrMalformedFrame},
		{name: "trailing extra bytes", input: append(append([]byte{}, valid...), 0x00), wantErr: ErrMalformedFrame},
		{
			name:    "declared body length above limit",
			input:   headerWithBodyLen(uint32(MaxBodyLen + 1)),
			wantErr: ErrBodyTooLarge,
		},
		{
			name:    "declared body length far above limit",
			input:   headerWithBodyLen(0xFFFFFFFF),
			wantErr: ErrBodyTooLarge,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Decode(tt.input)
			assert.ErrorIs(t, err, tt.wantErr)
		})
	}
}

// headerWithBodyLen 构造一个声明指定 body 长度的 11 字节帧头。
func headerWithBodyLen(n uint32) []byte {
	h := make([]byte, HeaderLen)
	h[0] = Ver
	binary.BigEndian.PutUint16(h[1:3], uint16(CmdMsgSend))
	binary.BigEndian.PutUint32(h[3:7], 1)
	binary.BigEndian.PutUint32(h[7:11], n)
	return h
}

// TestStreamReaderStickyPackets 将两个首尾相接的帧从同一个缓冲读入；
// 两次 ReadFrame 必须按序分别返回两帧。
func TestStreamReaderStickyPackets(t *testing.T) {
	f1 := Frame{Ver: Ver, Cmd: CmdAuth, Seq: 100, Body: []byte("first")}
	f2 := Frame{Ver: Ver, Cmd: CmdHeartbeat, Seq: 101, Body: nil}

	b1, err := Encode(f1)
	require.NoError(t, err)
	b2, err := Encode(f2)
	require.NoError(t, err)

	sr := NewStreamReader(bytes.NewReader(append(b1, b2...)))

	got1, err := sr.ReadFrame()
	require.NoError(t, err)
	assert.Equal(t, f1, got1)

	got2, err := sr.ReadFrame()
	require.NoError(t, err)
	assert.Equal(t, f2, got2)

	// 此后流已耗尽。
	_, err = sr.ReadFrame()
	assert.ErrorIs(t, err, io.EOF)
}

// gatedReader 是一个每次 Read 只发放一个分块、分块之间阻塞的
// io.Reader，使测试中"半包到达"的时序完全确定。
type gatedReader struct {
	chunks  chan []byte
	pending []byte
}

func (g *gatedReader) Read(p []byte) (int, error) {
	for len(g.pending) == 0 {
		chunk, ok := <-g.chunks
		if !ok {
			return 0, io.EOF
		}
		g.pending = chunk
	}
	n := copy(p, g.pending)
	g.pending = g.pending[n:]
	return n, nil
}

// TestStreamReaderPartialFrames 验证对慢写方的阻塞语义：
// 读取方必须等待剩余字节到达，而不是在半包读取上失败。
func TestStreamReaderPartialFrames(t *testing.T) {
	frame := Frame{Ver: Ver, Cmd: CmdMsgSend, Seq: 7, Body: []byte("split me")}
	raw, err := Encode(frame)
	require.NoError(t, err)

	g := &gatedReader{chunks: make(chan []byte)}
	type result struct {
		frame Frame
		err   error
	}
	resCh := make(chan result, 1)
	go func() {
		f, err := NewStreamReader(g).ReadFrame()
		resCh <- result{frame: f, err: err}
	}()

	// 只喂前 5 个帧头字节；ReadFrame 必须仍处于阻塞状态。
	g.chunks <- raw[:5]
	select {
	case res := <-resCh:
		t.Fatalf("ReadFrame returned on a partial header: %+v err=%v", res.frame, res.err)
	case <-time.After(50 * time.Millisecond):
	}

	// 喂帧头剩余部分加上 body 的前几个字节；仍不完整。
	g.chunks <- raw[5 : HeaderLen+3]
	select {
	case res := <-resCh:
		t.Fatalf("ReadFrame returned on a partial body: %+v err=%v", res.frame, res.err)
	case <-time.After(50 * time.Millisecond):
	}

	// 喂剩余的 body 字节；此刻必须完成。
	g.chunks <- raw[HeaderLen+3:]
	select {
	case res := <-resCh:
		require.NoError(t, res.err)
		assert.Equal(t, frame, res.frame)
	case <-time.After(2 * time.Second):
		t.Fatal("ReadFrame did not unblock after all bytes arrived")
	}
	close(g.chunks)
}

// TestStreamReaderErrors 覆盖超长声明与帧中途 EOF。
func TestStreamReaderErrors(t *testing.T) {
	t.Run("oversized declared body length", func(t *testing.T) {
		sr := NewStreamReader(bytes.NewReader(headerWithBodyLen(uint32(MaxBodyLen + 1))))
		_, err := sr.ReadFrame()
		assert.ErrorIs(t, err, ErrBodyTooLarge)
	})

	t.Run("stream ends mid-body yields ErrUnexpectedEOF", func(t *testing.T) {
		// 声明 8 字节 body，但一个字节都未提供（帧头已被消费）。
		sr := NewStreamReader(bytes.NewReader(headerWithBodyLen(8)))
		_, err := sr.ReadFrame()
		assert.ErrorIs(t, err, io.ErrUnexpectedEOF)
	})

	t.Run("stream ends mid-body with partial bytes yields ErrUnexpectedEOF", func(t *testing.T) {
		raw := append(headerWithBodyLen(8), []byte("abc")...) // 8 字节 body 只到 3 字节
		sr := NewStreamReader(bytes.NewReader(raw))
		_, err := sr.ReadFrame()
		assert.ErrorIs(t, err, io.ErrUnexpectedEOF)
	})

	t.Run("stream ends mid-header yields ErrUnexpectedEOF", func(t *testing.T) {
		sr := NewStreamReader(bytes.NewReader(make([]byte, 5)))
		_, err := sr.ReadFrame()
		assert.ErrorIs(t, err, io.ErrUnexpectedEOF)
	})
}

// TestStreamReaderStreamRoundTrip 将多帧连续写入流并经流读取器读出，
// 与 Decode 的结果逐一比对。
func TestStreamReaderStreamRoundTrip(t *testing.T) {
	frames := []Frame{
		{Ver: Ver, Cmd: CmdAuth, Seq: 1, Body: []byte("auth-body")},
		{Ver: Ver, Cmd: CmdHeartbeat, Seq: 2},
		{Ver: Ver, Cmd: CmdMsgSend, Seq: 3, Body: bytes.Repeat([]byte("z"), 1024)},
		{Ver: Ver, Cmd: CmdMsgPush, Seq: 4, Body: []byte("push")},
		{Ver: Ver, Cmd: CmdSyncResp, Seq: 5, Body: []byte("{}")},
	}
	var buf bytes.Buffer
	for _, f := range frames {
		b, err := Encode(f)
		require.NoError(t, err)
		buf.Write(b)
	}

	sr := NewStreamReader(&buf)
	for i, want := range frames {
		got, err := sr.ReadFrame()
		require.NoError(t, err, "frame %d", i)
		assert.Equal(t, want, got, "frame %d", i)
	}
	_, err := sr.ReadFrame()
	assert.ErrorIs(t, err, io.EOF)
}

// TestCmdString 覆盖所有已定义命令字的可读名称以及未知命令字的兜底格式。
func TestCmdString(t *testing.T) {
	tests := []struct {
		cmd  Cmd
		want string
	}{
		{CmdAuth, "AUTH"},
		{CmdAuthAck, "AUTH_ACK"},
		{CmdHeartbeat, "HEARTBEAT"},
		{CmdHeartbeatAck, "HEARTBEAT_ACK"},
		{CmdMsgSend, "MSG_SEND"},
		{CmdMsgSendAck, "MSG_SEND_ACK"},
		{CmdMsgPush, "MSG_PUSH"},
		{CmdMsgReceivedAck, "MSG_RECEIVED_ACK"},
		{CmdSyncPull, "SYNC_PULL"},
		{CmdSyncResp, "SYNC_RESP"},
		{CmdRecall, "RECALL"},
		{CmdTyping, "TYPING"},
		{Cmd(0x00), "UNKNOWN_0x0000"},
		{Cmd(0xFF), "UNKNOWN_0x00FF"},
		{Cmd(0x1234), "UNKNOWN_0x1234"},
	}
	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			assert.Equal(t, tt.want, CmdString(tt.cmd))
		})
	}
}

// TestConstantsGuard 固定设计文档 4.2 的线上格式常量。
func TestConstantsGuard(t *testing.T) {
	assert.Equal(t, uint8(1), Ver)
	assert.Equal(t, 11, HeaderLen)
	assert.Equal(t, 64*1024, MaxBodyLen)
}

// TestErrorSentinels 确保导出的哨兵错误互不相同，
// 且可与 errors.Is 配合使用。
func TestErrorSentinels(t *testing.T) {
	assert.False(t, errors.Is(ErrMalformedFrame, ErrBodyTooLarge))
	assert.False(t, errors.Is(ErrBodyTooLarge, ErrMalformedFrame))
}
