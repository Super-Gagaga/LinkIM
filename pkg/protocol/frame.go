// Package protocol 实现 LinkIM 基于 WebSocket 承载的二进制帧编解码器
// （见设计文档第 4 节）。
//
// 帧布局（多字节字段一律大端序）：
//
//	0        1        3        7        11       11+len
//	+--------+--------+--------+--------+--------+
//	| ver(8) | cmd(16)| seq(32)| len(32)|  body  |
//	+--------+--------+--------+--------+--------+
//	 1B        2B       4B       4B       len B
package protocol

import (
	"encoding/binary"
	"errors"
	"io"
)

const (
	// Ver 是当前协议版本号。
	Ver uint8 = 1

	// HeaderLen 是定长帧头大小：ver(1) + cmd(2) + seq(4) + len(4)。
	HeaderLen = 11

	// MaxBodyLen 是帧 body 的上限（64KB），用于防御恶意超大包（设计文档 4.2）。
	MaxBodyLen = 64 * 1024
)

// Frame 是一帧解码后的 LinkIM 协议帧。
type Frame struct {
	Ver  uint8
	Cmd  Cmd
	Seq  uint32
	Body []byte
}

// Encode 将 f 序列化为完整帧缓冲。
// 当 body 超过 MaxBodyLen 时返回 ErrBodyTooLarge。
func Encode(f Frame) ([]byte, error) {
	if len(f.Body) > MaxBodyLen {
		return nil, ErrBodyTooLarge
	}
	buf := make([]byte, HeaderLen+len(f.Body))
	putHeader(buf, f)
	copy(buf[HeaderLen:], f.Body)
	return buf, nil
}

// putHeader 将 f 的 11 字节帧头写入 buf（要求 len(buf) >= HeaderLen）。
func putHeader(buf []byte, f Frame) {
	buf[0] = f.Ver
	binary.BigEndian.PutUint16(buf[1:3], uint16(f.Cmd))
	binary.BigEndian.PutUint32(buf[3:7], f.Seq)
	binary.BigEndian.PutUint32(buf[7:11], uint32(len(f.Body)))
}

// Decode 从 data 中解析出一个完整帧。
// 当 data 短于帧头或末尾字节数与帧头长度字段不一致时返回 ErrMalformedFrame；
// 当声明的 body 长度超过 MaxBodyLen 时返回 ErrBodyTooLarge。
func Decode(data []byte) (Frame, error) {
	if len(data) < HeaderLen {
		return Frame{}, ErrMalformedFrame
	}
	bodyLen := binary.BigEndian.Uint32(data[7:11])
	if bodyLen > MaxBodyLen {
		return Frame{}, ErrBodyTooLarge
	}
	if uint32(len(data)-HeaderLen) != bodyLen {
		return Frame{}, ErrMalformedFrame
	}
	body := make([]byte, bodyLen)
	copy(body, data[HeaderLen:])
	return frameFromHeader(data, body), nil
}

// frameFromHeader 由 11 字节帧头与 body 切片重建一个 Frame。
func frameFromHeader(header []byte, body []byte) Frame {
	return Frame{
		Ver:  header[0],
		Cmd:  Cmd(binary.BigEndian.Uint16(header[1:3])),
		Seq:  binary.BigEndian.Uint32(header[3:7]),
		Body: body,
	}
}

// StreamReader 从底层字节流中读取完整帧，通过 io.ReadFull 正确处理
// 粘包（一次读到多帧）与半包（一帧分多次到达）。
type StreamReader struct {
	r      io.Reader
	header [HeaderLen]byte
}

// NewStreamReader 返回一个从 r 读取帧的 StreamReader。
func NewStreamReader(r io.Reader) *StreamReader {
	return &StreamReader{r: r}
}

// ReadFrame 阻塞直到读完一个完整帧并返回它。
// 帧与帧之间流干净结束时返回 io.EOF；帧读到一半流结束时返回
// io.ErrUnexpectedEOF。body 超长时返回 ErrBodyTooLarge——此时流位置
// 已不可确定，调用方应视该连接为已损坏。
func (sr *StreamReader) ReadFrame() (Frame, error) {
	if _, err := io.ReadFull(sr.r, sr.header[:]); err != nil {
		return Frame{}, err
	}
	bodyLen := binary.BigEndian.Uint32(sr.header[7:11])
	if bodyLen > MaxBodyLen {
		return Frame{}, ErrBodyTooLarge
	}
	// 心跳、ACK 等零长度 body 极为常见，避免为其分配内存。
	var body []byte
	if bodyLen > 0 {
		body = make([]byte, bodyLen)
		if _, err := io.ReadFull(sr.r, body); err != nil {
			// 帧头已被消费，因此此处即使收到"干净"的 EOF 也意味着帧不完整。
			if errors.Is(err, io.EOF) {
				return Frame{}, io.ErrUnexpectedEOF
			}
			return Frame{}, err
		}
	}
	return frameFromHeader(sr.header[:], body), nil
}
