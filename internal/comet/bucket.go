package comet

import (
	"strconv"
	"sync"
)

// bucketNum 是分片数量：uid%bucketNum 路由到对应 bucket，避免全局锁。
const bucketNum = 256

// Bucket 是按 uid 分片的连接表（设计文档 15.1：分片 map + 读写锁）。
type Bucket struct {
	mu    sync.RWMutex
	conns map[string]*Conn
}

// newBucket 创建空分片。
func newBucket() *Bucket { return &Bucket{conns: make(map[string]*Conn)} }

// put 写入连接；若同 key 已有旧连接则返回之（调用方负责关闭旧连接）。
func (b *Bucket) put(key string, c *Conn) *Conn {
	b.mu.Lock()
	defer b.mu.Unlock()
	old := b.conns[key]
	b.conns[key] = c
	return old
}

// get 读取连接；不存在返回 nil。
func (b *Bucket) get(key string) *Conn {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.conns[key]
}

// del 删除连接（存在时返回 true）。
func (b *Bucket) del(key string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.conns[key]; !ok {
		return false
	}
	delete(b.conns, key)
	return true
}

// size 返回当前连接数。
func (b *Bucket) size() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.conns)
}

// idleConns 返回空闲超过 timeout 秒的已鉴权连接（不执行关闭，
// 由调用方 Close；now 注入便于测试）。
func (b *Bucket) idleConns(now int64, timeoutSec int64) []*Conn {
	b.mu.RLock()
	defer b.mu.RUnlock()
	var victims []*Conn
	for _, c := range b.conns {
		if c.authed.Load() && c.idleSeconds(now) > timeoutSec {
			victims = append(victims, c)
		}
	}
	return victims
}

// bucketIndex 由 uid 计算分片下标。
func bucketIndex(uid int64) int {
	return int(uint64(uid) % bucketNum)
}

// routeField 返回路由表 hash 的 field 编码：platform:device_id
// （同 platform 互踢依赖该编码识别旧连接，见设计文档 7.3）。
func routeField(platform int32, deviceID string) string {
	return strconv.FormatInt(int64(platform), 10) + ":" + deviceID
}
