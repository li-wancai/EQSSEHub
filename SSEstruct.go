package EQSSEHub

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gin-contrib/sse"
	"github.com/gin-gonic/gin"
	"github.com/li-wancai/logger"
)

var (
	log             *logger.LogN
	eventCounter    uint64
	totalPushCount  uint64 // 总推送次数
	onlineConnCount uint64 // 当前在线连接数
)

func SetLogger(l *logger.LogN) {
	log = l //配置log信息
}

// genEventID 生成唯一事件ID（基础ID + 时间戳 + 原子计数器）
// 确保重连时可通过ID定位未接收的事件，避免数据丢失
func genEventID(baseID string) string {
	timestamp := time.Now().UnixMilli()           // 毫秒级时间戳（确保时序性）
	counter := atomic.AddUint64(&eventCounter, 1) // 原子计数器（避免并发ID重复）
	return fmt.Sprintf("%s-%d-%d", baseID, timestamp, counter)
}

// 客户端连接结构体
type clientConn struct {
	ctx      *gin.Context
	lastAct  int64           // 最后活跃时间（毫秒时间戳）
	sendChan chan *sse.Event // 消息发送通道（无缓冲）
	doneChan chan struct{}   // 关闭信号通道
	closed   uint32          // 通道关闭状态标记（0:未关闭，1:已关闭）- 新增状态标记
}

// 推送器
type PusherN struct {
	roomMap             sync.Map      // key: 房间ID(string), value: *sync.Map(key: 连接ID(string), value: *clientConn)
	heartbeatInterval   time.Duration // 心跳间隔
	slowClientThreshold time.Duration // 慢客户端判定阈值
	cleanupInterval     time.Duration // 连接清理间隔
}
