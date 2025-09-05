package EQSSEHub

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gin-contrib/sse"
	"github.com/gin-gonic/gin"
)

// NewPusher 创建推送器实例
// heartbeatInterval: 心跳间隔（建议3-5秒）
// slowClientThreshold: 慢客户端判定阈值（建议500毫秒）
// cleanupInterval: 过期连接清理间隔（建议10秒）
func Pusher(heartbeatInterval, slowClientThreshold, cleanupInterval time.Duration) *PusherN {
	// 零值兜底
	if heartbeatInterval <= 0 {
		heartbeatInterval = 4 * time.Second
	}
	if slowClientThreshold <= 0 {
		slowClientThreshold = 500 * time.Millisecond
	}
	if cleanupInterval <= 0 {
		cleanupInterval = 10 * time.Second
	}
	// 初始化推送器
	p := &PusherN{
		heartbeatInterval:   heartbeatInterval,
		slowClientThreshold: slowClientThreshold,
		cleanupInterval:     cleanupInterval,
	}
	// 启动后台清理协程
	go p.startCleanup()
	return p
}

func (p *PusherN) ToSSeEvent(
	dataStr string /*字符串数据*/, eventType string, /*自定义数据类型*/
	baseID string /*事件ID*/, retry int /*重连间隔（毫秒）*/) *sse.Event {
	if eventType == "" {
		eventType = "message" // 默认事件类型
	}
	if retry < 0 {
		retry = 300 // 默认重试次数（300毫秒）
	}
	// 直接复用SSE格式处理逻辑
	formattedData := formatSSEData(dataStr)
	return &sse.Event{
		Event: eventType,
		Data:  formattedData,
		Id:    genEventID(baseID),
		Retry: uint(retry),
	}
}

// ------------------------------
// 内部工具函数：处理SSE数据格式
// ------------------------------

// formatSSEData 将原始字符串转换为符合SSE规范的数据格式
// 规则：
// 1. 若数据为空，返回 "data: \n"（空数据的标准格式）
// 2. 若数据含换行符，每行开头补 "data: "
// 3. 最终结尾补空行（SSE事件分隔符）
func formatSSEData(rawData string) string {
	if rawData == "" {
		return "data: \n"
	}

	// 步骤1：给第一行补 "data: " 前缀
	formatted := "data: " + rawData
	// 步骤2：给后续每行（含换行符的行）补 "data: " 前缀
	formatted = strings.ReplaceAll(formatted, "\n", "\ndata: ")
	// 步骤3：结尾补空行（SSE要求每个事件以空行结束，确保客户端能识别事件边界）
	formatted += "\n"

	return formatted
}

// 生成唯一连接ID（客户端IP + 时间戳 + 随机数）
func genConnID(c *gin.Context) string {
	ip := c.ClientIP()
	timestamp := time.Now().UnixNano()
	random := uint32(time.Now().UnixNano() % 1000000)
	return fmt.Sprintf("%s-%d-%d", ip, timestamp, random)
}

// safeCloseDoneChan 安全关闭doneChan（核心优化：避免重复关闭）
// 通过原子操作CAS判断状态，确保通道仅被关闭一次
func (p *PusherN) safeCloseDoneChan(cc *clientConn) {
	if atomic.CompareAndSwapUint32(&cc.closedDone, 0, 1) {
		close(cc.doneChan)
		log.Debugf("客户端doneChan已关闭, connID=%s", genConnID(cc.ctx))
	}
}
func (p *PusherN) safeCloseSendChan(cc *clientConn) {
	if atomic.CompareAndSwapUint32(&cc.closedSend, 0, 1) {
		close(cc.sendChan)
		log.Debugf("客户端sendChan已关闭, connID=%s", genConnID(cc.ctx))
	}
}

// JoinRoom 客户端加入房间
func (p *PusherN) JoinRoom(c *gin.Context, roomID string) {
	// 1. 初始化SSE响应头
	c.Header("Content-Type", "text/event-stream")
	c.Header("Cache-Control", "no-cache")
	c.Header("Connection", "keep-alive")
	c.Header("Access-Control-Allow-Origin", "*") // 生产环境建议替换为具体域名
	c.Header("X-Accel-Buffering", "no")          // 禁用反向代理缓冲（如Nginx）

	// 2. 创建客户端连接
	connID := genConnID(c) // 生成唯一连接ID
	cc := &clientConn{
		ctx:      c,
		lastAct:  time.Now().UnixMilli(),
		sendChan: make(chan *sse.Event),
		doneChan: make(chan struct{}),
	}

	// 3. 加入房间映射
	roomConnMap, _ := p.roomMap.LoadOrStore(roomID, &sync.Map{})
	rcMap := roomConnMap.(*sync.Map)
	rcMap.Store(connID, cc)
	atomic.AddUint64(&onlineConnCount, 1)

	// 4. 延迟清理资源
	defer func() {
		rcMap.Delete(connID)
		atomic.AddUint64(&onlineConnCount, ^uint64(0)) // 原子减1
		p.safeCloseDoneChan(cc)                        // 安全关闭doneChan（核心优化）
		p.safeCloseSendChan(cc)                        // 关闭消息发送通道（仅关闭一次，无重复风险）
		// 清理空房间
		empty := true
		rcMap.Range(func(_, _ interface{}) bool {
			empty = false
			return false
		})
		if empty {
			p.roomMap.Delete(roomID)
		}
	}()

	// 5. 启动消息发送协程（非阻塞发送）
	go p.startSendLoop(cc)
	// 6. 启动心跳协程
	go p.startHeartbeat(cc)
	// 7. 监听客户端关闭信号（秒级感知）
	select {
	case <-c.Request.Context().Done():
		return
	case <-cc.doneChan:
		return
	}
}

// PushToRoom 向指定房间推送单条行情
func (p *PusherN) PushToRoom(roomID string, stockEvent *sse.Event) error {
	roomConnMap, ok := p.roomMap.Load(roomID)
	if !ok {
		// 没客户的房间会自动清除，房间不存在，忽略推送
		return nil
	}

	rcMap := roomConnMap.(*sync.Map)
	rcMap.Range(func(_, val interface{}) bool {
		cc := val.(*clientConn)
		select {
		case <-cc.doneChan:
			// 客户端已关闭，跳过
		case cc.sendChan <- stockEvent:
			atomic.AddUint64(&totalPushCount, 1)
			atomic.StoreInt64(&cc.lastAct, time.Now().UnixMilli()) // 更新活跃时间
		case <-time.After(p.slowClientThreshold):
			// 慢客户端：超过阈值未接收，主动关闭
			p.safeCloseDoneChan(cc) // 安全关闭doneChan（核心优化）
			log.Warnf("自动清理慢客户端, roomID=%s, connID=%s", roomID, cc.ctx.ClientIP())

		}
		return true
	})
	return nil
}

// BatchPushToRoom 向指定房间批量推送行情
func (p *PusherN) BatchPushToRoom(roomID string, events []*sse.Event) error {
	roomConnMap, ok := p.roomMap.Load(roomID)
	if !ok {
		// 没客户的房间会自动清除，房间不存在，忽略推送
		return nil
	}
	rcMap := roomConnMap.(*sync.Map)
	rcMap.Range(func(_, val interface{}) bool {
		cc := val.(*clientConn)
		for _, event := range events {
			select {
			case <-cc.doneChan:
				return false
			case cc.sendChan <- event:
				atomic.AddUint64(&totalPushCount, 1)
				atomic.StoreInt64(&cc.lastAct, time.Now().UnixMilli())
			case <-time.After(p.slowClientThreshold):
				p.safeCloseDoneChan(cc) // 安全关闭doneChan（核心优化）
				log.Warnf("自动清理慢客户端, roomID=%s, connID=%s", roomID, cc.ctx.ClientIP())
				return false // 退出当前客户端的批量推送

			}
		}
		return true
	})
	return nil
}

// Broadcast 向所有房间广播行情
func (p *PusherN) Broadcast(stockEvent *sse.Event) {
	p.roomMap.Range(func(roomID, val interface{}) bool {
		rcMap := val.(*sync.Map)
		rcMap.Range(func(_, connVal interface{}) bool {
			cc := connVal.(*clientConn)
			select {
			case <-cc.doneChan:

			case cc.sendChan <- stockEvent:
				atomic.AddUint64(&totalPushCount, 1)
				atomic.StoreInt64(&cc.lastAct, time.Now().UnixMilli())
			case <-time.After(p.slowClientThreshold):
				p.safeCloseDoneChan(cc) // 安全关闭doneChan（核心优化）
				log.Warnf("自动清理慢客户端, roomID=%s, connID=%s", roomID, cc.ctx.ClientIP())

			}
			return true
		})
		return true
	})
}

// Metrics 推送指标结构体
type Metrics struct {
	TotalPushCount  uint64 // 总推送次数
	OnlineConnCount uint64 // 当前在线连接数
	RoomCount       int    // 当前房间数
}

// GetMetrics 获取当前指标
func (p *PusherN) GetMetrics() Metrics {
	return Metrics{
		TotalPushCount:  atomic.LoadUint64(&totalPushCount),
		OnlineConnCount: atomic.LoadUint64(&onlineConnCount),
		RoomCount:       p.getRoomCount(),
	}
}

// 启动消息发送循环（核心无锁逻辑）
func (p *PusherN) startSendLoop(cc *clientConn) {
	for {
		select {
		case event, ok := <-cc.sendChan:
			if !ok {
				return
			}
			// 发送SSE消息（带错误处理）
			if err := sse.Encode(cc.ctx.Writer, *event); err != nil {
				p.safeCloseDoneChan(cc) // 安全关闭doneChan（核心优化）
				log.Errorf("SSE消息发送失败, roomID=%s, connID=%s, err=%v", cc.ctx.ClientIP(), event.Id, err)
				return
			}
			// 强制刷新缓冲区（避免消息堆积）
			cc.ctx.Writer.Flush()
		case <-cc.doneChan:
			return
		}
	}
}

// 启动心跳机制
func (p *PusherN) startHeartbeat(cc *clientConn) {
	ticker := time.NewTicker(p.heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			heartbeatEvent := &sse.Event{
				Event: "heartbeat",
				Data:  "",
				Retry: uint(p.heartbeatInterval.Milliseconds()),
			}
			select {
			case <-cc.doneChan:
				return
			default:
			}
			// 检查 sendChan 是否已关闭
			select {
			case cc.sendChan <- heartbeatEvent:
				atomic.StoreInt64(&cc.lastAct, time.Now().UnixMilli())
			case <-time.After(p.slowClientThreshold):
				p.safeCloseDoneChan(cc)
				log.Warnf("自动清理慢客户端, connID=%s", cc.ctx.ClientIP())
				return
			case <-cc.doneChan:
				return
			}
		case <-cc.doneChan:
			return
		}
	}
}

// 启动后台清理协程（清理超时/离线连接）
func (p *PusherN) startCleanup() {
	ticker := time.NewTicker(p.cleanupInterval)
	defer ticker.Stop()

	for range ticker.C {
		now := time.Now().UnixMilli()
		timeoutThreshold := p.heartbeatInterval*3 + p.slowClientThreshold // 3个心跳周期未活跃判定为超时

		p.roomMap.Range(func(roomID, val interface{}) bool {
			rcMap := val.(*sync.Map)
			rcMap.Range(func(connID, connVal interface{}) bool {
				cc := connVal.(*clientConn)
				lastAct := atomic.LoadInt64(&cc.lastAct)
				// 判定超时：当前时间 - 最后活跃时间 > 超时阈值
				if now-lastAct > timeoutThreshold.Milliseconds() {
					p.safeCloseDoneChan(cc) // 安全关闭doneChan（核心优化）
					rcMap.Delete(connID)
					atomic.AddUint64(&onlineConnCount, ^uint64(0))
				}
				return true
			})

			// 清理空房间
			empty := true
			rcMap.Range(func(_, _ interface{}) bool {
				empty = false
				return false
			})
			if empty {
				p.roomMap.Delete(roomID)
			}
			return true
		})
	}
}

// 获取当前房间数
func (p *PusherN) getRoomCount() int {
	count := 0
	p.roomMap.Range(func(_, _ interface{}) bool {
		count++
		return true
	})
	return count
}
