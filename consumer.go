package pulsar

import (
	"context"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/TuyaInc/pulsar-client-go/core/manage"
	"github.com/TuyaInc/pulsar-client-go/core/msg"
	"github.com/TuyaInc/tuya_pulsar_sdk_go/pkg/tylog"
)

type ConsumerConfig struct {
	Topic string
	Auth  AuthProvider
}

type consumerImpl struct {
	topic      string
	csm        *manage.ManagedConsumer
	cancelFunc context.CancelFunc
	stopFlag   uint32
	stopped    chan struct{}
}

func (c *consumerImpl) ReceiveAsync(ctx context.Context, queue chan Message) {
	go func() {
		err := c.csm.ReceiveAsync(ctx, queue)
		if err != nil {
			tylog.Debug("consumer stopped", tylog.String("topic", c.topic))
		}
	}()
}

func (c *consumerImpl) ReceiveAndHandle(ctx context.Context, handler PayloadHandler) {
	queue := make(chan Message, 228)
	ctx, cancel := context.WithCancel(ctx)
	c.cancelFunc = cancel
	go c.ReceiveAsync(ctx, queue)

	for {
		select {
		case <-ctx.Done():
			close(c.stopped)
			return
		case m := <-queue:
			if atomic.LoadUint32(&c.stopFlag) == 1 {
				close(c.stopped)
				return
			}
			tylog.Debug("consumerImpl receive message", tylog.String("topic", c.topic))
			bgCtx := context.Background()
			c.Handler(bgCtx, handler, &m)
		}
	}
}

func (c *consumerImpl) Handler(ctx context.Context, handler PayloadHandler, m *Message) {
	fields := make([]zap.Field, 0, 10)
	fields = append(fields, tylog.Any("msgID", m.Msg.GetMessageId()))
	fields = append(fields, tylog.String("topic", m.Topic))
	defer func(start time.Time) {
		spend := time.Since(start)
		fields = append(fields, tylog.String("total spend", spend.String()))
		tylog.Debug("Handler trace info", fields...)
	}(time.Now())

	now := time.Now()
	var list []*msg.SingleMessage
	var err error
	num := m.Meta.GetNumMessagesInBatch()
	if num > 0 && m.Meta.NumMessagesInBatch != nil {
		list, err = msg.DecodeBatchMessage(m)
		if err != nil {
			tylog.Error("DecodeBatchMessage failed", tylog.ErrorField(err))
			return
		}
	}
	spend := time.Since(now)
	fields = append(fields, tylog.String("decode spend", spend.String()))

	now = time.Now()
	if c.csm.Unactive() {
		tylog.Warn("unused msg because of consumer is unactivated", tylog.Any("payload", string(m.Payload)))
		return
	}
	spend = time.Since(now)
	fields = append(fields, tylog.String("Unactive spend", spend.String()))

	idCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	now = time.Now()
	if c.csm.ConsumerID(idCtx) != m.ConsumerID {
		tylog.Warn("unused msg because of different ConsumerID", tylog.Any("payload", string(m.Payload)))
		return
	}
	spend = time.Since(now)
	fields = append(fields, tylog.String("ConsumerID spend", spend.String()))
	cancel()

	now = time.Now()
	if len(list) == 0 {
		err = handler.HandlePayload(ctx, m, m.Payload)
	} else {
		for i := 0; i < len(list); i++ {
			err = handler.HandlePayload(ctx, m, list[i].SinglePayload)
			if err != nil {
				break
			}
		}
	}
	spend = time.Since(now)
	fields = append(fields, tylog.String("HandlePayload spend", spend.String()))
	if err != nil {
		tylog.Error("handle message failed", tylog.ErrorField(err),
			tylog.String("topic", m.Topic),
		)
	}

	now = time.Now()
	ackCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	err = c.csm.Ack(ackCtx, *m)
	cancel()
	spend = time.Since(now)
	fields = append(fields, tylog.String("Ack spend", spend.String()))
	if err != nil {
		tylog.Error("ack failed", tylog.ErrorField(err))
	}

}

func (c *consumerImpl) Stop() {
	atomic.AddUint32(&c.stopFlag, 1)
	c.cancelFunc()
	<-c.stopped
}
