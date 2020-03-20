package pulsar

import (
	"context"
	"sync"
	"time"

	"github.com/TuyaInc/tuya_pulsar_sdk_go/pkg/tylog"
)

type ConsumerList struct {
	list             []*consumerImpl
	FlowPeriodSecond int
	FlowPermit       uint32
	Topic            string
	Stopped          chan struct{}
}

func (l *ConsumerList) ReceiveAndHandle(ctx context.Context, handler PayloadHandler) {
	wg := sync.WaitGroup{}
	for i := 0; i < len(l.list); i++ {
		safe := i
		wg.Add(1)
		go func() {
			l.list[safe].ReceiveAndHandle(ctx, handler)
			wg.Done()
		}()
	}
	go l.CronFlow()
	wg.Wait()
}

func (l *ConsumerList) CronFlow() {
	if l.FlowPeriodSecond == 0 {
		return
	}
	if l.FlowPermit == 0 {
		return
	}
	tk := time.NewTicker(time.Duration(l.FlowPeriodSecond) * time.Second)
	for {
		select {
		case <-l.Stopped:
			tk.Stop()
			tylog.Info("stop CronFlow", tylog.String("topic", l.Topic))
			return
		case <-tk.C:
			for i := 0; i < len(l.list); i++ {
				c := l.list[i].csm.Consumer(context.Background())
				if c == nil {
					continue
				}
				if len(c.Overflow) > 0 {
					tylog.Info("RedeliverOverflow",
						tylog.Any("topic", c.Topic),
						tylog.Any("num", len(c.Overflow)),
					)
					_, err := c.RedeliverOverflow(context.Background())
					if err != nil {
						tylog.Warn("RedeliverOverflow failed",
							tylog.Any("topic", c.Topic),
							tylog.ErrorField(err),
						)
					}
				}

				if c.Unactive || len(c.Queue) > 0 {
					continue
				}
				err := c.Flow(l.FlowPermit)
				if err != nil {
					tylog.Error("flow failed", tylog.ErrorField(err), tylog.String("topic", c.Topic))
				}
			}
		}
	}
}

func (l *ConsumerList) Stop() {
	tylog.Info("consumer will stop, waiting...", tylog.String("topic", l.Topic))
	close(l.Stopped)
	wg := sync.WaitGroup{}
	for i := 0; i < len(l.list); i++ {
		safei := i
		wg.Add(1)
		go func() {
			l.list[safei].Stop()
			wg.Done()
		}()
	}
	wg.Wait()
	tylog.Info("consumer stopped", tylog.String("topic", l.Topic))
}
