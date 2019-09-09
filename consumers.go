package pulsar

import (
	"context"
	"sync"
)

type ConsumerList struct {
	list             []*consumerImpl
	FlowPeriodSecond int
	FlowPermit       uint32
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
	wg.Wait()
}
