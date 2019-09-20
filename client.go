package pulsar

import (
	"context"
	"crypto/tls"
	"fmt"
	"strings"
	"sync"

	"github.com/TuyaInc/tuya_pulsar_sdk_go/pkg/tylog"

	"github.com/TuyaInc/pulsar-client-go/core/manage"
	"github.com/TuyaInc/pulsar-client-go/core/msg"
)

const (
	DefaultFlowPeriodSecond = 30
	DefaultFlowPermit       = 10
)

type Message = msg.Message

type Client interface {
	NewConsumer(config ConsumerConfig) (Consumer, error)
}

type Consumer interface {
	ReceiveAndHandle(ctx context.Context, handler PayloadHandler)
}

type PayloadHandler interface {
	HandlePayload(ctx context.Context, msg *Message, payload []byte) error
}

type ClientConfig struct {
	PulsarAddr string
}

type client struct {
	pool *manage.ClientPool
	Addr string
}

func NewClient(cfg ClientConfig) Client {
	return &client{
		pool: manage.NewClientPool(),
		Addr: cfg.PulsarAddr,
	}
}

func subscriptionName(topic string) string {
	return getTenant(topic) + "-sub"
}

func getTenant(topic string) string {
	topic = strings.TrimPrefix(topic, "persistent://")
	end := strings.Index(topic, "/")
	return topic[:end]
}

func (c *client) NewConsumer(config ConsumerConfig) (Consumer, error) {
	errs := make(chan error, 10)
	cfg := manage.ConsumerConfig{
		ClientConfig: manage.ClientConfig{
			Addr:       c.Addr,
			AuthData:   config.Auth.AuthData(),
			AuthMethod: config.Auth.AuthMethod(),
			TLSConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
			Errs: errs,
		},

		Topic:   config.Topic,
		SubMode: manage.SubscriptionModeFailover,
		Name:    subscriptionName(config.Topic),
	}
	p := c.GetPartition(config.Topic, cfg.ClientConfig)
	wg := &sync.WaitGroup{}
	// partitioned topic
	if p > 0 {
		list := make([]*consumerImpl, 0, p)
		originTopic := cfg.Topic
		for i := 0; i < p; i++ {
			cfg.Topic = fmt.Sprintf("%s-partition-%d", originTopic, i)
			mc := manage.NewManagedConsumer(c.pool, cfg)
			list = append(list, &consumerImpl{csm: mc, wg: wg})
			go func() {
				for err := range errs {
					tylog.Error("async errors", tylog.ErrorField(err))
				}
			}()
		}
		consumerList := &ConsumerList{
			list:             list,
			FlowPeriodSecond: DefaultFlowPeriodSecond,
			FlowPermit:       DefaultFlowPermit,
		}
		return consumerList, nil
	}

	// single topic
	mc := manage.NewManagedConsumer(c.pool, cfg)
	go func() {
		for err := range errs {
			tylog.Error("async errors", tylog.ErrorField(err))
		}
	}()
	return &consumerImpl{csm: mc, wg: wg}, nil

}

func (c *client) GetPartition(topic string, config manage.ClientConfig) int {
	p, err := c.pool.Partitions(context.Background(), config, topic)
	if err != nil {
		return 0
	}
	return int(p.GetPartitions())
}
