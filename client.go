package pulsar

import (
	"context"
	"crypto/tls"
	"fmt"
	"strings"
	"time"

	"github.com/TuyaInc/tuya_pulsar_sdk_go/pkg/tylog"

	"github.com/TuyaInc/pulsar-client-go/core/manage"
	"github.com/TuyaInc/pulsar-client-go/core/msg"
)

const (
	DefaultFlowPeriodSecond = 30
	DefaultFlowPermit       = 10

	PulsarAddrCN = "pulsar://mqe.tuyacn.com:7285"
	PulsarAddrEU = "pulsar://mqe.tuyaeu.com:7285"
	PulsarAddrUS = "pulsar://mqe.tuyaus.com:7285"
)

type Message = msg.Message

type Client interface {
	NewConsumer(config ConsumerConfig) (Consumer, error)
}

type Consumer interface {
	ReceiveAndHandle(ctx context.Context, handler PayloadHandler)
	Stop()
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
	tylog.Info("start creating consumer",
		tylog.String("pulsar", c.Addr),
		tylog.String("topic", config.Topic),
	)

	errs := make(chan error, 10)
	go func() {
		for err := range errs {
			tylog.Error("async errors", tylog.ErrorField(err))
		}
	}()
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
		Topic:              config.Topic,
		SubMode:            manage.SubscriptionModeFailover,
		Name:               subscriptionName(config.Topic),
		NewConsumerTimeout: time.Minute,
	}
	p := c.GetPartition(config.Topic, cfg.ClientConfig)

	// partitioned topic
	if p > 0 {
		list := make([]*consumerImpl, 0, p)
		originTopic := cfg.Topic
		for i := 0; i < p; i++ {
			cfg.Topic = fmt.Sprintf("%s-partition-%d", originTopic, i)
			mc := manage.NewManagedConsumer(c.pool, cfg)
			list = append(list, &consumerImpl{
				csm:     mc,
				topic:   cfg.Topic,
				stopped: make(chan struct{}),
			})
		}
		consumerList := &ConsumerList{
			list:             list,
			FlowPeriodSecond: DefaultFlowPeriodSecond,
			FlowPermit:       DefaultFlowPermit,
			Topic:            config.Topic,
			Stopped:          make(chan struct{}),
		}
		return consumerList, nil
	}

	// single topic
	mc := manage.NewManagedConsumer(c.pool, cfg)
	tylog.Info("create consumer success",
		tylog.String("pulsar", c.Addr),
		tylog.String("topic", config.Topic),
	)
	return &consumerImpl{
		csm:     mc,
		topic:   cfg.Topic,
		stopped: make(chan struct{}),
	}, nil

}

func (c *client) GetPartition(topic string, config manage.ClientConfig) int {
	p, err := c.pool.Partitions(context.Background(), config, topic)
	if err != nil {
		return 0
	}
	return int(p.GetPartitions())
}

func TopicForAccessID(accessID string) string {
	topic := fmt.Sprintf("persistent://%s/out/event", accessID)
	return topic
}
