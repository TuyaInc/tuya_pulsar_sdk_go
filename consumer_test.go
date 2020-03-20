package pulsar

import (
	"context"
	"log"
	"testing"
	"time"

	"github.com/TuyaInc/tuya_pulsar_sdk_go/pkg/tylog"
)

func TestConsumerStop(t *testing.T) {
	tylog.SetGlobalLog("sdk", false)
	accessID := "xxx"
	accessKey := "xxx"
	topic := TopicForAccessID(accessID)

	// create client
	cfg := ClientConfig{
		PulsarAddr: PulsarAddrCN,
	}
	c := NewClient(cfg)

	// create consumer
	csmCfg := ConsumerConfig{
		Topic: topic,
		Auth:  NewAuthProvider(accessID, accessKey),
	}
	csm, _ := c.NewConsumer(csmCfg)

	// handle message
	sleep := time.Second * 5
	h := &helloHandler{
		AesSecret: accessKey[8:24],
		Sleep:     sleep,
	}
	go csm.ReceiveAndHandle(context.Background(), h)

	time.Sleep(5 * time.Second)
	now := time.Now()
	csm.Stop()
	since := time.Since(now)
	log.Println(since)
	if since > sleep+time.Second {
		t.Error("stop failed")
	}
}

type helloHandler struct {
	AesSecret string
	Sleep     time.Duration
}

func (h *helloHandler) HandlePayload(ctx context.Context, msg *Message, payload []byte) error {
	tylog.Info("received message and sleep...", tylog.Any("messageID", msg.Msg.GetMessageId()))
	time.Sleep(h.Sleep)
	tylog.Info("finish message", tylog.Any("messageID", msg.Msg.GetMessageId()))
	return nil
}
