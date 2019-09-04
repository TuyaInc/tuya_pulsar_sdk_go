package pulsar

import (
	"context"
	"fmt"

	"github.com/TuyaInc/tuya_pulsar_sdk_go/pkg/tylog"
)

func ExampleConsumer() {
	// SetInternalLogLevel(logrus.DebugLevel)
	tylog.SetGlobalLog("sdk", false)
	accessID := "accessID"
	accessKey := "accessKey"
	topic := fmt.Sprintf("persistent://%s/out/event", accessID)

	// create client
	cfg := ClientConfig{
		PulsarAddr: "pulsar://mqe.tuyacn.com:7285",
	}
	c := NewClient(cfg)

	// create consumer
	csmCfg := ConsumerConfig{
		Topic: topic,
		Auth:  NewAuthProvider(accessID, accessKey),
	}
	csm, _ := c.NewConsumer(csmCfg)

	// handle message
	csm.ReceiveAndHandle(context.Background(), &helloHandler{})
	// Output:
}

type helloHandler struct{}

func (h *helloHandler) HandlePayload(ctx context.Context, msg *Message, payload []byte) error {
	tylog.Info("payload", tylog.String("payload", string(payload)))
	return nil
}
