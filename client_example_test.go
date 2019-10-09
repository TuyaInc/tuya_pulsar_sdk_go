package pulsar

import (
	"context"
	"encoding/base64"
	"encoding/json"

	"github.com/TuyaInc/tuya_pulsar_sdk_go/pkg/tylog"
	"github.com/TuyaInc/tuya_pulsar_sdk_go/pkg/tyutils"
)

func ExampleConsumer() {
	// SetInternalLogLevel(logrus.DebugLevel)
	tylog.SetGlobalLog("sdk", false)
	accessID := "accessID"
	accessKey := "accessKey"
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
	csm.ReceiveAndHandle(context.Background(), &helloHandler{AesSecret: accessKey[8:24]})
	// Output:
}

type helloHandler struct {
	AesSecret string
}

func (h *helloHandler) HandlePayload(ctx context.Context, msg *Message, payload []byte) error {
	tylog.Info("payload preview", tylog.String("payload", string(payload)))

	// let's decode the payload with AES
	m := map[string]interface{}{}
	err := json.Unmarshal(payload, &m)
	if err != nil {
		tylog.Error("json unmarshal failed", tylog.ErrorField(err))
		return nil
	}
	bs := m["data"].(string)
	de, err := base64.StdEncoding.DecodeString(string(bs))
	if err != nil {
		tylog.Error("base64 decode failed", tylog.ErrorField(err))
		return nil
	}
	decode := tyutils.EcbDecrypt(de, []byte(h.AesSecret))
	tylog.Info("aes decode", tylog.ByteString("decode payload", decode))

	return nil
}
