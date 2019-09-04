# pulsar-client-go

## 使用前准备

1. AccessID 由涂鸦平台提供
2. AccessKey 由涂鸦平台提供
3. pulsar地址 根据不同的业务区域，选择pulsar地址。可以从涂鸦对接文档中查询

## Example

```
package main

import (
	"context"
	"fmt"
	"time"

	pulsar "github.com/TuyaInc/tuya_pulsar_sdk_go"
	"github.com/TuyaInc/tuya_pulsar_sdk_go/pkg/tylog"
	"github.com/sirupsen/logrus"
)

func main() {
	pulsar.SetInternalLogLevel(logrus.DebugLevel)
	tylog.SetGlobalLog("sdk", false)
	accessID := "accessID"
	accessKey := "accessKey"
	topic := fmt.Sprintf("persistent://%s/out/event", accessID)

	// create client
	cfg := pulsar.ClientConfig{
		PulsarAddr: "pulsar://mqe.tuyaus.com:7285",
	}
	c := pulsar.NewClient(cfg)

	// create consumer
	csmCfg := pulsar.ConsumerConfig{
		Topic: topic,
		Auth:  pulsar.NewAuthProvider(accessID, accessKey),
	}
	csm, _ := c.NewConsumer(csmCfg)

	// handle message
	csm.ReceiveAndHandle(context.Background(), &helloHandler{})

	time.Sleep(10 * time.Second)
}

type helloHandler struct{}

func (h *helloHandler) HandlePayload(ctx context.Context, msg *pulsar.Message, payload []byte) error {
	tylog.Info("payload", tylog.String("payload", string(payload)))
	return nil
}

```

## About debug

通过下面的代码，你可以在终端看到所有和pulsar服务的通信
```
func main(){
	pulsar.SetInternalLogLevel(logrus.DebugLevel)
	// other code
}
```

通过下面的代码，你可以看到tuya_pulsar_go_sdk的日志信息。
与此同时，日志会保存在logs/sdk.log文件中
```
func main(){
	tylog.SetGlobalLog("sdk", false)
}
```

在正式环境，你可能不希望sdk日志都输出在终端，此时建议你使用下面的代码。
将日志只输出到文件中。
```
func main(){
	tylog.SetGlobalLog("sdk", true)
}
```