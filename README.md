# pulsar-client-go

# example

```go
func ExampleConsumer() {
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
```

# About debug

通过下面的代码，你可以在终端看到所有和pulsar服务的通信
```go
func main(){
	pulsar.SetInternalLogLevel(logrus.DebugLevel)
	// other code
}
```

通过下面的代码，你可以看到tuya_pulsar_go_sdk的日志信息。
与此同时，日志会保存在logs/sdk.log文件中
```go
func main(){
	tylog.SetGlobalLog("sdk", false)
}
```

在正式环境，你可能不希望sdk日志都输出在终端，此时建议你使用下面的代码。
将日志只输出到文件中。
```go
func main(){
	tylog.SetGlobalLog("sdk", true)
}
```