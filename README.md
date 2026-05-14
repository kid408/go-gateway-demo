# go-gateway-demo

`go-gateway-demo` 现在是 gRPC 接入层。

职责：

1. 发现 `worker-grpc`
2. 对外提供 `GatewayService/OpenSession` 双向流
3. 将 `login / heartbeat / logout` 事件转发给某个 worker
4. 接收 worker 的 gRPC 状态上报
5. 将处理后的会话事件写入 Kafka

它保留了 HTTP 端口，只做：

- `/healthz`
- `/workers`
- `/dispatch`

真正的业务通信已经改成 gRPC。

## 默认端口

- HTTP：`18080`
- gRPC：`19080`
- Metrics：`12112`

## 关键环境变量

- `CONSUL_HTTP_ADDR`
- `TARGET_DISCOVERY_SERVICE_NAME=worker-grpc`
- `APP_PORT`
- `GRPC_PORT`
- `METRICS_PORT`
- `KAFKA_BROKERS`
- `KAFKA_TOPIC`

## 本地运行

```powershell
go mod tidy
$env:CONSUL_HTTP_ADDR="http://127.0.0.1:8500"
$env:TARGET_DISCOVERY_SERVICE_NAME="worker-grpc"
$env:APP_PORT="18080"
$env:GRPC_PORT="19080"
$env:METRICS_PORT="12112"
go run .
```
