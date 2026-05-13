# go-gateway-demo

`go-gateway-demo` 是“入口层”示例服务。

它的职责不是自己干重活，而是：

1. 在 Consul 中发现 `worker-http`
2. 周期性随机把任务分发给 worker
3. 接收 worker 主动回报的状态
4. 输出更像入口层的指标和日志

## 主要接口

- `GET /`
- `GET /healthz`
- `GET /health`
- `GET /workers`
- `GET /dispatch?task_type=checkout_cart&delay_ms=300`
- `POST /worker/report`
- `GET /metrics`

## 关键指标

- `go_gateway_process_up`
- `go_gateway_discovered_workers`
- `go_gateway_dispatch_total`
- `go_gateway_dispatch_duration_seconds`
- `go_gateway_online_users`
- `go_gateway_worker_reports_total`
- `go_gateway_last_reported_queue_depth`
- `go_gateway_last_reported_temperature_celsius`

## 本地直跑

```bash
go mod tidy
mkdir -p ./runtime-logs
SERVICE_NAME=gateway \
TARGET_SERVICE_NAME=worker \
TARGET_DISCOVERY_SERVICE_NAME=worker-http \
CONSUL_HTTP_ADDR=http://127.0.0.1:8500 \
APP_PORT=18080 \
METRICS_PORT=12112 \
APP_LOG_PATH=./runtime-logs/go-gateway-demo.log \
go run .
```

## Loki 查询

```text
{job="go-gateway-demo"}
```

