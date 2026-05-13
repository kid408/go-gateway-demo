FROM golang:1.23.3 AS builder

WORKDIR /src

ARG GOPROXY=https://goproxy.cn,direct
ARG GOSUMDB=sum.golang.org

ENV GOPROXY=${GOPROXY}
ENV GOSUMDB=${GOSUMDB}

COPY go.mod ./
COPY go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/go-gateway-demo .

FROM debian:bookworm-slim

WORKDIR /app

RUN mkdir -p /app/logs

COPY --from=builder /out/go-gateway-demo /app/go-gateway-demo

EXPOSE 18080 12112

ENV APP_LOG_PATH=/app/logs/go-gateway-demo.log

CMD ["/app/go-gateway-demo"]

