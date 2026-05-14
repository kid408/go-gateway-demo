job "gateway" {
  region      = var.region
  datacenters = var.datacenters
  namespace   = var.namespace
  type        = "service"

  group "gateway" {
    count = var.count

    volume "logs" {
      type   = "host"
      source = var.host_volume
    }

    network {
      port "http" {}
      port "grpc" {}
      port "metrics" {}
    }

    service {
      name         = "gateway-http"
      tags         = var.discovery_service_tags
      port         = "http"
      address_mode = "host"
      check {
        name     = "gateway HTTP Check"
        type     = "http"
        path     = "/healthz"
        interval = "10s"
        timeout  = "2s"
      }
    }

    service {
      name         = "gateway-grpc"
      tags         = var.discovery_service_tags
      port         = "grpc"
      address_mode = "host"
      check {
        name     = "gateway gRPC TCP Check"
        type     = "tcp"
        interval = "10s"
        timeout  = "2s"
      }
    }

    service {
      name         = "gateway-prom"
      tags         = concat(["prometheus"], var.consul_service_tags)
      port         = "metrics"
      address_mode = "host"
      check {
        name     = "gateway Metrics Check"
        type     = "http"
        path     = "/metrics"
        interval = "10s"
        timeout  = "2s"
      }
    }

    task "gateway" {
      driver = "docker"
      user   = "root"

      volume_mount {
        volume      = "logs"
        destination = "/app/logs"
      }

      config {
        image        = var.image
        network_mode = "host"
        force_pull   = false
      }

      env {
        TZ                            = "Asia/Shanghai"
        SERVICE_NAME                  = "gateway"
        TARGET_SERVICE_NAME           = "worker"
        TARGET_DISCOVERY_SERVICE_NAME = "worker-grpc"
        APP_PORT                      = "${NOMAD_PORT_http}"
        GRPC_PORT                     = "${NOMAD_PORT_grpc}"
        METRICS_PORT                  = "${NOMAD_PORT_metrics}"
        INSTANCE_ID                   = "${NOMAD_ALLOC_ID}"
        CONSUL_HTTP_ADDR              = var.consul_http_addr
        APP_LOG_PATH                  = "/app/logs/gateway/${NOMAD_ALLOC_ID}.log"
        PEER_REFRESH_INTERVAL_MS      = var.peer_refresh_interval_ms
        KAFKA_BROKERS                 = var.kafka_brokers
        KAFKA_TOPIC                   = var.kafka_topic
      }

      resources {
        cpu    = var.cpu
        memory = var.memory
      }
    }
  }
}

variable "region" {
  type = string
}

variable "datacenters" {
  type = list(string)
}

variable "namespace" {
  type    = string
  default = "default"
}

variable "image" {
  type = string
}

variable "consul_http_addr" {
  type    = string
  default = "http://127.0.0.1:8500"
}

variable "consul_service_tags" {
  type    = list(string)
  default = []
}

variable "discovery_service_tags" {
  type    = list(string)
  default = []
}

variable "count" {
  type    = number
  default = 5
}

variable "cpu" {
  type    = number
  default = 100
}

variable "memory" {
  type    = number
  default = 128
}

variable "peer_refresh_interval_ms" {
  type    = string
  default = "5000"
}

variable "kafka_brokers" {
  type    = string
  default = ""
}

variable "kafka_topic" {
  type    = string
  default = "user-session-events"
}

variable "host_volume" {
  type    = string
  default = "logs"
}
