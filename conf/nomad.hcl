data_dir  = "/opt/nomad/data"
bind_addr = "0.0.0.0"
log_level = "INFO"

advertise {
  http = "127.0.0.1"
  rpc  = "127.0.0.1"
  serf = "127.0.0.1"
}

server {
  enabled          = true
  bootstrap_expect = 1
}

client {
  enabled = true

  host_volume "logs" {
    path      = "/opt/monitoring/fluent-bit/logs"
    read_only = false
  }
}

consul {
  address = "127.0.0.1:8500"
}

plugin "docker" {
  config {
    allow_privileged = true
    volumes {
      enabled = true
    }
  }
}
