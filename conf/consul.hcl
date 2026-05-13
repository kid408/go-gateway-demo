datacenter = "dc1"
data_dir   = "/opt/consul/data"
log_level  = "INFO"
server     = true
bootstrap_expect = 1
bind_addr  = "127.0.0.1"
client_addr = "0.0.0.0"

ui_config {
  enabled = true
}
