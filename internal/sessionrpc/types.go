package sessionrpc

const (
	ActionLogin     = "login"
	ActionHeartbeat = "heartbeat"
	ActionLogout    = "logout"
)

type ClientEvent struct {
	EventID   string `json:"event_id,omitempty"`
	SessionID string `json:"session_id,omitempty"`
	ClientID  string `json:"client_id,omitempty"`
	UserID    uint64 `json:"user_id,omitempty"`
	DeviceID  string `json:"device_id,omitempty"`
	Action    string `json:"action,omitempty"`
	Payload   string `json:"payload,omitempty"`
	SentAt    string `json:"sent_at,omitempty"`
}

type GatewayAck struct {
	EventID           string `json:"event_id,omitempty"`
	SessionID         string `json:"session_id,omitempty"`
	Action            string `json:"action,omitempty"`
	Result            string `json:"result,omitempty"`
	Message           string `json:"message,omitempty"`
	GatewayID         string `json:"gateway_id,omitempty"`
	WorkerID          string `json:"worker_id,omitempty"`
	SnapshotObjectKey string `json:"snapshot_object_key,omitempty"`
	ProcessedAt       string `json:"processed_at,omitempty"`
}

type SessionEvent struct {
	EventID           string `json:"event_id,omitempty"`
	SessionID         string `json:"session_id,omitempty"`
	ClientID          string `json:"client_id,omitempty"`
	UserID            uint64 `json:"user_id,omitempty"`
	DeviceID          string `json:"device_id,omitempty"`
	Action            string `json:"action,omitempty"`
	Payload           string `json:"payload,omitempty"`
	GatewayID         string `json:"gateway_id,omitempty"`
	WorkerID          string `json:"worker_id,omitempty"`
	SnapshotObjectKey string `json:"snapshot_object_key,omitempty"`
	SentAt            string `json:"sent_at,omitempty"`
	ProcessedAt       string `json:"processed_at,omitempty"`
}

type SessionResult struct {
	EventID            string  `json:"event_id,omitempty"`
	SessionID          string  `json:"session_id,omitempty"`
	Action             string  `json:"action,omitempty"`
	Result             string  `json:"result,omitempty"`
	Message            string  `json:"message,omitempty"`
	GatewayID          string  `json:"gateway_id,omitempty"`
	WorkerID           string  `json:"worker_id,omitempty"`
	SnapshotObjectKey  string  `json:"snapshot_object_key,omitempty"`
	QueueDepth         int64   `json:"queue_depth,omitempty"`
	ActiveJobs         int64   `json:"active_jobs,omitempty"`
	TemperatureCelsius float64 `json:"temperature_celsius,omitempty"`
	ProcessedAt        string  `json:"processed_at,omitempty"`
}

type WorkerStatusReport struct {
	WorkerID           string  `json:"worker_id,omitempty"`
	GatewayID          string  `json:"gateway_id,omitempty"`
	QueueDepth         int64   `json:"queue_depth,omitempty"`
	ActiveJobs         int64   `json:"active_jobs,omitempty"`
	TemperatureCelsius float64 `json:"temperature_celsius,omitempty"`
	OnlineSessions     int64   `json:"online_sessions,omitempty"`
	Message            string  `json:"message,omitempty"`
	SentAt             string  `json:"sent_at,omitempty"`
}

type WorkerStatusAck struct {
	Result     string `json:"result,omitempty"`
	Message    string `json:"message,omitempty"`
	ReportedAt string `json:"reported_at,omitempty"`
}
