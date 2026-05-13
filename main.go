package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	httpRequestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "go_gateway_http_requests_total",
			Help: "Total HTTP requests received by the gateway demo service.",
		},
		[]string{"path", "method", "code"},
	)

	httpRequestDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "go_gateway_http_request_duration_seconds",
			Help:    "HTTP request duration of the gateway demo service in seconds.",
			Buckets: []float64{0.01, 0.05, 0.1, 0.3, 0.5, 1, 2, 5},
		},
		[]string{"path", "method", "code"},
	)

	processUp = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "go_gateway_process_up",
			Help: "Whether the gateway demo process is considered up.",
		},
	)

	discoveredWorkers = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "go_gateway_discovered_workers",
			Help: "Number of worker instances currently discovered from Consul.",
		},
		[]string{"service", "target_service"},
	)

	dispatchTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "go_gateway_dispatch_total",
			Help: "Total requests dispatched from gateway to worker instances.",
		},
		[]string{"target_service", "task_type", "result"},
	)

	dispatchDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "go_gateway_dispatch_duration_seconds",
			Help:    "Duration of gateway dispatch requests in seconds.",
			Buckets: []float64{0.01, 0.05, 0.1, 0.3, 0.5, 1, 2, 5},
		},
		[]string{"target_service", "task_type", "result"},
	)

	workerReportsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "go_gateway_worker_reports_total",
			Help: "Total reports received from worker instances.",
		},
		[]string{"source_service", "result"},
	)

	onlineUsersGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "go_gateway_online_users",
			Help: "Simulated online user count on the gateway layer.",
		},
	)

	lastReportedQueueDepth = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "go_gateway_last_reported_queue_depth",
			Help: "Last queue depth reported by workers grouped by source service.",
		},
		[]string{"source_service"},
	)

	lastReportedTemperature = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "go_gateway_last_reported_temperature_celsius",
			Help: "Last worker temperature reported to the gateway grouped by source service.",
		},
		[]string{"source_service"},
	)
)

type config struct {
	serviceName                string
	targetServiceName          string
	targetDiscoveryServiceName string
	instanceID                 string
	appPort                    string
	metricsPort                string
	consulHTTPAddr             string
	logPath                    string
	peerRefreshInterval        time.Duration
	dispatchInterval           time.Duration
	requestTimeout             time.Duration
	minTaskDelayMs             int
	maxTaskDelayMs             int
	taskCatalog                []string
}

type app struct {
	config      config
	startedAt   time.Time
	logger      *log.Logger
	httpClient  *http.Client
	random      *rand.Rand
	randMu      sync.Mutex
	workersMu   sync.RWMutex
	workers     []peer
	onlineUsers atomic.Int64
	userStep    atomic.Int64
}

type peer struct {
	ID      string `json:"id"`
	Service string `json:"service"`
	Address string `json:"address"`
	Port    int    `json:"port"`
}

type consulServiceEntry struct {
	Node struct {
		Address string `json:"Address"`
	} `json:"Node"`
	Service struct {
		ID      string `json:"ID"`
		Service string `json:"Service"`
		Address string `json:"Address"`
		Port    int    `json:"Port"`
	} `json:"Service"`
}

type workRequest struct {
	RequestID    string `json:"request_id"`
	FromService  string `json:"from_service"`
	FromInstance string `json:"from_instance"`
	TaskType     string `json:"task_type"`
	DelayMs      int    `json:"delay_ms"`
	SentAt       string `json:"sent_at"`
}

type workerReport struct {
	FromService        string  `json:"from_service"`
	FromInstance       string  `json:"from_instance"`
	QueueDepth         int     `json:"queue_depth"`
	ActiveJobs         int     `json:"active_jobs"`
	TemperatureCelsius float64 `json:"temperature_celsius"`
	Message            string  `json:"message"`
	SentAt             string  `json:"sent_at"`
}

type logEntry struct {
	Level             string  `json:"level"`
	Event             string  `json:"event"`
	Service           string  `json:"service"`
	InstanceID        string  `json:"instance_id"`
	TargetService     string  `json:"target_service,omitempty"`
	SourceService     string  `json:"source_service,omitempty"`
	PeerID            string  `json:"peer_id,omitempty"`
	PeerAddress       string  `json:"peer_address,omitempty"`
	Path              string  `json:"path,omitempty"`
	Method            string  `json:"method,omitempty"`
	Status            int     `json:"status,omitempty"`
	TaskType          string  `json:"task_type,omitempty"`
	Detail            string  `json:"detail,omitempty"`
	PeerCount         int     `json:"peer_count,omitempty"`
	OnlineUsers       int64   `json:"online_users,omitempty"`
	ReportQueueDepth  int     `json:"report_queue_depth,omitempty"`
	ReportActiveJobs  int     `json:"report_active_jobs,omitempty"`
	ReportTemperature float64 `json:"report_temperature_celsius,omitempty"`
	Timestamp         string  `json:"ts"`
}

func main() {
	prometheus.MustRegister(
		httpRequestsTotal,
		httpRequestDurationSeconds,
		processUp,
		discoveredWorkers,
		dispatchTotal,
		dispatchDurationSeconds,
		workerReportsTotal,
		onlineUsersGauge,
		lastReportedQueueDepth,
		lastReportedTemperature,
	)

	cfg := loadConfig()
	logger, logFile, err := newLogger(cfg.logPath)
	if err != nil {
		log.Fatalf("init logger failed: %v", err)
	}
	defer logFile.Close()

	application := &app{
		config:    cfg,
		startedAt: time.Now(),
		logger:    logger,
		httpClient: &http.Client{
			Timeout: cfg.requestTimeout,
		},
		random: rand.New(rand.NewSource(time.Now().UnixNano())),
	}

	application.onlineUsers.Store(1)
	application.userStep.Store(1)
	onlineUsersGauge.Set(1)
	discoveredWorkers.WithLabelValues(cfg.serviceName, cfg.targetServiceName).Set(0)
	processUp.Set(1)

	appMux := http.NewServeMux()
	appMux.HandleFunc("/", application.handleRoot)
	appMux.HandleFunc("/healthz", application.handleHealth)
	appMux.HandleFunc("/health", application.handleHealth)
	appMux.HandleFunc("/workers", application.handleWorkers)
	appMux.HandleFunc("/dispatch", application.handleDispatch)
	appMux.HandleFunc("/worker/report", application.handleWorkerReport)

	metricsMux := http.NewServeMux()
	metricsMux.Handle("/metrics", promhttp.Handler())

	appServer := &http.Server{
		Addr:              ":" + cfg.appPort,
		Handler:           application.withMetrics(appMux),
		ReadHeaderTimeout: 5 * time.Second,
	}

	metricsServer := &http.Server{
		Addr:              ":" + cfg.metricsPort,
		Handler:           metricsMux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go application.refreshWorkersLoop()
	go application.dispatchLoop()
	go application.onlineUsersLoop()

	go func() {
		application.writeLog(logEntry{Level: "info", Event: "application_server_start"})
		if err := appServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("application server failed: %v", err)
		}
	}()

	go func() {
		application.writeLog(logEntry{Level: "info", Event: "metrics_server_start"})
		if err := metricsServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("metrics server failed: %v", err)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	processUp.Set(0)
	application.writeLog(logEntry{Level: "info", Event: "shutdown_signal"})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_ = appServer.Shutdown(ctx)
	_ = metricsServer.Shutdown(ctx)
}

func loadConfig() config {
	serviceName := envOrDefault("SERVICE_NAME", "gateway")
	targetServiceName := envOrDefault("TARGET_SERVICE_NAME", "worker")
	targetDiscoveryServiceName := envOrDefault("TARGET_DISCOVERY_SERVICE_NAME", "worker-http")

	taskCatalog := []string{
		"browse_page",
		"checkout_cart",
		"search_catalog",
		"sync_profile",
	}

	if raw := strings.TrimSpace(os.Getenv("TASK_CATALOG")); raw != "" {
		taskCatalog = nil
		for _, part := range strings.Split(raw, "|") {
			part = strings.TrimSpace(part)
			if part != "" {
				taskCatalog = append(taskCatalog, part)
			}
		}
		if len(taskCatalog) == 0 {
			taskCatalog = []string{"browse_page"}
		}
	}

	instanceID := envOrDefault("INSTANCE_ID", envOrDefault("NOMAD_ALLOC_ID", hostnameOrDefault("gateway-demo")))

	return config{
		serviceName:                serviceName,
		targetServiceName:          targetServiceName,
		targetDiscoveryServiceName: targetDiscoveryServiceName,
		instanceID:                 instanceID,
		appPort:                    envOrDefault("APP_PORT", "18080"),
		metricsPort:                envOrDefault("METRICS_PORT", "12112"),
		consulHTTPAddr:             ensureHTTPPrefix(envOrDefault("CONSUL_HTTP_ADDR", "127.0.0.1:8500")),
		logPath:                    envOrDefault("APP_LOG_PATH", "/app/logs/go-gateway-demo.log"),
		peerRefreshInterval:        envDurationMillisOrDefault("PEER_REFRESH_INTERVAL_MS", 5000),
		dispatchInterval:           envDurationMillisOrDefault("DISPATCH_INTERVAL_MS", 3000),
		requestTimeout:             envDurationMillisOrDefault("HTTP_REQUEST_TIMEOUT_MS", 3000),
		minTaskDelayMs:             envIntOrDefault("MIN_TASK_DELAY_MS", 120),
		maxTaskDelayMs:             envIntOrDefault("MAX_TASK_DELAY_MS", 900),
		taskCatalog:                taskCatalog,
	}
}

func newLogger(logPath string) (*log.Logger, *os.File, error) {
	if err := os.MkdirAll(filepath.Dir(logPath), 0o755); err != nil {
		return nil, nil, err
	}

	file, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, nil, err
	}

	return log.New(io.MultiWriter(os.Stdout, file), "", 0), file, nil
}

func (a *app) handleRoot(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"service":                a.config.serviceName,
		"targetService":          a.config.targetServiceName,
		"targetDiscoveryService": a.config.targetDiscoveryServiceName,
		"instanceId":             a.config.instanceID,
		"onlineUsers":            a.onlineUsers.Load(),
		"workerCount":            len(a.snapshotWorkers()),
		"time":                   time.Now().Format(time.RFC3339),
		"uptimeSec":              int64(time.Since(a.startedAt).Seconds()),
	})
}

func (a *app) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"status":                 "ok",
		"service":                a.config.serviceName,
		"targetService":          a.config.targetServiceName,
		"targetDiscoveryService": a.config.targetDiscoveryServiceName,
		"instanceId":             a.config.instanceID,
		"onlineUsers":            a.onlineUsers.Load(),
		"workerCount":            len(a.snapshotWorkers()),
		"time":                   time.Now().Format(time.RFC3339),
		"uptimeSec":              int64(time.Since(a.startedAt).Seconds()),
	})
}

func (a *app) handleWorkers(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"service":       a.config.serviceName,
		"targetService": a.config.targetServiceName,
		"instanceId":    a.config.instanceID,
		"workerCount":   len(a.snapshotWorkers()),
		"workers":       a.snapshotWorkers(),
		"time":          time.Now().Format(time.RFC3339),
	})
}

func (a *app) handleDispatch(w http.ResponseWriter, r *http.Request) {
	worker, ok := a.pickRandomWorker()
	if !ok {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"message": "no workers discovered",
			"time":    time.Now().Format(time.RFC3339),
		})
		return
	}

	taskType := strings.TrimSpace(r.URL.Query().Get("task_type"))
	if taskType == "" {
		taskType = a.randomTaskType()
	}

	delayMs := envIntOrDefaultFromString(r.URL.Query().Get("delay_ms"), a.randomDelayMillis())

	if err := a.dispatchToWorker(worker, taskType, delayMs); err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]any{
			"message": err.Error(),
			"worker":  worker,
			"time":    time.Now().Format(time.RFC3339),
		})
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"message":  "dispatch sent",
		"worker":   worker,
		"taskType": taskType,
		"delayMs":  delayMs,
		"time":     time.Now().Format(time.RFC3339),
	})
}

func (a *app) handleWorkerReport(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"message": "method not allowed"})
		return
	}

	var payload workerReport
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"message": "invalid payload"})
		return
	}

	workerReportsTotal.WithLabelValues(payload.FromService, "received").Inc()
	lastReportedQueueDepth.WithLabelValues(payload.FromService).Set(float64(payload.QueueDepth))
	lastReportedTemperature.WithLabelValues(payload.FromService).Set(payload.TemperatureCelsius)

	a.writeLog(logEntry{
		Level:             "info",
		Event:             "worker_report_received",
		SourceService:     payload.FromService,
		Detail:            payload.Message,
		ReportQueueDepth:  payload.QueueDepth,
		ReportActiveJobs:  payload.ActiveJobs,
		ReportTemperature: payload.TemperatureCelsius,
	})

	writeJSON(w, http.StatusOK, map[string]any{
		"message":      "report received",
		"fromService":  payload.FromService,
		"fromInstance": payload.FromInstance,
		"time":         time.Now().Format(time.RFC3339),
	})
}

func (a *app) withMetrics(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		startedAt := time.Now()
		recorder := &statusRecorder{ResponseWriter: w, statusCode: http.StatusOK}

		next.ServeHTTP(recorder, r)

		codeLabel := strconv.Itoa(recorder.statusCode)
		httpRequestsTotal.WithLabelValues(r.URL.Path, r.Method, codeLabel).Inc()
		httpRequestDurationSeconds.WithLabelValues(r.URL.Path, r.Method, codeLabel).Observe(time.Since(startedAt).Seconds())
		a.writeLog(logEntry{
			Level:  "info",
			Event:  "http_request_processed",
			Path:   r.URL.Path,
			Method: r.Method,
			Status: recorder.statusCode,
		})
	})
}

func (a *app) refreshWorkersLoop() {
	ticker := time.NewTicker(a.config.peerRefreshInterval)
	defer ticker.Stop()

	a.refreshWorkers()
	for range ticker.C {
		a.refreshWorkers()
	}
}

func (a *app) refreshWorkers() {
	workers, err := a.fetchPeersFromConsul()
	if err != nil {
		a.writeLog(logEntry{
			Level:         "error",
			Event:         "worker_refresh_failed",
			TargetService: a.config.targetServiceName,
			Detail:        err.Error(),
		})
		return
	}

	a.workersMu.Lock()
	a.workers = workers
	a.workersMu.Unlock()

	discoveredWorkers.WithLabelValues(a.config.serviceName, a.config.targetServiceName).Set(float64(len(workers)))
	a.writeLog(logEntry{
		Level:         "info",
		Event:         "worker_list_refreshed",
		TargetService: a.config.targetServiceName,
		PeerCount:     len(workers),
	})
}

func (a *app) dispatchLoop() {
	ticker := time.NewTicker(a.config.dispatchInterval)
	defer ticker.Stop()

	a.writeLog(logEntry{
		Level:         "info",
		Event:         "dispatch_loop_started",
		TargetService: a.config.targetServiceName,
	})

	for range ticker.C {
		worker, ok := a.pickRandomWorker()
		if !ok {
			a.writeLog(logEntry{
				Level:         "info",
				Event:         "dispatch_skipped_no_workers",
				TargetService: a.config.targetServiceName,
			})
			continue
		}

		_ = a.dispatchToWorker(worker, a.randomTaskType(), a.randomDelayMillis())
	}
}

func (a *app) onlineUsersLoop() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		current := a.onlineUsers.Load()
		step := a.userStep.Load()
		next := current + step

		switch {
		case next >= 100:
			next = 100
			a.userStep.Store(-1)
		case next <= 1:
			next = 1
			a.userStep.Store(1)
		}

		a.onlineUsers.Store(next)
		onlineUsersGauge.Set(float64(next))
	}
}

func (a *app) dispatchToWorker(worker peer, taskType string, delayMs int) error {
	payload := workRequest{
		RequestID:    a.newRequestID(),
		FromService:  a.config.serviceName,
		FromInstance: a.config.instanceID,
		TaskType:     taskType,
		DelayMs:      delayMs,
		SentAt:       time.Now().Format(time.RFC3339),
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	url := fmt.Sprintf("http://%s:%d/work/execute", worker.Address, worker.Port)
	request, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")

	startedAt := time.Now()
	response, err := a.httpClient.Do(request)
	if err != nil {
		dispatchTotal.WithLabelValues(a.config.targetServiceName, taskType, "error").Inc()
		dispatchDurationSeconds.WithLabelValues(a.config.targetServiceName, taskType, "error").Observe(time.Since(startedAt).Seconds())
		a.writeLog(logEntry{
			Level:         "error",
			Event:         "dispatch_failed",
			TargetService: a.config.targetServiceName,
			PeerID:        worker.ID,
			PeerAddress:   peerAddress(worker),
			TaskType:      taskType,
			Detail:        err.Error(),
		})
		return err
	}
	defer response.Body.Close()

	result := "success"
	if response.StatusCode >= http.StatusBadRequest {
		result = "error"
	}

	dispatchTotal.WithLabelValues(a.config.targetServiceName, taskType, result).Inc()
	dispatchDurationSeconds.WithLabelValues(a.config.targetServiceName, taskType, result).Observe(time.Since(startedAt).Seconds())
	a.writeLog(logEntry{
		Level:         levelForResult(result),
		Event:         "dispatch_sent",
		TargetService: a.config.targetServiceName,
		PeerID:        worker.ID,
		PeerAddress:   peerAddress(worker),
		TaskType:      taskType,
		Status:        response.StatusCode,
	})

	if result == "error" {
		return fmt.Errorf("worker returned status %d", response.StatusCode)
	}
	return nil
}

func (a *app) fetchPeersFromConsul() ([]peer, error) {
	url := fmt.Sprintf("%s/v1/health/service/%s?passing=true", strings.TrimRight(a.config.consulHTTPAddr, "/"), a.config.targetDiscoveryServiceName)
	response, err := a.httpClient.Get(url)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()

	if response.StatusCode >= http.StatusBadRequest {
		return nil, fmt.Errorf("consul returned status %d", response.StatusCode)
	}

	var entries []consulServiceEntry
	if err := json.NewDecoder(response.Body).Decode(&entries); err != nil {
		return nil, err
	}

	workers := make([]peer, 0, len(entries))
	for _, entry := range entries {
		address := entry.Service.Address
		if address == "" {
			address = entry.Node.Address
		}
		if address == "" || entry.Service.Port == 0 {
			continue
		}
		workers = append(workers, peer{
			ID:      entry.Service.ID,
			Service: entry.Service.Service,
			Address: address,
			Port:    entry.Service.Port,
		})
	}

	slices.SortFunc(workers, func(a peer, b peer) int {
		if a.Address == b.Address {
			return strings.Compare(a.ID, b.ID)
		}
		return strings.Compare(a.Address, b.Address)
	})

	return workers, nil
}

func (a *app) snapshotWorkers() []peer {
	a.workersMu.RLock()
	defer a.workersMu.RUnlock()

	workers := make([]peer, len(a.workers))
	copy(workers, a.workers)
	return workers
}

func (a *app) pickRandomWorker() (peer, bool) {
	workers := a.snapshotWorkers()
	if len(workers) == 0 {
		return peer{}, false
	}

	a.randMu.Lock()
	defer a.randMu.Unlock()
	return workers[a.random.Intn(len(workers))], true
}

func (a *app) randomTaskType() string {
	a.randMu.Lock()
	defer a.randMu.Unlock()
	return a.config.taskCatalog[a.random.Intn(len(a.config.taskCatalog))]
}

func (a *app) randomDelayMillis() int {
	a.randMu.Lock()
	defer a.randMu.Unlock()

	if a.config.maxTaskDelayMs <= a.config.minTaskDelayMs {
		return a.config.minTaskDelayMs
	}
	return a.config.minTaskDelayMs + a.random.Intn(a.config.maxTaskDelayMs-a.config.minTaskDelayMs+1)
}

func (a *app) newRequestID() string {
	a.randMu.Lock()
	defer a.randMu.Unlock()
	return fmt.Sprintf("%d-%06d", time.Now().UnixNano(), a.random.Intn(1_000_000))
}

func (a *app) writeLog(entry logEntry) {
	entry.Service = a.config.serviceName
	entry.InstanceID = a.config.instanceID
	if entry.PeerCount == 0 {
		entry.PeerCount = len(a.snapshotWorkers())
	}
	entry.OnlineUsers = a.onlineUsers.Load()
	entry.Timestamp = time.Now().Format(time.RFC3339)

	data, err := json.Marshal(entry)
	if err != nil {
		return
	}

	a.logger.Println(string(data))
}

func writeJSON(w http.ResponseWriter, code int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(data)
}

func envOrDefault(key, fallback string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	return value
}

func envIntOrDefault(key string, fallback int) int {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func envIntOrDefaultFromString(value string, fallback int) int {
	value = strings.TrimSpace(value)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func envDurationMillisOrDefault(key string, fallbackMillis int) time.Duration {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return time.Duration(fallbackMillis) * time.Millisecond
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed <= 0 {
		return time.Duration(fallbackMillis) * time.Millisecond
	}
	return time.Duration(parsed) * time.Millisecond
}

func ensureHTTPPrefix(value string) string {
	if strings.HasPrefix(value, "http://") || strings.HasPrefix(value, "https://") {
		return value
	}
	return "http://" + value
}

func hostnameOrDefault(fallback string) string {
	hostname, err := os.Hostname()
	if err != nil || strings.TrimSpace(hostname) == "" {
		return fallback
	}
	return hostname
}

func peerAddress(value peer) string {
	if value.Address == "" || value.Port == 0 {
		return ""
	}
	return fmt.Sprintf("%s:%d", value.Address, value.Port)
}

func levelForResult(result string) string {
	if result == "error" {
		return "error"
	}
	return "info"
}

type statusRecorder struct {
	http.ResponseWriter
	statusCode int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.statusCode = code
	r.ResponseWriter.WriteHeader(code)
}
