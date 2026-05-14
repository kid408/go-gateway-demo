package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
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

	"go-gateway-demo/internal/sessionrpc"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/segmentio/kafka-go"
	"google.golang.org/grpc"
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

	sessionEventsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "go_gateway_session_events_total",
			Help: "Total gRPC session events handled by the gateway.",
		},
		[]string{"action", "result"},
	)

	sessionEventDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "go_gateway_session_event_duration_seconds",
			Help:    "Duration of session events processed by the gateway in seconds.",
			Buckets: []float64{0.005, 0.01, 0.05, 0.1, 0.3, 0.5, 1, 2, 5},
		},
		[]string{"action", "result"},
	)

	workerReportsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "go_gateway_worker_reports_total",
			Help: "Total worker status reports received by the gateway.",
		},
		[]string{"source_service", "result"},
	)

	onlineUsersGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "go_gateway_online_users",
			Help: "Current online users tracked by the gateway.",
		},
	)

	activeStreamsGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "go_gateway_active_streams",
			Help: "Current active client gRPC streams on the gateway.",
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

	kafkaPublishTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "go_gateway_kafka_publish_total",
			Help: "Total Kafka publish attempts from gateway.",
		},
		[]string{"topic", "action", "result"},
	)
)

type config struct {
	serviceName                string
	targetServiceName          string
	targetDiscoveryServiceName string
	staticWorkerAddrs          []string
	instanceID                 string
	appPort                    string
	grpcPort                   string
	metricsPort                string
	consulHTTPAddr             string
	logPath                    string
	peerRefreshInterval        time.Duration
	grpcRequestTimeout         time.Duration
	kafkaBrokers               []string
	kafkaTopic                 string
}

type app struct {
	sessionrpc.UnimplementedGatewayServiceServer

	config      config
	startedAt   time.Time
	logger      *log.Logger
	httpClient  *http.Client
	random      *rand.Rand
	randMu      sync.Mutex
	workersMu   sync.RWMutex
	workers     []peer
	kafkaWriter *kafka.Writer

	requestCount  atomic.Uint64
	onlineUsers   atomic.Int64
	activeStreams atomic.Int64
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
	Action            string  `json:"action,omitempty"`
	EventID           string  `json:"event_id,omitempty"`
	SessionID         string  `json:"session_id,omitempty"`
	ClientID          string  `json:"client_id,omitempty"`
	Detail            string  `json:"detail,omitempty"`
	PeerCount         int     `json:"peer_count,omitempty"`
	OnlineUsers       int64   `json:"online_users,omitempty"`
	ActiveStreams     int64   `json:"active_streams,omitempty"`
	ReportQueueDepth  int64   `json:"report_queue_depth,omitempty"`
	ReportActiveJobs  int64   `json:"report_active_jobs,omitempty"`
	ReportTemperature float64 `json:"report_temperature_celsius,omitempty"`
	Timestamp         string  `json:"ts"`
}

type statusRecorder struct {
	http.ResponseWriter
	statusCode int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.statusCode = code
	r.ResponseWriter.WriteHeader(code)
}

func main() {
	prometheus.MustRegister(
		httpRequestsTotal,
		httpRequestDurationSeconds,
		processUp,
		discoveredWorkers,
		sessionEventsTotal,
		sessionEventDurationSeconds,
		workerReportsTotal,
		onlineUsersGauge,
		activeStreamsGauge,
		lastReportedQueueDepth,
		lastReportedTemperature,
		kafkaPublishTotal,
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
			Timeout: 3 * time.Second,
		},
		random: rand.New(rand.NewSource(time.Now().UnixNano())),
	}

	if len(cfg.kafkaBrokers) > 0 && cfg.kafkaTopic != "" {
		application.kafkaWriter = &kafka.Writer{
			Addr:                   kafka.TCP(cfg.kafkaBrokers...),
			Topic:                  cfg.kafkaTopic,
			RequiredAcks:           kafka.RequireAll,
			AllowAutoTopicCreation: false,
			Async:                  false,
			Balancer:               &kafka.Hash{},
		}
		defer application.kafkaWriter.Close()
	}

	processUp.Set(1)
	discoveredWorkers.WithLabelValues(cfg.serviceName, cfg.targetServiceName).Set(0)
	onlineUsersGauge.Set(0)
	activeStreamsGauge.Set(0)

	appMux := http.NewServeMux()
	appMux.HandleFunc("/", application.handleRoot)
	appMux.HandleFunc("/healthz", application.handleHealth)
	appMux.HandleFunc("/health", application.handleHealth)
	appMux.HandleFunc("/workers", application.handleWorkers)
	appMux.HandleFunc("/dispatch", application.handleDebugDispatch)

	metricsMux := http.NewServeMux()
	metricsMux.Handle("/metrics", promhttp.Handler())

	httpServer := &http.Server{
		Addr:              ":" + cfg.appPort,
		Handler:           application.withMetrics(appMux),
		ReadHeaderTimeout: 5 * time.Second,
	}

	metricsServer := &http.Server{
		Addr:              ":" + cfg.metricsPort,
		Handler:           metricsMux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	grpcListener, err := net.Listen("tcp", ":"+cfg.grpcPort)
	if err != nil {
		log.Fatalf("listen grpc failed: %v", err)
	}

	grpcServer := grpc.NewServer(sessionrpc.DefaultServerOptions()...)
	sessionrpc.RegisterGatewayServiceServer(grpcServer, application)

	go application.refreshWorkersLoop()

	go func() {
		application.writeLog(logEntry{Level: "info", Event: "http_server_starting"})
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("http server failed: %v", err)
		}
	}()

	go func() {
		application.writeLog(logEntry{Level: "info", Event: "metrics_server_starting"})
		if err := metricsServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("metrics server failed: %v", err)
		}
	}()

	go func() {
		application.writeLog(logEntry{Level: "info", Event: "grpc_server_starting"})
		if err := grpcServer.Serve(grpcListener); err != nil {
			log.Fatalf("grpc server failed: %v", err)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	processUp.Set(0)
	application.writeLog(logEntry{Level: "info", Event: "shutdown_signal_received"})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	grpcServer.GracefulStop()
	_ = httpServer.Shutdown(ctx)
	_ = metricsServer.Shutdown(ctx)
}

func (a *app) OpenSession(stream sessionrpc.GatewayService_OpenSessionServer) error {
	streamCount := a.activeStreams.Add(1)
	activeStreamsGauge.Set(float64(streamCount))
	a.writeLog(logEntry{
		Level:         "info",
		Event:         "client_stream_opened",
		ActiveStreams: streamCount,
		OnlineUsers:   a.onlineUsers.Load(),
	})

	defer func() {
		current := a.activeStreams.Add(-1)
		if current < 0 {
			a.activeStreams.Store(0)
			current = 0
		}
		activeStreamsGauge.Set(float64(current))
		a.writeLog(logEntry{
			Level:         "info",
			Event:         "client_stream_closed",
			ActiveStreams: current,
			OnlineUsers:   a.onlineUsers.Load(),
		})
	}()

	loggedIn := false
	currentSessionID := ""

	for {
		clientEvent, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				if loggedIn {
					online := a.onlineUsers.Add(-1)
					if online < 0 {
						a.onlineUsers.Store(0)
						online = 0
					}
					onlineUsersGauge.Set(float64(online))
				}
				return nil
			}
			return err
		}

		startedAt := time.Now()
		result := "success"

		worker, ok := a.pickRandomWorker()
		if !ok {
			result = "no_worker"
			sessionEventsTotal.WithLabelValues(clientEvent.Action, result).Inc()
			sessionEventDurationSeconds.WithLabelValues(clientEvent.Action, result).Observe(time.Since(startedAt).Seconds())
			ack := &sessionrpc.GatewayAck{
				EventID:     clientEvent.EventID,
				SessionID:   clientEvent.SessionID,
				Action:      clientEvent.Action,
				Result:      "error",
				Message:     "no workers discovered",
				GatewayID:   a.config.instanceID,
				ProcessedAt: time.Now().Format(time.RFC3339),
			}
			if sendErr := stream.Send(ack); sendErr != nil {
				return sendErr
			}
			continue
		}

		sessionEvent := &sessionrpc.SessionEvent{
			EventID:     clientEvent.EventID,
			SessionID:   clientEvent.SessionID,
			ClientID:    clientEvent.ClientID,
			UserID:      clientEvent.UserID,
			DeviceID:    clientEvent.DeviceID,
			Action:      clientEvent.Action,
			Payload:     clientEvent.Payload,
			GatewayID:   a.config.instanceID,
			SentAt:      clientEvent.SentAt,
			ProcessedAt: time.Now().Format(time.RFC3339),
		}

		workerResult, callErr := a.dispatchToWorker(stream.Context(), worker, sessionEvent)
		if callErr != nil {
			result = "error"
			sessionEventsTotal.WithLabelValues(clientEvent.Action, result).Inc()
			sessionEventDurationSeconds.WithLabelValues(clientEvent.Action, result).Observe(time.Since(startedAt).Seconds())
			a.writeLog(logEntry{
				Level:         "error",
				Event:         "worker_dispatch_failed",
				TargetService: a.config.targetServiceName,
				PeerID:        worker.ID,
				PeerAddress:   peerAddress(worker),
				Action:        clientEvent.Action,
				EventID:       clientEvent.EventID,
				SessionID:     clientEvent.SessionID,
				ClientID:      clientEvent.ClientID,
				Detail:        callErr.Error(),
			})
			ack := &sessionrpc.GatewayAck{
				EventID:     clientEvent.EventID,
				SessionID:   clientEvent.SessionID,
				Action:      clientEvent.Action,
				Result:      "error",
				Message:     callErr.Error(),
				GatewayID:   a.config.instanceID,
				ProcessedAt: time.Now().Format(time.RFC3339),
			}
			if sendErr := stream.Send(ack); sendErr != nil {
				return sendErr
			}
			continue
		}

		sessionEventsTotal.WithLabelValues(clientEvent.Action, workerResult.Result).Inc()
		sessionEventDurationSeconds.WithLabelValues(clientEvent.Action, workerResult.Result).Observe(time.Since(startedAt).Seconds())

		switch clientEvent.Action {
		case sessionrpc.ActionLogin:
			if !loggedIn && workerResult.Result == "success" {
				loggedIn = true
				currentSessionID = clientEvent.SessionID
				online := a.onlineUsers.Add(1)
				onlineUsersGauge.Set(float64(online))
			}
		case sessionrpc.ActionLogout:
			if loggedIn && currentSessionID == clientEvent.SessionID && workerResult.Result == "success" {
				loggedIn = false
				currentSessionID = ""
				online := a.onlineUsers.Add(-1)
				if online < 0 {
					a.onlineUsers.Store(0)
					online = 0
				}
				onlineUsersGauge.Set(float64(online))
			}
		}

		ack := &sessionrpc.GatewayAck{
			EventID:           workerResult.EventID,
			SessionID:         workerResult.SessionID,
			Action:            workerResult.Action,
			Result:            workerResult.Result,
			Message:           workerResult.Message,
			GatewayID:         a.config.instanceID,
			WorkerID:          workerResult.WorkerID,
			SnapshotObjectKey: workerResult.SnapshotObjectKey,
			ProcessedAt:       workerResult.ProcessedAt,
		}

		if err := stream.Send(ack); err != nil {
			return err
		}

		enrichedEvent := *sessionEvent
		enrichedEvent.WorkerID = workerResult.WorkerID
		enrichedEvent.SnapshotObjectKey = workerResult.SnapshotObjectKey
		enrichedEvent.ProcessedAt = workerResult.ProcessedAt
		a.publishSessionEventAsync(stream.Context(), &enrichedEvent)

		a.writeLog(logEntry{
			Level:         levelForResult(workerResult.Result),
			Event:         "session_event_processed",
			TargetService: a.config.targetServiceName,
			PeerID:        worker.ID,
			PeerAddress:   peerAddress(worker),
			Action:        clientEvent.Action,
			EventID:       clientEvent.EventID,
			SessionID:     clientEvent.SessionID,
			ClientID:      clientEvent.ClientID,
			OnlineUsers:   a.onlineUsers.Load(),
			ActiveStreams: a.activeStreams.Load(),
			Detail:        workerResult.Message,
		})
	}
}

func (a *app) ReportWorkerStatus(ctx context.Context, report *sessionrpc.WorkerStatusReport) (*sessionrpc.WorkerStatusAck, error) {
	sourceService := a.config.targetServiceName
	if report.WorkerID != "" {
		sourceService = a.config.targetServiceName
	}
	workerReportsTotal.WithLabelValues(sourceService, "received").Inc()
	lastReportedQueueDepth.WithLabelValues(sourceService).Set(float64(report.QueueDepth))
	lastReportedTemperature.WithLabelValues(sourceService).Set(report.TemperatureCelsius)

	a.writeLog(logEntry{
		Level:             "info",
		Event:             "worker_report_received",
		SourceService:     sourceService,
		PeerID:            report.WorkerID,
		Action:            "status_report",
		ReportQueueDepth:  report.QueueDepth,
		ReportActiveJobs:  report.ActiveJobs,
		ReportTemperature: report.TemperatureCelsius,
		Detail:            report.Message,
	})

	return &sessionrpc.WorkerStatusAck{
		Result:     "success",
		Message:    "report received",
		ReportedAt: time.Now().Format(time.RFC3339),
	}, nil
}

func (a *app) handleRoot(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"service":                a.config.serviceName,
		"targetService":          a.config.targetServiceName,
		"targetDiscoveryService": a.config.targetDiscoveryServiceName,
		"instanceId":             a.config.instanceID,
		"appPort":                a.config.appPort,
		"grpcPort":               a.config.grpcPort,
		"metricsPort":            a.config.metricsPort,
		"workerCount":            len(a.snapshotWorkers()),
		"onlineUsers":            a.onlineUsers.Load(),
		"activeStreams":          a.activeStreams.Load(),
		"requestCount":           a.requestCount.Add(1),
		"uptimeSec":              int64(time.Since(a.startedAt).Seconds()),
		"time":                   time.Now().Format(time.RFC3339),
	})
}

func (a *app) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"status":                 "ok",
		"service":                a.config.serviceName,
		"targetService":          a.config.targetServiceName,
		"targetDiscoveryService": a.config.targetDiscoveryServiceName,
		"instanceId":             a.config.instanceID,
		"workerCount":            len(a.snapshotWorkers()),
		"onlineUsers":            a.onlineUsers.Load(),
		"activeStreams":          a.activeStreams.Load(),
		"time":                   time.Now().Format(time.RFC3339),
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

func (a *app) handleDebugDispatch(w http.ResponseWriter, r *http.Request) {
	worker, ok := a.pickRandomWorker()
	if !ok {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"message": "no workers discovered",
			"time":    time.Now().Format(time.RFC3339),
		})
		return
	}

	action := strings.TrimSpace(r.URL.Query().Get("action"))
	if action == "" {
		action = sessionrpc.ActionLogin
	}

	sessionID := strings.TrimSpace(r.URL.Query().Get("session_id"))
	if sessionID == "" {
		sessionID = "debug-" + strconv.FormatInt(time.Now().UnixNano(), 10)
	}

	event := &sessionrpc.SessionEvent{
		EventID:     "debug-" + strconv.FormatInt(time.Now().UnixNano(), 10),
		SessionID:   sessionID,
		ClientID:    "debug-client",
		UserID:      1000 + uint64(a.randomInt(500)),
		DeviceID:    "debug-device",
		Action:      action,
		Payload:     `{"source":"debug_dispatch"}`,
		GatewayID:   a.config.instanceID,
		SentAt:      time.Now().Format(time.RFC3339),
		ProcessedAt: time.Now().Format(time.RFC3339),
	}

	result, err := a.dispatchToWorker(r.Context(), worker, event)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]any{
			"message": err.Error(),
			"worker":  worker,
			"time":    time.Now().Format(time.RFC3339),
		})
		return
	}

	a.publishSessionEventAsync(r.Context(), event)
	writeJSON(w, http.StatusOK, map[string]any{
		"message": "debug dispatch sent",
		"worker":  worker,
		"result":  result,
		"time":    time.Now().Format(time.RFC3339),
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
	var (
		workers []peer
		err     error
	)

	if len(a.config.staticWorkerAddrs) > 0 {
		workers = buildStaticPeers(a.config.targetServiceName, a.config.staticWorkerAddrs)
	} else {
		workers, err = a.fetchPeersFromConsul()
		if err != nil {
			a.writeLog(logEntry{
				Level:         "error",
				Event:         "worker_refresh_failed",
				TargetService: a.config.targetServiceName,
				Detail:        err.Error(),
			})
			return
		}
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

func (a *app) dispatchToWorker(ctx context.Context, worker peer, event *sessionrpc.SessionEvent) (*sessionrpc.SessionResult, error) {
	target := fmt.Sprintf("%s:%d", worker.Address, worker.Port)
	conn, err := sessionrpc.DialContext(ctx, target)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	client := sessionrpc.NewWorkerServiceClient(conn)
	callCtx, cancel := context.WithTimeout(ctx, a.config.grpcRequestTimeout)
	defer cancel()

	result, err := client.ProcessSessionEvent(callCtx, event)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (a *app) publishSessionEventAsync(ctx context.Context, event *sessionrpc.SessionEvent) {
	if a.kafkaWriter == nil {
		return
	}

	body, err := json.Marshal(event)
	if err != nil {
		kafkaPublishTotal.WithLabelValues(a.config.kafkaTopic, event.Action, "marshal_error").Inc()
		a.writeLog(logEntry{
			Level:     "error",
			Event:     "kafka_event_marshal_failed",
			Action:    event.Action,
			EventID:   event.EventID,
			SessionID: event.SessionID,
			Detail:    err.Error(),
		})
		return
	}

	writeCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	err = a.kafkaWriter.WriteMessages(writeCtx, kafka.Message{
		Key:   []byte(event.SessionID),
		Value: body,
		Time:  time.Now(),
	})
	if err != nil {
		kafkaPublishTotal.WithLabelValues(a.config.kafkaTopic, event.Action, "error").Inc()
		a.writeLog(logEntry{
			Level:     "error",
			Event:     "kafka_event_publish_failed",
			Action:    event.Action,
			EventID:   event.EventID,
			SessionID: event.SessionID,
			Detail:    err.Error(),
		})
		return
	}

	kafkaPublishTotal.WithLabelValues(a.config.kafkaTopic, event.Action, "success").Inc()
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

	peers := make([]peer, 0, len(entries))
	for _, entry := range entries {
		address := strings.TrimSpace(entry.Service.Address)
		if address == "" {
			address = strings.TrimSpace(entry.Node.Address)
		}
		if address == "" || entry.Service.Port == 0 {
			continue
		}

		peers = append(peers, peer{
			ID:      entry.Service.ID,
			Service: entry.Service.Service,
			Address: address,
			Port:    entry.Service.Port,
		})
	}

	slices.SortFunc(peers, func(a, b peer) int {
		return strings.Compare(peerAddress(a), peerAddress(b))
	})
	return peers, nil
}

func (a *app) pickRandomWorker() (peer, bool) {
	workers := a.snapshotWorkers()
	if len(workers) == 0 {
		return peer{}, false
	}
	if len(workers) == 1 {
		return workers[0], true
	}
	return workers[a.randomInt(len(workers))], true
}

func buildStaticPeers(serviceName string, addresses []string) []peer {
	peers := make([]peer, 0, len(addresses))
	for idx, raw := range addresses {
		address, port, ok := parseStaticAddress(raw)
		if !ok {
			continue
		}
		peers = append(peers, peer{
			ID:      fmt.Sprintf("static-%s-%02d", serviceName, idx+1),
			Service: serviceName,
			Address: address,
			Port:    port,
		})
	}
	return peers
}

func parseStaticAddress(raw string) (string, int, bool) {
	host, portText, ok := strings.Cut(strings.TrimSpace(raw), ":")
	if !ok || strings.TrimSpace(host) == "" || strings.TrimSpace(portText) == "" {
		return "", 0, false
	}
	port, err := strconv.Atoi(strings.TrimSpace(portText))
	if err != nil || port <= 0 {
		return "", 0, false
	}
	return strings.TrimSpace(host), port, true
}

func (a *app) snapshotWorkers() []peer {
	a.workersMu.RLock()
	defer a.workersMu.RUnlock()
	result := make([]peer, len(a.workers))
	copy(result, a.workers)
	return result
}

func (a *app) randomInt(max int) int {
	a.randMu.Lock()
	defer a.randMu.Unlock()
	return a.random.Intn(max)
}

func loadConfig() config {
	serviceName := envOrDefault("SERVICE_NAME", "gateway")
	targetServiceName := envOrDefault("TARGET_SERVICE_NAME", "worker")
	targetDiscoveryServiceName := envOrDefault("TARGET_DISCOVERY_SERVICE_NAME", "worker-grpc")
	instanceID := envOrDefault("INSTANCE_ID", envOrDefault("NOMAD_ALLOC_ID", hostnameOrDefault()))

	return config{
		serviceName:                serviceName,
		targetServiceName:          targetServiceName,
		targetDiscoveryServiceName: targetDiscoveryServiceName,
		staticWorkerAddrs:          envCSV("STATIC_WORKER_ADDRS"),
		instanceID:                 instanceID,
		appPort:                    envOrDefault("APP_PORT", "18080"),
		grpcPort:                   envOrDefault("GRPC_PORT", "19080"),
		metricsPort:                envOrDefault("METRICS_PORT", "12112"),
		consulHTTPAddr:             ensureHTTPPrefix(envOrDefault("CONSUL_HTTP_ADDR", "127.0.0.1:8500")),
		logPath:                    envOrDefault("APP_LOG_PATH", "/app/logs/go-gateway-demo.log"),
		peerRefreshInterval:        envDurationMillisOrDefault("PEER_REFRESH_INTERVAL_MS", 5000),
		grpcRequestTimeout:         envDurationMillisOrDefault("GRPC_REQUEST_TIMEOUT_MS", 3000),
		kafkaBrokers:               envCSV("KAFKA_BROKERS"),
		kafkaTopic:                 envOrDefault("KAFKA_TOPIC", "user-session-events"),
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

func (a *app) writeLog(entry logEntry) {
	entry.Service = a.config.serviceName
	entry.InstanceID = a.config.instanceID
	entry.Timestamp = time.Now().Format(time.RFC3339)
	body, err := json.Marshal(entry)
	if err != nil {
		a.logger.Printf(`{"level":"error","event":"log_marshal_failed","service":"%s","instance_id":"%s","detail":%q,"ts":"%s"}`,
			a.config.serviceName,
			a.config.instanceID,
			err.Error(),
			time.Now().Format(time.RFC3339),
		)
		return
	}
	a.logger.Println(string(body))
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func envOrDefault(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

func envDurationMillisOrDefault(key string, fallback int) time.Duration {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return time.Duration(fallback) * time.Millisecond
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		return time.Duration(fallback) * time.Millisecond
	}
	return time.Duration(value) * time.Millisecond
}

func envCSV(key string) []string {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			result = append(result, part)
		}
	}
	return result
}

func ensureHTTPPrefix(value string) string {
	if strings.HasPrefix(value, "http://") || strings.HasPrefix(value, "https://") {
		return value
	}
	return "http://" + value
}

func hostnameOrDefault() string {
	name, err := os.Hostname()
	if err != nil || strings.TrimSpace(name) == "" {
		return "unknown-host"
	}
	return name
}

func peerAddress(item peer) string {
	return fmt.Sprintf("%s:%d", item.Address, item.Port)
}

func levelForResult(result string) string {
	if result == "success" {
		return "info"
	}
	return "error"
}
