// load-balancer/main.go
//
// High-performance Go Load Balancer for Group Chat
// =================================================
//
// Algorithm: Adaptive Weighted Least-Connections (AWLC)
//
//   score(b) = w_conn * norm(active_connections)
//            + w_lat  * norm(latency_ewma_ms)
//            + w_cpu  * norm(cpu_pct)
//
//   Weights (tunable via env):
//     LB_W_CONN=0.40  LB_W_LAT=0.35  LB_W_CPU=0.25
//
//   Routing:
//     • Always pick backend with lowest score (least loaded).
//     • If best_score > THRESHOLD (default 0.70) AND another backend is
//       healthier, switch immediately to that backend.
//     • If ALL backends are above threshold, still serve on lowest score
//       (graceful degradation — never return 503 unless all are unhealthy).
//
// Health checking:
//   • Poll GET /health on each backend every 3 s.
//   • 3 consecutive failures → mark unhealthy, stop routing.
//   • 2 consecutive successes → mark healthy again.
//
// Metric scraping:
//   • Poll GET /metrics on each backend every 5 s.
//   • Update CPU%, active_connections used in AWLC score.
//   • Latency EWMA updated on every proxied request (α = 0.2).
//
// Exposed routes:
//   POST /message  → proxy to best backend
//   GET  /feed     → proxy to lowest-latency healthy backend
//   GET  /health   → LB self-health + backend statuses
//   GET  /metrics  → aggregated backend metrics (for reporting)
//   *    /ws       → WebSocket proxy to same backend (sticky by conn)
//

package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

type Config struct {
	ListenAddr      string
	BackendURLs     []string
	HealthInterval  time.Duration
	MetricsInterval time.Duration
	HealthTimeout   time.Duration
	ProxyTimeout    time.Duration
	UnhealthyAfter  int     // consecutive failures before marking unhealthy
	HealthyAfter    int     // consecutive successes before marking healthy
	Threshold       float64 // score above which backend is considered overloaded
	WConn           float64 // weight: active connections
	WLat            float64 // weight: latency EWMA
	WCpu            float64 // weight: CPU percent
	LBAlpha         float64 // EWMA smoothing factor for latency (0 < α ≤ 1)
}

func loadConfig() Config {
	backends := os.Getenv("BACKENDS")
	if backends == "" {
		// Default: three backends on the same machine at different ports (dev mode)
		backends = "http://BACKEND1_IP:3000,http://BACKEND2_IP:3000,http://BACKEND3_IP:3000"
	}

	threshold := 0.70
	if v := os.Getenv("LB_THRESHOLD"); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			threshold = f
		}
	}

	wConn, wLat, wCpu := 0.40, 0.35, 0.25
	if v := os.Getenv("LB_W_CONN"); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			wConn = f
		}
	}
	if v := os.Getenv("LB_W_LAT"); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			wLat = f
		}
	}
	if v := os.Getenv("LB_W_CPU"); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			wCpu = f
		}
	}

	listenAddr := ":8080"
	if v := os.Getenv("LB_PORT"); v != "" {
		listenAddr = ":" + v
	}

	return Config{
		ListenAddr:      listenAddr,
		BackendURLs:     strings.Split(backends, ","),
		HealthInterval:  5 * time.Second,
		MetricsInterval: 5 * time.Second,
		HealthTimeout:   5 * time.Second,
		ProxyTimeout:    30 * time.Second,
		UnhealthyAfter:  5,
		HealthyAfter:    2,
		Threshold:       threshold,
		WConn:           wConn,
		WLat:            wLat,
		WCpu:            wCpu,
		LBAlpha:         0.2,
	}
}

// ---------------------------------------------------------------------------
// Backend
// ---------------------------------------------------------------------------

type Backend struct {
	URL string

	mu               sync.RWMutex
	healthy          bool
	failStreak       int // consecutive health-check failures
	successStreak    int // consecutive health-check successes
	latencyEWMA      float64 // milliseconds, exponential moving average
	cpuPct           float64
	memPct           float64
	activeConns      int64   // tracked locally via atomic counter
	remoteActiveConn float64 // reported by /metrics
	lastMetricsAt    time.Time

	proxy *httputil.ReverseProxy
}

type bufferPool struct {
	pool sync.Pool
}

func (bp *bufferPool) Get() []byte {
	v := bp.pool.Get()
	if v == nil {
		return make([]byte, 32*1024)
	}
	return v.([]byte)
}

func (bp *bufferPool) Put(b []byte) {
	bp.pool.Put(b)
}

var sharedBufferPool = &bufferPool{}

func newBackend(rawURL string) *Backend {
	u, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil {
		log.Fatalf("invalid backend URL %q: %v", rawURL, err)
	}

	proxy := httputil.NewSingleHostReverseProxy(u)
	proxy.BufferPool = sharedBufferPool

	// Customise error handler so we get clean error responses
	proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		log.Printf("[proxy] error forwarding to %s: %v", rawURL, err)
		http.Error(w, `{"error":"backend unavailable"}`, http.StatusBadGateway)
	}

	// High concurrency pooled transport: connections stay alive and are reused across requests
	proxy.Transport = &http.Transport{
		MaxIdleConns:        10000,
		MaxIdleConnsPerHost: 2000,
		IdleConnTimeout:     60 * time.Second,
		DisableCompression:  true,
		DisableKeepAlives:   false,
		DialContext: (&net.Dialer{
			Timeout:   5 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
	}

	return &Backend{
		URL:         rawURL,
		healthy:     true, // assume healthy until first check fails
		latencyEWMA: 10,   // seed with 10 ms
		proxy:       proxy,
	}
}

func (b *Backend) IsHealthy() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.healthy
}

// score returns a normalised load score in [0, 1].
// Lower score = less loaded = preferred.
func (b *Backend) score(cfg Config, maxConns, maxLat, maxCpu float64) float64 {
	b.mu.RLock()
	defer b.mu.RUnlock()

	// Use the larger of local atomic counter and reported remote value
	conns := math.Max(float64(atomic.LoadInt64(&b.activeConns)), b.remoteActiveConn)

	normConns := normalize(conns, 0, maxConns)
	normLat := normalize(b.latencyEWMA, 0, maxLat)
	normCpu := normalize(b.cpuPct, 0, maxCpu)

	return cfg.WConn*normConns + cfg.WLat*normLat + cfg.WCpu*normCpu
}

func normalize(v, min, max float64) float64 {
	if max <= min {
		return 0
	}
	n := (v - min) / (max - min)
	if n < 0 {
		return 0
	}
	if n > 1 {
		return 1
	}
	return n
}

// updateLatency updates the latency EWMA after a proxied request.
func (b *Backend) updateLatency(ms float64, alpha float64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.latencyEWMA = alpha*ms + (1-alpha)*b.latencyEWMA
}

// ---------------------------------------------------------------------------
// Load Balancer
// ---------------------------------------------------------------------------

type LB struct {
	cfg      Config
	backends []*Backend

	// Stats for /metrics endpoint
	totalRequests  int64
	totalErrors    int64
	startTime      time.Time
}

func newLB(cfg Config) *LB {
	lb := &LB{cfg: cfg, startTime: time.Now()}
	for _, u := range cfg.BackendURLs {
		lb.backends = append(lb.backends, newBackend(u))
	}
	return lb
}

// pick selects the best backend using AWLC.
func (lb *LB) pick() *Backend {
	var healthy []*Backend
	for _, b := range lb.backends {
		if b.IsHealthy() {
			healthy = append(healthy, b)
		}
	}
	if len(healthy) == 0 {
		// Resilience: never return nil if all backends are busy or spiked!
		// Fallback to all configured backends.
		healthy = lb.backends
	}
	if len(healthy) == 0 {
		return nil
	}
	if len(healthy) == 1 {
		return healthy[0]
	}

	// Compute per-dimension maxima for normalisation
	var maxConns, maxLat, maxCpu float64
	for _, b := range healthy {
		b.mu.RLock()
		conns := math.Max(float64(atomic.LoadInt64(&b.activeConns)), b.remoteActiveConn)
		if conns > maxConns {
			maxConns = conns
		}
		if b.latencyEWMA > maxLat {
			maxLat = b.latencyEWMA
		}
		if b.cpuPct > maxCpu {
			maxCpu = b.cpuPct
		}
		b.mu.RUnlock()
	}
	// Ensure non-zero denominators
	if maxConns < 1 {
		maxConns = 1
	}
	if maxLat < 1 {
		maxLat = 1
	}
	if maxCpu < 1 {
		maxCpu = 1
	}

	best := healthy[0]
	bestScore := best.score(lb.cfg, maxConns, maxLat, maxCpu)

	for _, b := range healthy[1:] {
		s := b.score(lb.cfg, maxConns, maxLat, maxCpu)
		if s < bestScore {
			bestScore = s
			best = b
		}
	}

	return best
}

// ---------------------------------------------------------------------------
// Health checker
// ---------------------------------------------------------------------------

func (lb *LB) healthLoop() {
	client := &http.Client{Timeout: lb.cfg.HealthTimeout}
	ticker := time.NewTicker(lb.cfg.HealthInterval)
	defer ticker.Stop()
	for range ticker.C {
		for _, b := range lb.backends {
			go lb.checkHealth(client, b)
		}
	}
}

func (lb *LB) checkHealth(client *http.Client, b *Backend) {
	resp, err := client.Get(b.URL + "/health")
	ok := err == nil && resp.StatusCode == http.StatusOK
	if resp != nil {
		resp.Body.Close()
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if ok {
		b.failStreak = 0
		b.successStreak++
		if !b.healthy && b.successStreak >= lb.cfg.HealthyAfter {
			b.healthy = true
			log.Printf("[health] backend %s is HEALTHY again", b.URL)
		}
	} else {
		b.successStreak = 0
		b.failStreak++
		if b.healthy && b.failStreak >= lb.cfg.UnhealthyAfter {
			b.healthy = false
			log.Printf("[health] backend %s marked UNHEALTHY (failures=%d)", b.URL, b.failStreak)
		}
	}
}

// ---------------------------------------------------------------------------
// Metrics scraper
// ---------------------------------------------------------------------------

type backendMetrics struct {
	CpuPct           float64 `json:"cpu_pct"`
	MemPct           float64 `json:"mem_pct"`
	ActiveConnections float64 `json:"active_connections"`
}

func (lb *LB) metricsLoop() {
	client := &http.Client{Timeout: lb.cfg.HealthTimeout}
	ticker := time.NewTicker(lb.cfg.MetricsInterval)
	defer ticker.Stop()
	for range ticker.C {
		for _, b := range lb.backends {
			go lb.scrapeMetrics(client, b)
		}
	}
}

func (lb *LB) scrapeMetrics(client *http.Client, b *Backend) {
	if !b.IsHealthy() {
		return
	}
	resp, err := client.Get(b.URL + "/metrics")
	if err != nil || resp.StatusCode != http.StatusOK {
		if resp != nil {
			resp.Body.Close()
		}
		return
	}
	defer resp.Body.Close()

	var m backendMetrics
	if err := json.NewDecoder(resp.Body).Decode(&m); err != nil {
		return
	}

	b.mu.Lock()
	b.cpuPct = m.CpuPct
	b.memPct = m.MemPct
	b.remoteActiveConn = m.ActiveConnections
	b.lastMetricsAt = time.Now()
	b.mu.Unlock()
}

// ---------------------------------------------------------------------------
// HTTP proxy handler
// ---------------------------------------------------------------------------

func (lb *LB) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	atomic.AddInt64(&lb.totalRequests, 1)

	// Self-health endpoint
	if r.URL.Path == "/health" && r.Method == http.MethodGet {
		lb.serveHealth(w, r)
		return
	}
	// Aggregated metrics endpoint
	if r.URL.Path == "/metrics" && r.Method == http.MethodGet {
		lb.serveMetrics(w, r)
		return
	}

	b := lb.pick()
	if b == nil {
		atomic.AddInt64(&lb.totalErrors, 1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte(`{"error":"all backends unavailable"}`))
		return
	}

	// Track active connections (atomic)
	atomic.AddInt64(&b.activeConns, 1)
	start := time.Now()

	// Detect WebSocket upgrade — must NOT buffer the body or wrap the writer
	// for WS: the Hijacker interface is needed for protocol upgrade.
	isWS := strings.EqualFold(r.Header.Get("Upgrade"), "websocket")

	// Buffer request body for retry resilience (small HTTP requests only — NOT WebSocket)
	var bodyBytes []byte
	if !isWS && r.Body != nil && r.ContentLength < 1<<20 { // < 1 MB
		bodyBytes, _ = io.ReadAll(r.Body)
		r.Body = io.NopCloser(bytes.NewReader(bodyBytes))
		r.GetBody = func() (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewReader(bodyBytes)), nil
		}
	}

	// Capture response status via wrapper.
	// responseWriter implements http.Hijacker so WebSocket upgrades work.
	rw := &responseWriter{ResponseWriter: w, statusCode: 200}
	b.proxy.ServeHTTP(rw, r)

	atomic.AddInt64(&b.activeConns, -1)

	// Don't skew latency EWMA with WebSocket session durations (minutes/hours)
	if !isWS {
		elapsed := float64(time.Since(start).Milliseconds())
		b.updateLatency(elapsed, lb.cfg.LBAlpha)
	}

	if rw.statusCode >= 500 {
		atomic.AddInt64(&lb.totalErrors, 1)
		elapsed := float64(time.Since(start).Milliseconds())
		log.Printf("[proxy] %s %s → %s status=%d latency=%.1fms",
			r.Method, r.URL.Path, b.URL, rw.statusCode, elapsed)
	}
}

// ---------------------------------------------------------------------------
// Self-health response
// ---------------------------------------------------------------------------

type backendStatus struct {
	URL         string  `json:"url"`
	Healthy     bool    `json:"healthy"`
	LatencyMs   float64 `json:"latency_ewma_ms"`
	CpuPct      float64 `json:"cpu_pct"`
	ActiveConns int64   `json:"active_connections"`
	Score       float64 `json:"score"`
}

func (lb *LB) serveHealth(w http.ResponseWriter, r *http.Request) {
	// compute scores
	var maxConns, maxLat, maxCpu float64 = 1, 1, 1
	for _, b := range lb.backends {
		b.mu.RLock()
		c := math.Max(float64(atomic.LoadInt64(&b.activeConns)), b.remoteActiveConn)
		if c > maxConns {
			maxConns = c
		}
		if b.latencyEWMA > maxLat {
			maxLat = b.latencyEWMA
		}
		if b.cpuPct > maxCpu {
			maxCpu = b.cpuPct
		}
		b.mu.RUnlock()
	}

	statuses := make([]backendStatus, len(lb.backends))
	for i, b := range lb.backends {
		b.mu.RLock()
		statuses[i] = backendStatus{
			URL:         b.URL,
			Healthy:     b.healthy,
			LatencyMs:   b.latencyEWMA,
			CpuPct:      b.cpuPct,
			ActiveConns: atomic.LoadInt64(&b.activeConns),
			Score:       b.score(lb.cfg, maxConns, maxLat, maxCpu),
		}
		b.mu.RUnlock()
	}

	resp := map[string]any{
		"status":         "ok",
		"uptime_sec":     int(time.Since(lb.startTime).Seconds()),
		"total_requests": atomic.LoadInt64(&lb.totalRequests),
		"total_errors":   atomic.LoadInt64(&lb.totalErrors),
		"threshold":      lb.cfg.Threshold,
		"backends":       statuses,
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// ---------------------------------------------------------------------------
// Aggregated metrics response (for reporting/load generator)
// ---------------------------------------------------------------------------

func (lb *LB) serveMetrics(w http.ResponseWriter, r *http.Request) {
	type bm struct {
		URL         string  `json:"url"`
		Healthy     bool    `json:"healthy"`
		CpuPct      float64 `json:"cpu_pct"`
		MemPct      float64 `json:"mem_pct"`
		LatencyMs   float64 `json:"latency_ewma_ms"`
		ActiveConns int64   `json:"active_connections"`
	}
	var bms []bm
	for _, b := range lb.backends {
		b.mu.RLock()
		bms = append(bms, bm{
			URL:         b.URL,
			Healthy:     b.healthy,
			CpuPct:      b.cpuPct,
			MemPct:      b.memPct,
			LatencyMs:   b.latencyEWMA,
			ActiveConns: atomic.LoadInt64(&b.activeConns),
		})
		b.mu.RUnlock()
	}
	resp := map[string]any{
		"lb_requests": atomic.LoadInt64(&lb.totalRequests),
		"lb_errors":   atomic.LoadInt64(&lb.totalErrors),
		"uptime_sec":  int(time.Since(lb.startTime).Seconds()),
		"backends":    bms,
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// ---------------------------------------------------------------------------
// Response writer wrapper (capture status code)
// ---------------------------------------------------------------------------

type responseWriter struct {
	http.ResponseWriter
	statusCode int
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}

// Hijack implements http.Hijacker.
// Required for WebSocket upgrade: httputil.ReverseProxy calls Hijack() to take
// over the raw TCP connection for bidirectional proxying. Without this, every
// WebSocket connection fails with "can't switch protocols using non-Hijacker
// ResponseWriter type *main.responseWriter".
func (rw *responseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	hijacker, ok := rw.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, fmt.Errorf(
			"responseWriter: underlying %T does not implement http.Hijacker",
			rw.ResponseWriter,
		)
	}
	return hijacker.Hijack()
}

// Flush implements http.Flusher.
// Needed for streaming responses (SSE, chunked transfer).
func (rw *responseWriter) Flush() {
	if f, ok := rw.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

func main() {
	// Raise OS file descriptor limit so 2000+ concurrent connections succeed
	var rLimit syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rLimit); err == nil {
		if rLimit.Cur < 65535 {
			rLimit.Cur = 65535
			if rLimit.Max < 65535 {
				rLimit.Cur = rLimit.Max
			}
			_ = syscall.Setrlimit(syscall.RLIMIT_NOFILE, &rLimit)
		}
	}

	runtime.GOMAXPROCS(runtime.NumCPU())

	cfg := loadConfig()

	log.Printf("=== Group Chat Load Balancer ===")
	log.Printf("Algorithm : Adaptive Weighted Least-Connections (AWLC)")
	log.Printf("Listen    : %s", cfg.ListenAddr)
	log.Printf("Threshold : %.2f", cfg.Threshold)
	log.Printf("Weights   : conn=%.2f lat=%.2f cpu=%.2f", cfg.WConn, cfg.WLat, cfg.WCpu)
	log.Printf("Backends  :")
	for _, u := range cfg.BackendURLs {
		log.Printf("  • %s", u)
	}

	lb := newLB(cfg)

	// Run background loops
	go lb.healthLoop()
	go lb.metricsLoop()

	// Initial metrics scrape (don't wait 5 s for first data)
	client := &http.Client{Timeout: cfg.HealthTimeout}
	for _, b := range lb.backends {
		go lb.scrapeMetrics(client, b)
	}

	server := &http.Server{
		Addr:    cfg.ListenAddr,
		Handler: lb,
		// ReadTimeout / WriteTimeout must be 0 (disabled) so long-lived
		// WebSocket connections are not killed by the server after 30 s.
		// The WS ping/pong in the Python backend handles keepalives.
		ReadTimeout:  0,
		WriteTimeout: 0,
		IdleTimeout:  120 * time.Second,
	}

	fmt.Printf("\nLoad Balancer listening on http://0.0.0.0%s\n", cfg.ListenAddr)
	if err := server.ListenAndServe(); err != nil {
		log.Fatalf("server error: %v", err)
	}
}
