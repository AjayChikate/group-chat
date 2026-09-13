// load-balancer/main.go
//
// High-performance Go Load Balancer for Group Chat
// =================================================
// Architecture:
//   1. Clamped TCP Socket Buffers (8KB) — bounds Linux kernel socket memory
//   2. Concurrency Semaphore (150 in-flight) — user-space queue prevents OOM
//   3. High-Availability Automatic Retries — 5xx or connection drops retry on peer
//   4. Lean Connection Pooling — 40 idle conns/host keeps socket overhead <2MB
//   5. Real-Time Heartbeat Monitor — logs memory, heap, goroutines, backend state

package main

import (
	"bytes"
	"context"
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
	UnhealthyAfter  int
	HealthyAfter    int
	Threshold       float64
	WConn           float64
	WLat            float64
	WCpu            float64
	LBAlpha         float64
}

func loadConfig() Config {
	backends := os.Getenv("BACKENDS")
	if backends == "" {
		backends = "http://172.17.0.99:5000,http://172.17.0.100:5000,http://172.17.0.101:5000"
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
// Clamped TCP Listener (bounds Linux kernel socket buffers to 8KB)
// ---------------------------------------------------------------------------

type clampedListener struct {
	net.Listener
}

func (l clampedListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	if tc, ok := c.(*net.TCPConn); ok {
		_ = tc.SetReadBuffer(8192)
		_ = tc.SetWriteBuffer(8192)
		_ = tc.SetNoDelay(true)
		_ = tc.SetKeepAlive(true)
		_ = tc.SetKeepAlivePeriod(15 * time.Second)
	}
	return c, nil
}

// ---------------------------------------------------------------------------
// Buffer Pool
// ---------------------------------------------------------------------------

type bufferPool struct {
	pool sync.Pool
}

func (bp *bufferPool) Get() []byte {
	v := bp.pool.Get()
	if v == nil {
		return make([]byte, 32*1024)
	}
	b := v.([]byte)
	if cap(b) < 32*1024 {
		return make([]byte, 32*1024)
	}
	return b[:32*1024]
}

func (bp *bufferPool) Put(b []byte) {
	if cap(b) >= 32*1024 {
		bp.pool.Put(b[:32*1024])
	}
}

var sharedBufferPool = &bufferPool{}

// ---------------------------------------------------------------------------
// Backend
// ---------------------------------------------------------------------------

type Backend struct {
	URL string

	mu               sync.RWMutex
	healthy          bool
	failStreak       int
	successStreak    int
	latencyEWMA      float64
	cpuPct           float64
	memPct           float64
	activeConns      int64
	remoteActiveConn float64
	lastMetricsAt    time.Time

	proxy *httputil.ReverseProxy
}

func newBackend(rawURL string) *Backend {
	u, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil {
		log.Fatalf("invalid backend URL %q: %v", rawURL, err)
	}

	proxy := httputil.NewSingleHostReverseProxy(u)
	proxy.BufferPool = sharedBufferPool

	proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		// Do not write headers here; caller's retryResponseWriter captures it
		w.WriteHeader(http.StatusBadGateway)
	}

	// Lean connection pool: 40 idle conns per host (120 total) keeps sockets <2MB
	proxy.Transport = &http.Transport{
		MaxIdleConns:        120,
		MaxIdleConnsPerHost: 40,
		IdleConnTimeout:     15 * time.Second,
		DisableCompression:  true,
		DisableKeepAlives:   false,
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			d := &net.Dialer{
				Timeout:   3 * time.Second,
				KeepAlive: 15 * time.Second,
			}
			c, err := d.DialContext(ctx, network, addr)
			if err != nil {
				return nil, err
			}
			if tc, ok := c.(*net.TCPConn); ok {
				_ = tc.SetReadBuffer(8192)
				_ = tc.SetWriteBuffer(8192)
				_ = tc.SetNoDelay(true)
			}
			return c, nil
		},
	}

	return &Backend{
		URL:         rawURL,
		healthy:     true,
		latencyEWMA: 10,
		proxy:       proxy,
	}
}

func (b *Backend) IsHealthy() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.healthy
}

func (b *Backend) score(cfg Config, maxConns, maxLat, maxCpu float64) float64 {
	b.mu.RLock()
	defer b.mu.RUnlock()

	conns := math.Max(float64(atomic.LoadInt64(&b.activeConns)), b.remoteActiveConn)
	normConn := normalize(conns, maxConns)
	normLat := normalize(b.latencyEWMA, maxLat)
	normCpu := normalize(b.cpuPct, maxCpu)

	return cfg.WConn*normConn + cfg.WLat*normLat + cfg.WCpu*normCpu
}

func normalize(val, maxVal float64) float64 {
	if maxVal <= 0 {
		return 0
	}
	n := val / maxVal
	if n < 0 {
		return 0
	}
	if n > 1 {
		return 1
	}
	return n
}

func (b *Backend) updateLatency(ms float64, alpha float64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.latencyEWMA = alpha*ms + (1-alpha)*b.latencyEWMA
}

// ---------------------------------------------------------------------------
// Load Balancer
// ---------------------------------------------------------------------------

type LB struct {
	cfg           Config
	backends      []*Backend
	sem           chan struct{} // Concurrency gate: max 150 in-flight requests to backends
	totalRequests int64
	totalErrors   int64
	startTime     time.Time
}

func newLB(cfg Config) *LB {
	lb := &LB{
		cfg:       cfg,
		startTime: time.Now(),
		sem:       make(chan struct{}, 150),
	}
	for _, u := range cfg.BackendURLs {
		lb.backends = append(lb.backends, newBackend(u))
	}
	return lb
}

func (lb *LB) pick() *Backend {
	return lb.pickExcluding(nil)
}

func (lb *LB) pickExcluding(exclude *Backend) *Backend {
	var candidates []*Backend
	for _, b := range lb.backends {
		if b != exclude && b.IsHealthy() {
			candidates = append(candidates, b)
		}
	}
	if len(candidates) == 0 {
		for _, b := range lb.backends {
			if b != exclude {
				candidates = append(candidates, b)
			}
		}
	}
	if len(candidates) == 0 {
		if len(lb.backends) > 0 {
			return lb.backends[0]
		}
		return nil
	}
	if len(candidates) == 1 {
		return candidates[0]
	}

	var maxConns, maxLat, maxCpu float64 = 1, 1, 1
	for _, b := range candidates {
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

	best := candidates[0]
	bestScore := best.score(lb.cfg, maxConns, maxLat, maxCpu)
	for _, b := range candidates[1:] {
		s := b.score(lb.cfg, maxConns, maxLat, maxCpu)
		if s < bestScore {
			bestScore = s
			best = b
		}
	}
	return best
}

// ---------------------------------------------------------------------------
// Health Checker
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
// Metrics Scraper
// ---------------------------------------------------------------------------

type backendMetrics struct {
	CpuPct            float64 `json:"cpu_pct"`
	MemPct            float64 `json:"mem_pct"`
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
// Retry Response Writer (enables transparent HA retries on 5xx)
// ---------------------------------------------------------------------------

type retryResponseWriter struct {
	target      http.ResponseWriter
	header      http.Header
	buf         bytes.Buffer
	statusCode  int
	wroteHeader bool
}

func (rw *retryResponseWriter) Header() http.Header {
	if rw.header == nil {
		rw.header = make(http.Header)
	}
	return rw.header
}

func (rw *retryResponseWriter) WriteHeader(code int) {
	rw.statusCode = code
	rw.wroteHeader = true
}

func (rw *retryResponseWriter) Write(b []byte) (int, error) {
	if !rw.wroteHeader {
		rw.statusCode = http.StatusOK
		rw.wroteHeader = true
	}
	return rw.buf.Write(b)
}

func (rw *retryResponseWriter) FlushToTarget() {
	for k, vv := range rw.header {
		for _, v := range vv {
			rw.target.Header().Add(k, v)
		}
	}
	code := rw.statusCode
	if code == 0 {
		code = http.StatusOK
	}
	rw.target.WriteHeader(code)
	if rw.buf.Len() > 0 {
		_, _ = rw.target.Write(rw.buf.Bytes())
	}
}

// ---------------------------------------------------------------------------
// HTTP Proxy Handler
// ---------------------------------------------------------------------------

func (lb *LB) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	atomic.AddInt64(&lb.totalRequests, 1)

	// Built-in health & metrics endpoints
	if r.URL.Path == "/health" && r.Method == http.MethodGet {
		lb.serveHealth(w, r)
		return
	}
	if r.URL.Path == "/metrics" && r.Method == http.MethodGet {
		lb.serveMetrics(w, r)
		return
	}

	isWS := strings.EqualFold(r.Header.Get("Upgrade"), "websocket")

	// 1. WebSocket Proxying (bypass queue & buffer for raw streaming)
	if isWS {
		b := lb.pick()
		if b == nil {
			atomic.AddInt64(&lb.totalErrors, 1)
			http.Error(w, `{"error":"no backends available"}`, http.StatusServiceUnavailable)
			return
		}
		atomic.AddInt64(&b.activeConns, 1)
		b.proxy.ServeHTTP(w, r)
		atomic.AddInt64(&b.activeConns, -1)
		return
	}

	// 2. Concurrency Semaphore Gate (max 150 in-flight requests to backends)
	select {
	case lb.sem <- struct{}{}:
		defer func() { <-lb.sem }()
	case <-r.Context().Done():
		atomic.AddInt64(&lb.totalErrors, 1)
		return
	case <-time.After(8 * time.Second):
		atomic.AddInt64(&lb.totalErrors, 1)
		http.Error(w, `{"error":"queue timeout"}`, http.StatusGatewayTimeout)
		return
	}

	// Buffer small request body for retry resilience (< 1MB)
	var bodyBytes []byte
	if (r.Method == http.MethodPost || r.Method == http.MethodPut) && r.Body != nil && r.ContentLength != 0 {
		bodyBytes, _ = io.ReadAll(io.LimitReader(r.Body, 1<<20))
	}

	// High Availability: Try primary backend, transparently retry on peer if 5xx or drop
	var chosen *Backend
	var lastStatus int = 502

	for attempt := 0; attempt < 2; attempt++ {
		chosen = lb.pickExcluding(chosen)
		if chosen == nil {
			break
		}

		if len(bodyBytes) > 0 {
			r.Body = io.NopCloser(bytes.NewReader(bodyBytes))
			r.ContentLength = int64(len(bodyBytes))
			r.GetBody = func() (io.ReadCloser, error) {
				return io.NopCloser(bytes.NewReader(bodyBytes)), nil
			}
		}

		atomic.AddInt64(&chosen.activeConns, 1)
		start := time.Now()

		rw := &retryResponseWriter{target: w}
		chosen.proxy.ServeHTTP(rw, r)

		atomic.AddInt64(&chosen.activeConns, -1)
		elapsed := float64(time.Since(start).Milliseconds())
		chosen.updateLatency(elapsed, lb.cfg.LBAlpha)

		lastStatus = rw.statusCode
		if lastStatus < 500 {
			// Succeeded: flush response to client
			rw.FlushToTarget()
			return
		}

		// Failed on this backend — log and retry
		log.Printf("[retry] %s %s on %s status=%d (retrying on peer)", r.Method, r.URL.Path, chosen.URL, lastStatus)
	}

	// All attempts failed
	atomic.AddInt64(&lb.totalErrors, 1)
	http.Error(w, `{"error":"backend failure"}`, http.StatusBadGateway)
}

// ---------------------------------------------------------------------------
// Health & Metrics Responses
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
			LatencyMs:   math.Round(b.latencyEWMA*100) / 100,
			CpuPct:      b.cpuPct,
			ActiveConns: atomic.LoadInt64(&b.activeConns),
			Score:       math.Round(b.score(lb.cfg, maxConns, maxLat, maxCpu)*1000) / 1000,
		}
		b.mu.RUnlock()
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"status":   "ok",
		"backends": statuses,
	})
}

func (lb *LB) serveMetrics(w http.ResponseWriter, r *http.Request) {
	type bm struct {
		URL               string  `json:"url"`
		Healthy           bool    `json:"healthy"`
		ActiveConnections float64 `json:"active_connections"`
		CpuPct            float64 `json:"cpu_pct"`
		MemPct            float64 `json:"mem_pct"`
		LatencyEWMA       float64 `json:"latency_ewma_ms"`
	}
	var bms []bm
	for _, b := range lb.backends {
		b.mu.RLock()
		bms = append(bms, bm{
			URL:               b.URL,
			Healthy:           b.healthy,
			ActiveConnections: math.Max(float64(atomic.LoadInt64(&b.activeConns)), b.remoteActiveConn),
			CpuPct:            b.cpuPct,
			MemPct:            b.memPct,
			LatencyEWMA:       math.Round(b.latencyEWMA*100) / 100,
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
// Main
// ---------------------------------------------------------------------------

func main() {
	// 1. Raise OS file descriptor limit to maximum allowed
	var rLimit syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rLimit); err == nil {
		oldCur := rLimit.Cur
		if rLimit.Cur < rLimit.Max {
			rLimit.Cur = rLimit.Max
			_ = syscall.Setrlimit(syscall.RLIMIT_NOFILE, &rLimit)
		}
		log.Printf("[system] File descriptors: soft=%d -> %d (max=%d)", oldCur, rLimit.Cur, rLimit.Max)
	}

	runtime.GOMAXPROCS(runtime.NumCPU())

	cfg := loadConfig()

	log.Printf("=== Group Chat Load Balancer (HA Bounded) ===")
	log.Printf("Listen    : %s", cfg.ListenAddr)
	log.Printf("Backends  : %s", strings.Join(cfg.BackendURLs, ", "))

	lb := newLB(cfg)

	// Run background health & metrics loops
	go lb.healthLoop()
	go lb.metricsLoop()

	// Initial metrics scrape
	client := &http.Client{Timeout: cfg.HealthTimeout}
	for _, b := range lb.backends {
		go lb.scrapeMetrics(client, b)
	}

	// 2. Real-Time Heartbeat Logger (every 5 seconds)
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		var m runtime.MemStats
		for range ticker.C {
			runtime.ReadMemStats(&m)
			totalReq := atomic.LoadInt64(&lb.totalRequests)
			totalErr := atomic.LoadInt64(&lb.totalErrors)
			inFlight := len(lb.sem)

			var bStats []string
			for _, b := range lb.backends {
				b.mu.RLock()
				h := "UP"
				if !b.healthy {
					h = "DOWN"
				}
				bStats = append(bStats, fmt.Sprintf("%s:%s(lat=%.0fms,act=%d)", b.URL, h, b.latencyEWMA, atomic.LoadInt64(&b.activeConns)))
				b.mu.RUnlock()
			}
			log.Printf("[heartbeat] reqs=%d errs=%d inflight=%d/150 heap=%dMB sys=%dMB g=%d | %s",
				totalReq, totalErr, inFlight, m.Alloc/(1024*1024), m.Sys/(1024*1024), runtime.NumGoroutine(), strings.Join(bStats, " "))
		}
	}()

	// 3. Create TCP listener with clamped 8KB socket buffers
	ln, err := net.Listen("tcp", cfg.ListenAddr)
	if err != nil {
		log.Fatalf("failed to listen on %s: %v", cfg.ListenAddr, err)
	}
	defer ln.Close()

	clampedLn := clampedListener{ln}

	server := &http.Server{
		Handler:      lb,
		ReadTimeout:  0,
		WriteTimeout: 0,
		IdleTimeout:  15 * time.Second,
	}

	fmt.Printf("\nLoad Balancer listening on http://0.0.0.0%s (clamped 8KB sockets, 150-worker gate, auto-retry)\n", cfg.ListenAddr)
	if err := server.Serve(clampedLn); err != nil {
		log.Fatalf("server error: %v", err)
	}
}
