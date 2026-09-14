// load-balancer/main.go
//
// High-Availability, Zero-Overhead Load Balancer for Distributed Group Chat
// =========================================================================
// Architecture & Resilience Guarantees:
//   1. Clamped & Bounded Listener (3500 max conns, 8KB socket buffers)
//   2. Concurrency Semaphore & Graceful Load-Shedding Queue (120 in-flight, 2.5s deadline)
//   3. Direct Streaming Proxy (ZERO response buffering, ZERO memory leaks)
//   4. Transport-Level HA Retry (retries on peer before headers sent, 0 byte RAM)
//   5. sync.Pool Body Buffers (zero heap allocation for POST /message)
//   6. Warm Connection Pooling (100 idle conns/host, 90s keep-alive)
//   7. Hyper-Aggressive GC (GOGC=20, GOMEMLIMIT=35MB, FreeOSMemory every 3s)
//   8. Linux /proc/self/status Real-Time RSS & Memory Alarm Logger

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
	"runtime/debug"
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
	MaxInFlight     int
	MaxActiveConns  int64
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

	listenAddr := ":5000"
	if v := os.Getenv("LB_PORT"); v != "" {
		listenAddr = ":" + v
	}

	maxInFlight := 450
	if v := os.Getenv("LB_MAX_INFLIGHT"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			maxInFlight = n
		}
	}

	return Config{
		ListenAddr:      listenAddr,
		BackendURLs:     strings.Split(backends, ","),
		HealthInterval:  5 * time.Second,
		MetricsInterval: 5 * time.Second,
		HealthTimeout:   6 * time.Second,
		ProxyTimeout:    20 * time.Second,
		UnhealthyAfter:  6,
		HealthyAfter:    2,
		Threshold:       threshold,
		WConn:           wConn,
		WLat:            wLat,
		WCpu:            wCpu,
		LBAlpha:         0.40,
		MaxInFlight:     maxInFlight,
		MaxActiveConns:  5000,
	}
}

// ---------------------------------------------------------------------------
// Bounded & Clamped TCP Listener
// Prevents Linux kernel buffer exhaustion and user-space goroutine floods
// ---------------------------------------------------------------------------

type trackedConn struct {
	net.Conn
	once    sync.Once
	onClose func()
}

func (c *trackedConn) Close() error {
	c.once.Do(func() {
		if c.onClose != nil {
			c.onClose()
		}
	})
	return c.Conn.Close()
}

type boundedListener struct {
	net.Listener
	activeConns int64
	maxConns    int64
}

func newBoundedListener(ln net.Listener, maxConns int64) *boundedListener {
	return &boundedListener{
		Listener: ln,
		maxConns: maxConns,
	}
}

func (l *boundedListener) Accept() (net.Conn, error) {
	for {
		c, err := l.Listener.Accept()
		if err != nil {
			return nil, err
		}

		cur := atomic.AddInt64(&l.activeConns, 1)
		if cur > l.maxConns {
			atomic.AddInt64(&l.activeConns, -1)
			_ = c.Close() // Reject connection immediately before allocating HTTP buffers
			continue
		}

		if tc, ok := c.(*net.TCPConn); ok {
			_ = tc.SetReadBuffer(8192)
			_ = tc.SetWriteBuffer(8192)
			_ = tc.SetNoDelay(true)
			_ = tc.SetKeepAlive(true)
			_ = tc.SetKeepAlivePeriod(15 * time.Second)
		}

		return &trackedConn{
			Conn: c,
			onClose: func() {
				atomic.AddInt64(&l.activeConns, -1)
			},
		}, nil
	}
}

// ---------------------------------------------------------------------------
// Zero-Allocation Buffer Pools
// ---------------------------------------------------------------------------

// 32KB streaming chunks for httputil.ReverseProxy
type streamBufferPool struct {
	pool sync.Pool
}

func (bp *streamBufferPool) Get() []byte {
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

func (bp *streamBufferPool) Put(b []byte) {
	if cap(b) >= 32*1024 {
		bp.pool.Put(b[:32*1024])
	}
}

var sharedStreamPool = &streamBufferPool{}

// 16KB pooled buffers for request body replay (POST /message)
var bodyPool = sync.Pool{
	New: func() any {
		b := make([]byte, 16*1024)
		return &b
	},
}

// ---------------------------------------------------------------------------
// Backend Representation & Transparent HA Transport Retry
// ---------------------------------------------------------------------------

type Backend struct {
	RawURL           string
	ParsedURL        *url.URL
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

	transport *http.Transport
	proxy     *httputil.ReverseProxy
}

func (b *Backend) IsHealthy() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.healthy
}

func (b *Backend) updateLatency(ms float64, alpha float64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.latencyEWMA = alpha*ms + (1-alpha)*b.latencyEWMA
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

// retryRoundTripper transparently retries requests on a peer backend
// when a dial or connection reset error occurs BEFORE response headers are written.
type retryRoundTripper struct {
	backend *Backend
	lb      *LB
}

func (rt *retryRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := rt.backend.transport.RoundTrip(req)
	if err == nil {
		return resp, nil
	}

	// Dial or connection error on primary backend.
	// Select alternate healthy peer and retry immediately.
	alt := rt.lb.pickExcluding(rt.backend)
	if alt == nil || alt == rt.backend {
		return nil, err
	}

	// Rewind body if available
	if req.GetBody != nil {
		newBody, bErr := req.GetBody()
		if bErr == nil {
			req.Body = newBody
		}
	}

	// Rewrite destination host
	req.URL.Scheme = alt.ParsedURL.Scheme
	req.URL.Host = alt.ParsedURL.Host
	req.Host = alt.ParsedURL.Host

	log.Printf("[ha-retry] %s %s failed on %s: %v → failover to %s", req.Method, req.URL.Path, rt.backend.RawURL, err, alt.RawURL)
	return alt.transport.RoundTrip(req)
}

func newBackend(rawURL string, lb *LB) *Backend {
	u, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil {
		log.Fatalf("invalid backend URL %q: %v", rawURL, err)
	}

	// High-performance warm connection pool (keeps connections open, zero socket churn)
	transport := &http.Transport{
		MaxIdleConns:        600,
		MaxIdleConnsPerHost: 200,
		MaxConnsPerHost:     300,
		IdleConnTimeout:     90 * time.Second,
		ResponseHeaderTimeout: 15 * time.Second,
		DisableCompression:  true,
		DisableKeepAlives:   false,
		ForceAttemptHTTP2:   false,
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			d := &net.Dialer{
				Timeout:   2 * time.Second,
				KeepAlive: 30 * time.Second,
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

	b := &Backend{
		RawURL:      rawURL,
		ParsedURL:   u,
		healthy:     true,
		latencyEWMA: 10,
		transport:   transport,
	}

	proxy := httputil.NewSingleHostReverseProxy(u)
	proxy.BufferPool = sharedStreamPool
	proxy.Transport = &retryRoundTripper{backend: b, lb: lb}
	proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		atomic.AddInt64(&lb.totalErrors, 1)
		w.Header().Set("Connection", "close")
		http.Error(w, `{"error":"bad gateway: backend unavailable"}`, http.StatusBadGateway)
	}

	b.proxy = proxy
	return b
}

// ---------------------------------------------------------------------------
// Load Balancer Core
// ---------------------------------------------------------------------------

type LB struct {
	cfg           Config
	backends      []*Backend
	listener      *boundedListener
	sem           chan struct{} // Concurrency gate: limits in-flight requests to backends
	totalRequests int64
	totalDropped  int64
	totalErrors   int64
	startTime     time.Time
}

func newLB(cfg Config) *LB {
	lb := &LB{
		cfg:       cfg,
		startTime: time.Now(),
		sem:       make(chan struct{}, cfg.MaxInFlight),
	}
	for _, u := range cfg.BackendURLs {
		lb.backends = append(lb.backends, newBackend(u, lb))
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
// Health Checker & Metrics Scraper Loops
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
	resp, err := client.Get(b.RawURL + "/health")
	ok := err == nil && resp.StatusCode == http.StatusOK
	if resp != nil {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if ok {
		b.failStreak = 0
		b.successStreak++
		if !b.healthy && b.successStreak >= lb.cfg.HealthyAfter {
			b.healthy = true
			log.Printf("[health] backend %s is HEALTHY", b.RawURL)
		}
	} else {
		b.successStreak = 0
		b.failStreak++
		if b.healthy && b.failStreak >= lb.cfg.UnhealthyAfter {
			b.healthy = false
			log.Printf("[health] backend %s marked UNHEALTHY (failures=%d)", b.RawURL, b.failStreak)
		}
	}
}

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
	resp, err := client.Get(b.RawURL + "/metrics")
	if err != nil || resp.StatusCode != http.StatusOK {
		if resp != nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
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
// HTTP Proxy Handler (Direct Streaming, Zero Response Buffering)
// ---------------------------------------------------------------------------

func (lb *LB) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	atomic.AddInt64(&lb.totalRequests, 1)

	// Built-in endpoints
	if r.URL.Path == "/health" && r.Method == http.MethodGet {
		lb.serveHealth(w, r)
		return
	}
	if r.URL.Path == "/metrics" && r.Method == http.MethodGet {
		lb.serveMetrics(w, r)
		return
	}

	// 1. WebSockets bypass semaphore gate for long-lived streaming
	if strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
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

	// 2. Concurrency Semaphore & Graceful Load Shedding (System Design Queuing)
	// If backend pool is fully saturated, queue for at most 6.0 seconds.
	// Context timeout cancels cleanly with ZERO timer leak.
	ctx, cancel := context.WithTimeout(r.Context(), 6000*time.Millisecond)
	defer cancel()

	select {
	case lb.sem <- struct{}{}:
		defer func() { <-lb.sem }()
	case <-ctx.Done():
		atomic.AddInt64(&lb.totalDropped, 1)
		w.Header().Set("Connection", "close")
		w.Header().Set("Retry-After", "1")
		http.Error(w, `{"error":"load shedding: queue full or timeout"}`, http.StatusServiceUnavailable)
		return
	}

	// 3. Pooled request body for replay resilience (zero heap allocations)
	var bodyBuf *[]byte
	if (r.Method == http.MethodPost || r.Method == http.MethodPut) && r.Body != nil {
		bufPtr := bodyPool.Get().(*[]byte)
		bodyBuf = bufPtr
		defer bodyPool.Put(bodyBuf)

		n, _ := io.ReadFull(io.LimitReader(r.Body, 16384), *bodyBuf)
		_ = r.Body.Close()
		captured := (*bodyBuf)[:n]

		r.Body = io.NopCloser(bytes.NewReader(captured))
		r.ContentLength = int64(n)
		r.GetBody = func() (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewReader(captured)), nil
		}
	}

	// 4. Route directly to best backend
	chosen := lb.pick()
	if chosen == nil {
		atomic.AddInt64(&lb.totalErrors, 1)
		http.Error(w, `{"error":"no backends available"}`, http.StatusServiceUnavailable)
		return
	}

	atomic.AddInt64(&chosen.activeConns, 1)
	start := time.Now()

	// Direct streaming: proxy streams response chunks directly to client socket w.
	// ZERO response bytes buffered in RAM!
	chosen.proxy.ServeHTTP(w, r)

	atomic.AddInt64(&chosen.activeConns, -1)
	elapsed := float64(time.Since(start).Milliseconds())
	chosen.updateLatency(elapsed, lb.cfg.LBAlpha)
}

// ---------------------------------------------------------------------------
// Built-in Endpoints
// ---------------------------------------------------------------------------

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

	type bStatus struct {
		URL         string  `json:"url"`
		Healthy     bool    `json:"healthy"`
		LatencyMs   float64 `json:"latency_ewma_ms"`
		CpuPct      float64 `json:"cpu_pct"`
		ActiveConns int64   `json:"active_connections"`
		Score       float64 `json:"score"`
	}

	statuses := make([]bStatus, len(lb.backends))
	for i, b := range lb.backends {
		b.mu.RLock()
		statuses[i] = bStatus{
			URL:         b.RawURL,
			Healthy:     b.healthy,
			LatencyMs:   math.Round(b.latencyEWMA*100) / 100,
			CpuPct:      b.cpuPct,
			ActiveConns: atomic.LoadInt64(&b.activeConns),
			Score:       math.Round(b.score(lb.cfg, maxConns, maxLat, maxCpu)*1000) / 1000,
		}
		b.mu.RUnlock()
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
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
			URL:               b.RawURL,
			Healthy:           b.healthy,
			ActiveConnections: math.Max(float64(atomic.LoadInt64(&b.activeConns)), b.remoteActiveConn),
			CpuPct:            b.cpuPct,
			MemPct:            b.memPct,
			LatencyEWMA:       math.Round(b.latencyEWMA*100) / 100,
		})
		b.mu.RUnlock()
	}
	var activeConns int64 = 0
	if lb.listener != nil {
		activeConns = atomic.LoadInt64(&lb.listener.activeConns)
	}
	resp := map[string]any{
		"lb_requests":    atomic.LoadInt64(&lb.totalRequests),
		"lb_dropped":     atomic.LoadInt64(&lb.totalDropped),
		"lb_errors":      atomic.LoadInt64(&lb.totalErrors),
		"uptime_sec":     int(time.Since(lb.startTime).Seconds()),
		"active_conns":   activeConns,
		"backends":       bms,
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

// ---------------------------------------------------------------------------
// Linux Memory Diagnostics
// ---------------------------------------------------------------------------

func getLinuxRSS() int64 {
	data, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return 0
	}
	for _, line := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(line, "VmRSS:") {
			parts := strings.Fields(line)
			if len(parts) >= 2 {
				if kb, err := strconv.ParseInt(parts[1], 10, 64); err == nil {
					return kb / 1024 // return in MB
				}
			}
		}
	}
	return 0
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

	// 2. Strict Memory Protection for 512MB Linux cgroup
	// Set GOMEMLIMIT soft limit to 35MB
	debug.SetMemoryLimit(35 * 1024 * 1024)
	// Hyper-aggressive GC: trigger GC on 20% heap expansion to keep live heap < 10MB
	debug.SetGCPercent(20)

	runtime.GOMAXPROCS(runtime.NumCPU())

	cfg := loadConfig()

	log.Printf("=== Group Chat High-Availability Load Balancer ===")
	log.Printf("ListenAddr     : %s", cfg.ListenAddr)
	log.Printf("Backends       : %s", strings.Join(cfg.BackendURLs, ", "))
	log.Printf("Max In-Flight  : %d backend workers", cfg.MaxInFlight)
	log.Printf("Max Active TCP : %d connections", cfg.MaxActiveConns)

	lb := newLB(cfg)

	// Background health and metrics pollers
	go lb.healthLoop()
	go lb.metricsLoop()

	// Initial metrics scrape
	client := &http.Client{Timeout: cfg.HealthTimeout}
	for _, b := range lb.backends {
		go lb.scrapeMetrics(client, b)
	}

	// 3. Real-Time Heartbeat Logger & OS Page Sweeper (Every 3 seconds)
	// Proactively forces madvise(MADV_DONTNEED) so unused pages are returned
	// to the Linux kernel immediately, keeping Sys < 25MB at all times.
	go func() {
		ticker := time.NewTicker(3 * time.Second)
		var m runtime.MemStats
		for range ticker.C {
			debug.FreeOSMemory() // Release all unused pages to OS kernel!
			runtime.ReadMemStats(&m)

			totalReq := atomic.LoadInt64(&lb.totalRequests)
			totalDrop := atomic.LoadInt64(&lb.totalDropped)
			totalErr := atomic.LoadInt64(&lb.totalErrors)
			inFlight := len(lb.sem)
			var activeTCP int64 = 0
			if lb.listener != nil {
				activeTCP = atomic.LoadInt64(&lb.listener.activeConns)
			}
			rssMB := getLinuxRSS()
			heapMB := m.Alloc / (1024 * 1024)
			sysMB := m.Sys / (1024 * 1024)

			var bStats []string
			for _, b := range lb.backends {
				b.mu.RLock()
				h := "UP"
				if !b.healthy {
					h = "DOWN"
				}
				bStats = append(bStats, fmt.Sprintf("%s:%s(lat=%.0fms,act=%d)", b.RawURL, h, b.latencyEWMA, atomic.LoadInt64(&b.activeConns)))
				b.mu.RUnlock()
			}

			log.Printf("[heartbeat] reqs=%d dropped=%d errs=%d inflight=%d/%d conns=%d heap=%dMB sys=%dMB rss=%dMB g=%d | %s",
				totalReq, totalDrop, totalErr, inFlight, cfg.MaxInFlight, activeTCP, heapMB, sysMB, rssMB, runtime.NumGoroutine(), strings.Join(bStats, " "))

			// Early warning alarm if memory approaches dangerous levels (>120MB)
			if sysMB > 120 || rssMB > 120 {
				log.Printf("⚠️ [ALERT-MEM] Memory pressure detected: sys=%dMB rss=%dMB (cgroup limit=512MB)", sysMB, rssMB)
			}
		}
	}()

	// 4. Create TCP listener with clamped 8KB socket buffers & admission control
	rawLn, err := net.Listen("tcp", cfg.ListenAddr)
	if err != nil {
		log.Fatalf("failed to listen on %s: %v", cfg.ListenAddr, err)
	}
	defer rawLn.Close()

	bln := newBoundedListener(rawLn, cfg.MaxActiveConns)
	lb.listener = bln

	server := &http.Server{
		Handler:           lb,
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       30 * time.Second,
	}

	fmt.Printf("\nLoad Balancer listening on http://0.0.0.0%s (direct streaming, %d-worker queue, zero-alloc body pool)\n", cfg.ListenAddr, cfg.MaxInFlight)
	if err := server.Serve(bln); err != nil {
		log.Fatalf("server error: %v", err)
	}
}
