package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	hdrhistogram "github.com/HdrHistogram/hdrhistogram-go"
)

// ─── CLI flags ────────────────────────────────────────────────────────────────

// headerFlags allows -H to be repeated: -H "X-Foo: bar" -H "X-Baz: qux"
type headerFlags []string

func (h *headerFlags) String() string     { return strings.Join(*h, ", ") }
func (h *headerFlags) Set(v string) error { *h = append(*h, v); return nil }

// config holds all runtime parameters derived from CLI flags.
type config struct {
	url     string
	method  string
	body    string // raw request body; empty = use default JSON payload for POST/PUT/PATCH
	rps     int
	dur     time.Duration
	timeout time.Duration
	headers http.Header
}

func parseFlags() *config {
	var extra headerFlags

	urlFlag := flag.String("url", "http://localhost:8080/api/checkout", "endpoint under test")
	methodFlag := flag.String("method", "POST", "HTTP method (GET, POST, PUT, PATCH, DELETE, HEAD)")
	bodyFlag := flag.String("body", "", "raw request body (JSON); omit to use default {user_id, action} payload")
	rpsFlag := flag.Int("rps", 100, "virtual users injected per second")
	durFlag := flag.Duration("duration", 30*time.Second, "total test duration (e.g. 30s, 2m)")
	timeoutFlag := flag.Duration("timeout", 5*time.Second, "per-request hard deadline")
	tokenFlag := flag.String("token", "", "Bearer token → Authorization: Bearer <token>")
	basicFlag := flag.String("basic", "", "Basic auth as user:pass")
	flag.Var(&extra, "H", "extra header as 'Key: Value' (repeatable)")

	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, "Usage:")
		fmt.Fprintln(os.Stderr, "Flags:")
		flag.PrintDefaults()
		fmt.Fprintln(os.Stderr, "\nExamples:")
		fmt.Fprintln(os.Stderr, "  Orion -url http://localhost:8080/clubes -method GET -rps 200 -duration 60s")
		fmt.Fprintln(os.Stderr, "  Orion -url http://api/order -body '{\"item\":\"book\"}' -token eyJ... -rps 50")
		fmt.Fprintln(os.Stderr, "  Orion -url http://api/checkout -basic admin:s3cr3t -H 'X-Tenant: acme'")
	}

	flag.Parse()

	method := strings.ToUpper(*methodFlag)
	validMethods := map[string]bool{
		"GET": true, "POST": true, "PUT": true, "PATCH": true,
		"DELETE": true, "HEAD": true, "OPTIONS": true,
	}
	if !validMethods[method] {
		fmt.Fprintf(os.Stderr, "error: unsupported HTTP method %q\n", *methodFlag)
		os.Exit(1)
	}

	if *rpsFlag < 1 {
		fmt.Fprintln(os.Stderr, "error: -rps must be ≥ 1")
		os.Exit(1)
	}

	headers := make(http.Header)

	switch {
	case *tokenFlag != "":
		headers.Set("Authorization", "Bearer "+*tokenFlag)
	case *basicFlag != "":
		encoded := base64.StdEncoding.EncodeToString([]byte(*basicFlag))
		headers.Set("Authorization", "Basic "+encoded)
	}

	for _, h := range extra {
		parts := strings.SplitN(h, ":", 2)
		if len(parts) != 2 {
			fmt.Fprintf(os.Stderr, "error: invalid header %q (expected 'Key: Value')\n", h)
			os.Exit(1)
		}
		headers.Set(strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1]))
	}

	return &config{
		url:     *urlFlag,
		method:  method,
		body:    *bodyFlag,
		rps:     *rpsFlag,
		dur:     *durFlag,
		timeout: *timeoutFlag,
		headers: headers,
	}
}

// ─── Result ───────────────────────────────────────────────────────────────────

type result struct {
	latency time.Duration
	status  int // 0 when err != nil and no HTTP response arrived
	err     error
}

// ─── Collector ────────────────────────────────────────────────────────────────

// collector receives result values from VU goroutines over a buffered channel
// and maintains all metric state in a single background goroutine.
// This eliminates mutex contention on the hot path: VUs only do a non-blocking
// channel send; the observer effect on measured latency is negligible.
//
// Fields total, success, and errors are written exclusively by run() and are
// safe to read without synchronisation only after the done channel is closed.
type collector struct {
	ch      chan result
	done    chan struct{}
	hist    *hdrhistogram.Histogram // latency in microseconds; single-writer, no mutex needed
	total   int64
	success int64
	errors  map[string]int64 // "timeout", "connection_error", "http_NNN"
}

func newCollector(bufSize int) *collector {
	return &collector{
		ch:   make(chan result, bufSize),
		done: make(chan struct{}),
		// 1 µs minimum, 60 s maximum, 3 significant figures → ~0.1% resolution
		hist:   hdrhistogram.New(1, 60_000_000, 3),
		errors: make(map[string]int64),
	}
}

// run is the sole goroutine that reads from ch and mutates collector state.
// It exits when ch is closed, then closes done to signal the main goroutine.
func (c *collector) run() {
	defer close(c.done)
	for r := range c.ch {
		c.total++
		if r.latency > 0 {
			// RecordValue returns an error only when the value exceeds the histogram
			// ceiling (60 s). We silently discard it to avoid polluting hot-path logs.
			_ = c.hist.RecordValue(r.latency.Microseconds())
		}
		if r.err != nil {
			c.errors[classifyErr(r.err)]++
			continue
		}
		if r.status >= 200 && r.status < 300 {
			c.success++
		} else {
			c.errors[fmt.Sprintf("http_%d", r.status)]++
		}
	}
}

// ─── HTTP Client ──────────────────────────────────────────────────────────────

// buildClient returns an *http.Client tuned for high-concurrency load testing.
//
// Pool sizing rationale: at rps req/s with a typical p99 of ~100 ms,
// peak concurrent connections ≈ rps × 0.1. Sizing pools at 2× rps gives
// comfortable headroom without risking socket exhaustion.
//
// No client-level Timeout is set: each request carries its own context deadline,
// which is more precise and avoids the double-counting issue in http.Client.Timeout.
func buildClient(rps int) *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:          rps * 2,
			MaxIdleConnsPerHost:   rps * 2,
			MaxConnsPerHost:       0, // no cap — let the OS and server enforce limits
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
			// Disable transparent decompression to measure raw wire latency.
			DisableCompression: true,
		},
	}
}

// ─── Virtual User ─────────────────────────────────────────────────────────────

var vuSeq int64 // monotonic counter; gives each VU a unique, stable user_id

// runVU executes one HTTP request and sends exactly one result to ch.
// Body logic:
//   - if cfg.body is set → send it literally for every request
//   - if method naturally carries a body (POST/PUT/PATCH) and no body was given
//     → generate the default {"user_id": N, "action": "checkout"} payload
//   - otherwise (GET, DELETE, HEAD, etc.) → no body
func runVU(client *http.Client, cfg *config, ch chan<- result) {
	var bodyReader io.Reader

	switch {
	case cfg.body != "":
		bodyReader = strings.NewReader(cfg.body)
	case methodHasBody(cfg.method):
		uid := atomic.AddInt64(&vuSeq, 1)
		b, _ := json.Marshal(struct {
			UserID int64  `json:"user_id"`
			Action string `json:"action"`
		}{UserID: uid, Action: "checkout"})
		bodyReader = bytes.NewReader(b)
	}

	ctx, cancel := context.WithTimeout(context.Background(), cfg.timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, cfg.method, cfg.url, bodyReader)
	if err != nil {
		ch <- result{err: err}
		return
	}
	if bodyReader != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	for k, vals := range cfg.headers {
		for _, v := range vals {
			req.Header.Set(k, v)
		}
	}

	start := time.Now()
	resp, err := client.Do(req)
	latency := time.Since(start)

	if err != nil {
		ch <- result{latency: latency, err: err}
		return
	}
	// Drain the body so the underlying TCP connection is returned to the pool.
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()

	ch <- result{latency: latency, status: resp.StatusCode}
}

func methodHasBody(method string) bool {
	return method == http.MethodPost || method == http.MethodPut || method == http.MethodPatch
}

// ─── Orchestrator — Open Model ────────────────────────────────────────────────

// main implements the Gatling Open Model: a time.Ticker fires at 1/RPS intervals
// and unconditionally launches a new VU goroutine. If the server is slow and
// goroutines pile up, the ticker is never blocked — injection rate is preserved.
//
// Graceful shutdown: SIGINT/SIGTERM cancels the injection context early. In both
// cases (natural expiry or signal), wg.Wait() drains all in-flight goroutines
// before the report is printed.
func main() {
	cfg := parseFlags()
	client := buildClient(cfg.rps)

	// Buffer large enough that VU goroutines never block on the channel send.
	// Formula: expected total results + 5-second overshoot headroom.
	bufSize := cfg.rps*int(cfg.dur.Seconds()+5) + 10_000
	col := newCollector(bufSize)
	go col.run()

	testCtx, testCancel := context.WithTimeout(context.Background(), cfg.dur)
	defer testCancel()

	// Handle Ctrl-C / SIGTERM gracefully: cancel injection, still drain and report.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		select {
		case sig := <-sigCh:
			fmt.Printf("\n[Orion] %s — stopping injection, draining in-flight requests...\n", sig)
			testCancel()
		case <-testCtx.Done():
		}
	}()

	// Compute ticker cadence.
	// For RPS ≤ 1000, tick interval = 1/RPS (e.g. 100 RPS → 10 ms).
	// For RPS > 1000, clamp to 1 ms and launch multiple VUs per tick
	// so the aggregate injection rate still matches cfg.rps.
	tickInterval := time.Second / time.Duration(cfg.rps)
	if tickInterval < time.Millisecond {
		tickInterval = time.Millisecond
	}
	// vuPerTick = RPS × tick_interval_in_seconds
	vuPerTick := int(time.Duration(cfg.rps) * tickInterval / time.Second)
	if vuPerTick < 1 {
		vuPerTick = 1
	}

	var (
		injected int64 // updated atomically; read by progress ticker in main goroutine
		wg       sync.WaitGroup
	)

	mainTicker := time.NewTicker(tickInterval)
	progressTicker := time.NewTicker(5 * time.Second)
	defer mainTicker.Stop()
	defer progressTicker.Stop()

	startTime := time.Now()
	fmt.Printf("[Orion] starting  %s %s  rps=%d  duration=%s  timeout=%s  tick=%s  vu/tick=%d\n",
		cfg.method, cfg.url, cfg.rps, cfg.dur, cfg.timeout, tickInterval, vuPerTick)
	if len(cfg.headers) > 0 {
		for k, vals := range cfg.headers {
			fmt.Printf("[Orion] header    %s: %s\n", k, strings.Join(vals, ", "))
		}
	}
	fmt.Println("[Orion] Ctrl+C stops injection early and still prints the report.")

injection:
	for {
		select {
		case <-testCtx.Done():
			break injection

		case <-progressTicker.C:
			elapsed := time.Since(startTime).Truncate(time.Second)
			fmt.Printf("[Orion] %5s elapsed — injected: %d VUs\n",
				elapsed, atomic.LoadInt64(&injected))

		case <-mainTicker.C:
			// Spawn vuPerTick goroutines in a tight loop. This is the key Open Model
			// property: the orchestrator never awaits a response before injecting the
			// next wave of users.
			for i := 0; i < vuPerTick; i++ {
				wg.Add(1)
				atomic.AddInt64(&injected, 1)
				go func() {
					defer wg.Done()
					runVU(client, cfg, col.ch)
				}()
			}
		}
	}

	elapsed := time.Since(startTime).Truncate(time.Millisecond)
	fmt.Printf("[Orion] injection ended (%s) — waiting for %d goroutines to drain...\n",
		elapsed, atomic.LoadInt64(&injected))

	wg.Wait()     // wait for every in-flight request to complete and send its result
	close(col.ch) // signal the collector that no more results will arrive
	<-col.done    // wait for the collector to process every buffered result

	col.report(cfg, elapsed)
}

// ─── Report ───────────────────────────────────────────────────────────────────

func (c *collector) report(cfg *config, elapsed time.Duration) {
	sep := strings.Repeat("─", 62)
	dbl := strings.Repeat("═", 62)

	row := func(label, value string) {
		fmt.Printf("  %-22s %s\n", label, value)
	}
	section := func(title string) {
		fmt.Println(sep)
		fmt.Println(" ", title)
	}

	successRate := 0.0
	actualRPS := 0.0
	if c.total > 0 {
		successRate = float64(c.success) / float64(c.total) * 100
	}
	if s := elapsed.Seconds(); s > 0 {
		actualRPS = float64(c.total) / s
	}

	fmt.Println()
	fmt.Println(dbl)
	fmt.Println("  Orion — Load Test Report")
	fmt.Println(dbl)
	row("URL:", cfg.method+" "+cfg.url)
	row("Duration:", elapsed.String())
	row("Target RPS:", fmt.Sprintf("%d req/s", cfg.rps))
	row("Timeout:", cfg.timeout.String())

	section("THROUGHPUT")
	row("Total requests:", fmt.Sprintf("%d", c.total))
	row("Successful:", fmt.Sprintf("%d  (%.2f%%)", c.success, successRate))
	row("Actual RPS:", fmt.Sprintf("%.2f req/s", actualRPS))

	if c.hist.TotalCount() > 0 {
		section("LATENCY")
		row("min:", formatµs(c.hist.Min()))
		row("p50  (median):", formatµs(c.hist.ValueAtQuantile(50)))
		row("p95:", formatµs(c.hist.ValueAtQuantile(95)))
		row("p99:", formatµs(c.hist.ValueAtQuantile(99)))
		row("p99.9:", formatµs(c.hist.ValueAtQuantile(99.9)))
		row("max:", formatµs(c.hist.Max()))
		row("mean:", formatµs(int64(c.hist.Mean())))
	}

	if len(c.errors) > 0 {
		section("ERRORS")
		for k, v := range c.errors {
			row(k+":", fmt.Sprintf("%d", v))
		}
	}

	fmt.Println(dbl)
	fmt.Println()
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

// classifyErr maps a transport-level error to a compact string key for the
// error breakdown table. url.Error.Timeout() returns true for both
// context.DeadlineExceeded (our per-request deadline) and net timeouts.
func classifyErr(err error) string {
	var ue *url.Error
	if errors.As(err, &ue) && ue.Timeout() {
		return "timeout"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	return "connection_error"
}

// formatµs converts a microsecond integer to a human-readable latency string.
func formatµs(µs int64) string {
	switch {
	case µs <= 0:
		return "n/a"
	case µs < 1_000:
		return fmt.Sprintf("%d µs", µs)
	case µs < 1_000_000:
		return fmt.Sprintf("%.2f ms", float64(µs)/1_000)
	default:
		return fmt.Sprintf("%.3f s", float64(µs)/1_000_000)
	}
}
