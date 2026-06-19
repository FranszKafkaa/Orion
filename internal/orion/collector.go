package orion

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"time"

	hdrhistogram "github.com/HdrHistogram/hdrhistogram-go"
)

const latencyHistogramMaxMicros = 60_000_000

type result struct {
	latency     time.Duration
	status      int // 0 when err != nil and no HTTP response arrived
	err         error
	endpointIdx int // index into scenario endpoints; -1 when not using a scenario
}

// snapshot captures per-second telemetry for the HTML report charts.
type snapshot struct {
	elapsed    int // seconds since test start
	successCnt int64
	errorCnt   int64
	p50ms      float64
	p95ms      float64
	p99ms      float64
}

type snapshotReq struct {
	elapsed int
	done    chan struct{}
}

type endpointMetrics struct {
	hist    *hdrhistogram.Histogram
	total   int64
	success int64
	errors  map[string]int64
}

// collector receives results from VU goroutines via a buffered channel and
// maintains all metric state in a single background goroutine, eliminating
// mutex contention on the hot path.
type collector struct {
	ch          chan result
	done        chan struct{}
	hist        *hdrhistogram.Histogram // cumulative; latency in microseconds
	windowHist  *hdrhistogram.Histogram // per-window; reset on each snapshot
	total       int64
	success     int64
	errors      map[string]int64 // "timeout", "connection_error", "http_NNN"
	snapshots   []snapshot
	snapCh      chan snapshotReq
	prevTotal   int64
	prevSuccess int64
	endpoints   []endpointMetrics
	// adaptive rate: if non-nil, receives a copy of every snapshot
	adaptiveCh chan snapshot
	// live dashboard: if non-nil, called on every snapshot from the collector goroutine
	dashboardFn func(snapshot)
}

func newCollector(bufSize, numEndpoints int) *collector {
	c := &collector{
		ch:         make(chan result, bufSize),
		done:       make(chan struct{}),
		hist:       newLatencyHistogram(),
		windowHist: newLatencyHistogram(),
		errors:     make(map[string]int64),
		snapCh:     make(chan snapshotReq, 1),
	}
	if numEndpoints > 0 {
		c.endpoints = make([]endpointMetrics, numEndpoints)
		for i := range c.endpoints {
			c.endpoints[i] = newEndpointMetrics()
		}
	}
	return c
}

func (c *collector) run() {
	defer close(c.done)
	for {
		select {
		case r, ok := <-c.ch:
			if !ok {
				return
			}
			c.recordResult(r)

		case req := <-c.snapCh:
			c.recordSnapshot(req)
		}
	}
}

func newLatencyHistogram() *hdrhistogram.Histogram {
	return hdrhistogram.New(1, latencyHistogramMaxMicros, 3)
}

func newEndpointMetrics() endpointMetrics {
	return endpointMetrics{
		hist:   newLatencyHistogram(),
		errors: make(map[string]int64),
	}
}

func (c *collector) recordResult(r result) {
	c.total++

	ep := c.endpoint(r.endpointIdx)
	if ep != nil {
		ep.total++
	}

	if r.latency > 0 {
		c.recordLatency(r.latency, ep)
	}
	if r.err != nil {
		c.recordError(classifyErr(r.err), ep)
		return
	}
	if isSuccessStatus(r.status) {
		c.success++
		if ep != nil {
			ep.success++
		}
		return
	}
	c.recordError(fmt.Sprintf("http_%d", r.status), ep)
}

func (c *collector) endpoint(idx int) *endpointMetrics {
	if idx < 0 || idx >= len(c.endpoints) {
		return nil
	}
	return &c.endpoints[idx]
}

func (c *collector) recordLatency(latency time.Duration, ep *endpointMetrics) {
	micros := latency.Microseconds()
	_ = c.hist.RecordValue(micros)
	_ = c.windowHist.RecordValue(micros)
	if ep != nil {
		_ = ep.hist.RecordValue(micros)
	}
}

func (c *collector) recordError(key string, ep *endpointMetrics) {
	c.errors[key]++
	if ep != nil {
		ep.errors[key]++
	}
}

func (c *collector) recordSnapshot(req snapshotReq) {
	snap := c.snapshot(req.elapsed)
	c.snapshots = append(c.snapshots, snap)
	c.prevTotal = c.total
	c.prevSuccess = c.success
	c.windowHist.Reset()
	req.done <- struct{}{}
	c.sendAdaptiveSnapshot(snap)
	c.sendDashboardSnapshot(snap)
}

func (c *collector) snapshot(elapsed int) snapshot {
	errDelta := (c.total - c.success) - (c.prevTotal - c.prevSuccess)
	snap := snapshot{
		elapsed:    elapsed,
		successCnt: c.success - c.prevSuccess,
		errorCnt:   errDelta,
	}
	if c.windowHist.TotalCount() > 0 {
		snap.p50ms = float64(c.windowHist.ValueAtQuantile(50)) / 1000.0
		snap.p95ms = float64(c.windowHist.ValueAtQuantile(95)) / 1000.0
		snap.p99ms = float64(c.windowHist.ValueAtQuantile(99)) / 1000.0
	}
	return snap
}

func (c *collector) sendAdaptiveSnapshot(snap snapshot) {
	if c.adaptiveCh == nil {
		return
	}
	select {
	case c.adaptiveCh <- snap:
	default:
	}
}

func (c *collector) sendDashboardSnapshot(snap snapshot) {
	if c.dashboardFn != nil {
		c.dashboardFn(snap)
	}
}

func isSuccessStatus(status int) bool {
	return status >= http.StatusOK && status < http.StatusMultipleChoices
}

// classifyErr maps transport errors to compact keys for the errors table.
// url.Error.Timeout() is true for both context.DeadlineExceeded and net timeouts.
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
