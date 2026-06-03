package main

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"time"

	hdrhistogram "github.com/HdrHistogram/hdrhistogram-go"
)

type result struct {
	latency time.Duration
	status  int // 0 when err != nil and no HTTP response arrived
	err     error
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
}

func newCollector(bufSize int) *collector {
	return &collector{
		ch:         make(chan result, bufSize),
		done:       make(chan struct{}),
		hist:       hdrhistogram.New(1, 60_000_000, 3),
		windowHist: hdrhistogram.New(1, 60_000_000, 3),
		errors:     make(map[string]int64),
		snapCh:     make(chan snapshotReq, 1),
	}
}

func (c *collector) run() {
	defer close(c.done)
	for {
		select {
		case r, ok := <-c.ch:
			if !ok {
				return
			}
			c.total++
			if r.latency > 0 {
				_ = c.hist.RecordValue(r.latency.Microseconds())
				_ = c.windowHist.RecordValue(r.latency.Microseconds())
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

		case req := <-c.snapCh:
			errDelta := (c.total - c.success) - (c.prevTotal - c.prevSuccess)
			snap := snapshot{
				elapsed:    req.elapsed,
				successCnt: c.success - c.prevSuccess,
				errorCnt:   errDelta,
			}
			if c.windowHist.TotalCount() > 0 {
				snap.p50ms = float64(c.windowHist.ValueAtQuantile(50)) / 1000.0
				snap.p95ms = float64(c.windowHist.ValueAtQuantile(95)) / 1000.0
				snap.p99ms = float64(c.windowHist.ValueAtQuantile(99)) / 1000.0
			}
			c.snapshots = append(c.snapshots, snap)
			c.prevTotal = c.total
			c.prevSuccess = c.success
			c.windowHist.Reset()
			req.done <- struct{}{}
		}
	}
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
