package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"time"
)

var vuSeq int64 // monotonic counter; gives each VU a unique, stable user_id

// buildClient returns an *http.Client tuned for high-concurrency load testing.
// Pool sized at 2×rps: at rps req/s with ~100ms p99, peak connections ≈ rps×0.1.
// No client Timeout: each request carries its own context deadline.
func buildClient(rps int) *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:          rps * 2,
			MaxIdleConnsPerHost:   rps * 2,
			MaxConnsPerHost:       0,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
			DisableCompression:    true, // measure raw wire latency, not decompression time
		},
	}
}

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
	// Drain body so the underlying TCP connection is returned to the pool.
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()

	ch <- result{latency: latency, status: resp.StatusCode}
}

func methodHasBody(method string) bool {
	return method == http.MethodPost || method == http.MethodPut || method == http.MethodPatch
}

// currentRPS returns the instantaneous injection rate at the given elapsed time.
// Grows linearly from 0 to target over rampUp, then stays constant.
func currentRPS(target int, rampUp, elapsed time.Duration) float64 {
	if elapsed >= rampUp {
		return float64(target)
	}
	return float64(target) * elapsed.Seconds() / rampUp.Seconds()
}
