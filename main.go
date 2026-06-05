package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

func main() {
	cfg := parseFlags()
	client := buildClient(cfg.rps)

	bufSize := cfg.rps*int(cfg.dur.Seconds()+5) + 10_000
	col := newCollector(bufSize)
	go col.run()

	testCtx, testCancel := context.WithTimeout(context.Background(), cfg.dur)
	defer testCancel()

	go handleSignals(testCancel)
	go runSnapshotTicker(col, testCtx)

	const tickDur = 100 * time.Millisecond

	var (
		injected    int64
		accumulator float64
		wg          sync.WaitGroup
	)

	mainTicker := time.NewTicker(tickDur)
	progressTicker := time.NewTicker(5 * time.Second)
	defer mainTicker.Stop()
	defer progressTicker.Stop()

	startTime := time.Now()
	printBanner(cfg)

injection:
	for {
		select {
		case <-testCtx.Done():
			break injection

		case <-progressTicker.C:
			elapsed := time.Since(startTime)
			fmt.Printf("[Orion] %5s elapsed — injected: %d VUs — current rps: %.0f/s\n",
				elapsed.Truncate(time.Second),
				atomic.LoadInt64(&injected),
				currentRPS(cfg.rps, cfg.rampUp, elapsed))

		case <-mainTicker.C:
			elapsed := time.Since(startTime)
			accumulator += currentRPS(cfg.rps, cfg.rampUp, elapsed) * tickDur.Seconds()
			toFire := int(accumulator)
			accumulator -= float64(toFire)

			for range toFire {
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

	wg.Wait()
	close(col.ch)
	<-col.done

	col.report(cfg, elapsed)
}

func handleSignals(cancel context.CancelFunc) {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	sig := <-sigCh
	fmt.Printf("\n[Orion] %s — stopping injection, draining in-flight requests...\n", sig)
	cancel()
}

// runSnapshotTicker sends a snapshot request to the collector every second so
// the HTML report has per-second time-series data for its charts.
func runSnapshotTicker(col *collector, ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	sec := 0
	for {
		select {
		case <-ticker.C:
			sec++
			req := snapshotReq{elapsed: sec, done: make(chan struct{}, 1)}
			select {
			case col.snapCh <- req:
				select {
				case <-req.done:
				case <-time.After(200 * time.Millisecond):
				}
			default:
			}
		case <-ctx.Done():
			return
		}
	}
}

func printBanner(cfg *config) {
	fmt.Printf("[Orion] starting  %s %s  rps=%d  ramp-up=%s  duration=%s  timeout=%s\n",
		cfg.method, cfg.url, cfg.rps, cfg.rampUp, cfg.dur, cfg.timeout)
	for k, vals := range cfg.headers {
		fmt.Printf("[Orion] header    %s: %s\n", k, strings.Join(vals, ", "))
	}
	fmt.Println("[Orion] Ctrl+C stops injection early and still prints the report.")
}
