package orion

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

func Main() {
	cfg := parseFlags()
	if cfg.dashboardLauncher {
		startDashboardLauncher(cfg.dashboardPort)
		return
	}

	numEndpoints := 0
	if cfg.scenario != nil {
		numEndpoints = len(cfg.scenario.Endpoints)
	}

	if cfg.browser {
		bufSize := cfg.browserN*int(cfg.dur.Seconds()+5) + 10_000
		col := newCollector(bufSize, numEndpoints)
		go col.run()

		testCtx, testCancel := context.WithTimeout(context.Background(), cfg.dur)
		defer testCancel()

		go handleSignals(testCancel)
		go runSnapshotTicker(col, testCtx)
		var dash *dashHub
		if cfg.dashboard {
			dash = startDashboard(cfg, col, testCtx)
		}

		elapsed := runBrowserMode(cfg, col, testCtx)
		close(col.ch)
		<-col.done
		htmlPath := col.report(cfg, elapsed)
		if dash != nil {
			dash.setReportPath(htmlPath)
			time.Sleep(2 * time.Minute) // keep server alive for user to view report
		}
		return
	}

	client := buildClient(cfg.rps)

	bufSize := cfg.rps*int(cfg.dur.Seconds()+5) + 10_000
	col := newCollector(bufSize, numEndpoints)
	go col.run()

	testCtx, testCancel := context.WithTimeout(context.Background(), cfg.dur)
	defer testCancel()

	var effectiveRPS atomic.Int64
	effectiveRPS.Store(int64(cfg.rps))

	if cfg.maxErrorRate > 0 {
		adaptCh := make(chan snapshot, 10)
		col.adaptiveCh = adaptCh
		go runAdaptiveController(adaptCh, &effectiveRPS, cfg.rps, cfg.maxErrorRate, cfg.rampUp, testCtx)
	}

	go handleSignals(testCancel)
	go runSnapshotTicker(col, testCtx)
	var dash *dashHub
	if cfg.dashboard {
		dash = startDashboard(cfg, col, testCtx)
	}

	const tickDur = 100 * time.Millisecond

	var (
		injected    atomic.Int64
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
			eff := int(effectiveRPS.Load())
			fmt.Printf("[Orion] %5s elapsed — injected: %d VUs — current rps: %.0f/s\n",
				elapsed.Truncate(time.Second),
				injected.Load(),
				currentRPS(eff, cfg.rampUp, elapsed))

		case <-mainTicker.C:
			elapsed := time.Since(startTime)
			eff := int(effectiveRPS.Load())
			accumulator += currentRPS(eff, cfg.rampUp, elapsed) * tickDur.Seconds()
			toFire := int(accumulator)
			accumulator -= float64(toFire)

			for range toFire {
				wg.Add(1)
				injected.Add(1)
				go func() {
					defer wg.Done()
					runVU(client, cfg, col.ch)
				}()
			}
		}
	}

	elapsed := time.Since(startTime).Truncate(time.Millisecond)
	fmt.Printf("[Orion] injection ended (%s) — waiting for %d goroutines to drain...\n",
		elapsed, injected.Load())

	wg.Wait()
	close(col.ch)
	<-col.done

	htmlPath := col.report(cfg, elapsed)
	if dash != nil {
		dash.setReportPath(htmlPath)
		time.Sleep(2 * time.Minute) // keep server alive for user to view report
	}
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
	if cfg.scenario != nil {
		fmt.Printf("[Orion] starting  scenario(%d endpoints)  rps=%d  ramp-up=%s  duration=%s  timeout=%s\n",
			len(cfg.scenario.Endpoints), cfg.rps, cfg.rampUp, cfg.dur, cfg.timeout)
		for i, ep := range cfg.scenario.Endpoints {
			fmt.Printf("[Orion] endpoint  [%d] weight=%-3d  %s %s\n", i+1, ep.Weight, ep.Method, ep.URL)
		}
	} else {
		fmt.Printf("[Orion] starting  %s %s  rps=%d  ramp-up=%s  duration=%s  timeout=%s\n",
			cfg.method, cfg.url, cfg.rps, cfg.rampUp, cfg.dur, cfg.timeout)
	}
	if cfg.maxErrorRate > 0 {
		fmt.Printf("[Orion] adaptive  max-error-rate=%.1f%%\n", cfg.maxErrorRate)
	}
	for k, vals := range cfg.headers {
		fmt.Printf("[Orion] header    %s: %s\n", k, strings.Join(vals, ", "))
	}
	if cfg.clientIPs != nil {
		fmt.Printf("[Orion] client-ip simulation  %d IPs via %s\n",
			len(cfg.clientIPs.ips), strings.Join(cfg.clientIPs.headers, ", "))
	}
	fmt.Println("[Orion] Ctrl+C stops injection early and still prints the report.")
}
