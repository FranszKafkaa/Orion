package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"net/http"
	"sync"
	"time"

	"github.com/chromedp/cdproto/runtime"
	"github.com/chromedp/chromedp"
)

// browserMetric is the object returned by the JS fetch wrapper running inside
// the headless tab. chromedp deserialises it directly via CDP returnByValue.
type browserMetric struct {
	Latency float64 `json:"latency"` // ms
	Status  int     `json:"status"`
	Error   string  `json:"error"`
}

func runBrowserMode(cfg *config, col *collector, ctx context.Context) time.Duration {
	start := time.Now()

	fmt.Printf("[Orion] browser(headless)  users=%d  think=%s  duration=%s\n",
		cfg.browserN, browserThinkDesc(cfg), cfg.dur)
	if cfg.scenario != nil {
		fmt.Printf("[Orion] scenario: %d endpoints\n", len(cfg.scenario.Endpoints))
		for i, ep := range cfg.scenario.Endpoints {
			fmt.Printf("[Orion] endpoint  [%d] weight=%-3d  %s %s\n", i+1, ep.Weight, ep.Method, ep.URL)
		}
	} else {
		fmt.Printf("[Orion] target: %s %s\n", cfg.method, cfg.url)
	}
	fmt.Println("[Orion] Ctrl+C stops early and still prints the report.")

	opts := append(chromedp.DefaultExecAllocatorOptions[:],
		chromedp.Flag("disable-web-security", true),
		chromedp.Flag("disable-features", "IsolateOrigins,site-per-process"),
	)

	allocCtx, allocCancel := chromedp.NewExecAllocator(context.Background(), opts...)
	defer allocCancel()

	// Silence CDP / Chrome log noise.
	browserCtx, browserCancel := chromedp.NewContext(allocCtx,
		chromedp.WithLogf(func(string, ...interface{}) {}),
	)
	defer browserCancel()

	// Warm up: actually start the Chrome process before spawning goroutines.
	if err := chromedp.Run(browserCtx, chromedp.Navigate("about:blank")); err != nil {
		fmt.Printf("[Orion] failed to launch Chrome: %v\n", err)
		fmt.Println("[Orion] Make sure Google Chrome or Chromium is installed.")
		fmt.Println("[Orion] Tip: for plain HTTP load testing, remove the -browser flag.")
		return time.Since(start).Truncate(time.Millisecond)
	}
	fmt.Printf("[Orion] Chrome launched — %d VU goroutines running\n", cfg.browserN)

	var wg sync.WaitGroup
	for i := 0; i < cfg.browserN; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			runHeadlessVU(ctx, browserCtx, cfg, col)
		}()
	}

	wg.Wait()
	return time.Since(start).Truncate(time.Millisecond)
}

func runHeadlessVU(ctx context.Context, browserCtx context.Context, cfg *config, col *collector) {
	tabCtx, tabCancel := chromedp.NewContext(browserCtx)
	defer tabCancel()

	// Cancel the tab's CDP context when the test ends so that any in-flight
	// chromedp.Run/Evaluate returns immediately.
	go func() {
		<-ctx.Done()
		tabCancel()
	}()

	if err := chromedp.Run(tabCtx, chromedp.Navigate("about:blank")); err != nil {
		return
	}

	timeoutMs := cfg.timeout.Milliseconds()

	for {
		if tabCtx.Err() != nil {
			return
		}

		idx, ep := pickBrowserEndpoint(cfg)
		script := buildFetchScript(ep, cfg.headers, timeoutMs)

		var m browserMetric
		err := chromedp.Run(tabCtx,
			chromedp.Evaluate(script, &m,
				func(p *runtime.EvaluateParams) *runtime.EvaluateParams {
					return p.WithAwaitPromise(true)
				}),
		)
		if err != nil {
			// tabCtx was cancelled (test over) or an unrecoverable CDP error.
			return
		}

		res := result{
			latency:     time.Duration(float64(time.Millisecond) * m.Latency),
			status:      m.Status,
			endpointIdx: idx,
		}
		if m.Error != "" {
			res.err = fmt.Errorf("%s", m.Error)
		}

		select {
		case col.ch <- res:
		case <-tabCtx.Done():
			return
		}

		if think := browserThinkDuration(cfg); think > 0 {
			select {
			case <-tabCtx.Done():
				return
			case <-time.After(think):
			}
		}
	}
}

func pickBrowserEndpoint(cfg *config) (int, EndpointDef) {
	if cfg.selector != nil {
		idx := cfg.selector()
		return idx, cfg.scenario.Endpoints[idx]
	}
	return -1, EndpointDef{Method: cfg.method, URL: cfg.url, Body: cfg.body}
}

// buildFetchScript returns a self-invoking async JS expression that executes
// one HTTP request and returns {latency, status, error} as a plain object.
// chromedp deserialises the resolved value via CDP returnByValue.
func buildFetchScript(ep EndpointDef, globalHeaders http.Header, timeoutMs int64) string {
	headers := make(map[string]string)
	for k, vals := range globalHeaders {
		if len(vals) > 0 {
			headers[k] = vals[0]
		}
	}
	for k, v := range ep.Headers {
		headers[k] = v
	}

	headersJSON, _ := json.Marshal(headers)

	bodyJS := ""
	if ep.Body != "" {
		bodyJSON, _ := json.Marshal(ep.Body)
		bodyJS = "opts.body=" + string(bodyJSON) + ";"
	}

	return fmt.Sprintf(`(async()=>{
const ctrl=new AbortController();
const t=setTimeout(()=>ctrl.abort(),	%d);
const t0=performance.now();
try{
  const opts={method:%q,headers:%s,signal:ctrl.signal};
  %s
  const r=await fetch(%q,opts);
  clearTimeout(t);
  const lat=performance.now()-t0;
  await r.text();
  return {latency:lat,status:r.status,error:''};
}catch(e){
  clearTimeout(t);
  return {latency:performance.now()-t0,status:0,
          error:e.name==='AbortError'?'timeout':(e.message||'network_error')};
}
})()`, timeoutMs, ep.Method, string(headersJSON), bodyJS, ep.URL)
}

func browserThinkDuration(cfg *config) time.Duration {
	if cfg.thinkMax > cfg.thinkMin {
		delta := int64(cfg.thinkMax - cfg.thinkMin)
		return cfg.thinkMin + time.Duration(rand.Int64N(delta))
	}
	return cfg.thinkMin
}

func browserThinkDesc(cfg *config) string {
	if cfg.thinkMax > cfg.thinkMin {
		return fmt.Sprintf("%s–%s", cfg.thinkMin, cfg.thinkMax)
	}
	if cfg.thinkMin == 0 {
		return "none"
	}
	return cfg.thinkMin.String()
}
