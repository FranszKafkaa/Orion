package orion

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os/exec"
	"runtime"
	"sync"
	"time"
)

// dashUpdate is the JSON payload pushed to the browser via SSE.
type dashUpdate struct {
	Elapsed      int     `json:"elapsed"`
	SuccessDelta int64   `json:"successDelta"`
	ErrorDelta   int64   `json:"errorDelta"`
	P50ms        float64 `json:"p50ms"`
	P95ms        float64 `json:"p95ms"`
	P99ms        float64 `json:"p99ms"`
	Done         bool    `json:"done,omitempty"`
	ReportURL    string  `json:"reportURL,omitempty"`
}

type dashHub struct {
	mu         sync.Mutex
	history    []dashUpdate
	clients    map[chan dashUpdate]struct{}
	reportPath string
	port       int
	srv        *http.Server
}

func newDashHub(port int) *dashHub {
	return &dashHub{
		clients: make(map[chan dashUpdate]struct{}),
		port:    port,
	}
}

func (h *dashHub) push(snap snapshot) {
	upd := dashUpdate{
		Elapsed:      snap.elapsed,
		SuccessDelta: snap.successCnt,
		ErrorDelta:   snap.errorCnt,
		P50ms:        snap.p50ms,
		P95ms:        snap.p95ms,
		P99ms:        snap.p99ms,
	}
	h.mu.Lock()
	h.history = append(h.history, upd)
	for ch := range h.clients {
		select {
		case ch <- upd:
		default:
		}
	}
	h.mu.Unlock()
}

// sendDone signals the end of injection. The SSE connection stays open so
// the browser can still receive the report URL when it is ready.
func (h *dashHub) sendDone() {
	h.broadcast(dashUpdate{Done: true})
}

// SetReportPath stores the generated HTML report path, broadcasts its URL to
// all connected clients, and keeps the server alive for 2 minutes so the user
// has time to open the report.
func (h *dashHub) setReportPath(absPath string) {
	if absPath == "" {
		return
	}
	reportURL := fmt.Sprintf("http://localhost:%d/report", h.port)
	h.mu.Lock()
	h.reportPath = absPath
	h.mu.Unlock()
	h.broadcast(dashUpdate{ReportURL: reportURL})

	fmt.Printf("[Orion] dashboard open for 2 min  → http://localhost:%d/\n", h.port)
	go func() {
		time.Sleep(2 * time.Minute)
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		h.srv.Shutdown(shutCtx)
	}()
}

func (h *dashHub) broadcast(upd dashUpdate) {
	h.mu.Lock()
	for ch := range h.clients {
		select {
		case ch <- upd:
		default:
		}
	}
	h.mu.Unlock()
}

// subscribe returns a channel pre-loaded with all historic updates so late
// browser connections catch up immediately.
func (h *dashHub) subscribe() chan dashUpdate {
	ch := make(chan dashUpdate, 256)
	h.mu.Lock()
	for _, u := range h.history {
		ch <- u
	}
	h.clients[ch] = struct{}{}
	h.mu.Unlock()
	return ch
}

func (h *dashHub) unsubscribe(ch chan dashUpdate) {
	h.mu.Lock()
	delete(h.clients, ch)
	h.mu.Unlock()
}

// startDashboard wires the hub into the collector, starts the HTTP server, and
// opens one real browser window. It returns immediately; the caller must call
// hub.sendDone() and hub.setReportPath() at the appropriate times.
func startDashboard(cfg *config, col *collector, ctx context.Context) *dashHub {
	hub := newDashHub(cfg.dashboardPort)
	col.dashboardFn = hub.push

	meta := cfg.method + " " + cfg.url
	if cfg.scenario != nil {
		meta = fmt.Sprintf("Scenario (%d endpoints)", len(cfg.scenario.Endpoints))
	}
	mode := fmt.Sprintf("%d req/s", cfg.rps)
	if cfg.browser {
		mode = fmt.Sprintf("%d users", cfg.browserN)
	}
	page := buildDashPage(meta, mode, int64(cfg.dur.Seconds()), cfg.dashboardPort)

	mux := http.NewServeMux()

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write([]byte(page))
	})

	mux.HandleFunc("/report", func(w http.ResponseWriter, r *http.Request) {
		hub.mu.Lock()
		path := hub.reportPath
		hub.mu.Unlock()
		if path == "" {
			http.Error(w, "report not ready yet", http.StatusServiceUnavailable)
			return
		}
		http.ServeFile(w, r, path)
	})

	mux.HandleFunc("/events", func(w http.ResponseWriter, r *http.Request) {
		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "streaming unsupported", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")

		ch := hub.subscribe()
		defer hub.unsubscribe(ch)

		for {
			select {
			case upd := <-ch:
				data, _ := json.Marshal(upd)
				fmt.Fprintf(w, "data: %s\n\n", data)
				flusher.Flush()
				if upd.ReportURL != "" {
					return // report delivered — close stream
				}
			case <-r.Context().Done():
				return
			}
		}
	})

	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.dashboardPort))
	if err != nil {
		fmt.Printf("[Orion] dashboard: cannot bind to :%d: %v\n", cfg.dashboardPort, err)
		return hub
	}

	hub.srv = &http.Server{Handler: mux}
	go hub.srv.Serve(ln)

	dashURL := fmt.Sprintf("http://localhost:%d/", cfg.dashboardPort)
	fmt.Printf("[Orion] dashboard  → %s\n", dashURL)
	openURL(dashURL)

	// Send done signal when the test context expires.
	go func() {
		<-ctx.Done()
		hub.sendDone()
	}()

	return hub
}

func openURL(url string) {
	switch runtime.GOOS {
	case "darwin":
		exec.Command("open", url).Start()
	case "linux":
		exec.Command("xdg-open", url).Start()
	case "windows":
		exec.Command("cmd", "/c", "start", url).Start()
	}
}

// ── Dashboard HTML ─────────────────────────────────────────────────────────────

func buildDashPage(meta, mode string, durationSec int64, port int) string {
	if durationSec < 1 {
		durationSec = 1
	}
	metaJSON, _ := json.Marshal(meta)
	modeJSON, _ := json.Marshal(mode)
	return fmt.Sprintf(`<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Orion — Live Dashboard</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }

  body {
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
    background: #0d1117;
    color: #c9d1d9;
    min-height: 100vh;
  }

  .container { max-width: 1280px; margin: 0 auto; padding: 2rem 1.5rem; }

  /* ── Header ── */
  header {
    display: flex;
    justify-content: space-between;
    align-items: flex-end;
    gap: 1.5rem;
    margin-bottom: 1.5rem;
    border-bottom: 1px solid #21262d;
    padding-bottom: 1.5rem;
  }
  header h1    { font-size: 2rem; color: #00d4aa; letter-spacing: -0.5px; }
  .subtitle    { color: #8b949e; margin-top: .25rem; font-size: .9rem; }
  .meta        { color: #8b949e; font-size: .8rem; margin-top: .5rem; font-family: monospace; word-break: break-all; }

  /* ── Status / badge ── */
  .status { display: flex; align-items: center; gap: .75rem; flex-wrap: wrap; justify-content: flex-end; }
  .badge {
    display: inline-flex; align-items: center; gap: .4rem;
    font-size: .72rem; padding: .22rem .65rem; border-radius: 99px;
    font-weight: 700; text-transform: uppercase; letter-spacing: .5px;
  }
  .badge::before { content: ''; width: .46rem; height: .46rem; border-radius: 50%%; background: currentColor; }
  .running { background: #1a3a1a; color: #3fb950; }
  .done    { background: #1a2a3a; color: #58a6ff; }
  .lost    { background: #3a231f; color: #f85149; }
  .report-btn {
    display: none; padding: .55rem .9rem; background: #00d4aa; color: #0d1117;
    border: none; border-radius: 6px; font-weight: 700; font-size: .85rem;
    cursor: pointer; text-decoration: none;
  }
  .report-btn:hover { background: #00b894; }

  /* ── Progress bar ── */
  .bar-bg { background: #21262d; border-radius: 99px; height: 6px; margin-bottom: 2rem; overflow: hidden; }
  .bar    { background: #00d4aa; height: 100%%; width: 0%%; transition: width .35s ease; }

  /* ── Summary cards ── */
  .cards {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
    gap: 1rem;
    margin-bottom: 2rem;
  }
  .card              { background: #161b22; border: 1px solid #21262d; border-radius: 8px; padding: 1.25rem; }
  .card-label        { font-size: .7rem; color: #8b949e; text-transform: uppercase; letter-spacing: .8px; }
  .card-value        { font-size: 1.65rem; font-weight: 700; color: #e6edf3; margin-top: .4rem; font-variant-numeric: tabular-nums; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
  .card-value.green  { color: #3fb950; }
  .card-value.yellow { color: #d29922; }
  .card-value.red    { color: #f85149; }

  /* ── Layout: charts + sidebar ── */
  .layout { display: grid; grid-template-columns: minmax(0, 1fr) 300px; gap: 1.5rem; align-items: start; }
  .charts-col { display: grid; gap: 1.25rem; }

  /* ── Shared panel (matches report) ── */
  .chart-box, .section {
    background: #161b22;
    border: 1px solid #21262d;
    border-radius: 8px;
    padding: 1.25rem;
  }
  .chart-head { display: flex; justify-content: space-between; align-items: center; margin-bottom: 1rem; }
  .chart-head h2, .section > h2 {
    font-size: .7rem; color: #8b949e; text-transform: uppercase; letter-spacing: .8px;
  }
  .chart-head h2 { margin-bottom: 0; }
  .section > h2  { margin-bottom: 1rem; }
  .hint { font-size: .78rem; color: #8b949e; font-family: monospace; }

  /* ── Tables (matches report) ── */
  table { width: 100%%; border-collapse: collapse; }
  th, td { padding: .55rem .75rem; text-align: left; border-bottom: 1px solid #21262d; font-size: .85rem; }
  th { color: #8b949e; font-weight: 500; }
  td { font-family: monospace; color: #e6edf3; }
  td.num { text-align: right; }
  tr:last-child th, tr:last-child td { border-bottom: none; }

  canvas { max-height: 310px; }

  /* ── Sidebar ── */
  .side { position: sticky; top: 1rem; display: grid; gap: 1rem; }
  .kv { display: grid; gap: .75rem; }
  .kv-row { border-bottom: 1px solid #21262d; padding-bottom: .75rem; }
  .kv-row:last-child { border-bottom: 0; padding-bottom: 0; }
  .kv-label { font-size: .68rem; color: #8b949e; text-transform: uppercase; letter-spacing: .7px; }
  .kv-value { font-size: .85rem; color: #e6edf3; font-family: monospace; margin-top: .25rem; word-break: break-all; }
  .stream-row {
    display: flex; align-items: center; gap: .5rem;
    margin-top: .9rem; padding-top: .9rem; border-top: 1px solid #21262d;
    font-size: .78rem; color: #8b949e;
  }
  .dot { width: .45rem; height: .45rem; border-radius: 50%%; display: inline-block; flex-shrink: 0; }
  .dot-green { background: #3fb950; }
  .dot-red   { background: #f85149; }
  .dot-grey  { background: #8b949e; }

  footer { text-align: center; color: #30363d; font-size: .75rem; padding: 2rem 0 1rem; }

  @media (max-width: 1050px) { .layout { grid-template-columns: 1fr; } .side { position: static; } .cards { grid-template-columns: repeat(2, minmax(0, 1fr)); } }
  @media (max-width: 700px)  { header { display: block; } .status { justify-content: flex-start; margin-top: 1rem; } .cards { grid-template-columns: 1fr; } .container { padding: 1.5rem 1rem; } }
</style>
</head>
<body>
<div class="container">

  <header>
    <div>
      <h1>⚡ Orion</h1>
      <p class="subtitle">Live Dashboard</p>
      <p class="meta" id="metaLine"></p>
    </div>
    <div class="status">
      <span class="badge running" id="badge">running</span>
      <a class="report-btn" id="reportBtn" target="_blank">View HTML Report</a>
    </div>
  </header>

  <div class="bar-bg"><div class="bar" id="bar"></div></div>

  <div class="cards">
    <div class="card"><div class="card-label">Total Requests</div><div class="card-value" id="cTotal">0</div></div>
    <div class="card"><div class="card-label">Success Rate</div><div class="card-value green" id="cRate">100.0%%</div></div>
    <div class="card"><div class="card-label">Current RPS</div><div class="card-value" id="cRPS">0</div></div>
    <div class="card"><div class="card-label">Avg RPS</div><div class="card-value" id="cAvgRPS">0</div></div>
    <div class="card"><div class="card-label">Errors</div><div class="card-value" id="cErrors">0</div></div>
    <div class="card"><div class="card-label">p95 Latency</div><div class="card-value yellow" id="cP95">—</div></div>
    <div class="card"><div class="card-label">p99 Latency</div><div class="card-value yellow" id="cP99">—</div></div>
    <div class="card"><div class="card-label">Elapsed</div><div class="card-value" id="cElapsed">0s</div></div>
  </div>

  <div class="layout">
    <div class="charts-col">
      <div class="chart-box">
        <div class="chart-head">
          <h2>Throughput (req/s)</h2>
          <span class="hint" id="lastTick">waiting…</span>
        </div>
        <canvas id="rpsChart"></canvas>
      </div>
      <div class="chart-box">
        <div class="chart-head">
          <h2>Latency Percentiles (ms)</h2>
          <span class="hint">per-second window</span>
        </div>
        <canvas id="latChart"></canvas>
      </div>
    </div>
    <aside class="side">
      <section class="section">
        <h2>Test</h2>
        <div class="kv">
          <div class="kv-row"><div class="kv-label">Target</div><div class="kv-value" id="targetLine"></div></div>
          <div class="kv-row"><div class="kv-label">Mode</div><div class="kv-value" id="modeLine"></div></div>
          <div class="kv-row"><div class="kv-label">Duration</div><div class="kv-value" id="durationLine"></div></div>
          <div class="kv-row"><div class="kv-label">Report</div><div class="kv-value" id="reportState">pending</div></div>
        </div>
        <div class="stream-row">
          <span class="dot dot-green" id="streamDot"></span>
          <span id="streamState">stream connected</span>
        </div>
      </section>
      <section class="section">
        <h2>Latency Distribution</h2>
        <table>
          <thead>
            <tr><th>Percentile</th><th style="text-align:right">Latency</th></tr>
          </thead>
          <tbody>
            <tr><th>p50</th><td class="num" id="tP50">—</td></tr>
            <tr><th>p95</th><td class="num" id="tP95">—</td></tr>
            <tr><th>p99</th><td class="num" id="tP99">—</td></tr>
          </tbody>
        </table>
      </section>
    </aside>
  </div>

  <footer>Orion Live Dashboard &nbsp;·&nbsp; port %d</footer>

</div>

<script>
const META = %s;
const MODE = %s;
const DUR  = Math.max(1, Number(%d) || 1);

const el = id => document.getElementById(id);
el('metaLine').textContent  = META + '  ·  ' + MODE + '  ·  ' + DUR + 's';
el('targetLine').textContent = META;
el('modeLine').textContent   = MODE;
el('durationLine').textContent = DUR + 's';

const grid = 'rgba(48,54,61,0.7)', tick = '#8b949e';
const baseScales = {
  x: { ticks: { color: tick, maxTicksLimit: 10 }, grid: { color: grid } },
  y: { beginAtZero: true, ticks: { color: tick }, grid: { color: grid } },
};
const baseOpts = {
  responsive: true,
  animation: false,
  interaction: { mode: 'index', intersect: false },
  plugins: { legend: { labels: { color: tick, boxWidth: 10, padding: 12 } } },
  scales: baseScales,
  maintainAspectRatio: false,
};

const rpsChart = new Chart(document.getElementById('rpsChart'), {
  type: 'line',
  data: { labels: [], datasets: [
    { label: 'success/s', data: [], borderColor: '#3fb950', backgroundColor: 'rgba(63,185,80,.12)', fill: true, tension: .35, pointRadius: 2, borderWidth: 2 },
    { label: 'errors/s',  data: [], borderColor: '#f85149', backgroundColor: 'rgba(248,81,73,.12)', fill: true, tension: .35, pointRadius: 2, borderWidth: 2 },
  ]},
  options: baseOpts,
});

const latChart = new Chart(document.getElementById('latChart'), {
  type: 'line',
  data: { labels: [], datasets: [
    { label: 'p50', data: [], borderColor: '#4ecdc4', tension: .35, pointRadius: 2, borderWidth: 2 },
    { label: 'p95', data: [], borderColor: '#f7dc6f', tension: .35, pointRadius: 2, borderWidth: 2 },
    { label: 'p99', data: [], borderColor: '#f85149', tension: .35, pointRadius: 2, borderWidth: 2 },
  ]},
  options: { ...baseOpts, scales: { ...baseScales,
    y: { ...baseScales.y, title: { display: true, text: 'ms', color: tick } } } },
});

let totalReqs = 0, totalSuccess = 0, totalErrors = 0;

function num(v) { const n = Number(v); return Number.isFinite(n) ? n : 0; }
function fmt(n) { n = num(n); return n >= 1000000 ? (n/1000000).toFixed(1)+'m' : n >= 1000 ? (n/1000).toFixed(1)+'k' : String(Math.round(n)); }
function ms(v)  { v = num(v); return v > 0 ? v.toFixed(2)+' ms' : '—'; }
function pushLimited(chart, label, values) {
  chart.data.labels.push(label);
  values.forEach((v, i) => chart.data.datasets[i].data.push(v));
  if (chart.data.labels.length > 90) {
    chart.data.labels.shift();
    chart.data.datasets.forEach(ds => ds.data.shift());
  }
  chart.update('none');
}

const es = new EventSource('/events');
es.onmessage = function(e) {
  const d = JSON.parse(e.data);

  if (d.done) {
    el('badge').textContent    = 'done';
    el('badge').className      = 'badge done';
    el('bar').style.width      = '100%%';
    el('streamState').textContent = 'stream complete';
    el('streamDot').className  = 'dot dot-grey';
    return;
  }

  if (d.reportURL) {
    const btn = el('reportBtn');
    btn.href          = d.reportURL;
    btn.style.display = 'inline-block';
    el('reportState').textContent = 'ready';
    es.close();
    return;
  }

  const success = num(d.successDelta);
  const errors  = num(d.errorDelta);
  const elapsed = Math.max(0, num(d.elapsed));
  const rps     = success + errors;

  totalReqs    += rps;
  totalSuccess += success;
  totalErrors  += errors;

  el('lastTick').textContent   = elapsed + 's';
  el('cTotal').textContent     = fmt(totalReqs);
  el('cRPS').textContent       = fmt(rps);
  el('cAvgRPS').textContent    = elapsed > 0 ? (totalReqs / elapsed).toFixed(1) : '0';
  el('cErrors').textContent    = fmt(totalErrors);
  el('cP95').textContent       = ms(d.p95ms);
  el('cP99').textContent       = ms(d.p99ms);
  el('cElapsed').textContent   = elapsed + 's';
  el('tP50').textContent       = ms(d.p50ms);
  el('tP95').textContent       = ms(d.p95ms);
  el('tP99').textContent       = ms(d.p99ms);

  const pct    = totalReqs > 0 ? totalSuccess / totalReqs * 100 : 100;
  const rateEl = el('cRate');
  rateEl.textContent = pct.toFixed(1) + '%%';
  rateEl.className   = 'card-value ' + (pct >= 99 ? 'green' : pct >= 95 ? 'yellow' : 'red');

  const lbl = elapsed + 's';
  pushLimited(rpsChart, lbl, [success, errors]);
  pushLimited(latChart, lbl, [num(d.p50ms) || null, num(d.p95ms) || null, num(d.p99ms) || null]);

  el('bar').style.width = Math.min(100, elapsed / DUR * 100).toFixed(1) + '%%';
};
es.onerror = function() {
  el('streamState').textContent = 'stream disconnected';
  el('streamDot').className     = 'dot dot-red';
  el('badge').textContent       = 'offline';
  el('badge').className         = 'badge lost';
  es.close();
};
</script>
</body>
</html>`, port, metaJSON, modeJSON, durationSec)
}
