package main

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
	Elapsed      int     `json:"elapsed,omitempty"`
	SuccessDelta int64   `json:"successDelta,omitempty"`
	ErrorDelta   int64   `json:"errorDelta,omitempty"`
	P50ms        float64 `json:"p50ms,omitempty"`
	P95ms        float64 `json:"p95ms,omitempty"`
	P99ms        float64 `json:"p99ms,omitempty"`
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
  *{box-sizing:border-box;margin:0;padding:0}
  body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;
       background:#0d1117;color:#c9d1d9;min-height:100vh}
  .container{max-width:1200px;margin:0 auto;padding:1.5rem}
  header{margin-bottom:1.5rem;border-bottom:1px solid #21262d;padding-bottom:1.25rem}
  header h1{font-size:1.8rem;color:#00d4aa}
  .meta{color:#8b949e;font-size:.82rem;font-family:monospace;margin-top:.25rem}
  .bar-bg{background:#21262d;border-radius:4px;height:5px;margin-top:.9rem;overflow:hidden}
  .bar{background:#00d4aa;height:100%%;width:0%%;transition:width 1s linear}
  .badge{display:inline-block;margin-left:.75rem;font-size:.7rem;padding:.15rem .55rem;
         border-radius:99px;vertical-align:middle;font-weight:600}
  .running{background:#1a3a1a;color:#3fb950}
  .done{background:#1a2a3a;color:#58a6ff}

  .report-btn{display:none;margin-top:1rem;padding:.5rem 1.1rem;background:#00d4aa;
              color:#0d1117;border:none;border-radius:6px;font-weight:700;
              font-size:.9rem;cursor:pointer;text-decoration:none}
  .report-btn:hover{background:#00b894}

  .cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));
         gap:1rem;margin-bottom:1.5rem}
  .card{background:#161b22;border:1px solid #21262d;border-radius:8px;padding:1.1rem}
  .card-label{font-size:.68rem;color:#8b949e;text-transform:uppercase;letter-spacing:.8px}
  .card-value{font-size:1.55rem;font-weight:700;color:#e6edf3;margin-top:.35rem;
              font-variant-numeric:tabular-nums}
  .green{color:#3fb950}.yellow{color:#d29922}.red{color:#f85149}

  .charts{display:grid;grid-template-columns:1fr 1fr;gap:1.25rem}
  @media(max-width:700px){.charts{grid-template-columns:1fr}}
  .chart-box{background:#161b22;border:1px solid #21262d;border-radius:8px;padding:1.1rem}
  .chart-box h2{font-size:.68rem;color:#8b949e;text-transform:uppercase;
                letter-spacing:.8px;margin-bottom:.9rem}
  footer{text-align:center;color:#30363d;font-size:.72rem;padding:1.5rem 0 .5rem}
</style>
</head>
<body>
<div class="container">
  <header>
    <h1>&#9889; Orion <span class="badge running" id="badge">running</span></h1>
    <p class="meta" id="metaLine"></p>
    <div class="bar-bg"><div class="bar" id="bar"></div></div>
    <a class="report-btn" id="reportBtn" target="_blank">View HTML Report &#8599;</a>
  </header>

  <div class="cards">
    <div class="card">
      <div class="card-label">Total Requests</div>
      <div class="card-value" id="cTotal">0</div>
    </div>
    <div class="card">
      <div class="card-label">Success Rate</div>
      <div class="card-value green" id="cRate">—</div>
    </div>
    <div class="card">
      <div class="card-label">RPS (last sec)</div>
      <div class="card-value" id="cRPS">0</div>
    </div>
    <div class="card">
      <div class="card-label">P99 (last sec)</div>
      <div class="card-value yellow" id="cP99">—</div>
    </div>
  </div>

  <div class="charts">
    <div class="chart-box"><h2>Throughput (req/s)</h2><canvas id="rpsChart"></canvas></div>
    <div class="chart-box"><h2>Latency Percentiles (ms)</h2><canvas id="latChart"></canvas></div>
  </div>

  <footer>Orion Live Dashboard &nbsp;·&nbsp; port %d</footer>
</div>

<script>
const META = %s;
const MODE = %s;
const DUR  = %d;

document.getElementById('metaLine').textContent = META + '  ·  ' + MODE + '  ·  ' + DUR + 's';

const grid='rgba(48,54,61,0.7)', tick='#8b949e';
const baseScales = {
  x:{ticks:{color:tick,maxTicksLimit:10},grid:{color:grid}},
  y:{beginAtZero:true,ticks:{color:tick},grid:{color:grid}},
};
const baseOpts = {
  responsive:true, animation:false,
  interaction:{mode:'index',intersect:false},
  plugins:{legend:{labels:{color:tick,boxWidth:10,padding:12}}},
  scales:baseScales,
};

const rpsChart = new Chart(document.getElementById('rpsChart'), {
  type:'line',
  data:{labels:[],datasets:[
    {label:'success/s',data:[],borderColor:'#3fb950',
     backgroundColor:'rgba(63,185,80,.1)',fill:true,tension:.35,pointRadius:2,borderWidth:2},
    {label:'errors/s', data:[],borderColor:'#f85149',
     backgroundColor:'rgba(248,81,73,.1)', fill:true,tension:.35,pointRadius:2,borderWidth:2},
  ]},
  options: baseOpts,
});

const latChart = new Chart(document.getElementById('latChart'), {
  type:'line',
  data:{labels:[],datasets:[
    {label:'p50',data:[],borderColor:'#4ecdc4',tension:.35,pointRadius:2,borderWidth:2},
    {label:'p95',data:[],borderColor:'#f7dc6f',tension:.35,pointRadius:2,borderWidth:2},
    {label:'p99',data:[],borderColor:'#f85149',tension:.35,pointRadius:2,borderWidth:2},
  ]},
  options:{...baseOpts, scales:{...baseScales,
    y:{...baseScales.y,title:{display:true,text:'ms',color:tick}}}},
});

let totalReqs=0, totalSuccess=0;

function fmt(n){return n>=1000?(n/1000).toFixed(1)+'k':String(n)}

const es = new EventSource('/events');
es.onmessage = function(e) {
  const d = JSON.parse(e.data);

  if (d.done) {
    document.getElementById('badge').textContent = 'done';
    document.getElementById('badge').className   = 'badge done';
    document.getElementById('bar').style.width   = '100%%';
    return;
  }

  if (d.reportURL) {
    const btn = document.getElementById('reportBtn');
    btn.href  = d.reportURL;
    btn.style.display = 'inline-block';
    es.close();
    return;
  }

  totalReqs    += d.successDelta + d.errorDelta;
  totalSuccess += d.successDelta;

  document.getElementById('cTotal').textContent = fmt(totalReqs);
  document.getElementById('cRPS').textContent   = d.successDelta + d.errorDelta;
  document.getElementById('cP99').textContent   = d.p99ms > 0 ? d.p99ms.toFixed(1)+' ms' : '—';

  const pct = totalReqs > 0 ? totalSuccess/totalReqs*100 : 100;
  const rateEl = document.getElementById('cRate');
  rateEl.textContent  = pct.toFixed(1)+'%%';
  rateEl.className    = 'card-value '+(pct>=99?'green':pct>=95?'yellow':'red');

  const lbl = d.elapsed+'s';
  rpsChart.data.labels.push(lbl);
  rpsChart.data.datasets[0].data.push(d.successDelta);
  rpsChart.data.datasets[1].data.push(d.errorDelta);
  rpsChart.update('none');

  latChart.data.labels.push(lbl);
  latChart.data.datasets[0].data.push(d.p50ms||null);
  latChart.data.datasets[1].data.push(d.p95ms||null);
  latChart.data.datasets[2].data.push(d.p99ms||null);
  latChart.update('none');

  document.getElementById('bar').style.width =
    Math.min(100, d.elapsed/DUR*100).toFixed(1)+'%%';
};
es.onerror = function(){ es.close(); };
</script>
</body>
</html>`, port, metaJSON, modeJSON, durationSec)
}
