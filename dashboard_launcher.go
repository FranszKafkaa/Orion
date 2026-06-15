package main

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

type launcherServer struct {
	mu     sync.Mutex
	tests  []launchedTest
	nextID int
}

type launchedTest struct {
	ID        int       `json:"id"`
	StartedAt time.Time `json:"startedAt"`
	Command   string    `json:"command"`
	LiveURL   string    `json:"liveURL"`
	PID       int       `json:"pid"`
	Status    string    `json:"status"`
}

type launchRequest struct {
	Name         string                  `json:"name"`
	RPS          int                     `json:"rps"`
	Duration     string                  `json:"duration"`
	RampUp       string                  `json:"rampUp"`
	Timeout      string                  `json:"timeout"`
	MaxErrorRate float64                 `json:"maxErrorRate"`
	Browser      bool                    `json:"browser"`
	Users        int                     `json:"users"`
	ThinkTime    string                  `json:"thinkTime"`
	ThinkMax     string                  `json:"thinkMax"`
	Token        string                  `json:"token"`
	Basic        string                  `json:"basic"`
	Headers      map[string]string       `json:"headers"`
	Endpoints    []launcherEndpointInput `json:"endpoints"`
}

type launcherEndpointInput struct {
	Weight  int               `json:"weight"`
	Method  string            `json:"method"`
	URL     string            `json:"url"`
	Body    string            `json:"body"`
	Headers map[string]string `json:"headers"`
}

func startDashboardLauncher(port int) {
	ls := &launcherServer{nextID: 1}
	mux := http.NewServeMux()

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write([]byte(buildLauncherPage(port)))
	})

	mux.HandleFunc("/api/tests", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			ls.writeTests(w)
		case http.MethodPost:
			ls.launch(w, r)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})

	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		fmt.Printf("[Orion] dashboard: cannot bind to :%d: %v\n", port, err)
		return
	}

	dashURL := fmt.Sprintf("http://localhost:%d/", port)
	fmt.Printf("[Orion] dashboard launcher  → %s\n", dashURL)
	openURL(dashURL)

	if err := http.Serve(ln, mux); err != nil && err != http.ErrServerClosed {
		fmt.Printf("[Orion] dashboard launcher stopped: %v\n", err)
	}
}

func (ls *launcherServer) writeTests(w http.ResponseWriter) {
	ls.mu.Lock()
	tests := append([]launchedTest(nil), ls.tests...)
	ls.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(tests)
}

func (ls *launcherServer) launch(w http.ResponseWriter, r *http.Request) {
	var req launchRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}

	args, livePort, cleanup, err := buildLaunchArgs(req)
	if cleanup != nil {
		defer cleanup()
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	exe, err := os.Executable()
	if err != nil {
		http.Error(w, "cannot locate Orion executable", http.StatusInternalServerError)
		return
	}

	cmd := exec.Command(exe, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		http.Error(w, "cannot start test: "+err.Error(), http.StatusInternalServerError)
		return
	}

	liveURL := fmt.Sprintf("http://localhost:%d/", livePort)
	launched := launchedTest{
		StartedAt: time.Now(),
		Command:   shellJoin(append([]string{filepath.Base(exe)}, args...)),
		LiveURL:   liveURL,
		PID:       cmd.Process.Pid,
		Status:    "running",
	}

	ls.mu.Lock()
	launched.ID = ls.nextID
	ls.nextID++
	ls.tests = append([]launchedTest{launched}, ls.tests...)
	ls.mu.Unlock()

	go func(id int) {
		_ = cmd.Wait()
		ls.mu.Lock()
		defer ls.mu.Unlock()
		for i := range ls.tests {
			if ls.tests[i].ID == id {
				ls.tests[i].Status = "finished"
				return
			}
		}
	}(launched.ID)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(launched)
}

func buildLaunchArgs(req launchRequest) ([]string, int, func(), error) {
	if req.RPS <= 0 {
		req.RPS = 100
	}
	if req.Duration == "" {
		req.Duration = "30s"
	}
	if req.Timeout == "" {
		req.Timeout = "5s"
	}
	if req.Users <= 0 {
		req.Users = 10
	}
	if len(req.Endpoints) == 0 {
		return nil, 0, nil, fmt.Errorf("add at least one endpoint")
	}

	for i := range req.Endpoints {
		ep := &req.Endpoints[i]
		ep.Method = strings.ToUpper(strings.TrimSpace(ep.Method))
		if ep.Method == "" {
			ep.Method = http.MethodGet
		}
		if !validHTTPMethod(ep.Method) {
			return nil, 0, nil, fmt.Errorf("endpoint %d has unsupported method %q", i+1, ep.Method)
		}
		if ep.Weight <= 0 {
			ep.Weight = 1
		}
		if _, err := url.ParseRequestURI(ep.URL); err != nil || !strings.HasPrefix(ep.URL, "http") {
			return nil, 0, nil, fmt.Errorf("endpoint %d has an invalid URL", i+1)
		}
	}

	livePort, err := freeTCPPort()
	if err != nil {
		return nil, 0, nil, fmt.Errorf("cannot allocate dashboard port: %w", err)
	}

	args := []string{
		"-dashboard",
		"-dashboard-port", strconv.Itoa(livePort),
		"-rps", strconv.Itoa(req.RPS),
		"-duration", req.Duration,
		"-timeout", req.Timeout,
	}
	if req.RampUp != "" {
		args = append(args, "-ramp-up", req.RampUp)
	}
	if req.MaxErrorRate > 0 {
		args = append(args, "-max-error-rate", strconv.FormatFloat(req.MaxErrorRate, 'f', -1, 64))
	}
	if req.Browser {
		args = append(args, "-browser", "-users", strconv.Itoa(req.Users))
		if req.ThinkTime != "" {
			args = append(args, "-think-time", req.ThinkTime)
		}
		if req.ThinkMax != "" {
			args = append(args, "-think-max", req.ThinkMax)
		}
	}
	if req.Token != "" {
		args = append(args, "-token", req.Token)
	}
	if req.Basic != "" {
		args = append(args, "-basic", req.Basic)
	}
	for k, v := range req.Headers {
		if strings.TrimSpace(k) == "" {
			continue
		}
		args = append(args, "-H", strings.TrimSpace(k)+": "+strings.TrimSpace(v))
	}

	if len(req.Endpoints) == 1 {
		ep := req.Endpoints[0]
		args = append(args, "-url", ep.URL, "-method", ep.Method)
		if ep.Body != "" {
			args = append(args, "-body", ep.Body)
		}
		for k, v := range ep.Headers {
			if strings.TrimSpace(k) == "" {
				continue
			}
			args = append(args, "-H", strings.TrimSpace(k)+": "+strings.TrimSpace(v))
		}
		return args, livePort, nil, nil
	}

	sc := Scenario{Endpoints: make([]EndpointDef, 0, len(req.Endpoints))}
	for _, ep := range req.Endpoints {
		sc.Endpoints = append(sc.Endpoints, EndpointDef{
			Weight:  ep.Weight,
			Method:  ep.Method,
			URL:     ep.URL,
			Body:    ep.Body,
			Headers: ep.Headers,
		})
	}
	data, err := json.MarshalIndent(sc, "", "  ")
	if err != nil {
		return nil, 0, nil, err
	}
	f, err := os.CreateTemp("", "orion-scenario-*.json")
	if err != nil {
		return nil, 0, nil, fmt.Errorf("cannot create scenario file: %w", err)
	}
	path := f.Name()
	if _, err := f.Write(data); err != nil {
		f.Close()
		os.Remove(path)
		return nil, 0, nil, fmt.Errorf("cannot write scenario file: %w", err)
	}
	if err := f.Close(); err != nil {
		os.Remove(path)
		return nil, 0, nil, fmt.Errorf("cannot close scenario file: %w", err)
	}
	args = append(args, "-scenario", path)

	return args, livePort, func() {}, nil
}

func validHTTPMethod(method string) bool {
	switch method {
	case "GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS":
		return true
	default:
		return false
	}
}

func freeTCPPort() (int, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port, nil
}

func shellJoin(parts []string) string {
	quoted := make([]string, len(parts))
	for i, p := range parts {
		if p == "" || strings.ContainsAny(p, " \t\n'\"{}[]():;") {
			quoted[i] = strconv.Quote(p)
		} else {
			quoted[i] = p
		}
	}
	return strings.Join(quoted, " ")
}

func buildLauncherPage(port int) string {
	return fmt.Sprintf(`<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Orion Dashboard</title>
<style>
  *{box-sizing:border-box;margin:0;padding:0}
  body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;background:#0d1117;color:#c9d1d9;min-height:100vh}
  .container{max-width:1280px;margin:0 auto;padding:2rem 1.5rem}
  header{display:flex;align-items:flex-end;justify-content:space-between;gap:1.5rem;margin-bottom:1.5rem;border-bottom:1px solid #21262d;padding-bottom:1.5rem}
  header h1{font-size:2rem;color:#00d4aa;letter-spacing:0}
  .subtitle{color:#8b949e;margin-top:.25rem;font-size:.9rem}
  .meta{color:#8b949e;font-size:.8rem;margin-top:.5rem;font-family:monospace}
  .header-actions{display:flex;gap:.75rem;align-items:center;flex-wrap:wrap}
  .cards{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:1rem;margin-bottom:1.5rem}
  .card,.section,.endpoint{background:#161b22;border:1px solid #21262d;border-radius:8px}
  .card{padding:1rem}
  .card-label{font-size:.7rem;color:#8b949e;text-transform:uppercase;letter-spacing:.8px}
  .card-value{font-size:1.35rem;font-weight:700;color:#e6edf3;margin-top:.35rem;font-variant-numeric:tabular-nums;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
  .card-value.green{color:#3fb950}.card-value.yellow{color:#d29922}
  .layout{display:grid;grid-template-columns:minmax(0,1fr) 380px;gap:1.5rem;align-items:start}
  .section{padding:1.25rem;margin-bottom:1.5rem}
  .section-title{display:flex;align-items:center;justify-content:space-between;gap:1rem;margin-bottom:1rem}
  .section h2{font-size:.7rem;color:#8b949e;text-transform:uppercase;letter-spacing:.8px}
  .section-note{color:#8b949e;font-size:.78rem;font-family:monospace}
  .config-grid{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:.75rem}
  .field{min-width:0}
  label,.ck{display:block;font-size:.7rem;color:#8b949e;text-transform:uppercase;letter-spacing:.6px;margin-bottom:.35rem}
  input,select,textarea{width:100%%;background:#0d1117;border:1px solid #30363d;color:#e6edf3;border-radius:6px;padding:.6rem .7rem;font:inherit;font-size:.85rem}
  input:focus,select:focus,textarea:focus{outline:1px solid #00d4aa;border-color:#00d4aa}
  textarea{min-height:84px;resize:vertical;font-family:ui-monospace,SFMono-Regular,Menlo,monospace;line-height:1.45}
  .cv{font-family:monospace;color:#e6edf3;font-size:.85rem;margin-top:.2rem;word-break:break-all}
  .check-row{display:flex;align-items:center;justify-content:space-between;gap:1rem;border-top:1px solid #21262d;margin-top:1rem;padding-top:1rem}
  .check{display:flex;align-items:center;gap:.5rem;color:#e6edf3;font-size:.85rem}.check input{width:auto}
  .actions{display:flex;gap:.75rem;align-items:center;margin-top:1rem;flex-wrap:wrap}
  button,a.btn{border:1px solid #30363d;border-radius:6px;padding:.55rem .85rem;font-weight:700;cursor:pointer;text-decoration:none;font-size:.85rem;line-height:1.1}
  .primary{background:#00d4aa;border-color:#00d4aa;color:#0d1117}.primary:hover{background:#00b894}
  .secondary{background:#21262d;color:#e6edf3}.secondary:hover{background:#30363d}
  .danger{background:#25171a;color:#f85149;border-color:#4a242b}
  .endpoint{padding:1rem;margin-bottom:1rem}
  .endpoint-head{display:flex;justify-content:space-between;align-items:center;margin-bottom:.85rem;color:#8b949e;font-size:.8rem;font-family:monospace}
  .endpoint-title{display:flex;gap:.5rem;align-items:center}
  .method-chip{display:inline-flex;min-width:58px;justify-content:center;border:1px solid #30363d;border-radius:99px;padding:.12rem .45rem;color:#00d4aa;background:#0d1117;font-size:.7rem;font-weight:700}
  .endpoint-grid{display:grid;grid-template-columns:132px minmax(260px,1fr) 92px;gap:.75rem;margin-bottom:.75rem}
  .endpoint-body{display:grid;grid-template-columns:1fr 1fr;gap:.75rem}
  .side{position:sticky;top:1rem}
  .tests{display:flex;flex-direction:column;gap:1rem}
  .test-card{padding:1rem;background:#0d1117;border:1px solid #21262d;border-radius:8px}
  .test-card strong{display:block;font-size:.95rem;color:#e6edf3;margin-bottom:.35rem}.muted{color:#8b949e;font-size:.78rem;line-height:1.45;word-break:break-all}
  .pill{display:inline-block;border-radius:99px;padding:.15rem .55rem;font-size:.7rem;font-weight:700;text-transform:uppercase;letter-spacing:.45px;margin-bottom:.65rem}
  .pill-running{background:#1a3a1a;color:#3fb950}
  .pill-finished{background:#3a301a;color:#d29922}
  .command{background:#0d1117;border:1px solid #21262d;border-radius:8px;padding:.85rem;color:#e6edf3;font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-size:.78rem;line-height:1.5;word-break:break-all}
  .mini-list{display:grid;grid-template-columns:1fr 1fr;gap:.75rem;margin-top:1rem}
  .mini{border:1px solid #21262d;border-radius:8px;padding:.75rem;background:#0d1117}
  .mini .ck{margin-bottom:.2rem}.mini .cv{font-size:.82rem}
  .err{color:#f85149;font-size:.82rem}.ok{color:#3fb950;font-size:.82rem}
  footer{text-align:center;color:#30363d;font-size:.75rem;padding:2rem 0 1rem}
  @media(max-width:1050px){.layout{grid-template-columns:1fr}.side{position:static}.cards{grid-template-columns:repeat(2,minmax(0,1fr))}}
  @media(max-width:760px){header{display:block}.header-actions{margin-top:1rem}.cards,.config-grid,.endpoint-grid,.endpoint-body,.mini-list{grid-template-columns:1fr}.container{padding:1.5rem 1rem}}
</style>
</head>
<body>
<div class="container">
  <header>
    <div>
      <h1>Orion</h1>
      <p class="subtitle">Dashboard</p>
      <p class="meta">Create and run HTTP load tests &nbsp;·&nbsp; port %d</p>
    </div>
    <div class="header-actions">
      <button class="secondary" id="addEndpointTop" type="button">Adicionar endpoint</button>
      <button class="primary" id="startTop" type="button">Iniciar teste</button>
    </div>
  </header>

  <div class="cards">
    <div class="card">
      <div class="card-label">Target RPS</div>
      <div class="card-value" id="sumRps">100</div>
    </div>
    <div class="card">
      <div class="card-label">Duration</div>
      <div class="card-value yellow" id="sumDuration">30s</div>
    </div>
    <div class="card">
      <div class="card-label">Endpoints</div>
      <div class="card-value" id="sumEndpoints">1</div>
    </div>
    <div class="card">
      <div class="card-label">Mode</div>
      <div class="card-value green" id="sumMode">HTTP</div>
    </div>
  </div>

  <div class="layout">
    <main>
    <section class="section">
      <div class="section-title">
        <h2>Novo teste</h2>
        <div class="section-note" id="profileNote">open model</div>
      </div>
      <div class="config-grid">
        <div class="field"><label>RPS alvo</label><input id="rps" type="number" min="1" value="100"></div>
        <div class="field"><label>Duração</label><input id="duration" value="30s"></div>
        <div class="field"><label>Ramp-up</label><input id="rampUp" placeholder="ex: 10s"></div>
        <div class="field"><label>Timeout</label><input id="timeout" value="5s"></div>
        <div class="field"><label>Erro máximo %%</label><input id="maxErrorRate" type="number" min="0" max="99" step="0.1" placeholder="0"></div>
        <div class="field"><label>Bearer token</label><input id="token" autocomplete="off"></div>
        <div class="field"><label>Basic auth</label><input id="basic" placeholder="usuario:senha" autocomplete="off"></div>
        <div class="field"><label>Headers globais</label><input id="headers" placeholder="X-App: Orion; X-Tenant: dev"></div>
      </div>
      <div class="check-row">
        <label class="check"><input id="browserMode" type="checkbox"> Executar em modo browser/headless</label>
        <span class="section-note">real browser VUs</span>
      </div>
      <div class="config-grid" id="browserFields" style="display:none;margin-top:.75rem">
        <div class="field"><label>Usuários browser</label><input id="users" type="number" min="1" value="10"></div>
        <div class="field"><label>Think time</label><input id="thinkTime" placeholder="500ms"></div>
        <div class="field"><label>Think max</label><input id="thinkMax" placeholder="2s"></div>
      </div>
    </section>

    <section class="section">
      <div class="section-title">
        <h2>Endpoints</h2>
        <div class="section-note" id="endpointNote">weighted scenario</div>
      </div>
      <div id="endpoints"></div>
      <div class="actions">
        <button class="secondary" id="addEndpoint" type="button">Adicionar endpoint</button>
        <button class="primary" id="start" type="button">Iniciar teste</button>
        <span id="msg"></span>
      </div>
    </section>
    </main>

    <aside class="side">
      <section class="section">
        <div class="section-title">
          <h2>Comando</h2>
          <div class="section-note">preview</div>
        </div>
        <div class="command" id="commandPreview"></div>
        <div class="mini-list">
          <div class="mini"><div class="ck">Auth</div><div class="cv" id="sumAuth">none</div></div>
          <div class="mini"><div class="ck">Adaptive</div><div class="cv" id="sumAdaptive">off</div></div>
        </div>
      </section>

      <section class="section">
        <div class="section-title">
          <h2>Testes passados</h2>
          <div class="section-note" id="testsCount">0</div>
        </div>
        <div class="tests" id="tests"></div>
      </section>
    </aside>
  </div>
  <footer>Orion Dashboard &nbsp;·&nbsp; local launcher</footer>
</div>

<template id="endpointTpl">
  <div class="endpoint">
    <div class="endpoint-head"><span class="endpoint-title"></span><button class="danger remove" type="button">Remover</button></div>
    <div class="endpoint-grid">
      <div><label>Método</label><select class="method"><option>GET</option><option>POST</option><option>PUT</option><option>PATCH</option><option>DELETE</option><option>HEAD</option><option>OPTIONS</option></select></div>
      <div><label>URL</label><input class="url" placeholder="http://localhost:8080/api/checkout"></div>
      <div><label>Peso</label><input class="weight" type="number" min="1" value="1"></div>
    </div>
    <div class="endpoint-body">
      <div><label>Body</label><textarea class="body" placeholder='{"item_id":99}'></textarea></div>
      <div><label>Headers do endpoint</label><textarea class="epHeaders" placeholder="X-Flow: checkout"></textarea></div>
    </div>
  </div>
</template>

<script>
const endpointsEl=document.getElementById('endpoints'), tpl=document.getElementById('endpointTpl');
const $=id=>document.getElementById(id);
function parseHeaders(text){
  const out={};
  text.split(/[;\n]/).map(s=>s.trim()).filter(Boolean).forEach(line=>{
    const i=line.indexOf(':'); if(i>0) out[line.slice(0,i).trim()]=line.slice(i+1).trim();
  });
  return out;
}
function addEndpoint(seed={}){
  const node=tpl.content.firstElementChild.cloneNode(true);
  node.querySelector('.method').value=seed.method||'GET';
  node.querySelector('.url').value=seed.url||'http://localhost:8080/api/checkout';
  node.querySelector('.weight').value=seed.weight||1;
  node.querySelector('.body').value=seed.body||'';
  node.querySelector('.epHeaders').value=seed.headers||'';
  node.querySelector('.remove').onclick=()=>{node.remove(); renumber();};
  node.querySelector('.method').onchange=()=>{renumber(); updatePreview();};
  node.querySelectorAll('input,textarea,select').forEach(el=>el.addEventListener('input',updatePreview));
  endpointsEl.appendChild(node); renumber();
}
function renumber(){
  [...endpointsEl.children].forEach((n,i)=>{
    const method=n.querySelector('.method').value||'GET';
    n.querySelector('.endpoint-title').innerHTML='<span class="method-chip">'+method+'</span><span>Endpoint '+(i+1)+'</span>';
  });
  [...endpointsEl.querySelectorAll('.remove')].forEach(b=>b.style.display=endpointsEl.children.length>1?'inline-block':'none');
  updatePreview();
}
function shellQuote(v){
  if(!v) return '';
  return /[\s"'{}[\]():;]/.test(v)?JSON.stringify(v):v;
}
function endpointData(){
  return [...endpointsEl.children].map(n=>({
    method:n.querySelector('.method').value,
    url:n.querySelector('.url').value.trim(),
    weight:Number(n.querySelector('.weight').value||1),
    body:n.querySelector('.body').value,
    headers:parseHeaders(n.querySelector('.epHeaders').value)
  }));
}
function updatePreview(){
  const eps=endpointData();
  const browser=$('browserMode').checked;
  $('sumRps').textContent=$('rps').value||'100';
  $('sumDuration').textContent=$('duration').value||'30s';
  $('sumEndpoints').textContent=String(eps.length);
  $('sumMode').textContent=browser?'Browser':'HTTP';
  $('sumAuth').textContent=$('token').value?'bearer':($('basic').value?'basic':'none');
  $('sumAdaptive').textContent=$('maxErrorRate').value?$('maxErrorRate').value+'%%':'off';
  $('profileNote').textContent=browser?($('users').value||10)+' browser users':'open model';
  $('endpointNote').textContent=eps.length>1?'scenario file':'single target';
  const parts=['./Orion','-dashboard','-rps',$('rps').value||'100','-duration',$('duration').value||'30s','-timeout',$('timeout').value||'5s'];
  if($('rampUp').value) parts.push('-ramp-up',$('rampUp').value);
  if($('maxErrorRate').value) parts.push('-max-error-rate',$('maxErrorRate').value);
  if(browser){parts.push('-browser','-users',$('users').value||'10'); if($('thinkTime').value) parts.push('-think-time',$('thinkTime').value); if($('thinkMax').value) parts.push('-think-max',$('thinkMax').value);}
  if(eps.length===1){parts.push('-method',eps[0].method,'-url',eps[0].url||'<url>'); if(eps[0].body) parts.push('-body',eps[0].body);}
  else parts.push('-scenario','scenario.json');
  $('commandPreview').textContent=parts.map(shellQuote).join(' ');
}
$('addEndpoint').onclick=()=>addEndpoint({url:''});
$('addEndpointTop').onclick=()=>addEndpoint({url:''});
$('browserMode').onchange=e=>{document.getElementById('browserFields').style.display=e.target.checked?'grid':'none'; updatePreview();};
['rps','duration','rampUp','timeout','maxErrorRate','token','basic','headers','users','thinkTime','thinkMax'].forEach(id=>$(id).addEventListener('input',updatePreview));
addEndpoint();

async function refreshTests(){
  const res=await fetch('/api/tests'); const tests=await res.json();
  const el=document.getElementById('tests');
  document.getElementById('testsCount').textContent=String(tests.length);
  el.innerHTML=tests.length?'':'<div class="muted">Nenhum teste passado ainda.</div>';
  tests.forEach(t=>{
    const card=document.createElement('div'); card.className='test-card';
    const finished=t.status==='finished';
    const badgeText=finished?'Finalizado':'Em andamento';
    const badgeClass=finished?'pill pill-finished':'pill pill-running';
    card.innerHTML='<span class="'+badgeClass+'">'+badgeText+'</span><strong>PID '+t.pid+'</strong><div class="muted">'+new Date(t.startedAt).toLocaleString()+'</div><div class="muted">'+t.command+'</div><div class="actions"><a class="btn secondary" target="_blank" href="'+t.liveURL+'">Abrir dashboard ao vivo</a></div>';
    el.appendChild(card);
  });
}
async function startTest(){
  const msg=document.getElementById('msg'); msg.className=''; msg.textContent='Iniciando...';
  const endpoints=endpointData();
  const payload={
    rps:Number(document.getElementById('rps').value||100),
    duration:document.getElementById('duration').value.trim(),
    rampUp:document.getElementById('rampUp').value.trim(),
    timeout:document.getElementById('timeout').value.trim(),
    maxErrorRate:Number(document.getElementById('maxErrorRate').value||0),
    browser:document.getElementById('browserMode').checked,
    users:Number(document.getElementById('users').value||10),
    thinkTime:document.getElementById('thinkTime').value.trim(),
    thinkMax:document.getElementById('thinkMax').value.trim(),
    token:document.getElementById('token').value.trim(),
    basic:document.getElementById('basic').value.trim(),
    headers:parseHeaders(document.getElementById('headers').value),
    endpoints
  };
  const res=await fetch('/api/tests',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(payload)});
  if(!res.ok){msg.className='err'; msg.textContent=await res.text(); return;}
  const t=await res.json(); msg.className='ok'; msg.textContent='Teste iniciado.';
  window.open(t.liveURL,'_blank'); refreshTests();
}
document.getElementById('start').onclick=startTest;
document.getElementById('startTop').onclick=startTest;
refreshTests(); setInterval(refreshTests,1000);
</script>
</body>
</html>`, port)
}
