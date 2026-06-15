package main

import (
	_ "embed"
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
	"text/template"
	"time"
)

//go:embed templates/launcher.html
var launcherTemplate string

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
	tmpl := template.Must(template.New("launcher").Parse(launcherTemplate))
	var buf strings.Builder
	if err := tmpl.Execute(&buf, struct{ Port int }{port}); err != nil {
		return fmt.Sprintf("<pre>template error: %v</pre>", err)
	}
	return buf.String()
}

