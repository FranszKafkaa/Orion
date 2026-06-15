package main

import (
	"encoding/base64"
	"flag"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"
)

// headerFlags allows -H to be repeated: -H "X-Foo: bar" -H "X-Baz: qux"
type headerFlags []string

func (h *headerFlags) String() string     { return strings.Join(*h, ", ") }
func (h *headerFlags) Set(v string) error { *h = append(*h, v); return nil }

type config struct {
	url          string
	method       string
	body         string
	rps          int
	rampUp       time.Duration
	dur          time.Duration
	timeout      time.Duration
	headers      http.Header
	scenario     *Scenario
	selector     func() int
	maxErrorRate float64

	// browser mode
	browser     bool
	browserN    int
	thinkMin    time.Duration
	thinkMax    time.Duration
	browserPort int

	// live dashboard
	dashboard         bool
	dashboardLauncher bool
	dashboardPort     int
}

func parseFlags() *config {
	var extra headerFlags

	urlFlag := flag.String("url", "http://localhost:8080/api/checkout", "endpoint under test")
	deleteReportsFlag := flag.Bool("deleteReports", false, "Delete all reports htmls")
	methodFlag := flag.String("method", "GET", "HTTP method (GET, POST, PUT, PATCH, DELETE, HEAD)")
	bodyFlag := flag.String("body", "", "raw request body (JSON); omit to use default {user_id, action} payload")
	rpsFlag := flag.Int("rps", 100, "virtual users injected per second (peak rate)")
	rampUpFlag := flag.Duration("ramp-up", 0, "linear ramp-up duration (e.g. 30s); default = full duration")
	durFlag := flag.Duration("duration", 30*time.Second, "total test duration (e.g. 30s, 2m)")
	timeoutFlag := flag.Duration("timeout", 5*time.Second, "per-request hard deadline")
	tokenFlag := flag.String("token", "", "Bearer token → Authorization: Bearer <token>")
	basicFlag := flag.String("basic", "", "Basic auth as user:pass")
	scenarioFlag := flag.String("scenario", "", "path to JSON scenario file (overrides -url/-method/-body)")
	maxErrRateFlag := flag.Float64("max-error-rate", 0, "adaptive rate: reduce RPS when error % exceeds this (0 = disabled)")
	browserFlag := flag.Bool("browser", false, "run in browser VU mode (opens real browser tabs)")
	usersFlag := flag.Int("users", 10, "number of browser VU tabs to open (browser mode only)")
	thinkTimeFlag := flag.Duration("think-time", 0, "think time between requests per browser VU (e.g. 1s, 500ms)")
	thinkMaxFlag := flag.Duration("think-max", 0, "max think time; if > think-time, delay is random in [think-time, think-max]")
	portFlag := flag.Int("port", 9090, "local server port for browser mode")
	dashboardFlag := flag.Bool("dashboard", false, "open a live dashboard in the browser during the test")
	dashboardPortFlag := flag.Int("dashboard-port", 9191, "local port for the live dashboard server")
	flag.Var(&extra, "H", "extra header as 'Key: Value' (repeatable)")

	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, "Usage:")
		fmt.Fprintln(os.Stderr, "Flags:")
		flag.PrintDefaults()
		fmt.Fprintln(os.Stderr, "\nExamples:")
		fmt.Fprintln(os.Stderr, "  carga -url http://localhost:8080/clubes -method GET -rps 200 -duration 60s")
		fmt.Fprintln(os.Stderr, "  carga -url http://api/order -body '{\"item\":\"book\"}' -token eyJ... -rps 50")
		fmt.Fprintln(os.Stderr, "  carga -url http://api/checkout -rps 500 -ramp-up 30s -duration 90s -max-error-rate 5")
		fmt.Fprintln(os.Stderr, "  carga -scenario scenario.json -rps 200 -duration 60s")
		fmt.Fprintln(os.Stderr, "  carga -browser -users 20 -duration 60s -think-time 1s -url http://api/checkout")
		fmt.Fprintln(os.Stderr, "  carga -browser -users 10 -scenario scenario.json -think-time 500ms -think-max 2s -duration 60s")
	}

	flag.Parse()
	visited := make(map[string]bool)
	flag.Visit(func(f *flag.Flag) {
		visited[f.Name] = true
	})

	if *deleteReportsFlag {
		deleteReportFlags()
	}

	method := strings.ToUpper(*methodFlag)
	validMethods := map[string]bool{
		"GET": true, "POST": true, "PUT": true, "PATCH": true,
		"DELETE": true, "HEAD": true, "OPTIONS": true,
	}
	if !validMethods[method] {
		fmt.Fprintf(os.Stderr, "error: unsupported HTTP method %q\n", *methodFlag)
		os.Exit(1)
	}
	if *rpsFlag < 1 {
		fmt.Fprintln(os.Stderr, "error: -rps must be ≥ 1")
		os.Exit(1)
	}
	if *rampUpFlag == 0 {
		*rampUpFlag = *durFlag
	}
	if *rampUpFlag > *durFlag {
		fmt.Fprintln(os.Stderr, "error: -ramp-up must not exceed -duration")
		os.Exit(1)
	}

	headers := make(http.Header)
	switch {
	case *tokenFlag != "":
		headers.Set("Authorization", "Bearer "+*tokenFlag)
	case *basicFlag != "":
		encoded := base64.StdEncoding.EncodeToString([]byte(*basicFlag))
		headers.Set("Authorization", "Basic "+encoded)
	}
	for _, h := range extra {
		parts := strings.SplitN(h, ":", 2)
		if len(parts) != 2 {
			fmt.Fprintf(os.Stderr, "error: invalid header %q (expected 'Key: Value')\n", h)
			os.Exit(1)
		}
		headers.Set(strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1]))
	}

	if *maxErrRateFlag < 0 || *maxErrRateFlag >= 100 {
		fmt.Fprintln(os.Stderr, "error: -max-error-rate must be between 0 and 100")
		os.Exit(1)
	}
	if *usersFlag < 1 {
		fmt.Fprintln(os.Stderr, "error: -users must be >= 1")
		os.Exit(1)
	}
	if *thinkMaxFlag > 0 && *thinkMaxFlag < *thinkTimeFlag {
		fmt.Fprintln(os.Stderr, "error: -think-max must be >= -think-time")
		os.Exit(1)
	}
	thinkMax := *thinkMaxFlag
	if thinkMax == 0 {
		thinkMax = *thinkTimeFlag
	}

	cfg := &config{
		url:               *urlFlag,
		method:            method,
		body:              *bodyFlag,
		rps:               *rpsFlag,
		rampUp:            *rampUpFlag,
		dur:               *durFlag,
		timeout:           *timeoutFlag,
		headers:           headers,
		maxErrorRate:      *maxErrRateFlag,
		browser:           *browserFlag,
		browserN:          *usersFlag,
		thinkMin:          *thinkTimeFlag,
		thinkMax:          thinkMax,
		browserPort:       *portFlag,
		dashboard:         *dashboardFlag,
		dashboardLauncher: *dashboardFlag && onlyDashboardFlags(visited),
		dashboardPort:     *dashboardPortFlag,
	}

	if *scenarioFlag != "" {
		sc, err := loadScenario(*scenarioFlag)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			os.Exit(1)
		}
		cfg.scenario = sc
		cfg.selector = buildSelector(sc.Endpoints)
	}

	return cfg
}

func onlyDashboardFlags(visited map[string]bool) bool {
	if !visited["dashboard"] {
		return false
	}
	for name := range visited {
		switch name {
		case "dashboard", "dashboard-port", "deleteReports":
		default:
			return false
		}
	}
	return true
}
