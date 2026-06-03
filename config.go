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
	url     string
	method  string
	body    string
	rps     int
	rampUp  time.Duration
	dur     time.Duration
	timeout time.Duration
	headers http.Header
}

func parseFlags() *config {
	var extra headerFlags

	urlFlag     := flag.String("url", "http://localhost:8080/api/checkout", "endpoint under test")
	methodFlag  := flag.String("method", "POST", "HTTP method (GET, POST, PUT, PATCH, DELETE, HEAD)")
	bodyFlag    := flag.String("body", "", "raw request body (JSON); omit to use default {user_id, action} payload")
	rpsFlag     := flag.Int("rps", 100, "virtual users injected per second (peak rate)")
	rampUpFlag  := flag.Duration("ramp-up", 0, "linear ramp-up duration (e.g. 30s); default = full duration")
	durFlag     := flag.Duration("duration", 30*time.Second, "total test duration (e.g. 30s, 2m)")
	timeoutFlag := flag.Duration("timeout", 5*time.Second, "per-request hard deadline")
	tokenFlag   := flag.String("token", "", "Bearer token → Authorization: Bearer <token>")
	basicFlag   := flag.String("basic", "", "Basic auth as user:pass")
	flag.Var(&extra, "H", "extra header as 'Key: Value' (repeatable)")

	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, "Usage:")
		fmt.Fprintln(os.Stderr, "Flags:")
		flag.PrintDefaults()
		fmt.Fprintln(os.Stderr, "\nExamples:")
		fmt.Fprintln(os.Stderr, "  Orion -url http://localhost:8080/clubes -method GET -rps 200 -duration 60s")
		fmt.Fprintln(os.Stderr, "  Orion -url http://api/order -body '{\"item\":\"book\"}' -token eyJ... -rps 50")
		fmt.Fprintln(os.Stderr, "  Orion -url http://api/checkout -basic admin:s3cr3t -H 'X-Tenant: acme'")
		fmt.Fprintln(os.Stderr, "  Orion -url http://api/checkout -rps 500 -ramp-up 30s -duration 90s")
	}

	flag.Parse()

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

	return &config{
		url:     *urlFlag,
		method:  method,
		body:    *bodyFlag,
		rps:     *rpsFlag,
		rampUp:  *rampUpFlag,
		dur:     *durFlag,
		timeout: *timeoutFlag,
		headers: headers,
	}
}
