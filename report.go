package main

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"html/template"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

//go:embed template.html
var htmlReportTemplate string

// ── Terminal report ────────────────────────────────────────────────────────────

func (c *collector) report(cfg *config, elapsed time.Duration) {
	sep := strings.Repeat("─", 62)
	dbl := strings.Repeat("═", 62)
	row := func(label, value string) { fmt.Printf("  %-22s %s\n", label, value) }
	section := func(title string) { fmt.Println(sep); fmt.Println(" ", title) }

	successRate, actualRPS := 0.0, 0.0
	if c.total > 0 {
		successRate = float64(c.success) / float64(c.total) * 100
	}
	if s := elapsed.Seconds(); s > 0 {
		actualRPS = float64(c.total) / s
	}

	fmt.Println()
	fmt.Println(dbl)
	fmt.Println("  Orion — Load Test Report")
	fmt.Println(dbl)
	row("URL:", cfg.method+" "+cfg.url)
	row("Duration:", elapsed.String())
	row("Target RPS:", fmt.Sprintf("%d req/s", cfg.rps))
	row("Ramp-up:", cfg.rampUp.String())
	row("Timeout:", cfg.timeout.String())

	section("THROUGHPUT")
	row("Total requests:", fmt.Sprintf("%d", c.total))
	row("Successful:", fmt.Sprintf("%d  (%.2f%%)", c.success, successRate))
	row("Actual RPS:", fmt.Sprintf("%.2f req/s", actualRPS))

	if c.hist.TotalCount() > 0 {
		section("LATENCY")
		row("min:", formatµs(c.hist.Min()))
		row("p50  (median):", formatµs(c.hist.ValueAtQuantile(50)))
		row("p95:", formatµs(c.hist.ValueAtQuantile(95)))
		row("p99:", formatµs(c.hist.ValueAtQuantile(99)))
		row("p99.9:", formatµs(c.hist.ValueAtQuantile(99.9)))
		row("max:", formatµs(c.hist.Max()))
		row("mean:", formatµs(int64(c.hist.Mean())))
	}

	if len(c.errors) > 0 {
		section("ERRORS")
		for k, v := range c.errors {
			row(k+":", fmt.Sprintf("%d", v))
		}
	}

	fmt.Println(dbl)

	htmlPath, err := c.generateHTMLReport(cfg, elapsed)
	if err != nil {
		fmt.Fprintf(os.Stderr, "\n[Orion] warning: HTML report skipped: %v\n", err)
	} else {
		fmt.Printf("\n  HTML report → file://%s\n", htmlPath)
	}
	fmt.Println()
}

// ── HTML report ────────────────────────────────────────────────────────────────

type htmlReportData struct {
	Title        string
	Date         string
	URL          string
	Method       string
	Duration     string
	TargetRPS    int
	RampUp       string
	Timeout      string
	TotalReqs    string
	SuccessRate  string
	ActualRPS    string
	LatencyMin   string
	LatencyP50   string
	LatencyP95   string
	LatencyP99   string
	LatencyP999  string
	LatencyMax   string
	LatencyMean  string
	HasErrors    bool
	ErrorRows    []errorRow
	HasSnapshots bool

	TimeLabelsJSON template.JS
	RPSDataJSON    template.JS
	ErrDataJSON    template.JS
	P50DataJSON    template.JS
	P95DataJSON    template.JS
	P99DataJSON    template.JS
}

type errorRow struct {
	Key   string
	Count int64
}

func (c *collector) generateHTMLReport(cfg *config, elapsed time.Duration) (string, error) {
	tmpl, err := template.New("report").Parse(htmlReportTemplate)
	if err != nil {
		return "", fmt.Errorf("parse template: %w", err)
	}

	successRate, actualRPS := 0.0, 0.0
	if c.total > 0 {
		successRate = float64(c.success) / float64(c.total) * 100
	}
	if s := elapsed.Seconds(); s > 0 {
		actualRPS = float64(c.total) / s
	}

	latency := func(µs int64) string {
		if c.hist.TotalCount() == 0 {
			return "n/a"
		}
		return formatµs(µs)
	}

	var errRows []errorRow
	for k, v := range c.errors {
		errRows = append(errRows, errorRow{k, v})
	}
	sort.Slice(errRows, func(i, j int) bool { return errRows[i].Key < errRows[j].Key })

	var timeLabels        []string
	var rpsVals, errVals  []float64
	var p50Vals, p95Vals, p99Vals []float64
	for _, s := range c.snapshots {
		timeLabels = append(timeLabels, fmt.Sprintf("%ds", s.elapsed))
		rpsVals = append(rpsVals, float64(s.successCnt))
		errVals = append(errVals, float64(s.errorCnt))
		p50Vals = append(p50Vals, s.p50ms)
		p95Vals = append(p95Vals, s.p95ms)
		p99Vals = append(p99Vals, s.p99ms)
	}

	toJS := func(v any) template.JS {
		b, _ := json.Marshal(v)
		return template.JS(b)
	}

	data := htmlReportData{
		Title:          "Orion — Load Test Report",
		Date:           time.Now().Format("2006-01-02 15:04:05"),
		URL:            cfg.url,
		Method:         cfg.method,
		Duration:       elapsed.String(),
		TargetRPS:      cfg.rps,
		RampUp:         cfg.rampUp.String(),
		Timeout:        cfg.timeout.String(),
		TotalReqs:      fmt.Sprintf("%d", c.total),
		SuccessRate:    fmt.Sprintf("%.2f%%", successRate),
		ActualRPS:      fmt.Sprintf("%.2f req/s", actualRPS),
		LatencyMin:     latency(c.hist.Min()),
		LatencyP50:     latency(c.hist.ValueAtQuantile(50)),
		LatencyP95:     latency(c.hist.ValueAtQuantile(95)),
		LatencyP99:     latency(c.hist.ValueAtQuantile(99)),
		LatencyP999:    latency(c.hist.ValueAtQuantile(99.9)),
		LatencyMax:     latency(c.hist.Max()),
		LatencyMean:    latency(int64(c.hist.Mean())),
		HasErrors:      len(errRows) > 0,
		ErrorRows:      errRows,
		HasSnapshots:   len(c.snapshots) > 0,
		TimeLabelsJSON: toJS(timeLabels),
		RPSDataJSON:    toJS(rpsVals),
		ErrDataJSON:    toJS(errVals),
		P50DataJSON:    toJS(p50Vals),
		P95DataJSON:    toJS(p95Vals),
		P99DataJSON:    toJS(p99Vals),
	}

	reportPath := fmt.Sprintf("orion-report-%s.html", time.Now().Format("20060102-150405"))
	f, err := os.Create(reportPath)
	if err != nil {
		return "", fmt.Errorf("create file: %w", err)
	}
	defer f.Close()

	if err := tmpl.Execute(f, data); err != nil {
		return "", fmt.Errorf("render template: %w", err)
	}

	abs, err := filepath.Abs(reportPath)
	if err != nil {
		return reportPath, nil
	}
	return abs, nil
}

// ── Helpers ────────────────────────────────────────────────────────────────────

func formatµs(µs int64) string {
	switch {
	case µs <= 0:
		return "n/a"
	case µs < 1_000:
		return fmt.Sprintf("%d µs", µs)
	case µs < 1_000_000:
		return fmt.Sprintf("%.2f ms", float64(µs)/1_000)
	default:
		return fmt.Sprintf("%.3f s", float64(µs)/1_000_000)
	}
}

