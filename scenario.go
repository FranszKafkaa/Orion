package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"time"
)

// EndpointDef describes one entry in a scenario file.
type EndpointDef struct {
	Weight  int               `json:"weight"`
	Method  string            `json:"method"`
	URL     string            `json:"url"`
	Body    string            `json:"body"`
	Headers map[string]string `json:"headers"`
}

// Scenario holds weighted endpoints loaded from a JSON file.
type Scenario struct {
	Endpoints []EndpointDef `json:"endpoints"`
}

func loadScenario(path string) (*Scenario, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read scenario file: %w", err)
	}
	var sc Scenario
	if err := json.Unmarshal(data, &sc); err != nil {
		return nil, fmt.Errorf("parse scenario JSON: %w", err)
	}
	if len(sc.Endpoints) == 0 {
		return nil, fmt.Errorf("scenario has no endpoints")
	}
	for i := range sc.Endpoints {
		sc.Endpoints[i].Method = strings.ToUpper(sc.Endpoints[i].Method)
		if sc.Endpoints[i].Method == "" {
			sc.Endpoints[i].Method = "GET"
		}
		if sc.Endpoints[i].Weight <= 0 {
			sc.Endpoints[i].Weight = 1
		}
	}
	return &sc, nil
}

// buildSelector returns a func that picks a random endpoint index by weight.
func buildSelector(endpoints []EndpointDef) func() int {
	total := 0
	for _, ep := range endpoints {
		total += ep.Weight
	}
	cumulative := make([]int, len(endpoints))
	acc := 0
	for i, ep := range endpoints {
		acc += ep.Weight
		cumulative[i] = acc
	}
	return func() int {
		r := rand.IntN(total)
		for i, c := range cumulative {
			if r < c {
				return i
			}
		}
		return len(endpoints) - 1
	}
}

// epHeaders merges global headers (auth, -H flags) with endpoint-specific ones.
// Endpoint-specific headers take precedence.
func epHeaders(ep *EndpointDef, global http.Header) http.Header {
	h := make(http.Header)
	for k, vals := range global {
		h[k] = vals
	}
	for k, v := range ep.Headers {
		h.Set(k, v)
	}
	return h
}

// runAdaptiveController watches per-second error rates from collector snapshots
// and adjusts effectiveRPS accordingly. It only activates after ramp-up completes.
//
// Reduction: 3 consecutive seconds above maxErrRate → cut RPS by 10%.
// Recovery:  5 consecutive seconds below maxErrRate/2 → raise RPS by 5% (capped at original).
func runAdaptiveController(
	snapCh <-chan snapshot,
	effectiveRPS *atomic.Int64,
	originalRPS int,
	maxErrRate float64,
	rampUp time.Duration,
	ctx context.Context,
) {
	var badStreak, goodStreak int
	minRPS := int64(max(1, originalRPS/10))
	start := time.Now()

	for {
		select {
		case <-ctx.Done():
			return
		case snap, ok := <-snapCh:
			if !ok {
				return
			}
			// Don't adapt during ramp-up to avoid interference with intentional growth.
			if time.Since(start) < rampUp {
				continue
			}
			total := snap.successCnt + snap.errorCnt
			errRate := 0.0
			if total > 0 {
				errRate = float64(snap.errorCnt) / float64(total) * 100
			}
			current := effectiveRPS.Load()

			switch {
			case errRate > maxErrRate:
				badStreak++
				goodStreak = 0
				if badStreak >= 3 {
					reduced := max(int64(float64(current)*0.9), minRPS)
					if reduced < current {
						effectiveRPS.Store(reduced)
						fmt.Printf("[Orion] adaptive: %.1f%% errors (> %.1f%%) → RPS %d → %d\n",
							errRate, maxErrRate, current, reduced)
					}
					badStreak = 0
				}
			case errRate <= maxErrRate/2:
				goodStreak++
				badStreak = 0
				if goodStreak >= 5 {
					restored := min(int64(float64(current)*1.05)+1, int64(originalRPS))
					if restored > current {
						effectiveRPS.Store(restored)
						fmt.Printf("[Orion] adaptive: %.1f%% errors OK → RPS %d → %d\n",
							errRate, current, restored)
					}
					goodStreak = 0
				}
			default:
				badStreak = 0
				goodStreak = 0
			}
		}
	}
}
