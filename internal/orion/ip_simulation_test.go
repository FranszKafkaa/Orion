package orion

import (
	"net/http"
	"testing"
)

func TestClientIPSimulatorRotatesList(t *testing.T) {
	sim, err := newClientIPSimulator("203.0.113.10,203.0.113.11", "", "X-Forwarded-For,X-Real-IP")
	if err != nil {
		t.Fatalf("newClientIPSimulator() error = %v", err)
	}

	h := make(http.Header)
	sim.apply(h)
	if got := h.Get("X-Forwarded-For"); got != "203.0.113.10" {
		t.Fatalf("first X-Forwarded-For = %q", got)
	}
	if got := h.Get("X-Real-IP"); got != "203.0.113.10" {
		t.Fatalf("first X-Real-IP = %q", got)
	}

	sim.apply(h)
	if got := h.Get("X-Forwarded-For"); got != "203.0.113.11" {
		t.Fatalf("second X-Forwarded-For = %q", got)
	}
}

func TestClientIPSimulatorExpandsCIDRWithoutNetworkAndBroadcast(t *testing.T) {
	sim, err := newClientIPSimulator("", "10.20.0.0/30", "")
	if err != nil {
		t.Fatalf("newClientIPSimulator() error = %v", err)
	}

	if got, want := len(sim.ips), 2; got != want {
		t.Fatalf("len(ips) = %d, want %d", got, want)
	}
	if sim.ips[0] != "10.20.0.1" || sim.ips[1] != "10.20.0.2" {
		t.Fatalf("ips = %#v", sim.ips)
	}
}

func TestClientIPSimulatorRejectsInvalidConfig(t *testing.T) {
	if _, err := newClientIPSimulator("not-an-ip", "", "X-Forwarded-For"); err == nil {
		t.Fatal("expected invalid IP error")
	}
	if _, err := newClientIPSimulator("203.0.113.10", "10.20.0.0/24", "X-Forwarded-For"); err == nil {
		t.Fatal("expected mutually exclusive config error")
	}
	if _, err := newClientIPSimulator("", "10.20.0.0/8", "X-Forwarded-For"); err == nil {
		t.Fatal("expected oversized CIDR error")
	}
}
