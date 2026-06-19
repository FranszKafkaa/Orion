package orion

import (
	"encoding/binary"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
)

type clientIPSimulator struct {
	headers []string
	ips     []string
	next    atomic.Uint64
}

func newClientIPSimulator(list, cidr, headers string) (*clientIPSimulator, error) {
	if list == "" && cidr == "" {
		return nil, nil
	}
	if list != "" && cidr != "" {
		return nil, fmt.Errorf("use either -client-ip-list or -client-ip-cidr, not both")
	}

	headerNames := splitCSV(headers)
	if len(headerNames) == 0 {
		headerNames = []string{"X-Forwarded-For", "X-Real-IP"}
	}
	for _, h := range headerNames {
		if !httpgutsValidHeaderFieldName(h) {
			return nil, fmt.Errorf("invalid client IP header name %q", h)
		}
	}

	var ips []string
	if list != "" {
		for _, ip := range splitCSV(list) {
			parsed := net.ParseIP(ip)
			if parsed == nil {
				return nil, fmt.Errorf("invalid IP in -client-ip-list: %q", ip)
			}
			ips = append(ips, parsed.String())
		}
	} else {
		generated, err := expandIPv4CIDR(cidr)
		if err != nil {
			return nil, err
		}
		ips = generated
	}
	if len(ips) == 0 {
		return nil, fmt.Errorf("client IP simulation has no IPs")
	}

	return &clientIPSimulator{headers: headerNames, ips: ips}, nil
}

func (s *clientIPSimulator) apply(h http.Header) {
	if s == nil {
		return
	}
	ip := s.ips[int(s.next.Add(1)-1)%len(s.ips)]
	for _, header := range s.headers {
		h.Set(header, ip)
	}
}

func splitCSV(s string) []string {
	var out []string
	for _, item := range strings.Split(s, ",") {
		item = strings.TrimSpace(item)
		if item != "" {
			out = append(out, item)
		}
	}
	return out
}

func expandIPv4CIDR(cidr string) ([]string, error) {
	ip, network, err := net.ParseCIDR(cidr)
	if err != nil {
		return nil, fmt.Errorf("invalid -client-ip-cidr %q: %w", cidr, err)
	}
	ip4 := ip.To4()
	if ip4 == nil {
		return nil, fmt.Errorf("-client-ip-cidr currently supports IPv4 only")
	}

	ones, bits := network.Mask.Size()
	if bits != 32 {
		return nil, fmt.Errorf("-client-ip-cidr currently supports IPv4 only")
	}
	total := uint64(1) << uint64(bits-ones)
	if total > 65536 {
		return nil, fmt.Errorf("-client-ip-cidr expands to %d addresses; use /16 or smaller range", total)
	}

	start := binary.BigEndian.Uint32(ip4)
	firstHost := uint64(0)
	lastExclusive := total
	if total > 2 {
		firstHost = 1
		lastExclusive = total - 1
	}

	ips := make([]string, 0, lastExclusive-firstHost)
	var b [4]byte
	for offset := firstHost; offset < lastExclusive; offset++ {
		binary.BigEndian.PutUint32(b[:], start+uint32(offset))
		ips = append(ips, net.IPv4(b[0], b[1], b[2], b[3]).String())
	}
	return ips, nil
}

func httpgutsValidHeaderFieldName(v string) bool {
	if v == "" {
		return false
	}
	for _, r := range v {
		if !strings.ContainsRune("!#$%&'*+-.^_`|~0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ", r) {
			return false
		}
	}
	return true
}
