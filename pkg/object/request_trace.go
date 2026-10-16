/*
 * JuiceFS, Copyright 2026 Juicedata, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package object

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"net/http/httptrace"
	"sync"
	"time"
)

const maxTrackedDials = 8

// RequestTrace collects bounded diagnostics for one object operation, including
// SDK-internal HTTP requests. Its zero value is ready to use. Snapshots remain
// safe when a timed-out operation still has callbacks running.
type RequestTrace struct {
	mu      sync.Mutex
	enabled bool
	now     func() time.Time

	requests, reused, dropped          uint64
	connection, connected              time.Time
	written, dns, tls, body            time.Time
	responded                          bool
	requestDone                        bool
	terminalError                      string
	activeDNS, activeTLS, activeWrites int
	ambiguous                          [7]bool
	dials                              map[string]time.Time
	phases                             [7]time.Duration
	observed                           [7]bool
	retryable                          [len(requestErrorNames)]uint64
}

var requestPhaseNames = [...]string{"connection_wait", "dns", "tcp_connect", "tls", "request_write", "first_byte", "body_read"}
var requestErrorNames = [...]string{"http_408", "http_429", "http_500", "http_502", "http_503", "http_504", "http_5xx_other", "http_4xx_other", "canceled", "deadline", "connection", "grpc_unavailable", "grpc_resource_exhausted", "grpc_internal", "other"}

// RequestTraceSnapshot contains no request identities or payloads. Phases are
// totals across the operation's HTTP requests, not an additive decomposition:
// connection_wait includes DNS, TCP and TLS, and parallel dials can overlap.
type RequestTraceSnapshot struct {
	Enabled             bool               `json:"-"`
	ConnectionAttempts  uint64             `json:"connection_attempts"`
	ReusedConnections   uint64             `json:"reused_connections"`
	DroppedDials        uint64             `json:"untracked_dials,omitempty"`
	PhaseSeconds        map[string]float64 `json:"phase_seconds"`
	PendingPhaseSeconds map[string]float64 `json:"pending_phase_seconds,omitempty"`
	RetryableErrors     map[string]uint64  `json:"retryable_errors,omitempty"`
	TerminalError       string             `json:"terminal_error,omitempty"`
	OmittedPhases       []string           `json:"omitted_phases,omitempty"`
}

func (t *RequestTrace) clock() time.Time {
	if t.now != nil {
		return t.now()
	}
	return time.Now()
}

// finishPhase is called with mu held. Missing callbacks are omitted rather than
// presented as zero latency. An early response can precede WroteRequest.
func (t *RequestTrace) finishPhase(phase int, start time.Time) {
	if start.IsZero() || t.ambiguous[phase] {
		return
	}
	if elapsed := t.clock().Sub(start); elapsed >= 0 {
		t.phases[phase] += elapsed
		t.observed[phase] = true
	}
}

func (t *RequestTrace) context(ctx context.Context) context.Context {
	t.mu.Lock()
	t.enabled = true
	t.mu.Unlock()
	return httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{
		GetConn: func(string) {
			t.mu.Lock()
			defer t.mu.Unlock()
			t.requests++
			if !t.connection.IsZero() {
				t.ambiguous[0] = true
			}
			if t.activeWrites > 0 {
				t.ambiguous[4], t.ambiguous[5] = true, true
			}
			t.connection = t.clock()
			t.connected, t.written = time.Time{}, time.Time{}
			t.responded = false
			t.requestDone = false
		},
		GotConn: func(info httptrace.GotConnInfo) {
			t.mu.Lock()
			defer t.mu.Unlock()
			if t.requestDone {
				return
			}
			t.activeWrites++
			t.finishPhase(0, t.connection)
			t.connection = time.Time{}
			t.connected = t.clock()
			if info.Reused {
				t.reused++
			}
		},
		DNSStart: func(httptrace.DNSStartInfo) {
			t.mu.Lock()
			t.activeDNS++
			if t.activeDNS > 1 {
				t.ambiguous[1] = true
			}
			t.dns = t.clock()
			t.mu.Unlock()
		},
		DNSDone: func(httptrace.DNSDoneInfo) {
			t.mu.Lock()
			if t.activeDNS > 0 {
				t.activeDNS--
			}
			t.finishPhase(1, t.dns)
			t.dns = time.Time{}
			t.mu.Unlock()
		},
		ConnectStart: func(network, addr string) {
			t.mu.Lock()
			defer t.mu.Unlock()
			key := network + "\x00" + addr
			if _, exists := t.dials[key]; exists {
				t.ambiguous[2] = true
				return
			}
			if len(t.dials) >= maxTrackedDials {
				t.ambiguous[2] = true
				t.dropped++
				return
			}
			if t.dials == nil {
				t.dials = make(map[string]time.Time)
			}
			// Addresses are used only to pair concurrent dial callbacks. They
			// never enter a snapshot, a metric label, or a log.
			t.dials[key] = t.clock()
		},
		ConnectDone: func(network, addr string, _ error) {
			t.mu.Lock()
			key := network + "\x00" + addr
			t.finishPhase(2, t.dials[key])
			delete(t.dials, key)
			t.mu.Unlock()
		},
		TLSHandshakeStart: func() {
			t.mu.Lock()
			t.activeTLS++
			if t.activeTLS > 1 {
				t.ambiguous[3] = true
			}
			t.tls = t.clock()
			t.mu.Unlock()
		},
		TLSHandshakeDone: func(tls.ConnectionState, error) {
			t.mu.Lock()
			if t.activeTLS > 0 {
				t.activeTLS--
			}
			t.finishPhase(3, t.tls)
			t.tls = time.Time{}
			t.mu.Unlock()
		},
		WroteRequest: func(info httptrace.WroteRequestInfo) {
			t.mu.Lock()
			defer t.mu.Unlock()
			if t.activeWrites > 0 {
				t.activeWrites--
			}
			if t.requestDone {
				return
			}
			t.finishPhase(4, t.connected)
			t.connected = time.Time{}
			if !t.responded && info.Err == nil {
				t.written = t.clock()
			}
		},
		GotFirstResponseByte: func() {
			t.mu.Lock()
			t.responded = true
			t.finishPhase(5, t.written)
			t.written = time.Time{}
			t.mu.Unlock()
		},
	})
}

// finishHTTPRequest stops unmatched HTTP phases at the SDK result boundary,
// before retry backoff. Failed or canceled phases retain their elapsed time.
// Active callback counts survive this boundary to detect late overlapping events.
func (t *RequestTrace) finishHTTPRequest() {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.requestDone = true
	for phase, start := range []*time.Time{&t.connection, nil, nil, nil, &t.connected, &t.written} {
		if start != nil {
			t.finishPhase(phase, *start)
			*start = time.Time{}
		}
	}
	// A background dial may outlive the HTTP request when an idle connection
	// wins. Preserve callback pairing but omit its incomplete phase totals.
	if !t.dns.IsZero() {
		t.ambiguous[1] = true
		t.dns = time.Time{}
	}
	if !t.tls.IsZero() {
		t.ambiguous[3] = true
		t.tls = time.Time{}
	}
	for key := range t.dials {
		t.ambiguous[2] = true
		t.dials[key] = time.Time{}
	}
}

func (t *RequestTrace) setTerminalError(category string) {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.terminalError == "" {
		t.terminalError = category
	}
}

func (t *RequestTrace) retryableError(category string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for i, name := range requestErrorNames {
		if category == name {
			t.retryable[i]++
			return
		}
	}
	t.retryable[len(t.retryable)-1]++
}

func (t *RequestTrace) Snapshot() RequestTraceSnapshot {
	if t == nil {
		return RequestTraceSnapshot{}
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.enabled {
		return RequestTraceSnapshot{}
	}
	s := RequestTraceSnapshot{Enabled: true, ConnectionAttempts: t.requests, ReusedConnections: t.reused, DroppedDials: t.dropped, TerminalError: t.terminalError,
		PhaseSeconds: make(map[string]float64), PendingPhaseSeconds: make(map[string]float64), RetryableErrors: make(map[string]uint64)}
	for i, seen := range t.observed {
		if t.ambiguous[i] {
			s.OmittedPhases = append(s.OmittedPhases, requestPhaseNames[i])
		}
		if seen && !t.ambiguous[i] {
			s.PhaseSeconds[requestPhaseNames[i]] = t.phases[i].Seconds()
		}
	}
	for i, count := range t.retryable {
		if count > 0 {
			s.RetryableErrors[requestErrorNames[i]] = count
		}
	}
	now := t.clock()
	for phase, start := range []time.Time{t.connection, t.dns, {}, t.tls, t.connected, t.written, t.body} {
		if !t.ambiguous[phase] && !start.IsZero() && !now.Before(start) {
			s.PendingPhaseSeconds[requestPhaseNames[phase]] = now.Sub(start).Seconds()
		}
	}
	for _, start := range t.dials {
		if !t.ambiguous[2] && !start.IsZero() && !now.Before(start) {
			s.PendingPhaseSeconds["tcp_connect"] += now.Sub(start).Seconds()
		}
	}
	return s
}

func (s RequestTraceSnapshot) String() string {
	if !s.Enabled {
		return "unavailable"
	}
	b, _ := json.Marshal(s)
	return string(b)
}
