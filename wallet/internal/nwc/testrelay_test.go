package nwc

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/getAlby/go-nostr"
)

// testRelay is a test relay that records every message the service sends, in
// order. With a control key it also applies the two rules of our own relay that
// decide the order of the wallet's messages: before the first allow-list only an
// allow-list is accepted, and a REQ by #p may name only listed service keys. A list
// must be signed by the control key and be newer than the saved one, as there.
type testRelay struct {
	t       testing.TB
	control string // Hex public key, or "" for a relay that accepts everything.
	server  *httptest.Server

	mu       sync.Mutex
	frames   []json.RawMessage // Every message received, in order.
	listAt   int64             // The saved list's created_at, or 0 for none.
	services map[string]bool   // Listed service keys.
	pairs    [][2]string       // The saved list's pairs, in order.
	lists    int               // Lists accepted.
	refused  []string          // Every refusal sent, in order.
	// refuse, when set, gives a reason to refuse the next list with, once.
	refuse string
	// holdOK, while set, delays the OK to a list until it is closed.
	holdOK chan struct{}
	got    chan struct{} // A signal after each message.
}

func newTestRelay(t testing.TB, control string) *testRelay {
	t.Helper()
	r := &testRelay{t: t, control: control, services: map[string]bool{}, got: make(chan struct{}, 1)}
	r.server = httptest.NewServer(http.HandlerFunc(r.serve))
	t.Cleanup(r.server.Close)
	return r
}

func (r *testRelay) url() string { return "ws" + strings.TrimPrefix(r.server.URL, "http") }

func (r *testRelay) serve(w http.ResponseWriter, req *http.Request) {
	conn, err := websocket.Accept(w, req, nil)
	if err != nil {
		return
	}
	conn.SetReadLimit(-1)
	defer func() { _ = conn.CloseNow() }()
	ctx := req.Context()
	var writeMu sync.Mutex
	write := func(v ...any) {
		data, _ := json.Marshal(v)
		writeMu.Lock()
		defer writeMu.Unlock()
		_ = conn.Write(ctx, websocket.MessageText, data)
	}
	for {
		_, data, err := conn.Read(ctx)
		if err != nil {
			return
		}
		var message []json.RawMessage
		if json.Unmarshal(data, &message) != nil || len(message) < 2 {
			continue
		}
		r.mu.Lock()
		r.frames = append(r.frames, data)
		r.mu.Unlock()
		select {
		case r.got <- struct{}{}:
		default:
		}
		switch string(message[0]) {
		case `"EVENT"`:
			var event nostr.Event
			_ = json.Unmarshal(message[1], &event)
			ok, reason, hold := r.offer(event)
			r.note(reason)
			if hold != nil {
				go func() {
					<-hold
					write("OK", event.ID, ok, reason)
				}()
				continue
			}
			write("OK", event.ID, ok, reason)
		case `"REQ"`:
			var subID string
			_ = json.Unmarshal(message[1], &subID)
			var filter nostr.Filter
			if len(message) > 2 {
				_ = json.Unmarshal(message[2], &filter)
			}
			if reason := r.read(filter); reason != "" {
				r.note(reason)
				write("CLOSED", subID, reason)
				continue
			}
			write("EOSE", subID)
		}
	}
}

// offer applies the write rules to one event.
func (r *testRelay) offer(event nostr.Event) (ok bool, reason string, hold chan struct{}) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.control == "" {
		return true, "", nil
	}
	if event.Kind != kindAllowList {
		if r.listAt == 0 {
			return false, "restricted: no allow-list yet", nil
		}
		if (event.Kind == kindInfo || event.Kind == kindResponse) && !r.services[event.PubKey] {
			return false, "restricted: not a listed service key", nil
		}
		return true, "", nil
	}
	if event.PubKey != r.control || !event.CheckID() {
		return false, "restricted: the allow-list must be signed by the control key", nil
	}
	if ok, _ := event.CheckSignature(); !ok {
		return false, "invalid: signature is invalid", nil
	}
	if r.refuse != "" {
		reason, r.refuse = r.refuse, ""
		return false, reason, nil
	}
	if int64(event.CreatedAt) <= r.listAt {
		return false, fmt.Sprintf("invalid: not newer than the saved allow-list, created_at %d", r.listAt), nil
	}
	r.listAt = int64(event.CreatedAt)
	r.services = map[string]bool{}
	r.pairs = nil
	for _, tag := range event.Tags {
		if len(tag) == 3 && tag[0] == "pair" {
			r.services[tag[2]] = true
			r.pairs = append(r.pairs, [2]string{tag[1], tag[2]})
		}
	}
	r.lists++
	return true, "", r.holdOK
}

// read applies the read rule for the wallet's filter.
func (r *testRelay) read(filter nostr.Filter) string {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.control == "" {
		return ""
	}
	if r.listAt == 0 {
		return "restricted: no allow-list yet"
	}
	keys := filter.Tags["p"]
	if len(keys) == 0 {
		return "invalid: bad filter"
	}
	for _, key := range keys {
		if !r.services[key] {
			return "invalid: bad filter"
		}
	}
	return ""
}

// note keeps a refusal.
func (r *testRelay) note(reason string) {
	if reason == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.refused = append(r.refused, reason)
}

// refusals returns the refusals so far.
func (r *testRelay) refusals() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.refused...)
}

// saved returns the saved list's pairs and the number of lists accepted.
func (r *testRelay) saved() ([][2]string, int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([][2]string(nil), r.pairs...), r.lists
}

// received returns a copy of the messages so far.
func (r *testRelay) received() []json.RawMessage {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]json.RawMessage(nil), r.frames...)
}

// waitFor waits until the messages received meet done, or fails the test.
func (r *testRelay) waitFor(what string, done func(frames []json.RawMessage) bool) []json.RawMessage {
	r.t.Helper()
	deadline := time.After(15 * time.Second)
	for {
		frames := r.received()
		if done(frames) {
			return frames
		}
		select {
		case <-r.got:
		case <-time.After(20 * time.Millisecond):
		case <-deadline:
			r.t.Fatalf("relay: waited too long for %s; got %d messages:\n%s", what, len(frames), joinFrames(frames))
		}
	}
}

func joinFrames(frames []json.RawMessage) string {
	lines := make([]string, len(frames))
	for i, f := range frames {
		lines[i] = string(f)
	}
	return strings.Join(lines, "\n")
}

// frameKind gives a message's type, and for an EVENT its kind.
func frameKind(frame json.RawMessage) (string, int) {
	var message []json.RawMessage
	if json.Unmarshal(frame, &message) != nil || len(message) < 2 {
		return "", 0
	}
	var name string
	_ = json.Unmarshal(message[0], &name)
	if name != "EVENT" {
		return name, 0
	}
	var event nostr.Event
	_ = json.Unmarshal(message[1], &event)
	return name, event.Kind
}

// count counts the messages of a type, and for EVENT of a kind (0 for any).
func count(frames []json.RawMessage, name string, kind int) int {
	n := 0
	for _, f := range frames {
		if gotName, gotKind := frameKind(f); gotName == name && (kind == 0 || gotKind == kind) {
			n++
		}
	}
	return n
}
