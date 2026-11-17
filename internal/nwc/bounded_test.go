package nwc

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/getAlby/go-nostr"

	"github.com/2rdzy/nwc4ln/internal/nwc/nwctest"
)

// floodRelay is a test relay that answers the first REQ with the given events, then
// an EOSE, as fast as the socket takes them. It never stores anything.
type floodRelay struct {
	ack    bool // Answer OK to what the service publishes.
	server *httptest.Server
	sent   atomic.Int64
	done   chan struct{}
}

func newFloodRelay(t *testing.T, ack bool, events func(subID string) [][]byte) *floodRelay {
	t.Helper()
	f := &floodRelay{ack: ack, done: make(chan struct{})}
	f.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		conn.SetReadLimit(-1)
		defer func() { _ = conn.CloseNow() }()
		ctx := r.Context()
		for {
			_, data, err := conn.Read(ctx)
			if err != nil {
				return
			}
			var message []json.RawMessage
			if json.Unmarshal(data, &message) != nil || len(message) < 2 {
				continue
			}
			if string(message[0]) == `"EVENT"` && f.ack {
				var event nostr.Event
				_ = json.Unmarshal(message[1], &event)
				ok, _ := json.Marshal([]any{"OK", event.ID, true, ""})
				_ = conn.Write(ctx, websocket.MessageText, ok)
				continue
			}
			if string(message[0]) != `"REQ"` {
				continue
			}
			var subID string
			_ = json.Unmarshal(message[1], &subID)
			go func() {
				defer close(f.done)
				for _, frame := range events(subID) {
					if conn.Write(ctx, websocket.MessageText, frame) != nil {
						return
					}
					f.sent.Add(1)
				}
				eose, _ := json.Marshal([]any{"EOSE", subID})
				_ = conn.Write(ctx, websocket.MessageText, eose)
			}()
		}
	}))
	t.Cleanup(f.server.Close)
	return f
}

func (f *floodRelay) url() string { return "ws" + strings.TrimPrefix(f.server.URL, "http") }

// strangerEvents signs n requests to servicePub from a key that has no connection.
func strangerEvents(t *testing.T, servicePub string, n int, content string, extraTags int) []nostr.Event {
	t.Helper()
	key := nostr.GeneratePrivateKey()
	tags := nostr.Tags{{"p", servicePub}}
	for i := range extraTags {
		tags = append(tags, nostr.Tag{"t", strings.Repeat("x", 8) + string(rune('a'+i%26))})
	}
	out := make([]nostr.Event, n)
	for i := range out {
		out[i] = nostr.Event{Kind: kindRequest, CreatedAt: nostr.Now() + nostr.Timestamp(i%60), Content: content, Tags: tags}
		out[i].Content = content + strings.Repeat(" ", i%7) // Distinct IDs.
		if err := out[i].Sign(key); err != nil {
			t.Fatal(err)
		}
	}
	return out
}

func frames(subID string, events []nostr.Event) [][]byte {
	out := make([][]byte, len(events))
	for i, event := range events {
		out[i], _ = json.Marshal([]any{"EVENT", subID, event})
	}
	return out
}

// silentPublish makes every publish wait until the service stops, as a relay that
// acknowledges nothing does, and counts the publishes that wait.
func silentPublish(s *Service) *atomic.Int64 {
	var waiting atomic.Int64
	s.publish = func(ctx context.Context, _ nostr.Event) error {
		waiting.Add(1)
		<-ctx.Done()
		return ctx.Err()
	}
	return &waiting
}

// Catches: requests over the rate each holding a goroutine, a decryption and a
// publish while the relay acknowledges nothing (S02). Goroutines, replies waiting to
// be published and heap stay under fixed ceilings, and 50 requests (the burst) are
// stored.
func TestRateLimitedFloodIsBounded(t *testing.T) {
	e := newEnv(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer func() { cancel(); e.service.work.Wait() }()
	waiting := silentPublish(e.service)
	for range publishers {
		e.service.work.Go(func() { e.service.publishLoop(ctx) })
	}
	const flood = 3000
	events := make([]*nostr.Event, flood)
	for i := range events {
		events[i] = e.request(nwctest.NIP44, "get_info")
	}
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	goroutines := runtime.NumGoroutine()
	for _, event := range events {
		e.service.receive(ctx, event)
	}
	time.Sleep(200 * time.Millisecond) // Let the workers reach the queue.
	grown := runtime.NumGoroutine() - goroutines
	runtime.GC()
	runtime.ReadMemStats(&after)
	// Every running request and refusal reply, plus slack for the database driver.
	ceiling := maxRunningEach + replyBudget + 20
	t.Logf("%d events: %d goroutines above the start, %d replies waiting, heap %+d KiB, %d rows", flood, grown,
		int(waiting.Load())+len(e.service.outbox), (int64(after.HeapAlloc)-int64(before.HeapAlloc))>>10, e.requests())
	if grown > ceiling {
		t.Errorf("%d events from one connection: %d goroutines held, ceiling %d", flood, grown, ceiling)
	}
	if replies := int(waiting.Load()) + len(e.service.outbox); replies > publishers+publishQueue {
		t.Errorf("%d replies waiting for the relay", replies)
	}
	if heap := int64(after.HeapAlloc) - int64(before.HeapAlloc); heap > 64<<20 {
		t.Errorf("heap grew by %d MiB", heap>>20)
	}
	if rows := e.requests(); rows != rateBurst {
		t.Errorf("%d request rows, want the burst of %d", rows, rateBurst)
	}
}

// Catches: one holder taking every running place, so another connection is not
// served (S02). The holder's ninth request at once counts as over the rate, while a
// second connection still gets a place.
func TestOneHolderCannotTakeEveryPlace(t *testing.T) {
	e := newEnv(t)
	other := e.addApp("second")
	e.restart()
	var held []*accepted
	for i := range maxRunningEach + 1 {
		a := e.service.accept(t.Context(), e.request(nwctest.NIP44, "get_info"))
		if a == nil {
			t.Fatalf("request %d dropped", i)
		}
		if i < maxRunningEach && !a.reserved {
			t.Fatalf("request %d has no place", i)
		}
		if i == maxRunningEach && (a.reserved || !a.limited) {
			t.Fatalf("request %d past the cap: reserved=%v limited=%v", i, a.reserved, a.limited)
		}
		held = append(held, a)
	}
	event, err := other.Request(nwctest.NIP44, "get_info", e.timestamp())
	if err != nil {
		t.Fatal(err)
	}
	if a := e.service.accept(t.Context(), event); a == nil || !a.reserved {
		t.Fatalf("second connection not served: %+v", a)
	}
	for _, a := range held {
		if a.reserved {
			e.service.release(a.connection.id)
		}
	}
	if a := e.service.accept(t.Context(), e.request(nwctest.NIP44, "get_info")); a == nil || !a.reserved {
		t.Fatal("no place after the others ended")
	}
}

// Catches: requests past the service's cap still running, and a place not given
// back when the request cannot be stored, which would refuse the connection for good.
func TestRunningCapAndRelease(t *testing.T) {
	e := newEnv(t)
	other := e.addApp("second")
	e.restart()
	e.service.maxRunning = 2
	for range 2 {
		if a := e.service.accept(t.Context(), e.request(nwctest.NIP44, "get_info")); a == nil || !a.reserved {
			t.Fatalf("no place under the cap: %+v", a)
		}
	}
	event, err := other.Request(nwctest.NIP44, "get_info", e.timestamp())
	if err != nil {
		t.Fatal(err)
	}
	if a := e.service.accept(t.Context(), event); a == nil || a.reserved || !a.limited {
		t.Fatalf("past the service's cap: %+v", a)
	}

	e = newEnv(t)
	e.service.maxRunningEach = 1
	if _, err := e.store.DB.Exec(`CREATE TRIGGER rv4 BEFORE INSERT ON requests
		BEGIN SELECT RAISE(ABORT, 'injected failure'); END`); err != nil {
		t.Fatal(err)
	}
	for range 3 {
		if a := e.service.accept(t.Context(), e.request(nwctest.NIP44, "get_info")); a != nil {
			t.Fatalf("stored despite the failure: %+v", a)
		}
	}
	if _, err := e.store.DB.Exec(`DROP TRIGGER rv4`); err != nil {
		t.Fatal(err)
	}
	if a := e.service.accept(t.Context(), e.request(nwctest.NIP44, "get_info")); a == nil || !a.reserved {
		t.Fatalf("no place after failed stores: %+v", a)
	}
}

// Catches: a refusal reply for each request over the budget (S02): past it, events
// are dropped before anything is decrypted or signed.
func TestRefusalRepliesHaveABudget(t *testing.T) {
	e := newEnv(t)
	e.service.burst = 0 // Every request is over the rate.
	e.service.maxRunningEach = 0
	ctx, cancel := context.WithCancel(context.Background())
	defer func() { cancel(); e.service.work.Wait() }()
	waiting := silentPublish(e.service)
	for range publishers {
		e.service.work.Go(func() { e.service.publishLoop(ctx) })
	}
	for range 200 {
		e.service.receive(ctx, e.request(nwctest.NIP44, "get_info"))
	}
	for deadline := time.Now().Add(5 * time.Second); int(waiting.Load())+len(e.service.outbox) < replyRate &&
		time.Now().Before(deadline); {
		time.Sleep(time.Millisecond)
	}
	time.Sleep(100 * time.Millisecond) // Room for any more to arrive.
	// The clock stands still, so the rate allows its burst and no more.
	if replies := int(waiting.Load()) + len(e.service.outbox); replies != replyRate {
		t.Errorf("%d RATE_LIMITED replies built for 200 requests, want %d", replies, replyRate)
	}
}

// Catches: a stranger's flood piling up while the relay loop waits on a relay that
// acknowledges nothing (S03): the info events no longer hold up the loop.
func TestStrangerFloodWhileRelaySilent(t *testing.T) {
	e := newEnv(t)
	events := strangerEvents(t, e.client.ServicePub, 5000, "AAAA?iv=AAAA", 0)
	relay := newFloodRelay(t, false, func(subID string) [][]byte { return frames(subID, events) })
	e.service.relay = relay.url()
	ctx, cancel := context.WithCancel(context.Background())
	goroutines := runtime.NumGoroutine()
	stopped := make(chan struct{})
	go func() { defer close(stopped); e.service.Run(ctx) }()
	peak := 0
	deadline := time.Now().Add(2 * time.Minute) // The race detector slows the signature checks.
	for drained := 0; drained < 100 && time.Now().Before(deadline); {
		peak = max(peak, runtime.NumGoroutine()-goroutines)
		select {
		case <-relay.done:
			drained++
		default:
		}
		time.Sleep(2 * time.Millisecond)
	}
	cancel()
	<-stopped
	if relay.sent.Load() != int64(len(events)) {
		t.Fatalf("only %d of %d events sent", relay.sent.Load(), len(events))
	}
	t.Logf("%d stranger events, relay acknowledges nothing: peak %d goroutines", len(events), peak)
	if peak > 1000 {
		t.Errorf("%d stranger events: peak %d goroutines", len(events), peak)
	}
}

// Catches: an event too large for any request, signed by a real client, being
// hashed, stored or run; and a limit that refuses a request of normal size (S03).
func TestOversizeRequestDropped(t *testing.T) {
	e := newEnv(t)
	tag := func(n int, item string) nostr.Tags {
		tags := nostr.Tags{}
		for range n {
			tags = append(tags, nostr.Tag{"t", item})
		}
		return tags
	}
	for _, c := range []struct {
		name    string
		content string
		tags    nostr.Tags
		dropped bool
	}{
		{"content at the limit", strings.Repeat("A", maxContent), nil, false},
		{"content over", strings.Repeat("A", maxContent+1), nil, true},
		{"4 MB content", strings.Repeat("A", 4<<20), nil, true},
		{"tags at the limit", "x", tag(maxTags-1, "a"), false}, // The p tag is one.
		{"too many tags", "x", tag(maxTags, "a"), true},
		{"20000 tags", "x", tag(20000, "a"), true},
		{"tag item over", "x", tag(1, strings.Repeat("a", maxTagItem+1)), true},
		{"tag too long", "x", nostr.Tags{make(nostr.Tag, maxTagItems+1)}, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			event := nostr.Event{Kind: kindRequest, CreatedAt: e.timestamp(), Content: c.content,
				Tags: append(nostr.Tags{{"p", e.client.ServicePub}}, c.tags...)}
			if err := event.Sign(e.client.Secret); err != nil {
				t.Fatal(err)
			}
			a := e.service.accept(t.Context(), &event)
			if a != nil && a.reserved {
				e.service.release(a.connection.id)
			}
			if dropped := a == nil; dropped != c.dropped {
				t.Errorf("dropped=%v, want %v", dropped, c.dropped)
			}
		})
	}
}

// countHandler counts log records.
type countHandler struct{ records atomic.Int64 }

func (h *countHandler) Enabled(context.Context, slog.Level) bool  { return true }
func (h *countHandler) Handle(context.Context, slog.Record) error { h.records.Add(1); return nil }
func (h *countHandler) WithAttrs([]slog.Attr) slog.Handler        { return h }
func (h *countHandler) WithGroup(string) slog.Handler             { return h }

// Catches: a relay filling the log with notices (S03).
func TestRelayNoticesBounded(t *testing.T) {
	e := newEnv(t)
	counter := &countHandler{}
	e.service.log = slog.New(counter)
	for range 10000 {
		e.service.notice("slow down")
	}
	if got := counter.records.Load(); got != noticeBurst {
		t.Errorf("%d notices logged with the clock still, want %d", got, noticeBurst)
	}
	e.now.Add(int64(noticeEvery / time.Second))
	e.service.notice("slow down")
	if got := counter.records.Load(); got != noticeBurst+1 {
		t.Errorf("%d notices logged after %s, want %d", got, noticeEvery, noticeBurst+1)
	}
}
