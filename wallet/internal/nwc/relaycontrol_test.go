package nwc

import (
	"context"
	"encoding/json"
	"flag"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/getAlby/go-nostr"

	"github.com/2rdzy/nwc4ln/wallet/internal/ledger"
	"github.com/2rdzy/nwc4ln/wallet/internal/nwc/nwctest"
)

var recordControlOff = flag.Bool("record-control-off", false, "rewrite testdata/control_off.golden")

// fixedNow is the clock of the recorded runs: 2026-11-19 00:00:00 UTC.
const fixedNow = 1795046400

// onRelay starts a second service on the env's database and ledger, on url, with
// setup run before Run, and makes it the env's service. It stops with the test.
func (e *env) onRelay(url string, setup func(*Service)) *Service {
	e.t.Helper()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	service, err := New(e.t.Context(), e.store, e.node, e.ledger, url, log)
	if err != nil {
		e.t.Fatal(err)
	}
	service.now = func() time.Time { return time.Unix(e.now.Load(), 0) }
	if err := service.load(e.t.Context()); err != nil { // By the test clock.
		e.t.Fatal(err)
	}
	e.service = service
	if setup != nil {
		setup(service)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		service.Run(ctx)
	}()
	e.t.Cleanup(func() {
		// go-nostr's Publish reads its result unguarded if its context ends while the
		// OK arrives, so a test stops only once the last list is answered.
		if service.control != nil {
			waitSettled(e.t, service)
		}
		cancel()
		<-done
	})
	return service
}

// waitSettled waits until the relay has accepted a list with every change, and
// the service has heard so.
func waitSettled(t testing.TB, s *Service) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		c := s.control
		c.mu.Lock()
		settled := c.listed != nil && c.accepted >= c.wanted && c.problem == ""
		c.mu.Unlock()
		if settled {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("the service did not hear the relay accept its list")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// keyNames names each connection's keys by connection ID, as client1 and service1,
// so that a recording does not depend on the random keys.
func (e *env) keyNames() map[string]string {
	e.t.Helper()
	rows, err := e.store.DB.Query(`SELECT client_pubkey, service_key FROM connections ORDER BY id`)
	if err != nil {
		e.t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	names := map[string]string{}
	for i := 1; rows.Next(); i++ {
		var client, secret string
		if err := rows.Scan(&client, &secret); err != nil {
			e.t.Fatal(err)
		}
		service, err := nostr.GetPublicKey(secret)
		if err != nil {
			e.t.Fatal(err)
		}
		names[client] = "client" + string(rune('0'+i))
		names[service] = "service" + string(rune('0'+i))
	}
	return names
}

// normalize turns recorded messages into lines that are the same on every run:
// keys become names, subscription IDs and event IDs and signatures are dropped,
// and each run of EVENT messages is sorted, because info events go out in map
// order.
func normalize(t testing.TB, frames []json.RawMessage, names map[string]string) []string {
	t.Helper()
	rename := func(s string) string {
		if name, ok := names[s]; ok {
			return name
		}
		return s
	}
	var out, events []string
	flush := func() {
		slices.Sort(events)
		out = append(out, events...)
		events = nil
	}
	for _, frame := range frames {
		var message []json.RawMessage
		if err := json.Unmarshal(frame, &message); err != nil {
			t.Fatal(err)
		}
		var name string
		_ = json.Unmarshal(message[0], &name)
		switch name {
		case "EVENT":
			var event nostr.Event
			if err := json.Unmarshal(message[1], &event); err != nil {
				t.Fatal(err)
			}
			tags := make([][]string, len(event.Tags))
			for i, tag := range event.Tags {
				tags[i] = make([]string, len(tag))
				for j, item := range tag {
					tags[i][j] = rename(item)
				}
			}
			line, _ := json.Marshal(map[string]any{
				"kind": event.Kind, "created_at": event.CreatedAt, "pubkey": rename(event.PubKey),
				"content": event.Content, "tags": tags,
			})
			events = append(events, "EVENT "+string(line))
			continue
		case "REQ":
			flush()
			var filter nostr.Filter
			_ = json.Unmarshal(message[2], &filter)
			keys := make([]string, 0, len(filter.Tags["p"]))
			for _, key := range filter.Tags["p"] {
				keys = append(keys, rename(key))
			}
			slices.Sort(keys)
			filter.Tags["p"] = keys
			line, _ := json.Marshal(filter)
			out = append(out, "REQ "+string(line)+" filters="+string(rune('0'+len(message)-2)))
		default:
			flush()
			out = append(out, name)
		}
	}
	flush()
	return out
}

// Catches: relay control off changing what the wallet sends to a relay, such as an
// allow-list sent to a relay that is not ours, or a new message or a new order.
// The reference was recorded on the code before relay control existed (9bb7dc3),
// with a fixed clock: signatures and event IDs change on every run, so they are
// left out, and so are the random keys.
func TestControlOffSendsWhatItDidBefore(t *testing.T) {
	e := newEnv(t)
	e.now.Store(fixedNow)
	relay := newTestRelay(t, "")
	service := e.onRelay(relay.url(), nil)
	relay.waitFor("the first subscription and info event", func(f []json.RawMessage) bool {
		return count(f, "REQ", 0) == 1 && count(f, "EVENT", kindInfo) == 1
	})
	if _, err := service.AddApp(t.Context(), "second", ledger.DefaultApp()); err != nil {
		t.Fatal(err)
	}
	relay.waitFor("the second subscription, and the first closed", func(f []json.RawMessage) bool {
		return count(f, "REQ", 0) == 2 && count(f, "EVENT", kindInfo) == 3 && count(f, "CLOSE", 0) == 1
	})
	if err := service.DeleteApp(t.Context(), e.accountOf(e.client)); err != nil {
		t.Fatal(err)
	}
	frames := relay.waitFor("the third subscription, and the second closed", func(f []json.RawMessage) bool {
		return count(f, "REQ", 0) == 3 && count(f, "EVENT", kindInfo) == 4 && count(f, "CLOSE", 0) == 2
	})
	got := strings.Join(normalize(t, frames, e.keyNames()), "\n") + "\n"
	path := filepath.Join("testdata", "control_off.golden")
	if *recordControlOff {
		if err := os.WriteFile(path, []byte(got), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if got != string(want) {
		t.Fatalf("control off sends something else than before:\ngot:\n%swant:\n%s", got, want)
	}
}

// useControl turns relay control on for a service that onRelay starts.
func useControl(t testing.TB) func(*Service) {
	return func(s *Service) {
		if err := s.UseRelayControl(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
}

// controlRelay makes the env's control key and a test relay that takes lists
// signed by it.
func (e *env) controlRelay() *testRelay {
	e.t.Helper()
	if err := EnsureControlKey(e.t.Context(), e.store); err != nil {
		e.t.Fatal(err)
	}
	pub, _, found, err := ControlPublicKey(e.t.Context(), e.store)
	if err != nil || !found {
		e.t.Fatalf("control key: found %v, %v", found, err)
	}
	return newTestRelay(e.t, pub)
}

// waitPairs waits until the relay's saved list holds the named pairs, in order,
// such as "client1 service1".
func (e *env) waitPairs(relay *testRelay, want ...string) {
	e.t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		names := e.keyNames()
		pairs, _ := relay.saved()
		got := make([]string, len(pairs))
		for i, pair := range pairs {
			got[i] = names[pair[0]] + " " + names[pair[1]]
		}
		if slices.Equal(got, want) {
			return
		}
		if time.Now().After(deadline) {
			e.t.Fatalf("relay's list: got %q, want %q", got, want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func noRefusals(t testing.TB, relay *testRelay) {
	t.Helper()
	if refused := relay.refusals(); len(refused) > 0 {
		t.Fatalf("the relay refused %d messages: %q", len(refused), refused)
	}
}

// Catches: the allow-list and the connections the wallet serves disagreeing, after
// a create, a replace, a revoke, and the end of a revoked key's 30 days; and the
// list, subscription and info events going out in an order the relay refuses.
func TestControlListFollowsConnections(t *testing.T) {
	e := newEnv(t)
	relay := e.controlRelay()
	service := e.onRelay(relay.url(), useControl(t))
	e.waitPairs(relay, "client1 service1")
	frames := relay.waitFor("the subscription and the info event", func(f []json.RawMessage) bool {
		return count(f, "REQ", 0) == 1 && count(f, "EVENT", kindInfo) == 1
	})
	var order []string
	for _, f := range frames {
		name, kind := frameKind(f)
		order = append(order, name+" "+strconv.Itoa(kind))
	}
	if want := []string{"EVENT 29194", "REQ 0", "EVENT 13194"}; !slices.Equal(order, want) {
		t.Fatalf("on connect: got %q, want %q", order, want)
	}

	second, err := service.AddApp(t.Context(), "second", ledger.DefaultApp())
	if err != nil {
		t.Fatal(err)
	}
	e.waitPairs(relay, "client1 service1", "client2 service2")
	secondClient, err := nwctest.Parse(second)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.ReplaceConnection(t.Context(), e.accountOf(secondClient)); err != nil {
		t.Fatal(err)
	}
	// The replaced key is revoked, and stays listed for its 30 days.
	e.waitPairs(relay, "client1 service1", "client2 service2", "client3 service3")
	_, before := relay.saved()
	if err := service.DeleteApp(t.Context(), e.accountOf(e.client)); err != nil {
		t.Fatal(err)
	}
	relay.waitFor("the list after the delete", func([]json.RawMessage) bool {
		_, lists := relay.saved()
		return lists > before
	})
	e.waitPairs(relay, "client1 service1", "client2 service2", "client3 service3")

	e.now.Add(int64((revokedKeep + time.Minute) / time.Second)) // Revokes used the real clock.
	if err := service.reload(t.Context()); err != nil {
		t.Fatal(err)
	}
	e.waitPairs(relay, "client3 service3")
	noRefusals(t, relay)
}

// Catches: a second list in the same second stamped no later than the first, which
// the relay drops as not newer.
func TestControlTwoChangesInOneSecond(t *testing.T) {
	e := newEnv(t)
	e.now.Store(fixedNow)
	relay := e.controlRelay()
	service := e.onRelay(relay.url(), useControl(t))
	e.waitPairs(relay, "client1 service1")
	for _, name := range []string{"second", "third"} {
		if _, err := service.AddApp(t.Context(), name, ledger.DefaultApp()); err != nil {
			t.Fatal(err)
		}
	}
	e.waitPairs(relay, "client1 service1", "client2 service2", "client3 service3")
	noRefusals(t, relay)
	var last nostr.Timestamp
	for _, f := range relay.received() {
		if _, kind := frameKind(f); kind != kindAllowList {
			continue
		}
		var message []json.RawMessage
		var event nostr.Event
		_ = json.Unmarshal(f, &message)
		_ = json.Unmarshal(message[1], &event)
		if event.CreatedAt <= last {
			t.Fatalf("list stamped %d after one stamped %d", event.CreatedAt, last)
		}
		last = event.CreatedAt
	}
}

// Catches: a wallet that cannot recover when the relay holds a list newer than its
// own clock, and loops on backoff instead of stamping past the relay's list.
func TestControlNotNewerRestamps(t *testing.T) {
	e := newEnv(t)
	e.now.Store(fixedNow)
	relay := e.controlRelay()
	relay.listAt = fixedNow + 30
	e.onRelay(relay.url(), useControl(t))
	e.waitPairs(relay, "client1 service1")
	refused := relay.refusals()
	if len(refused) != 1 || !strings.Contains(refused[0], "not newer") {
		t.Fatalf("refusals: %q", refused)
	}
	relay.mu.Lock()
	defer relay.mu.Unlock()
	if relay.listAt != fixedNow+31 || relay.lists != 1 {
		t.Fatalf("list at %d after %d lists, want %d after 1", relay.listAt, relay.lists, fixedNow+31)
	}
}

// Catches: the wallet taking the time in a clock refusal for the relay's saved
// list, which would stamp every later list far ahead; and a refusal the admin
// cannot see, or one that stays shown after the relay accepts a list.
func TestControlClockRefusal(t *testing.T) {
	e := newEnv(t)
	e.now.Store(fixedNow)
	relay := e.controlRelay()
	relay.refuse = "invalid: created_at 1795049999 is more than 1m0s ahead of the relay's clock (1795046400)"
	service := e.onRelay(relay.url(), useControl(t))
	relay.waitFor("the refusal", func([]json.RawMessage) bool { return len(relay.refusals()) == 1 })
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, _, problem := service.RelayState(); strings.Contains(problem, "ahead of the relay's clock") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the refusal is not shown")
		}
		time.Sleep(10 * time.Millisecond)
	}
	e.waitPairs(relay, "client1 service1") // After the backoff.
	relay.mu.Lock()
	at := relay.listAt
	relay.mu.Unlock()
	if at != fixedNow+1 {
		t.Fatalf("list after the refusal stamped %d, want %d", at, fixedNow+1)
	}
	waitSettled(t, service) // The refusal is no longer shown.
}

// Catches: a relay that keeps the old pairs after the last connection goes, because
// the wallet does not connect with nothing to serve; and a subscription that names
// no key, which the relay refuses. A new connection must then be served.
func TestControlNoConnections(t *testing.T) {
	e := newEnv(t)
	if err := e.service.DeleteApp(t.Context(), e.accountOf(e.client)); err != nil {
		t.Fatal(err)
	}
	e.now.Add(int64((revokedKeep + time.Minute) / time.Second)) // Revokes used the real clock.
	relay := e.controlRelay()
	relay.listAt = 1 // A stale list from before.
	relay.pairs = [][2]string{{strings.Repeat("a", 64), strings.Repeat("b", 64)}}
	service := e.onRelay(relay.url(), useControl(t))
	e.waitPairs(relay)
	time.Sleep(200 * time.Millisecond)
	if n := count(relay.received(), "REQ", 0); n != 0 {
		t.Fatalf("%d subscriptions with no connection", n)
	}
	if _, err := service.AddApp(t.Context(), "second", ledger.DefaultApp()); err != nil {
		t.Fatal(err)
	}
	e.waitPairs(relay, "client2 service2")
	relay.waitFor("a subscription", func(f []json.RawMessage) bool { return count(f, "REQ", 0) == 1 })
	noRefusals(t, relay)
}

// Catches: the two programs drifting apart on the list format, or the list sent
// with the wrong kind, such as the request kind 23194. testdata/allowlist.json is
// a copy of the relay's golden list.
func TestControlGoldenList(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("testdata", "allowlist.json"))
	if err != nil {
		t.Fatal(err)
	}
	var golden nostr.Event
	if err := json.Unmarshal(data, &golden); err != nil {
		t.Fatal(err)
	}
	pub := func(secret string) string {
		key, err := nostr.GetPublicKey(strings.Repeat("0", 62) + secret)
		if err != nil {
			t.Fatal(err)
		}
		return key
	}
	e := newEnv(t)
	e.service.control = &control{secret: strings.Repeat("0", 63) + "1"}
	e.now.Store(int64(golden.CreatedAt))
	// Given out of order: the list is by connection ID.
	event, err := e.service.allowList([]connection{
		{id: 8, clientPub: pub("b1"), servicePub: pub("b2")},
		{id: 3, clientPub: pub("a1"), servicePub: pub("a2")},
	}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if event.Kind != 29194 || event.ID != golden.ID || event.PubKey != golden.PubKey {
		t.Fatalf("list: kind %d, id %s, pubkey %s; golden: kind %d, id %s, pubkey %s",
			event.Kind, event.ID, event.PubKey, golden.Kind, golden.ID, golden.PubKey)
	}
	if ok, err := event.CheckSignature(); !ok || err != nil {
		t.Fatalf("signature: %v, %v", ok, err)
	}
}

// Catches: the allow-list sent in clear text across the network, or a local
// relay refused.
func TestCheckControlRelay(t *testing.T) {
	for relay, ok := range map[string]bool{
		"wss://relay.example.com":    true,
		"ws://127.0.0.1:7447":        true,
		"ws://127.0.0.1/":            true,
		"ws://localhost:7447":        true,
		"ws://[::1]:7447":            true,
		"ws://relay.example.com":     false,
		"ws://192.168.1.5:7447":      false,
		"ws://127.0.0.1.example.com": false,
	} {
		if err := CheckControlRelay(relay); (err == nil) != ok {
			t.Errorf("%s: %v", relay, err)
		}
	}
}

// Catches: a list too large for the relay sent anyway, so the wallet loops on
// refusals, or the admin not told why.
func TestControlListTooLarge(t *testing.T) {
	e := newEnv(t)
	e.service.control = &control{secret: nostr.GeneratePrivateKey(), acceptedNow: make(chan struct{})}
	e.service.connectionsMu.Lock()
	for i := range 1000 {
		c := connection{id: int64(i + 100), clientPub: nostr.GeneratePrivateKey(), servicePub: nostr.GeneratePrivateKey()}
		e.service.connections[c.servicePub] = c
	}
	e.service.connectionsMu.Unlock()
	if _, err := e.service.sendList(t.Context(), nil); err == nil || !strings.Contains(err.Error(), "over the relay's limit") {
		t.Fatalf("got %v", err)
	}
	if _, _, problem := e.service.RelayState(); !strings.Contains(problem, "1001 connections") {
		t.Fatalf("problem shown: %q", problem)
	}
}

// Catches: a connection string shown before the relay has the list that names
// it, so the app's first request is refused; or a page that waits for ever when
// the relay does not answer.
func TestWaitListed(t *testing.T) {
	e := newEnv(t)
	relay := e.controlRelay()
	service := e.onRelay(relay.url(), useControl(t))
	e.waitPairs(relay, "client1 service1")
	hold := make(chan struct{})
	relay.mu.Lock()
	relay.holdOK = hold
	relay.mu.Unlock()
	service.control.wait = 200 * time.Millisecond
	if _, err := service.AddApp(t.Context(), "second", ledger.DefaultApp()); err != nil {
		t.Fatal(err)
	}
	if service.WaitListed(t.Context()) {
		t.Fatal("listed before the relay answered")
	}
	close(hold)
	service.control.wait = 5 * time.Second
	if !service.WaitListed(t.Context()) {
		t.Fatal("not listed after the relay answered")
	}
}

// Catches: a control key made again, which would lock the wallet out of a relay
// set up with the first one.
func TestControlKeyMadeOnce(t *testing.T) {
	e := newEnv(t)
	if _, _, found, err := ControlPublicKey(t.Context(), e.store); found || err != nil {
		t.Fatalf("before: found %v, %v", found, err)
	}
	if err := EnsureControlKey(t.Context(), e.store); err != nil {
		t.Fatal(err)
	}
	first, npub, _, _ := ControlPublicKey(t.Context(), e.store)
	if err := EnsureControlKey(t.Context(), e.store); err != nil {
		t.Fatal(err)
	}
	again, _, _, _ := ControlPublicKey(t.Context(), e.store)
	if first != again || !strings.HasPrefix(npub, "npub1") {
		t.Fatalf("key %s, then %s (%s)", first, again, npub)
	}
}

// Catches: an info event published for a key the relay has not listed yet, which
// the relay refuses; the list that names the key asks for it again.
func TestInfoWaitsForTheList(t *testing.T) {
	e := newEnv(t)
	e.addApp("second")
	if err := e.service.load(t.Context()); err != nil {
		t.Fatal(err)
	}
	e.service.control = &control{listed: map[string]bool{e.client.ServicePub: true}}
	var published []string
	e.service.publish = func(_ context.Context, event nostr.Event) error {
		published = append(published, event.PubKey)
		return nil
	}
	e.service.publishInfo(t.Context())
	if !slices.Equal(published, []string{e.client.ServicePub}) {
		t.Fatalf("info events for %q, want only the listed %s", published, e.client.ServicePub)
	}
}
