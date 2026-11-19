package allowlist

import (
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"fiatjaf.com/nostr"
)

var update = flag.Bool("update", false, "rewrite testdata/allowlist.json")

// Fixed keys, so the golden file is the same on every run.
var (
	controlSecret = mustSecret("0000000000000000000000000000000000000000000000000000000000000001")
	otherSecret   = mustSecret("0000000000000000000000000000000000000000000000000000000000000002")
	control       = controlSecret.Public()
	clientA       = mustSecret("00000000000000000000000000000000000000000000000000000000000000a1").Public().Hex()
	serviceA      = mustSecret("00000000000000000000000000000000000000000000000000000000000000a2").Public().Hex()
	clientB       = mustSecret("00000000000000000000000000000000000000000000000000000000000000b1").Public().Hex()
	serviceB      = mustSecret("00000000000000000000000000000000000000000000000000000000000000b2").Public().Hex()
)

// goldenTime is the golden list's created_at: 2026-11-19 00:00:00 UTC.
const goldenTime = 1795046400

func mustSecret(h string) nostr.SecretKey {
	key, err := nostr.SecretKeyFromHex(h)
	if err != nil {
		panic(err)
	}
	return key
}

func signed(t testing.TB, secret nostr.SecretKey, created int64, tags nostr.Tags) nostr.Event {
	t.Helper()
	event := nostr.Event{Kind: Kind, CreatedAt: nostr.Timestamp(created), Tags: tags}
	if err := event.Sign(secret); err != nil {
		t.Fatal(err)
	}
	return event
}

func pairs(keys ...string) nostr.Tags {
	tags := nostr.Tags{}
	for i := 0; i+1 < len(keys); i += 2 {
		tags = append(tags, nostr.Tag{"pair", keys[i], keys[i+1]})
	}
	return tags
}

// The golden file is the list format the wallet must write (WP17). A change to
// the format changes this file, and WP18's system test catches a wallet that was
// not changed with it.
func TestGoldenList(t *testing.T) {
	path := filepath.Join("testdata", "allowlist.json")
	if *update {
		event := signed(t, controlSecret, goldenTime, pairs(clientA, serviceA, clientB, serviceB))
		data, err := event.MarshalJSON()
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, append(data, '\n'), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	list, err := Read(data, control)
	if err != nil {
		t.Fatal(err)
	}
	if list.Len() != 2 || int64(list.Event.CreatedAt) != goldenTime {
		t.Fatalf("golden list: %d pairs at %d", list.Len(), list.Event.CreatedAt)
	}
	for client, service := range map[string]string{clientA: serviceA, clientB: serviceB} {
		got, ok := list.ServiceOf(nostr.MustPubKeyFromHex(client))
		if !ok || got != service {
			t.Errorf("client %s: service %q, %v", client, got, ok)
		}
	}
	if !list.IsServiceHex(serviceA) || list.IsServiceHex(clientA) {
		t.Error("service keys and client keys are mixed up")
	}
}

func TestParseRefuses(t *testing.T) {
	upper := strings.ToUpper(serviceA)
	cases := map[string]nostr.Event{
		"other kind": func() nostr.Event {
			e := nostr.Event{Kind: 29195, CreatedAt: goldenTime, Tags: pairs(clientA, serviceA)}
			_ = e.Sign(controlSecret)
			return e
		}(),
		"other signer":       signed(t, otherSecret, goldenTime, pairs(clientA, serviceA)),
		"uppercase hex":      signed(t, controlSecret, goldenTime, pairs(clientA, upper)),
		"short key":          signed(t, controlSecret, goldenTime, pairs(clientA, serviceA[:63])),
		"other tag":          signed(t, controlSecret, goldenTime, nostr.Tags{{"p", serviceA}}),
		"extra field":        signed(t, controlSecret, goldenTime, nostr.Tags{{"pair", clientA, serviceA, "x"}}),
		"repeated client":    signed(t, controlSecret, goldenTime, pairs(clientA, serviceA, clientA, serviceB)),
		"repeated service":   signed(t, controlSecret, goldenTime, pairs(clientA, serviceA, clientB, serviceA)),
		"client is service":  signed(t, controlSecret, goldenTime, pairs(clientA, serviceA, serviceA, serviceB)),
		"same key twice":     signed(t, controlSecret, goldenTime, pairs(clientA, clientA)),
		"control key listed": signed(t, controlSecret, goldenTime, pairs(control.Hex(), serviceA)),
		"content": func() nostr.Event {
			e := nostr.Event{Kind: Kind, CreatedAt: goldenTime, Content: "x"}
			_ = e.Sign(controlSecret)
			return e
		}(),
		"bad signature": func() nostr.Event {
			e := signed(t, controlSecret, goldenTime, pairs(clientA, serviceA))
			e.Sig[0] ^= 1
			return e
		}(),
		"changed after signing": func() nostr.Event {
			e := signed(t, controlSecret, goldenTime, pairs(clientA, serviceA))
			e.Tags[0][2] = serviceB
			return e
		}(),
	}
	for name, event := range cases {
		if _, err := Parse(event, control); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	if _, err := Parse(signed(t, controlSecret, goldenTime, nil), control); err != nil {
		t.Errorf("empty list refused: %v", err)
	}
}

func TestOfferWindowAndReplay(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, control)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(goldenTime, 0)
	first := signed(t, controlSecret, goldenTime, pairs(clientA, serviceA))
	if res := store.Offer(first, now); !res.Accepted {
		t.Fatalf("first list refused: %s", res.Reason)
	}
	if res := store.Offer(first, now); !res.Accepted {
		t.Fatalf("the same list sent again was refused: %s", res.Reason)
	}
	older := signed(t, controlSecret, goldenTime-1, pairs(clientB, serviceB))
	res := store.Offer(older, now)
	if res.Accepted || !res.FromControl || !strings.Contains(res.Reason, "created_at 1795046400") {
		t.Fatalf("older list: %+v", res)
	}
	sameTime := signed(t, controlSecret, goldenTime, pairs(clientB, serviceB))
	if res := store.Offer(sameTime, now); res.Accepted {
		t.Fatal("a different list with the same time was accepted")
	}
	future := signed(t, controlSecret, goldenTime+61, pairs(clientB, serviceB))
	if res := store.Offer(future, now); res.Accepted || !strings.Contains(res.Reason, "ahead") {
		t.Fatalf("list 61 s ahead: %+v", res)
	}
	stranger := signed(t, otherSecret, goldenTime+1, pairs(clientB, serviceB))
	if res := store.Offer(stranger, now); res.Accepted || res.FromControl {
		t.Fatalf("stranger's list: %+v", res)
	}
	newer := signed(t, controlSecret, goldenTime+60, pairs(clientB, serviceB))
	if res := store.Offer(newer, now); !res.Accepted {
		t.Fatalf("newer list refused: %s", res.Reason)
	}
	if store.Current().IsClient(nostr.MustPubKeyFromHex(clientA)) {
		t.Fatal("the old pair is still listed")
	}

	// A restart keeps the list, and the file allows its owner only.
	info, err := os.Stat(filepath.Join(dir, FileName))
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("saved file: %v, %v", info, err)
	}
	again, err := Open(dir, control)
	if err != nil {
		t.Fatal(err)
	}
	if again.Current() == nil || again.Current().Event.ID != newer.ID {
		t.Fatal("a restart lost the list")
	}
	if res := again.Offer(first, now); res.Accepted {
		t.Fatal("after a restart, the older list was accepted again")
	}

	// With the file gone, an old list is still refused by its age.
	if err := os.Remove(filepath.Join(dir, FileName)); err != nil {
		t.Fatal(err)
	}
	lost, err := Open(dir, control)
	if err != nil {
		t.Fatal(err)
	}
	if res := lost.Offer(first, now.Add(MaxAge+time.Second)); res.Accepted || !strings.Contains(res.Reason, "behind") {
		t.Fatalf("old list replayed to a relay that lost its file: %+v", res)
	}
}

func TestOpenRefusesBadFile(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, FileName), []byte("{"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dir, control); err == nil || !strings.Contains(err.Error(), "delete the file") {
		t.Fatalf("broken file: %v", err)
	}
	// A list signed by another key, for example after the control key changed.
	data, _ := signed(t, otherSecret, goldenTime, nil).MarshalJSON()
	if err := os.WriteFile(filepath.Join(dir, FileName), data, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dir, control); err == nil {
		t.Fatal("a list signed by another key was loaded")
	}
}

// FuzzAllowList feeds the reader of saved and offered lists crafted input. The
// seeds are valid lists, so mutations reach the tag checks before the signature.
func FuzzAllowList(f *testing.F) {
	golden, err := os.ReadFile(filepath.Join("testdata", "allowlist.json"))
	if err != nil {
		f.Fatal(err)
	}
	f.Add(golden)
	empty, _ := signed(f, controlSecret, goldenTime, nil).MarshalJSON()
	f.Add(empty)
	f.Add([]byte(`{"kind":29194,"tags":[["pair"]],"pubkey":"` + control.Hex() + `"}`))
	f.Fuzz(func(t *testing.T, data []byte) {
		list, err := Read(data, control)
		if err != nil {
			return
		}
		// A list that reads must be signed by the control key and keep its pairs.
		if list.Event.PubKey != control || !list.Event.VerifySignature() {
			t.Fatal("accepted a list not signed by the control key")
		}
		if list.Len() != len(list.Event.Tags) {
			t.Fatalf("%d pairs from %d tags", list.Len(), len(list.Event.Tags))
		}
	})
}
