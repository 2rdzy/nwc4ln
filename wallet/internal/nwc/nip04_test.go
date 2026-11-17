package nwc

import (
	"bytes"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"github.com/getAlby/go-nostr"

	"github.com/2rdzy/nwc4ln/wallet/internal/nwc/nwctest"
)

// Catches: one malformed NIP-04 event stopping the service. handle runs on the
// test goroutine, so a panic is caught here; in the service, the same panic on a
// worker goroutine ends the process. Both contents made the library panic before
// the pin move: a short IV, and a ciphertext that is not whole blocks.
func TestMalformedNIP04Content(t *testing.T) {
	contents := map[string]string{
		"short IV":            "AA==?iv=AA==",
		"one-byte ciphertext": "AA==?iv=AAAAAAAAAAAAAAAAAAAAAA==",
	}
	for name, content := range contents {
		for _, state := range []string{"live", "revoked", "over the rate"} {
			for _, tagged := range []bool{true, false} {
				t.Run(fmt.Sprintf("%s/%s/tag=%v", name, state, tagged), func(t *testing.T) {
					e := newEnv(t)
					switch state {
					case "revoked":
						if _, err := e.store.DB.Exec(`UPDATE connections SET revoked_at = ?`, e.now.Load()); err != nil {
							t.Fatal(err)
						}
					case "over the rate":
						for range 200 {
							e.handle(e.request(nwctest.NIP44, "get_info"))
						}
					}
					var tags []nostr.Tag
					if tagged {
						tags = append(tags, nostr.Tag{"encryption", nwctest.NIP04})
					}
					event, err := e.client.Seal(content, e.timestamp(), tags...)
					if err != nil {
						t.Fatal(err)
					}
					defer func() {
						if r := recover(); r != nil {
							t.Fatalf("panic: %v", r)
						}
					}()
					reply := e.handle(event)
					if state == "live" {
						if reply == nil {
							t.Fatal("no reply")
						}
						response, err := e.client.Open(nwctest.NIP04, event, reply)
						if err != nil {
							t.Fatal(err)
						}
						wantCode(t, response, codeOther)
					}
				})
			}
		}
	}
}

// Catches: the recover around decryption removed, so a future panic in the
// library stops the service again, or a caught panic that is not logged.
func TestDecryptPanicIsCaught(t *testing.T) {
	e := newEnv(t)
	var logged bytes.Buffer
	e.service.log = slog.New(slog.NewTextHandler(&logged, nil))
	for _, scheme := range []string{nwctest.NIP04, nwctest.NIP44} {
		event := e.request(scheme, "get_info")
		saved04, saved44 := nip04Decrypt, nip44Decrypt
		nip04Decrypt = func(string, []byte) (string, error) { panic("library fault") }
		nip44Decrypt = func(string, [32]byte) (string, error) { panic("library fault") }
		response := func() *nwctest.Response {
			defer func() { nip04Decrypt, nip44Decrypt = saved04, saved44 }()
			return e.answer(event, scheme)
		}()
		wantCode(t, response, codeOther)
	}
	if got := strings.Count(logged.String(), "level=ERROR msg=\"request content made decryption panic\""); got != 2 {
		t.Fatalf("%d error lines, want 2:\n%s", got, logged.String())
	}
}

// Catches: the old encryption still reachable after the operator turned it off,
// or NIP-44 cut off with it.
func TestNIP04Off(t *testing.T) {
	e := newEnv(t)
	e.service.DisableNIP04()

	for _, scheme := range []string{nwctest.NIP04, nwctest.NoTag} {
		wantCode(t, e.answer(e.request(scheme, "get_info"), nwctest.NIP44), codeUnsupportedEncryption)
	}
	if response := e.answer(e.request(nwctest.NIP44, "get_info"), nwctest.NIP44); response.Error != nil {
		t.Fatalf("NIP-44: %+v", response.Error)
	}

	info, err := e.service.infoEvent(e.service.connections[e.client.ServicePub])
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := tagValue(info, "encryption"); got != "nip44_v2" {
		t.Fatalf("encryption %q", got)
	}

	out := &sent{}
	e.service.publish = out.publish
	invoice := e.makeInvoice(e.client, 1_000)
	if err := e.node.Settle(invoice["payment_hash"].(string)); err != nil {
		t.Fatal(err)
	}
	e.service.notify(t.Context(), <-e.service.notifications)
	if len(out.events) != 1 || out.events[0].Kind != kindNotification {
		t.Fatalf("notifications: %+v", out.events)
	}
}
