package nwc

import (
	"strconv"
	"testing"
	"time"

	"github.com/getAlby/go-nostr"

	"github.com/2rdzy/nwc4ln/internal/ledger"
	"github.com/2rdzy/nwc4ln/internal/nwc/nwctest"
)

// Catches: a revoked key that can grow the requests table, or make the service work,
// for as long as it runs. It must still learn it was revoked.
func TestRevokedConnectionCostsNothing(t *testing.T) {
	e := newEnv(t)
	if _, err := e.store.DB.Exec(`UPDATE connections SET revoked_at = ?`, e.now.Load()); err != nil {
		t.Fatal(err)
	}
	before := e.requests()
	wantCode(t, e.answer(e.request(nwctest.NIP44, "get_info"), nwctest.NIP44), codeUnauthorized)
	for range 30 {
		if reply := e.handle(e.request(nwctest.NIP44, "get_info")); reply != nil {
			t.Fatal("a second reply within the minute")
		}
	}
	e.waitRefusal()
	wantCode(t, e.answer(e.request(nwctest.NIP44, "get_info"), nwctest.NIP44), codeUnauthorized)
	if got := e.requests() - before; got != 0 {
		t.Fatalf("%d rows stored for a revoked key", got)
	}
}

// Catches: a key revoked long ago still listened for, or one revoked recently already
// dropped, so its client never hears UNAUTHORIZED.
func TestRevokedKeyDroppedAfter30Days(t *testing.T) {
	e := newEnv(t)
	revokedAt := e.now.Load()
	if _, err := e.store.DB.Exec(`UPDATE connections SET revoked_at = ?`, revokedAt); err != nil {
		t.Fatal(err)
	}
	e.now.Store(revokedAt + int64((revokedKeep-time.Hour)/time.Second))
	if err := e.service.reload(t.Context()); err != nil {
		t.Fatal(err)
	}
	if len(e.service.connectionList()) != 1 {
		t.Fatal("a key revoked under 30 days ago was dropped")
	}
	e.now.Store(revokedAt + int64((revokedKeep+time.Hour)/time.Second))
	if err := e.service.reload(t.Context()); err != nil {
		t.Fatal(err)
	}
	if len(e.service.connectionList()) != 0 {
		t.Fatal("a key revoked over 30 days ago is still listened for")
	}
}

// Catches: a payment request a relay held back running after the client gave up, a
// fresh one refused, the limit applied when the client set its own expiration, or
// applied to make_invoice.
func TestPayAgeLimit(t *testing.T) {
	e := newEnv(t)
	pay := func(age time.Duration, tags ...nostr.Tag) *nwctest.Response {
		t.Helper()
		params := map[string]any{"invoice": e.node.OutsideInvoice(sat)}
		event, err := e.client.RequestWith(nwctest.NIP44, "pay_invoice", params,
			nostr.Timestamp(e.now.Load()-int64(age/time.Second)), tags...)
		if err != nil {
			t.Fatal(err)
		}
		reply := e.handle(event)
		if reply == nil {
			t.Fatal("no reply")
		}
		response, err := e.client.Open(nwctest.NIP44, event, reply)
		if err != nil {
			t.Fatal(err)
		}
		return response
	}
	if response := pay(59 * time.Second); response.Error != nil {
		t.Fatalf("59 seconds old: %+v", response.Error)
	}
	response := pay(61 * time.Second)
	wantCode(t, response, codeOther)
	if response.ResultType != "pay_invoice" {
		t.Fatalf("result_type %q", response.ResultType)
	}
	future := nostr.Tag{"expiration", strconv.FormatInt(e.now.Load()+600, 10)}
	if response := pay(5*time.Minute, future); response.Error != nil {
		t.Fatalf("5 minutes old with an expiration tag: %+v", response.Error)
	}
	event, err := e.client.RequestWith(nwctest.NIP44, "make_invoice", map[string]any{"amount": sat},
		nostr.Timestamp(e.now.Load()-5*3600))
	if err != nil {
		t.Fatal(err)
	}
	reply, err := e.client.Open(nwctest.NIP44, event, e.handle(event))
	if err != nil || reply.Error != nil {
		t.Fatalf("make_invoice 5 hours old: %+v, %v", reply, err)
	}
}

// Catches: invoices made faster than a person needs, or the limit spilling over to
// other methods, or never refilling.
func TestMakeInvoiceRate(t *testing.T) {
	e := newEnv(t)
	params := map[string]any{"amount": sat}
	for i := range invoiceBurst {
		if response := e.call(e.client, "make_invoice", params); response.Error != nil {
			t.Fatalf("invoice %d: %+v", i, response.Error)
		}
	}
	response := e.call(e.client, "make_invoice", params)
	wantCode(t, response, codeRateLimited)
	if response.ResultType != "make_invoice" {
		t.Fatalf("result_type %q", response.ResultType)
	}
	if response := e.call(e.client, "get_info", nil); response.Error != nil {
		t.Fatalf("get_info after the invoice limit: %+v", response.Error)
	}
	e.now.Add(int64(invoiceEvery / time.Second))
	if response := e.call(e.client, "make_invoice", params); response.Error != nil {
		t.Fatalf("after the refill: %+v", response.Error)
	}
}

// Catches: a prune that removes a row a payment refers to, which breaks the link or the
// foreign key, or a recent row, which lets a repeat run again.
func TestPruneRequests(t *testing.T) {
	e := newEnv(t)
	now := e.now.Load()
	old := now - int64((pruneAfter+time.Hour)/time.Second)
	recent := now - int64((pruneAfter-time.Hour)/time.Second)
	insert := func(id string, created int64) int64 {
		t.Helper()
		result, err := e.store.DB.Exec(`INSERT INTO requests (nostr_event_id, state, created_at) VALUES (?, 'done', ?)`,
			id, created)
		if err != nil {
			t.Fatal(err)
		}
		rowID, err := result.LastInsertId()
		if err != nil {
			t.Fatal(err)
		}
		return rowID
	}
	insert("old", old)
	insert("recent", recent)
	paid := insert("old-paid", old)
	// Only the ledger writes transactions, so the row comes from it.
	_, err := e.ledger.MakeInvoice(t.Context(), ledger.InvoiceRequest{
		AccountID: e.accountOf(e.client), AmountMsat: sat, RequestID: paid,
	})
	if err != nil {
		t.Fatal(err)
	}
	if removed, err := e.service.prune(t.Context()); err != nil || removed != 1 {
		t.Fatalf("removed %d, %v", removed, err)
	}
	for id, want := range map[string]int{"old": 0, "recent": 1, "old-paid": 1} {
		var count int
		if err := e.store.DB.QueryRow(`SELECT count(*) FROM requests WHERE nostr_event_id = ?`, id).Scan(&count); err != nil ||
			count != want {
			t.Errorf("%s: %d rows, %v", id, count, err)
		}
	}
}

// Catches: a client told a payment failed, or nothing clear, while the node is behind
// the chain and payments out are paused.
func TestPayWhileNodeNotSynced(t *testing.T) {
	e := newEnv(t)
	e.node.SetSynced(false)
	e.restart() // The ledger reads the node's state when it starts.
	response := e.call(e.client, "pay_invoice", map[string]any{"invoice": e.node.OutsideInvoice(sat)})
	wantCode(t, response, codeOther)
	if response.Error.Message != "the node is catching up with the chain, try again later" {
		t.Fatalf("message %q", response.Error.Message)
	}
}
