package nwc

import (
	"context"
	"encoding/json"
	"maps"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/getAlby/go-nostr"

	"github.com/2rdzy/nwc4ln/internal/ledger"
	"github.com/2rdzy/nwc4ln/internal/node"
	"github.com/2rdzy/nwc4ln/internal/node/fake"
	"github.com/2rdzy/nwc4ln/internal/nwc/nwctest"
)

const sat = int64(1_000)

// call sends a request with params and returns the reply.
func (e *env) call(client *nwctest.Client, method string, params any) *nwctest.Response {
	e.t.Helper()
	event, err := client.RequestWith(nwctest.NIP44, method, params, e.timestamp())
	if err != nil {
		e.t.Fatal(err)
	}
	reply := e.handle(event)
	if reply == nil {
		e.t.Fatal("no reply")
	}
	response, err := client.Open(nwctest.NIP44, event, reply)
	if err != nil {
		e.t.Fatal(err)
	}
	return response
}

func (e *env) result(client *nwctest.Client, method string, params any) map[string]any {
	e.t.Helper()
	response := e.call(client, method, params)
	if response.Error != nil {
		e.t.Fatalf("%s: %+v", method, response.Error)
	}
	var result map[string]any
	if err := json.Unmarshal(response.Result, &result); err != nil {
		e.t.Fatalf("%s: %q: %v", method, response.Result, err)
	}
	return result
}

func (e *env) makeInvoice(client *nwctest.Client, amount int64) map[string]any {
	e.t.Helper()
	return e.result(client, "make_invoice", map[string]any{"amount": amount, "description": "for tests"})
}

func (e *env) lookup(client *nwctest.Client, hash any) map[string]any {
	e.t.Helper()
	return e.result(client, "lookup_invoice", map[string]any{"payment_hash": hash})
}

func (e *env) list(client *nwctest.Client, params any) []any {
	e.t.Helper()
	return e.result(client, "list_transactions", params)["transactions"].([]any)
}

func (e *env) waitState(client *nwctest.Client, hash any, want string) map[string]any {
	e.t.Helper()
	var got map[string]any
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(5 * time.Millisecond) {
		if got = e.lookup(client, hash); got["state"] == want {
			return got
		}
	}
	e.t.Fatalf("state stayed %v, want %s", got["state"], want)
	return nil
}

func keys(result map[string]any) []string { return slices.Sorted(maps.Keys(result)) }

func wantKeys(t *testing.T, what string, result map[string]any, want ...string) {
	t.Helper()
	slices.Sort(want)
	if got := keys(result); !slices.Equal(got, want) {
		t.Errorf("%s: keys %v, want %v", what, got, want)
	}
}

// Catches: wrong field names or units, against the examples in NIP-47 and NWC-05.
func TestCommandShapes(t *testing.T) {
	e := newEnv(t)
	e.node.SetSpendable(node.Spendable{ActiveMsat: 5_000 * sat, AllMsat: 5_000 * sat})
	wantKeys(t, "get_balance", e.result(e.client, "get_balance", nil), "balance")

	invoice := e.makeInvoice(e.client, 2_500)
	wantKeys(t, "make_invoice", invoice, "type", "state", "invoice", "description", "payment_hash", "amount",
		"fees_paid", "created_at", "expires_at")
	if invoice["type"] != "incoming" || invoice["state"] != "pending" || invoice["amount"] != float64(2_500) ||
		invoice["description"] != "for tests" {
		t.Errorf("make_invoice: %v", invoice)
	}

	if err := e.node.Settle(invoice["payment_hash"].(string)); err != nil {
		t.Fatal(err)
	}
	settled := e.waitState(e.client, invoice["payment_hash"], "settled")
	wantKeys(t, "lookup_invoice", settled, "type", "state", "invoice", "description", "preimage", "payment_hash",
		"amount", "fees_paid", "created_at", "expires_at", "settled_at")

	paid := e.result(e.client, "pay_invoice", map[string]any{"invoice": e.node.OutsideInvoice(3 * sat)})
	wantKeys(t, "pay_invoice", paid, "preimage", "fees_paid")

	listed := e.result(e.client, "list_transactions", map[string]any{})
	wantKeys(t, "list_transactions", listed, "transactions", "total_count")
	if listed["total_count"] != float64(2) {
		t.Errorf("total_count %v", listed["total_count"])
	}
	row := listed["transactions"].([]any)[0].(map[string]any)
	if row["type"] != "outgoing" || row["amount"] != float64(3*sat) {
		t.Errorf("row %v", row)
	}
}

// Catches: the SDK rejecting a result: it needs an array, and a balance that is
// there even at 0.
func TestEmptyResultsKeepTheirFields(t *testing.T) {
	e := newEnv(t)
	response := e.call(e.client, "list_transactions", nil)
	if !strings.Contains(string(response.Result), `"transactions":[]`) {
		t.Errorf("list_transactions: %s", response.Result)
	}
	response = e.call(e.client, "get_balance", nil)
	if !strings.Contains(string(response.Result), `"balance":0`) {
		t.Errorf("get_balance: %s", response.Result)
	}
}

// Catches: a wrong balance, one below 0, and a made-up figure with the node away.
func TestGetBalance(t *testing.T) {
	e := newEnv(t)
	balance := func() any { return e.result(e.client, "get_balance", nil)["balance"] }
	e.node.SetSpendable(node.Spendable{ActiveMsat: 500 * sat, AllMsat: 900 * sat})
	if got := balance(); got != float64(500*sat) {
		t.Fatalf("got %v", got)
	}
	e.node.ScriptPay(fake.PayTimeout)
	wantCode(t, e.call(e.client, "pay_invoice", map[string]any{"invoice": e.node.OutsideInvoice(100 * sat)}), codeOther)
	if got := balance(); got != float64(390*sat) { // 100 sat plus the 10 sat reserve.
		t.Fatalf("with a hold: got %v", got)
	}
	e.node.SetSpendable(node.Spendable{ActiveMsat: 50 * sat})
	if got := balance(); got != float64(0) {
		t.Fatalf("below 0: got %v", got)
	}
	e.node.SetUnreachable(true)
	wantCode(t, e.call(e.client, "get_balance", nil), codeInternal)
}

// Catches: PAYMENT_FAILED for anything that may still pay, which makes a client pay
// again. Each refusal also leaves no hold.
func TestPayInvoiceReplies(t *testing.T) {
	e := newEnv(t)
	pay := func(invoice string) *nwctest.Response {
		return e.call(e.client, "pay_invoice", map[string]any{"invoice": invoice})
	}

	t.Run("Settled", func(t *testing.T) {
		e.node.SetRouteFee(7)
		defer e.node.SetRouteFee(0)
		invoice := e.node.OutsideInvoice(1_000 * sat)
		var result struct {
			Preimage string `json:"preimage"`
			FeesPaid int64  `json:"fees_paid"`
		}
		response := pay(invoice)
		if err := json.Unmarshal(response.Result, &result); err != nil {
			t.Fatal(err)
		}
		if len(result.Preimage) != 64 || result.FeesPaid != 7 {
			t.Fatalf("got %+v", result)
		}
	})
	t.Run("NodeSaysFailed", func(t *testing.T) {
		e.node.ScriptPay(fake.PayFail)
		wantCode(t, pay(e.node.OutsideInvoice(1_000*sat)), codePaymentFailed)
	})
	t.Run("AlreadyPaid", func(t *testing.T) {
		invoice := e.node.OutsideInvoice(1_000 * sat)
		if response := pay(invoice); response.Error != nil {
			t.Fatalf("got %+v", response.Error)
		}
		wantCode(t, pay(invoice), codeOther)
	})
	t.Run("OwnInvoiceClosed", func(t *testing.T) {
		invoice := e.makeInvoice(e.client, 1_000*sat)
		if err := e.node.Settle(invoice["payment_hash"].(string)); err != nil {
			t.Fatal(err)
		}
		e.waitState(e.client, invoice["payment_hash"], "settled")
		wantCode(t, pay(invoice["invoice"].(string)), codeOther)
	})
	t.Run("BadAmount", func(t *testing.T) {
		response := e.call(e.client, "pay_invoice", map[string]any{"invoice": e.node.OutsideInvoice(1_000 * sat), "amount": 5})
		wantCode(t, response, codeOther)
	})
	t.Run("NoInvoice", func(t *testing.T) { wantCode(t, e.call(e.client, "pay_invoice", nil), codeOther) })
	t.Run("BadParams", func(t *testing.T) {
		wantCode(t, e.call(e.client, "pay_invoice", map[string]any{"invoice": 12}), codeOther)
	})
	t.Run("NodeCannotRead", func(t *testing.T) { wantCode(t, pay("not an invoice"), codeInternal) })
}

// Catches: the mapping of errors no test above reaches on the fake node.
func TestRefusedMapping(t *testing.T) {
	e := newEnv(t)
	for _, c := range []struct {
		err  error
		code string
	}{
		{ledger.ErrAlreadyPaid, codeOther},
		{ledger.ErrInvoiceClosed, codeOther},
		{ledger.ErrInvoiceExpired, codeOther},
		{ledger.ErrOwnInvoice, codeOther},
		{ledger.ErrBadAmount, codeOther},
		{ledger.ErrAccountDeleted, codeUnauthorized},
		{ledger.ErrNotRunning, codeInternal},
		{context.DeadlineExceeded, codeInternal},
	} {
		if got := e.service.refused("test", c.err); got.Code != c.code {
			t.Errorf("%v: got %q", c.err, got.Code)
		}
	}
}

// blockedPay holds Pay until released, like a real payment that takes a while.
type blockedPay struct {
	*fake.Node
	release chan struct{}
}

func (b *blockedPay) Pay(ctx context.Context, request node.PayRequest) (node.Payment, error) {
	select {
	case <-b.release:
		return b.Node.Pay(ctx, request)
	case <-ctx.Done():
		return node.Payment{}, ctx.Err()
	}
}

// Catches: a false failure when the wait ends, which makes the client pay again. The
// client must read "payment pending", then the result.
func TestSlowPayment(t *testing.T) {
	e := newEnv(t)
	slow := &blockedPay{Node: e.node, release: make(chan struct{})}
	e.restartOn(slow)
	e.service.payWait = 20 * time.Millisecond
	invoice := e.node.OutsideInvoice(1_000 * sat)
	event, err := e.client.RequestWith(nwctest.NIP44, "pay_invoice", map[string]any{"invoice": invoice}, e.timestamp())
	if err != nil {
		t.Fatal(err)
	}
	response := e.answer(event, nwctest.NIP44)
	wantCode(t, response, codeOther)
	if response.Error.Message != "payment pending" {
		t.Fatalf("got %+v", response.Error)
	}
	if got := e.requestState(event.ID); got != statePending {
		t.Fatalf("request state %q", got)
	}
	found := e.result(e.client, "lookup_invoice", map[string]any{"invoice": invoice})
	if found["state"] != "pending" {
		t.Fatalf("got %v", found)
	}
	close(slow.release)
	if got := e.waitState(e.client, found["payment_hash"], "settled"); len(got["preimage"].(string)) != 64 {
		t.Fatalf("got %v", got)
	}
}

// Catches: a stop reading as failed, and a state lost because its context ended.
func TestStopWhileWaitingForPayment(t *testing.T) {
	e := newEnv(t)
	slow := &blockedPay{Node: e.node, release: make(chan struct{})}
	defer close(slow.release)
	e.restartOn(slow)
	event, err := e.client.RequestWith(nwctest.NIP44, "pay_invoice",
		map[string]any{"invoice": e.node.OutsideInvoice(1_000 * sat)}, e.timestamp())
	if err != nil {
		t.Fatal(err)
	}
	ctx, stop := context.WithCancel(t.Context())
	request := e.service.accept(ctx, event)
	done := make(chan *nostr.Event)
	go func() { done <- e.service.run(ctx, request) }()
	time.Sleep(20 * time.Millisecond)
	stop()
	reply := <-done
	if reply == nil {
		t.Fatal("no reply built")
	}
	response, err := e.client.Open(nwctest.NIP44, event, reply)
	if err != nil {
		t.Fatal(err)
	}
	if response.Error == nil || response.Error.Message != "payment pending" {
		t.Fatalf("got %+v", response)
	}
	if got := e.requestState(event.ID); got != statePending {
		t.Fatalf("request state %q", got)
	}
}

// Catches: a relay repeating a pay_invoice and paying twice.
func TestSamePaymentRunsOnce(t *testing.T) {
	e := newEnv(t)
	event, err := e.client.RequestWith(nwctest.NIP44, "pay_invoice",
		map[string]any{"invoice": e.node.OutsideInvoice(1_000 * sat)}, e.timestamp())
	if err != nil {
		t.Fatal(err)
	}
	if e.handle(event) == nil {
		t.Fatal("no reply")
	}
	if e.handle(event) != nil {
		t.Fatal("repeat was run")
	}
	if got := len(e.node.PayRequests()); got != 1 {
		t.Fatalf("%d payments", got)
	}
}

// Catches: a payment or invoice that loses the link to the request behind it.
func TestRowsKeepTheirRequest(t *testing.T) {
	e := newEnv(t)
	e.makeInvoice(e.client, 1_000)
	e.result(e.client, "pay_invoice", map[string]any{"invoice": e.node.OutsideInvoice(1_000 * sat)})
	var linked int
	err := e.store.DB.QueryRow(`SELECT count(*) FROM transactions t JOIN requests r ON r.id = t.request_id
		WHERE r.method IN ('make_invoice', 'pay_invoice')`).Scan(&linked)
	if err != nil {
		t.Fatal(err)
	}
	if linked != 2 {
		t.Fatalf("%d rows linked", linked)
	}
}

// Catches: the secret of an unpaid invoice shown to a client, which could then claim
// the payment.
func TestPreimageHiddenUntilPaid(t *testing.T) {
	e := newEnv(t)
	invoice := e.makeInvoice(e.client, 1_000)
	hash := invoice["payment_hash"].(string)
	var stored string
	if err := e.store.DB.QueryRow(`SELECT preimage FROM transactions WHERE payment_hash = ?`, hash).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	if stored == "" {
		t.Fatal("the row must hold a preimage, or this test proves nothing")
	}
	listed := e.list(e.client, map[string]any{"unpaid": true})
	for what, result := range map[string]map[string]any{
		"make_invoice":      invoice,
		"lookup_invoice":    e.lookup(e.client, hash),
		"list_transactions": listed[0].(map[string]any),
	} {
		if _, shown := result["preimage"]; shown {
			t.Errorf("%s shows it: %v", what, result)
		}
	}
	e.now.Add(2 * 3600)
	if got := e.lookup(e.client, hash); got["preimage"] != nil {
		t.Errorf("expired: %v", got)
	}
	if err := e.node.Settle(hash); err != nil {
		t.Fatal(err)
	}
	if got := e.waitState(e.client, hash, "settled"); got["preimage"] != stored {
		t.Errorf("settled: %v", got)
	}
}

// Catches: a client waiting on a dead invoice. The ledger closes such rows only now
// and then, so the state is worked out from expires_at.
func TestExpiredInvoice(t *testing.T) {
	e := newEnv(t)
	invoice := e.result(e.client, "make_invoice", map[string]any{"amount": 1_000, "expiry": 60})
	hash := invoice["payment_hash"]
	if got := e.lookup(e.client, hash)["state"]; got != "pending" {
		t.Fatalf("got %v", got)
	}
	e.now.Store(int64(invoice["expires_at"].(float64)) + 1)
	if got := e.lookup(e.client, hash)["state"]; got != "expired" {
		t.Fatalf("got %v", got)
	}
	if err := e.node.Settle(hash.(string)); err != nil {
		t.Fatal(err)
	}
	e.waitState(e.client, hash, "settled") // Paid at the last moment: it is not expired.
}

// Catches: an invoice the node would make with a date in the past.
func TestMakeInvoiceRefusesBadInput(t *testing.T) {
	e := newEnv(t)
	wantCode(t, e.call(e.client, "make_invoice", map[string]any{"amount": 1_000, "expiry": -1}), codeOther)
	wantCode(t, e.call(e.client, "make_invoice", map[string]any{"amount": 0}), codeOther)
}

// Catches: a client that leaves out params being refused. NIP-47 shows params, but
// nothing needs them for a command with only optional fields.
func TestRequestWithoutParams(t *testing.T) {
	e := newEnv(t)
	content, err := e.client.Encrypt(nwctest.NIP44, `{"method":"list_transactions"}`)
	if err != nil {
		t.Fatal(err)
	}
	event, err := e.client.Seal(content, e.timestamp(), nostr.Tag{"encryption", nwctest.NIP44})
	if err != nil {
		t.Fatal(err)
	}
	if response := e.answer(event, nwctest.NIP44); response.Error != nil {
		t.Fatalf("got %+v", response.Error)
	}
}

// Catches: a retry or an own invoice showing the wrong row, so a client reads an old
// failure over a payment that went through.
func TestLookupPicksTheRow(t *testing.T) {
	e := newEnv(t)
	t.Run("RetriedInvoice", func(t *testing.T) {
		invoice := e.node.OutsideInvoice(1_000 * sat)
		e.node.ScriptPay(fake.PayFail)
		wantCode(t, e.call(e.client, "pay_invoice", map[string]any{"invoice": invoice}), codePaymentFailed)
		found := e.result(e.client, "lookup_invoice", map[string]any{"invoice": invoice})
		if found["state"] != "failed" {
			t.Fatalf("got %v", found)
		}
		e.result(e.client, "pay_invoice", map[string]any{"invoice": invoice})
		if got := e.lookup(e.client, found["payment_hash"]); got["state"] != "settled" {
			t.Fatalf("got %v", got)
		}
	})
	t.Run("OwnInvoicePaid", func(t *testing.T) {
		invoice := e.makeInvoice(e.client, 1_000*sat)
		e.result(e.client, "pay_invoice", map[string]any{"invoice": invoice["invoice"]})
		if got := e.lookup(e.client, invoice["payment_hash"]); got["type"] != "outgoing" || got["state"] != "settled" {
			t.Fatalf("got %v", got)
		}
	})
	t.Run("ByUpperCaseInvoice", func(t *testing.T) {
		invoice := e.makeInvoice(e.client, 1_000)
		found := e.result(e.client, "lookup_invoice", map[string]any{"invoice": strings.ToUpper(invoice["invoice"].(string))})
		if found["payment_hash"] != invoice["payment_hash"] {
			t.Fatalf("got %v", found)
		}
	})
	t.Run("Unknown", func(t *testing.T) {
		wantCode(t, e.call(e.client, "lookup_invoice", map[string]any{"payment_hash": strings.Repeat("0", 64)}), codeNotFound)
		wantCode(t, e.call(e.client, "lookup_invoice", nil), codeOther)
	})
}

// Catches: one account reading another's history.
func TestHistoryStaysWithItsAccount(t *testing.T) {
	e := newEnv(t)
	other := e.addApp("second")
	e.restart()
	invoice := e.makeInvoice(e.client, 1_000)
	hash := invoice["payment_hash"].(string)
	if err := e.node.Settle(hash); err != nil {
		t.Fatal(err)
	}
	e.waitState(e.client, hash, "settled")
	wantCode(t, e.call(other, "lookup_invoice", map[string]any{"payment_hash": hash}), codeNotFound)
	wantCode(t, e.call(other, "lookup_invoice", map[string]any{"invoice": invoice["invoice"]}), codeNotFound)
	if got := e.list(other, map[string]any{"unpaid": true}); len(got) != 0 {
		t.Fatalf("got %v", got)
	}
}

// Catches: a wrong page, a wrong filter, and a page too big to encrypt.
func TestListTransactions(t *testing.T) {
	e := newEnv(t)
	start := e.now.Load()
	var hashes []any
	for range maxPage + 3 {
		invoice := e.makeInvoice(e.client, 1_000)
		hashes = append(hashes, invoice["payment_hash"])
	}
	hash := func(row any) any { return row.(map[string]any)["payment_hash"] }
	e.now.Add(60) // The ledger's clock is the real one, and until defaults to this one.

	if got := e.list(e.client, nil); len(got) != 0 {
		t.Fatalf("unpaid rows shown by default: %v", got)
	}
	all := e.result(e.client, "list_transactions", map[string]any{"unpaid": true, "limit": 1000})
	rows := all["transactions"].([]any)
	if len(rows) != maxPage || all["total_count"] != float64(maxPage+3) {
		t.Fatalf("%d rows, total %v", len(rows), all["total_count"])
	}
	if hash(rows[0]) != hashes[len(hashes)-1] {
		t.Fatal("not newest first")
	}
	page := e.list(e.client, map[string]any{"unpaid": true, "limit": 2, "offset": 1})
	if len(page) != 2 || hash(page[0]) != hashes[len(hashes)-2] {
		t.Fatalf("page %v", page)
	}
	if got := e.list(e.client, map[string]any{"unpaid": true, "from": e.now.Load() + 3600}); len(got) != 0 {
		t.Fatalf("from: %v", got)
	}
	if got := e.list(e.client, map[string]any{"unpaid": true, "until": start - 3600}); len(got) != 0 {
		t.Fatalf("until: %v", got)
	}
	if got := e.list(e.client, map[string]any{"unpaid": true, "type": "outgoing"}); len(got) != 0 {
		t.Fatalf("type filter: %v", got)
	}
	if got := e.list(e.client, map[string]any{"unpaid": true, "type": "incoming", "limit": 1}); len(got) != 1 {
		t.Fatalf("type filter: %v", got)
	}
	wantCode(t, e.call(e.client, "list_transactions", map[string]any{"type": "both"}), codeOther)
	wantCode(t, e.call(e.client, "list_transactions", map[string]any{"limit": -1}), codeOther)
}

// sent keeps what the service publishes.
type sent struct {
	mu     sync.Mutex
	events []nostr.Event
}

func (s *sent) publish(_ context.Context, event nostr.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, event)
	return nil
}

// Catches: a wrong kind or payload, and a notification going to the wrong client.
func TestNotifications(t *testing.T) {
	e := newEnv(t)
	e.addApp("second")
	e.restart()
	out := &sent{}
	e.service.publish = out.publish
	invoice := e.makeInvoice(e.client, 1_000)
	hash := invoice["payment_hash"].(string)
	if err := e.node.Settle(hash); err != nil {
		t.Fatal(err)
	}
	e.service.notify(t.Context(), <-e.service.notifications)
	e.result(e.client, "pay_invoice", map[string]any{"invoice": e.node.OutsideInvoice(1_000 * sat)})
	e.service.notify(t.Context(), <-e.service.notifications)

	if len(out.events) != 4 {
		t.Fatalf("%d events", len(out.events))
	}
	for i, want := range []struct {
		kind    int
		scheme  string
		name    string
		rowType string
	}{
		{23197, nwctest.NIP44, "payment_received", "incoming"},
		{23196, nwctest.NIP04, "payment_received", "incoming"},
		{23197, nwctest.NIP44, "payment_sent", "outgoing"},
		{23196, nwctest.NIP04, "payment_sent", "outgoing"},
	} {
		event := out.events[i]
		if event.Kind != want.kind || event.PubKey != e.client.ServicePub {
			t.Fatalf("event %d: %+v", i, event)
		}
		if got, _ := tagValue(&event, "p"); got != e.client.Public {
			t.Fatalf("event %d is for %s", i, got)
		}
		if valid, err := event.CheckSignature(); err != nil || !valid {
			t.Fatalf("event %d: signature %v %v", i, valid, err)
		}
		plain, err := e.client.Decrypt(want.scheme, event.Content)
		if err != nil {
			t.Fatalf("event %d: %v", i, err)
		}
		var payload struct {
			Type         string         `json:"notification_type"`
			Notification map[string]any `json:"notification"`
		}
		if err := json.Unmarshal([]byte(plain), &payload); err != nil {
			t.Fatal(err)
		}
		if payload.Type != want.name || payload.Notification["type"] != want.rowType ||
			payload.Notification["state"] != "settled" || len(payload.Notification["preimage"].(string)) != 64 {
			t.Fatalf("event %d: %s", i, plain)
		}
	}
}

// Catches: a revoked client still hearing about money.
func TestNoNotificationWhenRevoked(t *testing.T) {
	e := newEnv(t)
	out := &sent{}
	e.service.publish = out.publish
	if _, err := e.store.DB.Exec(`UPDATE connections SET revoked_at = 1`); err != nil {
		t.Fatal(err)
	}
	e.service.notify(t.Context(), ledger.Transaction{
		AccountID: e.service.connections[e.client.ServicePub].accountID, Type: ledger.TypeIncoming, State: ledger.StateSettled,
	})
	if len(out.events) != 0 {
		t.Fatalf("got %+v", out.events)
	}
}

// Catches: a slow relay stopping the ledger, which calls Notify on its own goroutines.
func TestFullQueueDoesNotBlock(t *testing.T) {
	e := newEnv(t)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for range notifyQueue + 5 {
			e.service.Notify(ledger.Transaction{})
		}
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Notify blocked")
	}
}
