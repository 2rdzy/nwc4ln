package nwc

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/2rdzy/nwc4ln/internal/ledger"
	"github.com/2rdzy/nwc4ln/internal/node"
	"github.com/2rdzy/nwc4ln/internal/nwc/nwctest"
)

// callWith sends a request in one encryption, and returns the reply with the size
// of its decrypted text.
func (e *env) callWith(client *nwctest.Client, scheme, method string, params any) (*nwctest.Response, int) {
	e.t.Helper()
	event, err := client.RequestWith(scheme, method, params, e.timestamp())
	if err != nil {
		e.t.Fatal(err)
	}
	reply := e.handle(event)
	if reply == nil {
		e.t.Fatalf("%s %s: no reply", scheme, method)
	}
	response, err := client.Open(scheme, event, reply)
	if err != nil {
		e.t.Fatal(err)
	}
	plain, err := client.Decrypt(scheme, reply.Content)
	if err != nil {
		e.t.Fatal(err)
	}
	return response, len(plain)
}

// Catches: a page of large rows that cannot be encrypted, so list_transactions gets
// no reply (S09); a page cut by size whose offsets skip or repeat a row; a
// total_count that counts the page instead of the matching rows; a description cut
// to make a page fit; and NIP-04 replies left without the limit.
func TestHistoryPagedBySize(t *testing.T) {
	e := newEnv(t)
	account, connection := e.accountOf(e.client), e.connectionOf(e.client)
	description := strings.Repeat("<", ledger.MaxDescriptionBytes) // 6 bytes each in JSON.
	for range 20 {
		if _, err := e.ledger.MakeInvoice(t.Context(), ledger.InvoiceRequest{
			AccountID: account, ConnectionID: connection, AmountMsat: sat, Description: description,
		}); err != nil {
			t.Fatal(err)
		}
	}
	for _, scheme := range []string{nwctest.NIP44, nwctest.NIP04} {
		seen := map[string]bool{}
		pages := 0
		for offset := 0; offset < 20; pages++ {
			response, size := e.callWith(e.client, scheme, "list_transactions", map[string]any{"unpaid": true, "offset": offset})
			if response.Error != nil {
				t.Fatalf("%s offset %d: %+v", scheme, offset, response.Error)
			}
			if size > maxPlain {
				t.Fatalf("%s offset %d: reply of %d bytes", scheme, offset, size)
			}
			var result struct {
				Transactions []transaction `json:"transactions"`
				TotalCount   int64         `json:"total_count"`
			}
			if err := json.Unmarshal(response.Result, &result); err != nil {
				t.Fatal(err)
			}
			if result.TotalCount != 20 || len(result.Transactions) == 0 {
				t.Fatalf("%s offset %d: total %d, %d rows", scheme, offset, result.TotalCount, len(result.Transactions))
			}
			for _, row := range result.Transactions {
				if seen[row.PaymentHash] || row.Description != description {
					t.Fatalf("%s offset %d: row repeated or cut: %s, %d bytes of description", scheme, offset,
						row.PaymentHash, len(row.Description))
				}
				seen[row.PaymentHash] = true
			}
			offset += len(result.Transactions)
		}
		if len(seen) != 20 || pages < 2 {
			t.Fatalf("%s: %d rows in %d pages", scheme, len(seen), pages)
		}
	}
}

// Catches: a row too large for any reply that silently gets no reply or is cut,
// in list_transactions and in every other reply (lookup_invoice), on NIP-04 too.
// The ledger no longer stores such a row, so the limit is lowered instead.
func TestRowTooLargeForReply(t *testing.T) {
	e := newEnv(t)
	saved := maxPlain
	maxPlain = 3_000
	t.Cleanup(func() { maxPlain = saved })
	// About 4.4 KB as JSON, so even the make_invoice reply is too large.
	made := e.call(e.client, "make_invoice", map[string]any{
		"amount": sat, "description": strings.Repeat("<", ledger.MaxDescriptionBytes),
	})
	if made.Error == nil || made.Error.Message != "the reply is too large to send" {
		t.Fatalf("make_invoice: %+v", made.Error)
	}
	var large string
	if err := e.store.DB.QueryRow(`SELECT payment_hash FROM transactions`).Scan(&large); err != nil {
		t.Fatal(err)
	}
	e.now.Add(10) // Newer, so it comes first in the history.
	e.makeInvoice(e.client, sat)
	for _, scheme := range []string{nwctest.NIP44, nwctest.NIP04} {
		page, _ := e.callWith(e.client, scheme, "list_transactions", map[string]any{"unpaid": true})
		var result listResult
		if page.Error != nil || json.Unmarshal(page.Result, &result) != nil || len(result.Transactions) != 1 ||
			result.TotalCount != 2 {
			t.Fatalf("%s first page: %+v %s", scheme, page.Error, page.Result)
		}
		next, _ := e.callWith(e.client, scheme, "list_transactions", map[string]any{"unpaid": true, "offset": 1})
		if next.Error == nil || next.Error.Code != codeOther || next.Error.Message != errRowTooLarge.Message {
			t.Fatalf("%s second page: %+v %s", scheme, next.Error, next.Result)
		}
		lookup, _ := e.callWith(e.client, scheme, "lookup_invoice", map[string]any{"payment_hash": large})
		if lookup.ResultType != "lookup_invoice" || lookup.Error == nil || lookup.Error.Code != codeOther ||
			lookup.Error.Message != "the reply is too large to send" {
			t.Fatalf("%s lookup: %+v %+v", scheme, lookup.ResultType, lookup.Error)
		}
	}
}

// Catches: an invoice longer than LND's limit that reaches the node and is stored
// as a row too large to send; and a limit off by one.
func TestPayInvoiceTooLong(t *testing.T) {
	e := newEnv(t)
	for length, refused := range map[int]bool{ledger.MaxInvoiceBytes: false, ledger.MaxInvoiceBytes + 1: true} {
		response := e.call(e.client, "pay_invoice", map[string]any{"invoice": "lnbc" + strings.Repeat("q", length-4)})
		tooLong := response.Error != nil && response.Error.Message == "invoice is too long"
		if tooLong != refused {
			t.Errorf("%d characters: %+v", length, response.Error)
		}
	}
	if len(e.node.PayRequests()) != 0 {
		t.Fatalf("the node got %d payments", len(e.node.PayRequests()))
	}
}

// replaced is an account whose first connection made an invoice and a payment,
// both settled, and was then replaced. The new connection made one invoice and one
// payment of its own. The rows are kept to notify about later, as if they settled
// after the replace.
type replaced struct {
	e                            *env
	account                      int64
	client                       *nwctest.Client // The new connection.
	oldIn, oldOut, newIn, newOut ledger.Transaction
	oldHash, outHash, own        string
	ownInvoice                   string
}

func newReplaced(t *testing.T) *replaced {
	e := newEnv(t)
	r := &replaced{e: e, account: e.accountOf(e.client)}
	r.oldHash = e.makeInvoice(e.client, sat)["payment_hash"].(string)
	if err := e.node.Settle(r.oldHash); err != nil {
		t.Fatal(err)
	}
	r.oldIn = <-e.service.notifications
	e.result(e.client, "pay_invoice", map[string]any{"invoice": e.node.OutsideInvoice(2 * sat)})
	r.oldOut = <-e.service.notifications
	r.outHash = r.oldOut.PaymentHash
	connectionString, err := e.service.ReplaceConnection(t.Context(), r.account)
	if err != nil {
		t.Fatal(err)
	}
	if r.client, err = nwctest.Parse(connectionString); err != nil {
		t.Fatal(err)
	}
	made := e.makeInvoice(r.client, 3*sat)
	r.own, r.ownInvoice = made["payment_hash"].(string), made["invoice"].(string)
	if err := e.node.Settle(r.own); err != nil {
		t.Fatal(err)
	}
	r.newIn = <-e.service.notifications
	e.result(r.client, "pay_invoice", map[string]any{"invoice": e.node.OutsideInvoice(4 * sat)})
	r.newOut = <-e.service.notifications
	return r
}

func (r *replaced) setPay(on bool) {
	r.e.t.Helper()
	settings := ledger.DefaultApp()
	settings.CanInvoice, settings.CanPay = true, on
	if err := r.e.service.EditApp(r.e.t.Context(), r.account, settings); err != nil {
		r.e.t.Fatal(err)
	}
}

// Catches: a connection with pay off told about payments (S10), its own included, or about invoices
// a connection before it made; one with pay on, or its own invoices, losing
// notifications; a rule that holds for one encryption only.
func TestNotificationsFollowReadScope(t *testing.T) {
	r := newReplaced(t)
	for _, pay := range []bool{false, true} {
		r.setPay(pay)
		for _, c := range []struct {
			name string
			row  ledger.Transaction
			told bool
		}{
			{"payment_sent", r.oldOut, pay},
			{"payment_sent", r.newOut, pay},
			{"payment_received", r.oldIn, pay},
			{"payment_received", r.newIn, true},
		} {
			out := &sent{}
			r.e.service.publish = out.publish
			r.e.service.notify(t.Context(), c.row)
			want := 0
			if c.told {
				want = 2
			}
			if len(out.events) != want {
				t.Fatalf("pay %v, %s of %s: %d events, want %d", pay, c.name, c.row.PaymentHash, len(out.events), want)
			}
			for i, event := range out.events {
				kind, scheme := []int{kindNotification, kindNotificationNIP04}[i], []string{nwctest.NIP44, nwctest.NIP04}[i]
				if got, _ := tagValue(&event, "p"); event.Kind != kind || got != r.client.Public {
					t.Fatalf("event %d: kind %d for %s", i, event.Kind, got)
				}
				plain, err := r.client.Decrypt(scheme, event.Content)
				if err != nil || !strings.Contains(plain, `"notification_type":"`+c.name+`"`) {
					t.Fatalf("event %d: %v %s", i, err, plain)
				}
			}
		}
	}
}

// Catches: lookup_invoice as a second read path past the rule: a connection with
// pay off reading a payment, preimage included, its own too, or an invoice it did not make; and
// one with pay on, or its own invoices, refused.
func TestLookupFollowsReadScope(t *testing.T) {
	r := newReplaced(t)
	for _, pay := range []bool{false, true} {
		r.setPay(pay)
		for _, c := range []struct {
			name   string
			params map[string]any
			found  bool
		}{
			{"own invoice by hash", map[string]any{"payment_hash": r.own}, true},
			{"own invoice by text", map[string]any{"invoice": r.ownInvoice}, true},
			{"earlier connection's invoice", map[string]any{"payment_hash": r.oldHash}, pay},
			{"payment", map[string]any{"payment_hash": r.outHash}, pay},
			{"own payment", map[string]any{"payment_hash": r.newOut.PaymentHash}, pay},
		} {
			response := r.e.call(r.client, "lookup_invoice", c.params)
			found := response.Error == nil
			if found != c.found || !found && response.Error.Code != codeNotFound {
				t.Errorf("pay %v, %s: %+v %s", pay, c.name, response.Error, response.Result)
			}
		}
	}
}

// madeDuring calls a hook when the node makes an invoice.
type madeDuring struct {
	node.Node
	hook func()
}

func (n *madeDuring) MakeInvoice(ctx context.Context, request node.InvoiceRequest) (node.Invoice, error) {
	if hook := n.hook; hook != nil {
		n.hook = nil
		hook()
	}
	return n.Node.MakeInvoice(ctx, request)
}

// Catches: a connection replaced while the node makes the invoice that still gets
// the invoice, and a row stored for it.
func TestReplacedDuringMakeInvoice(t *testing.T) {
	e := newEnv(t)
	wrapped := &madeDuring{Node: e.node}
	e.restartOn(wrapped)
	account := e.accountOf(e.client)
	wrapped.hook = func() {
		if _, err := e.service.ReplaceConnection(t.Context(), account); err != nil {
			t.Errorf("replace: %v", err)
		}
	}
	response := e.call(e.client, "make_invoice", map[string]any{"amount": sat, "description": "x"})
	var rows int
	if err := e.store.DB.QueryRow(`SELECT count(*) FROM transactions`).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if response.Error == nil || response.Error.Code != codeUnauthorized || rows != 0 {
		t.Fatalf("reply %+v %s, %d rows stored", response.Error, response.Result, rows)
	}
}

// Catches: a connection with pay off told by get_info or its info event that it
// will get payment_sent, which the read rule never sends it; and one with pay on
// that no longer lists it.
func TestAdvertisedNotificationsFollowReadScope(t *testing.T) {
	e := newEnv(t)
	for pay, want := range map[bool]string{false: "payment_received", true: "payment_received payment_sent"} {
		e.edit(func(s *ledger.AppSettings) { s.CanPay = pay })
		var info struct {
			Notifications []string `json:"notifications"`
		}
		if err := json.Unmarshal(e.call(e.client, "get_info", nil).Result, &info); err != nil {
			t.Fatal(err)
		}
		event, err := e.service.infoEvent(e.service.connections[e.client.ServicePub])
		if err != nil {
			t.Fatal(err)
		}
		tag, _ := tagValue(event, "notifications")
		if got := strings.Join(info.Notifications, " "); got != want || tag != want {
			t.Errorf("pay %v: get_info %q, info event %q, want %q", pay, got, tag, want)
		}
	}
}
