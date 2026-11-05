// Package nodetest holds the behaviour every node.Node must show. The ledger
// depends on all of it, and the fake must match the real node, or tests that use
// the fake prove nothing.
package nodetest

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/2rdzy/nwc4ln/internal/node"
)

// Harness connects the tests to a node and to someone outside it.
type Harness struct {
	Node node.Node
	// OutsideInvoice returns an invoice from someone else, which Node can pay.
	OutsideInvoice func(t *testing.T, amountMsat int64) string
	// PayFromOutside pays an invoice made on Node, and returns once it settles.
	PayFromOutside func(t *testing.T, invoice node.Invoice)
}

const (
	amountMsat = 21_000
	feeLimit   = 10_000
	wait       = 20 * time.Second
)

func unknownHash() string { return strings.Repeat("cd", 32) }

func (h Harness) invoice(t *testing.T) node.Invoice {
	t.Helper()
	invoice, err := h.Node.MakeInvoice(t.Context(), node.InvoiceRequest{AmountMsat: amountMsat, Description: "nodetest"})
	if err != nil {
		t.Fatal(err)
	}
	return invoice
}

func (h Harness) settled(t *testing.T) node.Invoice {
	t.Helper()
	invoice := h.invoice(t)
	h.PayFromOutside(t, invoice)
	settled, err := h.Node.LookupInvoice(t.Context(), invoice.Hash)
	if err != nil {
		t.Fatal(err)
	}
	if settled.State != node.InvoiceSettled {
		t.Fatalf("invoice state %d, want settled", settled.State)
	}
	return settled
}

func request(invoice string) node.PayRequest {
	return node.PayRequest{Invoice: invoice, FeeLimitMsat: feeLimit, Timeout: wait}
}

func checkPreimage(t *testing.T, preimage, hash string) {
	t.Helper()
	raw, err := hex.DecodeString(preimage)
	if err != nil {
		t.Fatalf("preimage %q: %v", preimage, err)
	}
	if sum := sha256.Sum256(raw); hex.EncodeToString(sum[:]) != hash {
		t.Fatalf("preimage %s does not hash to %s", preimage, hash)
	}
}

// Run runs every test on the harness.
func Run(t *testing.T, h Harness) {
	tests := []struct {
		name string
		run  func(*testing.T, Harness)
	}{
		{"Info", testInfo},
		{"InvoiceRoundTrip", testInvoiceRoundTrip},
		{"DescriptionHash", testDescriptionHash},
		{"LookupUnknownInvoice", testLookupUnknownInvoice},
		{"SettledInvoiceIsPaid", testSettledInvoiceIsPaid},
		{"CancelOpenInvoice", testCancelOpenInvoice},
		{"CancelSettledInvoiceFails", testCancelSettledInvoiceFails},
		{"CancelUnknownInvoiceFails", testCancelUnknownInvoiceFails},
		{"TrackUnknownHash", testTrackUnknownHash},
		{"PayAndTrack", testPayAndTrack},
		{"RepeatPaymentIsNotFailure", testRepeatPaymentIsNotFailure},
		{"SelfPaymentLeavesNoRecord", testSelfPaymentLeavesNoRecord},
		{"PayRefusesZeroFeeLimit", testPayRefusesZeroFeeLimit},
		{"SubscribeResumesFromIndex", testSubscribeResumesFromIndex},
		{"SubscribeFromZeroHasNoBacklog", testSubscribeFromZeroHasNoBacklog},
		{"SubscribeStopsOnHandlerError", testSubscribeStopsOnHandlerError},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) { test.run(t, h) })
	}
}

// Catches: get_info fields left empty.
func testInfo(t *testing.T, h Harness) {
	info, err := h.Node.Info(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if info.PubKey == "" || info.Network == "" || info.BlockHeight == 0 || info.BlockHash == "" {
		t.Fatalf("incomplete info: %+v", info)
	}
}

// Catches: wrong units or fields between a new invoice, its lookup and its
// decoding, and a preimage that does not match, which R10 hands to the payer.
func testInvoiceRoundTrip(t *testing.T, h Harness) {
	ctx := t.Context()
	made, err := h.Node.MakeInvoice(ctx, node.InvoiceRequest{AmountMsat: 1_500, Description: "sub-sat", ExpirySeconds: 600})
	if err != nil {
		t.Fatal(err)
	}
	if made.State != node.InvoiceOpen || made.AmountMsat != 1_500 || made.Description != "sub-sat" ||
		made.PaymentRequest == "" || !made.ExpiresAt.Equal(made.CreatedAt.Add(600*time.Second)) {
		t.Fatalf("made %+v", made)
	}
	checkPreimage(t, made.Preimage, made.Hash)
	found, err := h.Node.LookupInvoice(ctx, made.Hash)
	if err != nil {
		t.Fatal(err)
	}
	if found.Hash != made.Hash || found.PaymentRequest != made.PaymentRequest || found.Preimage != made.Preimage {
		t.Fatalf("found %+v, made %+v", found, made)
	}
	decoded, err := h.Node.DecodeInvoice(ctx, made.PaymentRequest)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Hash != made.Hash || decoded.AmountMsat != 1_500 || decoded.Description != "sub-sat" ||
		decoded.Destination == "" || !decoded.ExpiresAt.Equal(made.ExpiresAt) {
		t.Fatalf("decoded %+v, made %+v", decoded, made)
	}
	if _, err := h.Node.DecodeInvoice(ctx, "not an invoice"); err == nil {
		t.Fatal("decoded a bad invoice")
	}
}

// Catches: make_invoice dropping description_hash, which LNURL pay needs.
func testDescriptionHash(t *testing.T, h Harness) {
	hash := strings.Repeat("ef", 32)
	made, err := h.Node.MakeInvoice(t.Context(), node.InvoiceRequest{AmountMsat: 1_000, DescriptionHash: hash})
	if err != nil {
		t.Fatal(err)
	}
	if made.DescriptionHash != hash {
		t.Fatalf("description hash %q", made.DescriptionHash)
	}
}

// Catches: an unknown invoice reported as INTERNAL, not NOT_FOUND.
func testLookupUnknownInvoice(t *testing.T, h Harness) {
	_, err := h.Node.LookupInvoice(t.Context(), unknownHash())
	if !errors.Is(err, node.ErrNotFound) {
		t.Fatalf("got %v, want ErrNotFound", err)
	}
}

// Catches: income credited with the wrong amount or no settle index.
func testSettledInvoiceIsPaid(t *testing.T, h Harness) {
	settled := h.settled(t)
	if settled.PaidMsat != amountMsat || settled.SettleIndex == 0 || settled.SettledAt.IsZero() {
		t.Fatalf("settled %+v", settled)
	}
}

// Catches: R10 trusting a cancel that did not stop the invoice. A second cancel
// must pass too, so that a retry after a lost reply does not refuse the payment.
func testCancelOpenInvoice(t *testing.T, h Harness) {
	ctx := t.Context()
	invoice := h.invoice(t)
	for range 2 {
		if err := h.Node.CancelInvoice(ctx, invoice.Hash); err != nil {
			t.Fatal(err)
		}
	}
	found, err := h.Node.LookupInvoice(ctx, invoice.Hash)
	if err != nil {
		t.Fatal(err)
	}
	if found.State != node.InvoiceCanceled {
		t.Fatalf("state %d, want canceled", found.State)
	}
}

// Catches: R10 double credit, by settling inside the ledger an invoice that
// someone already paid from outside.
func testCancelSettledInvoiceFails(t *testing.T, h Harness) {
	ctx := t.Context()
	settled := h.settled(t)
	if err := h.Node.CancelInvoice(ctx, settled.Hash); err == nil {
		t.Fatal("cancel passed on a settled invoice")
	}
	found, err := h.Node.LookupInvoice(ctx, settled.Hash)
	if err != nil {
		t.Fatal(err)
	}
	if found.State != node.InvoiceSettled {
		t.Fatalf("state %d, want settled", found.State)
	}
}

// Catches: R10 settling an invoice the node does not know.
func testCancelUnknownInvoiceFails(t *testing.T, h Harness) {
	if err := h.Node.CancelInvoice(t.Context(), unknownHash()); err == nil {
		t.Fatal("cancel passed on an unknown invoice")
	}
}

// Catches: a hold never released after a crash before the node call (R12).
func testTrackUnknownHash(t *testing.T, h Harness) {
	payment, err := h.Node.TrackPayment(t.Context(), unknownHash())
	if err != nil {
		t.Fatal(err)
	}
	if payment.State != node.PaymentNoRecord {
		t.Fatalf("state %d, want no record", payment.State)
	}
}

// Catches: wrong amount, fee or preimage reaching the ledger, from either call.
func testPayAndTrack(t *testing.T, h Harness) {
	ctx := t.Context()
	paid, err := h.Node.Pay(ctx, request(h.OutsideInvoice(t, amountMsat)))
	if err != nil {
		t.Fatal(err)
	}
	if paid.State != node.PaymentSucceeded || paid.AmountMsat != amountMsat || paid.FeeMsat > feeLimit {
		t.Fatalf("paid %+v", paid)
	}
	checkPreimage(t, paid.Preimage, paid.Hash)
	tracked, err := h.Node.TrackPayment(ctx, paid.Hash)
	if err != nil {
		t.Fatal(err)
	}
	if tracked != paid {
		t.Fatalf("tracked %+v, paid %+v", tracked, paid)
	}
}

// Catches: funds released for a paid invoice, by reading a Pay error as failure.
func testRepeatPaymentIsNotFailure(t *testing.T, h Harness) {
	ctx := t.Context()
	invoice := h.OutsideInvoice(t, amountMsat)
	paid, err := h.Node.Pay(ctx, request(invoice))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.Node.Pay(ctx, request(invoice)); err == nil {
		t.Fatal("second Pay passed")
	}
	tracked, err := h.Node.TrackPayment(ctx, paid.Hash)
	if err != nil {
		t.Fatal(err)
	}
	if tracked.State != node.PaymentSucceeded {
		t.Fatalf("state %d, want succeeded", tracked.State)
	}
}

// Catches: a hold stuck after the node refuses a payment outright. The ledger
// resolves any Pay error through TrackPayment, which must then say no record.
func testSelfPaymentLeavesNoRecord(t *testing.T, h Harness) {
	ctx := t.Context()
	invoice := h.invoice(t)
	if _, err := h.Node.Pay(ctx, request(invoice.PaymentRequest)); err == nil {
		t.Fatal("self-payment passed")
	}
	payment, err := h.Node.TrackPayment(ctx, invoice.Hash)
	if err != nil {
		t.Fatal(err)
	}
	if payment.State != node.PaymentNoRecord {
		t.Fatalf("state %d, want no record", payment.State)
	}
}

// Catches: a missing fee limit reaching the node.
func testPayRefusesZeroFeeLimit(t *testing.T, h Harness) {
	invoice := h.OutsideInvoice(t, amountMsat)
	bad := request(invoice)
	bad.FeeLimitMsat = 0
	if _, err := h.Node.Pay(t.Context(), bad); err == nil {
		t.Fatal("Pay passed with no fee limit")
	}
}

// subscribe collects settled invoices until the test ends or stop is called.
func (h Harness) subscribe(t *testing.T, from uint64) (<-chan node.Invoice, context.CancelFunc) {
	t.Helper()
	ctx, stop := context.WithCancel(t.Context())
	t.Cleanup(stop)
	seen := make(chan node.Invoice, 16)
	go func() {
		_ = h.Node.SubscribeInvoices(ctx, from, func(invoice node.Invoice) error {
			seen <- invoice
			return nil
		})
	}()
	return seen, stop
}

func next(t *testing.T, seen <-chan node.Invoice) node.Invoice {
	t.Helper()
	select {
	case invoice := <-seen:
		return invoice
	case <-time.After(wait):
		t.Fatal("no settled invoice arrived")
		return node.Invoice{}
	}
}

// Catches: income missed during downtime, or credited from the wrong invoice.
func testSubscribeResumesFromIndex(t *testing.T, h Harness) {
	first := h.settled(t)
	second := h.settled(t)
	seen, _ := h.subscribe(t, first.SettleIndex)
	got := next(t, seen)
	if got.Hash != second.Hash || got.SettleIndex != second.SettleIndex || got.PaidMsat != amountMsat ||
		got.State != node.InvoiceSettled {
		t.Fatalf("got %+v, want %+v", got, second)
	}
	// And carries on with new ones, which only then would a live node deliver.
	third := h.settled(t)
	if got := next(t, seen); got.Hash != third.Hash {
		t.Fatalf("got %s, want %s", got.Hash, third.Hash)
	}
}

// Catches: the ledger believing index 0 replays everything. It does not, so the
// ledger must find those invoices with LookupInvoice (R11).
func testSubscribeFromZeroHasNoBacklog(t *testing.T, h Harness) {
	old := h.settled(t)
	seen, _ := h.subscribe(t, 0)
	// The subscription may not be in place yet, and new invoices sent before it
	// are lost at index 0, so pay until one arrives.
	deadline := time.Now().Add(wait)
	for time.Now().Before(deadline) {
		h.settled(t)
		select {
		case got := <-seen:
			if got.Hash == old.Hash {
				t.Fatal("index 0 replayed a past settlement")
			}
			return
		case <-time.After(500 * time.Millisecond):
		}
	}
	t.Fatal("no settled invoice arrived")
}

// Catches: a credit that failed being skipped, so that income is lost.
func testSubscribeStopsOnHandlerError(t *testing.T, h Harness) {
	first := h.settled(t)
	h.settled(t)
	failure := errors.New("database is locked")
	calls := 0
	ctx, cancel := context.WithTimeout(t.Context(), wait)
	defer cancel()
	err := h.Node.SubscribeInvoices(ctx, first.SettleIndex, func(node.Invoice) error {
		calls++
		return failure
	})
	if !errors.Is(err, failure) || calls != 1 {
		t.Fatalf("err %v after %d calls", err, calls)
	}
}
