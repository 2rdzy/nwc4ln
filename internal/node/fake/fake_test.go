package fake

import (
	"errors"
	"testing"
	"time"

	"github.com/2rdzy/nwc4ln/internal/node"
	"github.com/2rdzy/nwc4ln/internal/node/nodetest"
)

// Catches: the fake drifting from the real node, so later tests prove nothing.
func TestBehaviour(t *testing.T) {
	fake := New()
	nodetest.Run(t, nodetest.Harness{
		Node: fake,
		OutsideInvoice: func(_ *testing.T, amountMsat int64) string {
			return fake.OutsideInvoice(amountMsat)
		},
		PayFromOutside: func(t *testing.T, invoice node.Invoice) {
			if err := fake.Settle(invoice.Hash); err != nil {
				t.Fatal(err)
			}
		},
	})
}

func pay(invoice string) node.PayRequest {
	return node.PayRequest{Invoice: invoice, FeeLimitMsat: 10_000, Timeout: time.Minute}
}

// Catches: a test of "node times out, then reports" that never sees that order.
func TestPayTimeoutThenFinish(t *testing.T) {
	for name, succeeded := range map[string]bool{"success": true, "failure": false} {
		t.Run(name, func(t *testing.T) {
			ctx := t.Context()
			fake := New()
			fake.ScriptPay(PayTimeout)
			invoice := fake.OutsideInvoice(1_000)
			if _, err := fake.Pay(ctx, pay(invoice)); !errors.Is(err, ErrPayTimeout) {
				t.Fatalf("Pay error %v", err)
			}
			decoded, err := fake.DecodeInvoice(ctx, invoice)
			if err != nil {
				t.Fatal(err)
			}
			if got, err := fake.TrackPayment(ctx, decoded.Hash); err != nil || got.State != node.PaymentInFlight {
				t.Fatalf("got %+v, %v", got, err)
			}
			if err := fake.Finish(decoded.Hash, succeeded); err != nil {
				t.Fatal(err)
			}
			want := node.PaymentFailed
			if succeeded {
				want = node.PaymentSucceeded
			}
			if got, err := fake.TrackPayment(ctx, decoded.Hash); err != nil || got.State != want {
				t.Fatalf("got %+v, %v", got, err)
			}
		})
	}
}

// Catches: a crash test where the fake remembers a payment it never received.
func TestPayLostLeavesNoRecord(t *testing.T) {
	ctx := t.Context()
	fake := New()
	fake.ScriptPay(PayLost)
	invoice := fake.OutsideInvoice(1_000)
	if _, err := fake.Pay(ctx, pay(invoice)); err == nil {
		t.Fatal("no error")
	}
	decoded, _ := fake.DecodeInvoice(ctx, invoice)
	if got, err := fake.TrackPayment(ctx, decoded.Hash); err != nil || got.State != node.PaymentNoRecord {
		t.Fatalf("got %+v, %v", got, err)
	}
}

// Catches: a retry after a failure being refused, which R9 allows.
func TestPayFailThenRetry(t *testing.T) {
	ctx := t.Context()
	fake := New()
	fake.ScriptPay(PayFail)
	invoice := fake.OutsideInvoice(1_000)
	failed, err := fake.Pay(ctx, pay(invoice))
	if err != nil || failed.State != node.PaymentFailed {
		t.Fatalf("got %+v, %v", failed, err)
	}
	if got, err := fake.Pay(ctx, pay(invoice)); err != nil || got.State != node.PaymentSucceeded {
		t.Fatalf("got %+v, %v", got, err)
	}
}

// Catches: fee handling tests with no fee: the limit must bind and be recorded.
func TestRouteFeeAgainstLimit(t *testing.T) {
	ctx := t.Context()
	fake := New()
	fake.SetRouteFee(10_000)
	paid, err := fake.Pay(ctx, pay(fake.OutsideInvoice(1_000)))
	if err != nil || paid.State != node.PaymentSucceeded || paid.FeeMsat != 10_000 {
		t.Fatalf("got %+v, %v", paid, err)
	}
	fake.SetRouteFee(10_001)
	failed, err := fake.Pay(ctx, pay(fake.OutsideInvoice(1_000)))
	if err != nil || failed.State != node.PaymentFailed || failed.FeeMsat != 0 {
		t.Fatalf("got %+v, %v", failed, err)
	}
	if got := fake.PayRequests(); len(got) != 2 || got[0].FeeLimitMsat != 10_000 {
		t.Fatalf("requests %+v", got)
	}
}

// Catches: the internal payment test (R10) unable to make the cancel fail.
func TestFailNextCancel(t *testing.T) {
	ctx := t.Context()
	fake := New()
	invoice, err := fake.MakeInvoice(ctx, node.InvoiceRequest{AmountMsat: 1_000})
	if err != nil {
		t.Fatal(err)
	}
	failure := errors.New("cancel refused")
	fake.FailNextCancel(failure)
	if err := fake.CancelInvoice(ctx, invoice.Hash); !errors.Is(err, failure) {
		t.Fatalf("got %v", err)
	}
	if got, _ := fake.LookupInvoice(ctx, invoice.Hash); got.State != node.InvoiceOpen {
		t.Fatalf("state %d after failed cancel", got.State)
	}
	if err := fake.CancelInvoice(ctx, invoice.Hash); err != nil {
		t.Fatal(err)
	}
}

// Catches: "node unreachable" tests where a call still answers, above all
// TrackPayment reporting no record, and a subscription that stays quiet.
func TestUnreachable(t *testing.T) {
	ctx := t.Context()
	fake := New()
	done := make(chan error, 1)
	go func() {
		done <- fake.SubscribeInvoices(ctx, 0, func(node.Invoice) error { return nil })
	}()
	fake.SetUnreachable(true)
	select {
	case err := <-done:
		if !errors.Is(err, ErrUnreachable) {
			t.Fatalf("subscription ended with %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("subscription still open")
	}
	if got, err := fake.TrackPayment(ctx, "ab"); !errors.Is(err, ErrUnreachable) {
		t.Fatalf("got %+v, %v", got, err)
	}
	if _, err := fake.Spendable(ctx); !errors.Is(err, ErrUnreachable) {
		t.Fatalf("Spendable: %v", err)
	}
	fake.SetUnreachable(false)
	if _, err := fake.Spendable(ctx); err != nil {
		t.Fatal(err)
	}
}
