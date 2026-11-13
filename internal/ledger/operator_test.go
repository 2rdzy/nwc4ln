package ledger

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/2rdzy/nwc4ln/internal/node"
	"github.com/2rdzy/nwc4ln/internal/node/fake"
)

func (e *env) alerts() []Alert {
	e.t.Helper()
	alerts, err := e.ledger.Alerts(context.Background())
	if err != nil {
		e.t.Fatal(err)
	}
	return alerts
}

func (e *env) wantAlert(kind string) Alert {
	e.t.Helper()
	for _, alert := range e.alerts() {
		if alert.Kind == kind {
			return alert
		}
	}
	e.t.Fatalf("no %s alert in %+v", kind, e.alerts())
	return Alert{}
}

// Catches: a dashboard that fills with copies of one event, or an alert the admin
// closed that comes back on the next check.
func TestAlertRaisedOnceAndStaysClosed(t *testing.T) {
	e := newEnv(t)
	for range 3 {
		e.ledger.raise(context.Background(), e.store.DB, AlertCheck, "key", "message")
	}
	alert := e.wantAlert(AlertCheck)
	if len(e.alerts()) != 1 {
		t.Fatalf("alerts %+v", e.alerts())
	}
	if err := e.ledger.CloseAlert(context.Background(), alert.ID); err != nil {
		t.Fatal(err)
	}
	e.ledger.raise(context.Background(), e.store.DB, AlertCheck, "key", "message")
	if len(e.alerts()) != 0 {
		t.Fatalf("a closed alert came back: %+v", e.alerts())
	}
	if err := e.ledger.CloseAlert(context.Background(), alert.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("second close: %v", err)
	}
}

// Catches: a money error that stays a log line: each place writes its alert.
func TestEachMoneyErrorRaisesAnAlert(t *testing.T) {
	t.Run("AmountDiffers", func(t *testing.T) {
		e := newEnv(t)
		e.start()
		invoice := e.node.OutsideInvoice(0)
		e.ledger.beforePay = func() {
			_, _ = e.node.Pay(context.Background(), node.PayRequest{
				Invoice: invoice, AmountMsat: sat, FeeLimitMsat: minReserveMsat, Timeout: time.Second,
			})
		}
		if _, err := e.ledger.Pay(context.Background(), PayRequest{AccountID: e.acct, Invoice: invoice, AmountMsat: 900 * sat}); err != nil {
			t.Fatal(err)
		}
		e.wantAlert(AlertAmountDiffers)
	})
	t.Run("FeeAboveReserve", func(t *testing.T) {
		e := newEnv(t)
		e.start()
		e.node.ScriptPay(fake.PayTimeout)
		row := e.pay(e.node.OutsideInvoice(5 * sat))
		err := e.ledger.applySuccess(context.Background(), row, node.Payment{
			State: node.PaymentSucceeded, AmountMsat: row.AmountMsat, FeeMsat: row.ReserveMsat + 1, Preimage: "00",
		})
		if err != nil {
			t.Fatal(err)
		}
		e.wantAlert(AlertFeeAboveReserve)
	})
	t.Run("PaidAfterFailed", func(t *testing.T) {
		e := newEnv(t)
		e.start()
		invoice := e.node.OutsideInvoice(5 * sat)
		e.node.ScriptPay(fake.PayTimeout)
		row := e.pay(invoice)
		if err := failRow(context.Background(), e.store.DB, row.ID); err != nil {
			t.Fatal(err)
		}
		if err := e.node.Finish(row.PaymentHash, true); err != nil {
			t.Fatal(err)
		}
		if _, err := e.ledger.Pay(context.Background(), PayRequest{AccountID: e.acct, Invoice: invoice}); !errors.Is(err, ErrAlreadyPaid) {
			t.Fatalf("retry: %v", err)
		}
		e.wantAlert(AlertPaidAfterFailed)
	})
	t.Run("IncomeOnClosedInvoice", func(t *testing.T) {
		e := newEnv(t)
		e.start()
		row := e.invoice(5 * sat)
		if err := failRow(context.Background(), e.store.DB, row.ID); err != nil {
			t.Fatal(err)
		}
		err := e.ledger.credit(context.Background(), node.Invoice{Hash: row.PaymentHash, PaidMsat: 5 * sat,
			State: node.InvoiceSettled}, false)
		if err != nil {
			t.Fatal(err)
		}
		e.wantAlert(AlertIncomeOnClosed)
	})
}

// Catches: a shortfall alarm on a short dip, none when it lasts, or one that never
// closes after the money comes back.
func TestShortfallAlert(t *testing.T) {
	e := newEnv(t)
	e.start()
	wallet := e.addSubWallet()
	e.fund(wallet, 100*sat)
	e.node.SetSpendable(node.Spendable{}) // The node can send nothing: 100 sat short.
	e.sweep()                             // The shortfall starts.
	e.advance(20 * time.Minute)
	e.sweep()
	if len(e.alerts()) != 0 {
		t.Fatalf("alert after 20 minutes: %+v", e.alerts())
	}
	e.advance(11 * time.Minute)
	e.sweep()
	e.wantAlert(AlertShortfall)
	e.node.SetSpendable(node.Spendable{ActiveMsat: testFunds, AllMsat: testFunds})
	e.advance(10 * time.Minute)
	e.sweep()
	if len(e.alerts()) != 0 {
		t.Fatalf("alert still open after the shortfall ended: %+v", e.alerts())
	}
}

// Catches: a check that reports problems on a ledger that agrees with the node.
func TestCheckCleanLedger(t *testing.T) {
	e := newEnv(t)
	e.start()
	wallet := e.addSubWallet()
	e.fund(wallet, 1_000*sat)
	if _, err := e.ledger.Pay(context.Background(), PayRequest{AccountID: wallet, Invoice: e.node.OutsideInvoice(10 * sat)}); err != nil {
		t.Fatal(err)
	}
	failed := e.node.OutsideInvoice(10 * sat)
	e.node.ScriptPay(fake.PayFail)
	e.pay(failed)
	e.pay(failed) // A retry that settles.
	if _, err := e.ledger.Withdraw(context.Background(), wallet, 100*sat); err != nil {
		t.Fatal(err)
	}
	own := e.invoice(5 * sat)
	e.pay(own.Invoice) // Between accounts.
	mismatches, err := e.ledger.Check(context.Background(), time.Unix(0, 0))
	if err != nil || len(mismatches) != 0 {
		t.Fatalf("mismatches %v, %v", mismatches, err)
	}
}

// Catches: a real mismatch the check misses.
func TestCheckFindsEachMismatch(t *testing.T) {
	e := newEnv(t)
	e.start()
	exec := func(query string, args ...any) {
		t.Helper()
		if _, err := e.store.DB.Exec(query, args...); err != nil {
			t.Fatal(err)
		}
	}
	paid := e.pay(e.node.OutsideInvoice(10 * sat))
	exec(`UPDATE transactions SET amount_msat = amount_msat + 1 WHERE id = ?`, paid.ID)
	late := e.node.OutsideInvoice(10 * sat)
	e.node.ScriptPay(fake.PayTimeout)
	row := e.pay(late)
	exec(`UPDATE transactions SET state = 'failed' WHERE id = ?`, row.ID)
	if err := e.node.Finish(row.PaymentHash, true); err != nil {
		t.Fatal(err)
	}
	open := e.invoice(5 * sat)
	exec(`UPDATE transactions SET state = 'settled' WHERE id = ?`, open.ID)
	wallet := e.addSubWallet()
	e.fund(wallet, 100*sat)
	if _, err := e.ledger.Withdraw(context.Background(), wallet, 10*sat); err != nil {
		t.Fatal(err)
	}
	exec(`UPDATE transactions SET amount_msat = 1 WHERE account_id IS NULL AND type = 'incoming'`)
	exec(`UPDATE transactions SET amount_msat = 1 WHERE account_id = ? AND type = 'incoming'`, wallet)
	mismatches, err := e.ledger.Check(context.Background(), time.Unix(0, 0))
	if err != nil {
		t.Fatal(err)
	}
	found := map[string]bool{}
	for _, m := range mismatches {
		found[m.Problem] = true
	}
	for _, want := range []string{"amount_differs", "paid_after_failed", "income_not_settled", "income_differs",
		"internal_unbalanced", "negative_balance"} {
		if !found[want] {
			t.Errorf("missed %s in %v", want, mismatches)
		}
	}
}

// Catches: a check on another node, whose answers mean nothing.
func TestCheckRefusesAnotherNode(t *testing.T) {
	e := newEnv(t)
	e.start()
	if _, err := e.store.DB.Exec(`UPDATE settings SET value = '03other' WHERE key = 'node_pubkey'`); err != nil {
		t.Fatal(err)
	}
	if _, err := e.ledger.Check(context.Background(), time.Unix(0, 0)); !errors.Is(err, ErrNodeChanged) {
		t.Fatalf("got %v", err)
	}
}

// Catches: a mismatch the regular check finds and nobody sees.
func TestRegularCheckRaisesAlert(t *testing.T) {
	e := newEnv(t)
	e.start()
	paid := e.pay(e.node.OutsideInvoice(10 * sat))
	if _, err := e.store.DB.Exec(`UPDATE transactions SET fee_msat = fee_msat + 1 WHERE id = ?`, paid.ID); err != nil {
		t.Fatal(err)
	}
	e.sweep() // The first sweep runs the check.
	e.wantAlert(AlertCheck)
}

// Catches: a payment with wrong deadlines while the node is behind the chain, or
// income, or a move between accounts, stopped with it.
func TestNodeNotSynced(t *testing.T) {
	e := newEnv(t)
	e.start()
	wallet := e.addSubWallet()
	e.node.SetSynced(false)
	e.sweep()
	before := e.rowCount()
	if _, err := e.ledger.Pay(context.Background(), PayRequest{AccountID: e.acct, Invoice: e.node.OutsideInvoice(5 * sat)}); !errors.Is(err, ErrNodeNotSynced) {
		t.Fatalf("payment out: %v", err)
	}
	if e.rowCount() != before {
		t.Fatal("a refused payment wrote a row")
	}
	e.fund(wallet, 100*sat) // Income still credits.
	if row, err := e.ledger.Withdraw(context.Background(), wallet, 10*sat); err != nil || row.State != StateSettled {
		t.Fatalf("withdraw %+v, %v", row, err)
	}
	own := e.invoice(5 * sat)
	if row := e.pay(own.Invoice); row.State != StateSettled {
		t.Fatalf("payment between accounts: %s", row.State)
	}
	e.node.SetSynced(true)
	e.sweep()
	if row := e.pay(e.node.OutsideInvoice(5 * sat)); row.State != StateSettled {
		t.Fatalf("after sync: %s", row.State)
	}
}
