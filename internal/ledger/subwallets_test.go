package ledger

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/2rdzy/nwc4ln/internal/node"
	"github.com/2rdzy/nwc4ln/internal/node/fake"
)

// addSubWallet makes a sub-wallet the way the service does: every command on, no limit.
func (e *env) addSubWallet() int64 {
	e.t.Helper()
	result, err := e.store.DB.Exec(`INSERT INTO accounts (type, name, can_pay, can_invoice, created_at)
		VALUES ('sub_wallet', 'wallet', 1, 1, 0)`)
	if err != nil {
		e.t.Fatal(err)
	}
	id, err := result.LastInsertId()
	if err != nil {
		e.t.Fatal(err)
	}
	return id
}

func (e *env) balance(id int64) int64 {
	e.t.Helper()
	balance, err := e.ledger.Balance(context.Background(), id)
	if err != nil {
		e.t.Fatal(err)
	}
	return balance
}

// fund pays a sub-wallet from outside, as a holder's payer would.
func (e *env) fund(id, amountMsat int64) {
	e.t.Helper()
	before := e.balance(id)
	row, err := e.ledger.MakeInvoice(context.Background(), InvoiceRequest{AccountID: id, AmountMsat: amountMsat})
	if err != nil {
		e.t.Fatal(err)
	}
	if err := e.node.Settle(row.PaymentHash); err != nil {
		e.t.Fatal(err)
	}
	e.waitUntil("the income is credited", func() bool { return e.balance(id) == before+amountMsat })
}

func (e *env) available() int64 {
	e.t.Helper()
	available, err := e.ledger.MainAvailable(context.Background())
	if err != nil {
		e.t.Fatal(err)
	}
	return available
}

func stuck(count int) []fake.PayStep {
	steps := make([]fake.PayStep, count)
	for i := range steps {
		steps[i] = fake.PayTimeout
	}
	return steps
}

// Catches: a balance overspent under load. Each payment stays pending, so every hold
// counts against the next check, and the count is exact.
func TestConcurrentPaymentsStopAtBalance(t *testing.T) {
	e := newEnv(t)
	e.start()
	e.acct = e.addSubWallet()
	e.fund(e.acct, 5*110*sat+109*sat) // Five payments, and one sat short of a sixth.
	e.node.ScriptPay(stuck(30)...)
	held, refused := e.payMany(30, 100*sat, ErrInsufficientBalance)
	if held != 5 || refused != 25 {
		t.Fatalf("held %d, refused %d", held, refused)
	}
	if got := e.balance(e.acct); got != 109*sat {
		t.Fatalf("balance %d", got)
	}
}

// Catches: apps reaching sub-wallet money, because the total is left out of R5.
func TestAppCannotSpendSubWalletFunds(t *testing.T) {
	e := newEnv(t)
	e.start()
	e.node.SetSpendable(node.Spendable{ActiveMsat: 0})
	wallet := e.addSubWallet()
	e.fund(wallet, 1_000*sat)
	if got := e.available(); got != 0 {
		t.Fatalf("main wallet %d, want 0: all the money is the sub-wallet's", got)
	}
	if _, err := e.tryPay(1 * sat); !errors.Is(err, ErrInsufficientBalance) {
		t.Fatalf("app paid from sub-wallet funds: %v", err)
	}
	balance, err := e.ledger.AppBalance(context.Background(), e.acct)
	if err != nil || balance != 0 {
		t.Fatalf("app told %d, %v", balance, err)
	}
}

// Catches: an app and a sub-wallet both passing on the same money, under load.
func TestAppAndSubWalletAtOnce(t *testing.T) {
	e := newEnv(t)
	e.start()
	e.node.SetSpendable(node.Spendable{ActiveMsat: 5 * 110 * sat}) // The main wallet: five app payments.
	wallet := e.addSubWallet()
	e.fund(wallet, 3*110*sat)
	e.node.ScriptPay(stuck(40)...)
	type result struct{ held, refused int }
	apps, wallets := make(chan result), make(chan result)
	both := *e
	both.acct = wallet
	go func() {
		held, refused := e.payMany(20, 100*sat, ErrInsufficientBalance)
		apps <- result{held, refused}
	}()
	go func() {
		held, refused := both.payMany(20, 100*sat, ErrInsufficientBalance)
		wallets <- result{held, refused}
	}()
	if got := <-apps; got != (result{5, 15}) {
		t.Errorf("app %+v", got)
	}
	if got := <-wallets; got != (result{3, 17}) {
		t.Errorf("sub-wallet %+v", got)
	}
	if got := e.available(); got != 0 {
		t.Errorf("main wallet %d", got)
	}
}

// Catches: an app's node figure going stale when a sub-wallet payment settles after
// the read. The figure still holds the money, the rows no longer do, and the app could
// spend the same money twice (R7).
func TestSubWalletSettlementWaitsForAppCheck(t *testing.T) {
	e := newEnv(t)
	e.start()
	e.node.SetSpendable(node.Spendable{ActiveMsat: 0})
	wallet := e.addSubWallet()
	e.fund(wallet, 1_000*sat)
	e.node.ScriptPay(fake.PayTimeout)
	invoice := e.node.OutsideInvoice(500 * sat)
	row, err := e.ledger.Pay(context.Background(), PayRequest{AccountID: wallet, Invoice: invoice})
	if err != nil {
		t.Fatal(err)
	}
	e.ledger.afterNodeRead = func() {
		e.ledger.afterNodeRead = nil
		if err := e.node.Finish(row.PaymentHash, true); err != nil {
			t.Error(err)
		}
		done := make(chan struct{})
		go func() {
			defer close(done)
			if err := e.ledger.trackPending(context.Background()); err != nil {
				t.Error(err)
			}
		}()
		select { // With the lock held the sweep cannot finish. Give it the chance to.
		case <-done:
		case <-time.After(100 * time.Millisecond):
		}
	}
	if _, err := e.tryPay(400 * sat); !errors.Is(err, ErrInsufficientBalance) {
		t.Fatalf("app paid with the sub-wallet's money: %v", err)
	}
	e.waitUntil("the sub-wallet payment settles", func() bool { return e.get(row.ID).State == StateSettled })
}

// Catches: the reserve left out of R7, the fee not charged, the unused reserve not
// returned. All of the spec's worked example, and the fee on the node's own figure.
func TestSubWalletPaysUpToItsReserve(t *testing.T) {
	e := newEnv(t)
	e.start()
	e.acct = e.addSubWallet()
	e.invoice(5 * sat) // Unpaid, so it counts for nothing.
	if got := e.balance(e.acct); got != 0 {
		t.Fatalf("new sub-wallet holds %d", got)
	}
	e.fund(e.acct, 1_000*sat)
	if _, err := e.tryPay(990*sat + 1); !errors.Is(err, ErrInsufficientBalance) {
		t.Fatalf("one msat over: %v", err)
	}
	e.node.SetRouteFee(3 * sat)
	before, _ := e.node.Spendable(context.Background())
	tx, err := e.tryPay(990 * sat)
	if err != nil {
		t.Fatal(err)
	}
	if tx.State != StateSettled || tx.FeeMsat != 3*sat {
		t.Fatalf("tx %+v", tx)
	}
	if got := e.balance(e.acct); got != 7*sat {
		t.Fatalf("balance %d, want 7 sat", got)
	}
	after, _ := e.node.Spendable(context.Background())
	if moved := before.ActiveMsat - after.ActiveMsat; moved != 993*sat {
		t.Fatalf("the node sent %d, the ledger charged %d", moved, 993*sat)
	}
	// Under the reserve, so it can pay only inside this node, where there is none.
	if _, err := e.tryPay(1 * sat); !errors.Is(err, ErrInsufficientBalance) {
		t.Fatalf("outside: %v", err)
	}
	own := e.invoice(7 * sat)
	if paid := e.pay(own.Invoice); paid.State != StateSettled {
		t.Fatalf("internal: %+v", paid)
	}
	if got := e.balance(e.acct); got != 7*sat {
		t.Fatalf("balance %d after paying itself", got)
	}
}

// Catches: a balance lost, or a hold released early.
func TestFailedPaymentReturnsTheHold(t *testing.T) {
	t.Run("AtOnce", func(t *testing.T) {
		e := newEnv(t)
		e.start()
		e.acct = e.addSubWallet()
		e.fund(e.acct, 1_000*sat)
		e.node.ScriptPay(fake.PayFail)
		tx, err := e.tryPay(100 * sat)
		if err != nil || tx.State != StateFailed {
			t.Fatalf("tx %+v, %v", tx, err)
		}
		if got := e.balance(e.acct); got != 1_000*sat {
			t.Fatalf("balance %d", got)
		}
	})
	t.Run("AfterATimeout", func(t *testing.T) {
		e := newEnv(t)
		e.start()
		e.acct = e.addSubWallet()
		e.fund(e.acct, 1_000*sat)
		e.node.ScriptPay(fake.PayTimeout)
		tx, err := e.tryPay(100 * sat)
		if err != nil || tx.State != StatePending {
			t.Fatalf("tx %+v, %v", tx, err)
		}
		if got := e.balance(e.acct); got != 890*sat {
			t.Fatalf("while pending, balance %d", got)
		}
		if err := e.node.Finish(tx.PaymentHash, false); err != nil {
			t.Fatal(err)
		}
		e.advance(noRecordGrace + time.Second)
		e.sweep()
		e.wantState(tx.ID, StateFailed)
		if got := e.balance(e.acct); got != 1_000*sat {
			t.Fatalf("balance %d", got)
		}
	})
}

// Catches: R5 applied to a sub-wallet, which would refuse a holder's own money when
// the main wallet is low or the node's figure cannot be read.
func TestSubWalletDoesNotNeedTheMainWallet(t *testing.T) {
	t.Run("MainWalletBelowZero", func(t *testing.T) {
		e := newEnv(t)
		e.start()
		e.acct = e.addSubWallet()
		e.fund(e.acct, 1_000*sat)
		e.node.SetSpendable(node.Spendable{ActiveMsat: 900 * sat})
		tx, err := e.tryPay(100 * sat)
		if err != nil || tx.State != StateSettled {
			t.Fatalf("tx %+v, %v", tx, err)
		}
	})
	t.Run("NoNodeFigure", func(t *testing.T) {
		e := newEnv(t)
		e.ledger = e.fresh(noSpendable{e.node})
		e.start()
		e.acct = e.addSubWallet()
		e.fund(e.acct, 1_000*sat)
		tx, err := e.tryPay(100 * sat)
		if err != nil || tx.State != StateSettled {
			t.Fatalf("tx %+v, %v", tx, err)
		}
		if got := e.balance(e.acct); got != 900*sat {
			t.Fatalf("balance %d", got)
		}
	})
	t.Run("PaymentTheNodeCannotMake", func(t *testing.T) {
		e := newEnv(t)
		e.start()
		e.acct = e.addSubWallet()
		e.fund(e.acct, 1_000*sat)
		e.node.SetSpendable(node.Spendable{ActiveMsat: 50 * sat})
		tx, err := e.tryPay(100 * sat)
		if err != nil || tx.State != StateFailed {
			t.Fatalf("tx %+v, %v", tx, err)
		}
		if got := e.balance(e.acct); got != 1_000*sat {
			t.Fatalf("balance %d", got)
		}
	})
}

// Catches: one wallet's negative balance netting off another's, which would hand
// the second holder's money to apps. A balance below 0 is a bug, but it must stay
// one wallet's problem.
func TestNegativeBalanceFreesNothing(t *testing.T) {
	e := newEnv(t)
	e.start()
	bad, good := e.addSubWallet(), e.addSubWallet()
	e.fund(good, 1_000*sat)
	row := Transaction{
		AccountID: bad, Type: TypeOutgoing, State: StateSettled, AmountMsat: 400 * sat,
		PaymentHash: strings.Repeat("ab", 32), CreatedAt: e.ledger.now(),
	}
	if _, err := insert(context.Background(), e.store.DB, row); err != nil {
		t.Fatal(err)
	}
	total, err := subWalletTotal(context.Background(), e.store.DB)
	if err != nil || total != 1_000*sat {
		t.Fatalf("total %d, %v", total, err)
	}
}

// Catches: a limit stored on a sub-wallet reaching a client as a budget. The service
// never stores one, so this stands in for a bad row.
func TestSubWalletHasNoBudget(t *testing.T) {
	e := newEnv(t)
	e.start()
	wallet := e.addSubWallet()
	if _, err := e.store.DB.Exec(`UPDATE accounts SET limit_msat = 1 WHERE id = ?`, wallet); err != nil {
		t.Fatal(err)
	}
	budget, err := e.ledger.AppBudget(context.Background(), wallet)
	if err != nil || budget != nil {
		t.Fatalf("budget %+v, %v", budget, err)
	}
}

// Catches: an app's history or income counted as sub-wallet money.
func TestAppIncomeIsNotSubWalletMoney(t *testing.T) {
	e := newEnv(t)
	e.start()
	e.addSubWallet()
	row := e.invoice(500 * sat)
	if err := e.node.Settle(row.PaymentHash); err != nil {
		t.Fatal(err)
	}
	e.waitState(row.ID, StateSettled)
	total, err := subWalletTotal(context.Background(), e.store.DB)
	if err != nil || total != 0 {
		t.Fatalf("total %d, %v", total, err)
	}
}

// Catches: one account growing the list of invoices the ledger asks the node about
// on each pass, and a holder locked out by invoices that have expired or settled.
func TestOpenInvoiceCap(t *testing.T) {
	e := newEnv(t)
	e.start()
	e.ledger.maxOpenInvoices = 2
	ctx := context.Background()
	make := func(account int64, seconds int64) (Transaction, error) {
		return e.ledger.MakeInvoice(ctx, InvoiceRequest{AccountID: account, AmountMsat: 1 * sat, ExpirySeconds: seconds})
	}
	first, err := make(e.acct, 60)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := make(e.acct, 3600); err != nil {
		t.Fatal(err)
	}
	if _, err := make(e.acct, 3600); !errors.Is(err, ErrTooManyInvoices) {
		t.Fatalf("at the cap: %v", err)
	}
	if _, err := make(e.addSubWallet(), 3600); err != nil {
		t.Fatalf("the cap is per account: %v", err)
	}
	e.advance(61 * time.Second) // first has expired, and its row is still pending.
	e.wantState(first.ID, StatePending)
	second, err := make(e.acct, 3600)
	if err != nil {
		t.Fatalf("after an expiry: %v", err)
	}
	if _, err := make(e.acct, 3600); !errors.Is(err, ErrTooManyInvoices) {
		t.Fatalf("at the cap again: %v", err)
	}
	if err := e.node.Settle(second.PaymentHash); err != nil {
		t.Fatal(err)
	}
	e.waitState(second.ID, StateSettled)
	if _, err := make(e.acct, 3600); err != nil {
		t.Fatalf("after a settlement: %v", err)
	}
}

// Catches: a description the invoice cannot hold reaching the node, and a valid one
// refused. The limit is in bytes.
func TestDescriptionCap(t *testing.T) {
	e := newEnv(t)
	e.start()
	cases := []struct {
		description string
		want        error
	}{
		{strings.Repeat("a", MaxDescriptionBytes), nil},
		{strings.Repeat("a", MaxDescriptionBytes+1), ErrDescriptionTooLong},
		{strings.Repeat("é", (MaxDescriptionBytes+1)/2), ErrDescriptionTooLong},
	}
	for _, c := range cases {
		_, err := e.ledger.MakeInvoice(context.Background(), InvoiceRequest{
			AccountID: e.acct, AmountMsat: 1 * sat, Description: c.description,
		})
		if !errors.Is(err, c.want) {
			t.Errorf("%d bytes: %v", len(c.description), err)
		}
	}
}
