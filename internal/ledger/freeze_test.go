package ledger

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/2rdzy/nwc4ln/internal/node"
)

func (e *env) setPaused(paused bool) {
	e.t.Helper()
	if err := e.ledger.SetSpendingPaused(context.Background(), paused); err != nil {
		e.t.Fatal(err)
	}
}

// Catches: a sub-wallet payment that slips through in the moment the switch turns.
// Once SetSpendingPaused returns, no new hold may appear, however many payments run.
func TestPauseStopsConcurrentPayments(t *testing.T) {
	e := newEnv(t)
	e.start()
	wallet := e.addSubWallet()
	e.fund(wallet, 100_000*sat)
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for range 8 {
		wg.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
				}
				_, err := e.ledger.Pay(context.Background(), PayRequest{AccountID: wallet, Invoice: e.node.OutsideInvoice(sat)})
				if err != nil && !errors.Is(err, ErrSpendingPaused) {
					t.Error(err)
					return
				}
			}
		})
	}
	time.Sleep(20 * time.Millisecond)
	e.setPaused(true)
	held := e.rowCount()
	time.Sleep(20 * time.Millisecond)
	close(stop)
	wg.Wait()
	if got := e.rowCount(); got != held {
		t.Fatalf("%d rows after the pause, %d when it returned", got, held)
	}
}

// Catches: a pause that blocks what it must leave alone, or misses a sub-wallet paying
// another account on this node.
func TestPauseCoversOnlySubWalletSpending(t *testing.T) {
	e := newEnv(t)
	e.start()
	wallet := e.addSubWallet()
	other := e.addSubWallet()
	e.fund(wallet, 1_000*sat)
	e.setPaused(true)
	pay := func(account int64, invoice string) error {
		_, err := e.ledger.Pay(context.Background(), PayRequest{AccountID: account, Invoice: invoice})
		return err
	}
	if err := pay(wallet, e.node.OutsideInvoice(sat)); !errors.Is(err, ErrSpendingPaused) {
		t.Fatalf("sub-wallet paying out: %v", err)
	}
	toOther, err := e.ledger.MakeInvoice(context.Background(), InvoiceRequest{AccountID: other, AmountMsat: sat})
	if err != nil {
		t.Fatal(err)
	}
	if err := pay(wallet, toOther.Invoice); !errors.Is(err, ErrSpendingPaused) {
		t.Fatalf("sub-wallet paying another sub-wallet: %v", err)
	}
	if err := pay(e.acct, e.node.OutsideInvoice(sat)); err != nil {
		t.Fatalf("app: %v", err)
	}
	if err := pay(e.acct, toOther.Invoice); err != nil {
		t.Fatalf("app paying into a sub-wallet: %v", err)
	}
	e.fund(wallet, 10*sat) // Receiving goes on.
	if row, err := e.ledger.Withdraw(context.Background(), wallet, 100*sat); err != nil || row.State != StateSettled {
		t.Fatalf("withdraw %+v, %v", row, err)
	}
	if err := e.ledger.DeleteSubWallet(context.Background(), wallet); err != nil {
		t.Fatal(err)
	}
	e.sweep()
	if got := e.balance(wallet); got != 0 {
		t.Fatalf("deleted sub-wallet still holds %d msat", got)
	}
	e.setPaused(false)
	if err := pay(other, e.node.OutsideInvoice(sat)); err != nil && !errors.Is(err, ErrInsufficientBalance) {
		t.Fatalf("after resume: %v", err)
	}
}

// Catches: a restart that silently resumes spending.
func TestPauseSurvivesRestart(t *testing.T) {
	e := newEnv(t)
	e.start()
	wallet := e.addSubWallet()
	e.fund(wallet, 1_000*sat)
	e.setPaused(true)
	e.restart()
	if _, err := e.ledger.Pay(context.Background(), PayRequest{AccountID: wallet, Invoice: e.node.OutsideInvoice(sat)}); !errors.Is(err, ErrSpendingPaused) {
		t.Fatalf("after restart: %v", err)
	}
}

// Catches: a run on the bank that goes on unattended during a lasting shortfall, an
// admin's resume undone in the same shortfall, or no pause in the next one.
func TestShortfallPausesSpending(t *testing.T) {
	e := newEnv(t)
	e.start()
	wallet := e.addSubWallet()
	e.fund(wallet, 100*sat)
	paused := func() bool {
		t.Helper()
		got, err := e.ledger.SpendingPaused(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		return got
	}
	short := func() {
		e.node.SetSpendable(node.Spendable{})
		e.sweep()
		e.advance(31 * time.Minute)
		e.sweep()
	}
	short()
	if !paused() {
		t.Fatal("a 31-minute shortfall did not pause spending")
	}
	e.setPaused(false) // The admin looked and resumed.
	e.advance(10 * time.Minute)
	e.sweep()
	if paused() {
		t.Fatal("the same shortfall paused again after the admin resumed")
	}
	e.node.SetSpendable(node.Spendable{ActiveMsat: testFunds, AllMsat: testFunds})
	e.advance(10 * time.Minute)
	e.sweep()
	if paused() {
		t.Fatal("paused with no shortfall")
	}
	e.advance(10 * time.Minute)
	short()
	if !paused() {
		t.Fatal("a new shortfall did not pause spending")
	}
}
