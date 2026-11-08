package ledger

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/2rdzy/nwc4ln/internal/node"
	"github.com/2rdzy/nwc4ln/internal/node/fake"
)

// lockedBuffer collects log lines from any goroutine.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) count(text string) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return strings.Count(b.buf.String(), text)
}

func (e *env) shortfall() int64 {
	e.t.Helper()
	shortfall, err := e.ledger.Shortfall(context.Background())
	if err != nil {
		e.t.Fatal(err)
	}
	return shortfall
}

func (e *env) payFrom(account int64, amountMsat int64) (Transaction, error) {
	return e.ledger.Pay(context.Background(), PayRequest{AccountID: account, Invoice: e.node.OutsideInvoice(amountMsat)})
}

const lowFundsLog = "app refused: main wallet too low"

// Catches: a drain outside nwc4ln that changes the ledger, lets apps spend money
// promised to sub-wallets, stops sub-wallets from trying, goes unlogged or unreported,
// or does not resume after a refill. Also a withdraw that does not settle the loss.
func TestShortfall(t *testing.T) {
	e := newEnv(t)
	logs := &lockedBuffer{}
	e.ledger.log = slog.New(slog.NewTextHandler(logs, nil))
	e.start()
	wallet := e.addSubWallet()
	e.fund(wallet, 1_000*sat)
	if got := e.shortfall(); got != 1_000*sat-(testFunds+1_000*sat) {
		t.Fatalf("shortfall %d with the node full", got)
	}

	e.node.SetSpendable(node.Spendable{ActiveMsat: 400 * sat, AllMsat: 400 * sat}) // Drained from outside.
	if got := e.shortfall(); got != 600*sat {
		t.Fatalf("shortfall %d, want 600 sat", got)
	}
	rows, sent := e.rowCount(), len(e.node.PayRequests())
	if _, err := e.tryPay(10 * sat); !errors.Is(err, ErrInsufficientBalance) {
		t.Fatalf("app paid from promised funds: %v", err)
	}
	if e.rowCount() != rows || len(e.node.PayRequests()) != sent {
		t.Fatal("a refused app payment left a row or reached the node")
	}
	if got := logs.count(lowFundsLog); got != 1 {
		t.Fatalf("%d log lines for the refusal", got)
	}
	if _, err := e.ledger.Pay(context.Background(), PayRequest{
		AccountID: e.addLimitedApp(0), Invoice: e.node.OutsideInvoice(sat),
	}); !errors.Is(err, ErrQuotaExceeded) || logs.count(lowFundsLog) != 1 {
		t.Fatalf("a limit refusal: %v, logged as low funds: %t", err, logs.count(lowFundsLog) != 1)
	}

	failed, err := e.payFrom(wallet, 500*sat) // The node holds only 400 sat.
	if err != nil || failed.State != StateFailed {
		t.Fatalf("sub-wallet payment %+v, %v", failed, err)
	}
	if got := e.balance(wallet); got != 1_000*sat {
		t.Fatalf("a failed payment changed the balance to %d", got)
	}
	paid, err := e.payFrom(wallet, 300*sat) // First come, first served.
	if err != nil || paid.State != StateSettled {
		t.Fatalf("sub-wallet payment %+v, %v", paid, err)
	}
	if got := e.shortfall(); got != 600*sat {
		t.Fatalf("shortfall %d after the sub-wallet paid what the node had", got)
	}

	e.move(e.ledger.Withdraw, wallet, 200*sat) // The admin settles part of the loss.
	if got := e.shortfall(); got != 400*sat {
		t.Fatalf("shortfall %d after a 200 sat withdraw", got)
	}
	if got := e.balance(wallet); got != 500*sat {
		t.Fatalf("balance %d", got)
	}

	e.node.SetSpendable(node.Spendable{ActiveMsat: testFunds, AllMsat: testFunds}) // A refill.
	if got := e.shortfall(); got != 500*sat-testFunds {
		t.Fatalf("shortfall %d after the refill", got)
	}
	if row, err := e.tryPay(10 * sat); err != nil || row.State != StateSettled {
		t.Fatalf("app after the refill: %+v, %v", row, err)
	}
	if row, err := e.payFrom(wallet, 100*sat); err != nil || row.State != StateSettled {
		t.Fatalf("sub-wallet after the refill: %+v, %v", row, err)
	}
}

// Catches: a peer that is offline raising a false shortfall, because the figure used
// active channels only. Apps still stop, as they cannot pay over that channel.
func TestShortfallCountsOfflinePeers(t *testing.T) {
	e := newEnv(t)
	e.start()
	wallet := e.addSubWallet()
	e.fund(wallet, 1_000*sat)
	e.node.SetSpendable(node.Spendable{ActiveMsat: 0, AllMsat: testFunds + 1_000*sat})
	if got := e.shortfall(); got != -testFunds {
		t.Fatalf("shortfall %d with the peer offline", got)
	}
	if _, err := e.tryPay(10 * sat); !errors.Is(err, ErrInsufficientBalance) {
		t.Fatalf("app paid over an offline channel: %v", err)
	}
}

// Catches: a payment in flight counted twice, once by the node and once as its hold,
// which raises a false shortfall and understates the main wallet; and a payment made
// outside nwc4ln added back as if the ledger held it, which hides a drain.
func TestInFlightCountsOnce(t *testing.T) {
	e := newEnv(t)
	e.start()
	wallet := e.addSubWallet()
	e.fund(wallet, 1_000*sat)
	e.node.SetSpendable(node.Spendable{ActiveMsat: 1_000 * sat, AllMsat: 1_000 * sat}) // Fully allocated.
	e.node.ScriptPay(fake.PayTimeout)
	held, err := e.payFrom(wallet, 100*sat)
	if err != nil || held.State != StatePending {
		t.Fatalf("payment %+v, %v", held, err)
	}
	if got := e.shortfall(); got != 0 {
		t.Fatalf("shortfall %d with a sub-wallet payment in flight", got)
	}
	if got := e.available(); got != 0 {
		t.Fatalf("main wallet %d with a sub-wallet payment in flight", got)
	}

	e.node.ScriptPay(fake.PayTimeout) // Someone pays 50 sat with lncli.
	outside := e.node.OutsideInvoice(50 * sat)
	request := node.PayRequest{Invoice: outside, FeeLimitMsat: sat, Timeout: time.Second}
	if _, err := e.node.Pay(context.Background(), request); !errors.Is(err, fake.ErrPayTimeout) {
		t.Fatal(err)
	}
	if got := e.shortfall(); got != 50*sat {
		t.Fatalf("shortfall %d with an outside payment in flight", got)
	}
	if got := e.available(); got != -50*sat {
		t.Fatalf("main wallet %d with an outside payment in flight", got)
	}

	if err := e.node.Finish(held.PaymentHash, true); err != nil {
		t.Fatal(err)
	}
	e.advance(noRecordGrace + time.Second)
	e.sweep()
	e.wantState(held.ID, StateSettled)
	if got := e.shortfall(); got != 50*sat {
		t.Fatalf("shortfall %d after the sub-wallet payment settled", got)
	}
}

// Catches: an HTLC on an offline channel added back into the active figure (R5), which
// it never left, or one dropped from the figure over all channels (R14); and an HTLC
// added back for a payment the ledger does not hold.
func TestInFlightMsat(t *testing.T) {
	e := newEnv(t)
	e.start()
	e.node.ScriptPay(fake.PayTimeout)
	held := e.pay(e.node.OutsideInvoice(100 * sat))
	htlcs := []node.HTLC{
		{Hash: held.PaymentHash, CostMsat: 60 * sat, Active: true},
		{Hash: held.PaymentHash, CostMsat: 41 * sat, Active: false},
		{Hash: "other", CostMsat: 1_000 * sat, Active: true},
	}
	for _, c := range []struct {
		activeOnly bool
		want       int64
	}{{true, 60 * sat}, {false, 101 * sat}} {
		if got, err := inFlightMsat(context.Background(), e.store.DB, htlcs, c.activeOnly); err != nil || got != c.want {
			t.Errorf("active only %t: %d, %v, want %d", c.activeOnly, got, err, c.want)
		}
	}
}
