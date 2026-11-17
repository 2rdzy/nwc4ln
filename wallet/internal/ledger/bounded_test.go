package ledger

import (
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/2rdzy/nwc4ln/wallet/internal/node"
	"github.com/2rdzy/nwc4ln/wallet/internal/node/fake"
)

// slowNode is a node whose lookups each use a whole node-call timeout on the test
// clock and then fail, and whose Info can fail.
type slowNode struct {
	node.Node
	e        *env
	slow     atomic.Bool
	infoDown atomic.Bool

	mu        sync.Mutex
	looked    []string  // Hashes, in the order asked.
	balanceAt time.Time // When Spendable was first asked, after watch began.
	watch     bool
}

const slowCall = 30 * time.Second // The node adapters' call timeout.

func (n *slowNode) lookup(hash string) error {
	n.mu.Lock()
	n.looked = append(n.looked, hash)
	n.mu.Unlock()
	n.e.advance(slowCall)
	return errors.New("test: lookup timed out")
}

func (n *slowNode) LookupInvoice(ctx context.Context, hash string) (node.Invoice, error) {
	if n.slow.Load() {
		return node.Invoice{}, n.lookup(hash)
	}
	return n.Node.LookupInvoice(ctx, hash)
}

func (n *slowNode) TrackPayment(ctx context.Context, hash string) (node.Payment, error) {
	if n.slow.Load() {
		return node.Payment{}, n.lookup(hash)
	}
	return n.Node.TrackPayment(ctx, hash)
}

func (n *slowNode) Spendable(ctx context.Context) (node.Spendable, error) {
	n.mu.Lock()
	if n.watch && n.balanceAt.IsZero() {
		n.balanceAt = time.Unix(n.e.now.Load(), 0)
	}
	n.mu.Unlock()
	return n.Node.Spendable(ctx)
}

func (n *slowNode) Info(ctx context.Context) (node.Info, error) {
	if n.infoDown.Load() {
		return node.Info{}, errors.New("test: node unreachable")
	}
	return n.Node.Info(ctx)
}

func (n *slowNode) asked() []string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return slices.Clone(n.looked)
}

func newSlowEnv(t *testing.T) (*env, *slowNode) {
	e := newEnv(t)
	slow := &slowNode{Node: e.node, e: e}
	e.ledger = e.fresh(slow)
	e.start()
	return e, slow
}

// Catches: the shortfall check waiting behind a long list of slow lookups (S11). With
// 1,000 pending invoices and 50 pending payments whose lookups each use 30 seconds,
// a due check runs at the start of the next sweep, and one sweep lasts at most both
// pass budgets plus one call each.
func TestShortfallCheckOnTime(t *testing.T) {
	e, slow := newSlowEnv(t)
	for range 1000 {
		if _, err := e.ledger.MakeInvoice(context.Background(), InvoiceRequest{AccountID: e.acct, ConnectionID: e.connection(e.acct), AmountMsat: sat}); err != nil {
			t.Fatal(err)
		}
	}
	e.node.ScriptPay(fake.PayTimeout)
	for range 50 {
		e.pay(e.node.OutsideInvoice(sat))
	}
	slow.slow.Store(true)
	limit := 2 * (passBudget + slowCall)
	for pass := range 3 {
		e.advance(shortfallCheckEvery) // The check is due when the sweep begins.
		slow.mu.Lock()
		slow.watch, slow.balanceAt = true, time.Time{}
		slow.mu.Unlock()
		began := time.Unix(e.now.Load(), 0)
		_ = e.ledger.sweep(context.Background()) // The lookups fail.
		took := time.Unix(e.now.Load(), 0).Sub(began)
		slow.mu.Lock()
		waited := slow.balanceAt.Sub(began)
		checked := !slow.balanceAt.IsZero()
		slow.mu.Unlock()
		if !checked || waited > 0 {
			t.Fatalf("sweep %d: shortfall check ran=%v, %s after the sweep began", pass, checked, waited)
		}
		if took > limit {
			t.Errorf("sweep %d took %s, above %s", pass, took, limit)
		}
	}
	if got := len(slow.asked()); got == 0 || got > 3*4 {
		t.Errorf("%d lookups in 3 sweeps", got)
	}
}

// Catches: a pass that stops at its budget and never reaches the rows after that
// point, or does not carry on until the next due pass (S11).
func TestPassCarriesOn(t *testing.T) {
	e, slow := newSlowEnv(t)
	var hashes []string
	for range 5 {
		row, err := e.ledger.MakeInvoice(context.Background(), InvoiceRequest{AccountID: e.acct, ConnectionID: e.connection(e.acct), AmountMsat: sat})
		if err != nil {
			t.Fatal(err)
		}
		hashes = append(hashes, row.PaymentHash)
	}
	// A stored settle index: only a due pass, or one that is unfinished, looks.
	if _, err := e.store.DB.Exec(`INSERT INTO settings (key, value) VALUES (?, '7')`, settleIndexKey); err != nil {
		t.Fatal(err)
	}
	slow.slow.Store(true)
	e.advance(catchUpEvery)
	for range 6 { // One lookup fits each budget; the sixth sweep has nothing due.
		_ = e.ledger.sweep(context.Background())
	}
	if got := slow.asked(); !slices.Equal(got, hashes) {
		t.Errorf("lookups %v, want each invoice once, in order: %v", got, hashes)
	}
}

// Catches: a payment that leaves the node while its sync state cannot be read, one
// refused for that after the state is read again, and one refused only because a
// slow sweep left a good read old (R18).
func TestSyncStateUnknown(t *testing.T) {
	e, slow := newSlowEnv(t)
	e.advance(10 * time.Minute) // A long sweep: the last read worked.
	if e.ledger.NodeSyncUnknown() {
		t.Fatal("unknown after a good read, with no failed one")
	}
	e.readSync()
	slow.infoDown.Store(true)
	e.advance(syncStaleAfter)
	_ = e.ledger.sweep(context.Background())
	if _, err := e.tryPay(sat); err != nil {
		t.Fatalf("at the limit: %v", err)
	}
	e.advance(time.Second)
	_ = e.ledger.sweep(context.Background())
	if !e.ledger.NodeSyncUnknown() {
		t.Error("state still known after the limit")
	}
	if _, err := e.tryPay(sat); !errors.Is(err, ErrSyncUnknown) {
		t.Fatalf("past the limit: %v, want ErrSyncUnknown", err)
	}
	internal := e.invoice(sat)
	if _, err := e.ledger.Pay(context.Background(), PayRequest{AccountID: e.acct, ConnectionID: e.connection(e.acct),
		Invoice: internal.Invoice}); err != nil && errors.Is(err, ErrSyncUnknown) {
		t.Errorf("payment between accounts refused: %v", err)
	}
	slow.infoDown.Store(false)
	_ = e.ledger.sweep(context.Background())
	if _, err := e.tryPay(sat); err != nil {
		t.Fatalf("after a fresh read: %v", err)
	}
}
