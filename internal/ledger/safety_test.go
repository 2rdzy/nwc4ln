package ledger

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/2rdzy/nwc4ln/internal/node"
	"github.com/2rdzy/nwc4ln/internal/node/fake"
)

// wipeNode swaps in a node with the same key and no history, as after a restore
// from seed, and starts a new ledger process on it. It returns Start's error.
func (e *env) wipeNode() error {
	e.t.Helper()
	e.ledger.Close()
	e.node = fake.New()
	e.node.SetSpendable(node.Spendable{ActiveMsat: testFunds, AllMsat: testFunds})
	e.advance(3 * time.Minute) // Past the no-record grace.
	e.ledger = e.fresh(e.node)
	return e.ledger.Start(context.Background())
}

// Catches: holds released for payments that went out, after the node lost its
// payment history but kept its key.
func TestWipedNodeIsRefusedByItsPayments(t *testing.T) {
	e := newEnv(t)
	e.start()
	e.pay(e.node.OutsideInvoice(5 * sat)) // Settled: the node must know it.
	e.node.ScriptPay(fake.PayTimeout)
	pending := e.pay(e.node.OutsideInvoice(5 * sat))
	if err := e.wipeNode(); !errors.Is(err, ErrNodeHistory) {
		if err == nil {
			e.ledger.Close()
		}
		t.Fatalf("got %v; want ErrNodeHistory for a node with no record of a settled payment", err)
	}
	e.wantState(pending.ID, StatePending)
}

// Catches: the same, when the ledger's only history is income.
func TestWipedNodeIsRefusedByItsInvoices(t *testing.T) {
	e := newEnv(t)
	e.start()
	row := e.invoice(5 * sat)
	if err := e.node.Settle(row.PaymentHash); err != nil {
		t.Fatal(err)
	}
	e.waitState(row.ID, StateSettled)
	e.node.ScriptPay(fake.PayTimeout)
	pending := e.pay(e.node.OutsideInvoice(5 * sat))
	if err := e.wipeNode(); !errors.Is(err, ErrNodeHistory) {
		if err == nil {
			e.ledger.Close()
		}
		t.Fatalf("got %v; want ErrNodeHistory for a node with no record of a settled invoice", err)
	}
	e.wantState(pending.ID, StatePending)
}

// Catches: the history check refusing a healthy first start, or a restart on the
// same node.
func TestHistoryCheckAllowsTheSameNode(t *testing.T) {
	e := newEnv(t)
	e.start() // Fresh ledger, fresh node.
	e.pay(e.node.OutsideInvoice(5 * sat))
	row := e.invoice(5 * sat)
	if err := e.node.Settle(row.PaymentHash); err != nil {
		t.Fatal(err)
	}
	e.waitState(row.ID, StateSettled)
	e.restart()
}

// Catches: a wiped node read as a node that is down, so StartRetrying loops for
// ever and the operator never learns why nothing is served.
func TestStartRetryingStopsOnWipedNode(t *testing.T) {
	e := newEnv(t)
	e.start()
	e.pay(e.node.OutsideInvoice(5 * sat))
	if err := e.wipeNode(); err == nil {
		e.ledger.Close()
		t.Fatal("started on a wiped node")
	}
	done := make(chan error, 1)
	go func() { done <- e.ledger.StartRetrying(context.Background()) }()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("StartRetrying started on a wiped node")
		}
	case <-time.After(time.Second):
		t.Fatal("StartRetrying keeps retrying on a wiped node")
	}
}

// Catches: a connection error during the history check read as "no record".
func TestHistoryCheckWithNodeDown(t *testing.T) {
	e := newEnv(t)
	e.start()
	e.pay(e.node.OutsideInvoice(5 * sat))
	e.ledger.Close()
	e.ledger = e.fresh(&downAfterInfo{Node: e.node})
	if err := e.ledger.Start(context.Background()); err == nil || errors.Is(err, ErrNodeChanged) {
		t.Fatalf("got %v, want a plain error that can be retried", err)
	}
}

// downAfterInfo answers Info, then fails every call, as a node that drops during
// the start.
type downAfterInfo struct{ *fake.Node }

func (d *downAfterInfo) TrackPayment(context.Context, string) (node.Payment, error) {
	return node.Payment{}, fake.ErrUnreachable
}

func (d *downAfterInfo) LookupInvoice(context.Context, string) (node.Invoice, error) {
	return node.Invoice{}, fake.ErrUnreachable
}

// Catches: a hold stuck until restart when the write of a final failure fails once.
func TestFailedWriteOfFailureIsRetried(t *testing.T) {
	e := newEnv(t)
	e.start()
	_, err := e.store.DB.Exec(`CREATE TRIGGER block_fail BEFORE UPDATE OF state ON transactions
		WHEN NEW.state = 'failed' BEGIN SELECT RAISE(ABORT, 'disk full'); END`)
	if err != nil {
		t.Fatal(err)
	}
	e.node.ScriptPay(fake.PayFail)
	row := e.pay(e.node.OutsideInvoice(5 * sat))
	e.wantState(row.ID, StatePending)
	if _, err := e.store.DB.Exec(`DROP TRIGGER block_fail`); err != nil {
		t.Fatal(err)
	}
	e.sweep()
	e.wantState(row.ID, StateFailed)
}

// Catches: an account charged for an invoice the node already paid, or is paying,
// with another tool.
func TestInvoicePaidOutsideIsRefused(t *testing.T) {
	for _, step := range []fake.PayStep{fake.PaySucceed, fake.PayTimeout} {
		e := newEnv(t)
		e.start()
		invoice := e.node.OutsideInvoice(10 * sat)
		e.node.ScriptPay(step)
		_, _ = e.node.Pay(context.Background(), node.PayRequest{ // lncli, outside nwc4ln.
			Invoice: invoice, FeeLimitMsat: 100 * sat, Timeout: time.Second,
		})
		before := e.rowCount()
		_, err := e.ledger.Pay(context.Background(), PayRequest{AccountID: e.acct, Invoice: invoice})
		if !errors.Is(err, ErrAlreadyPaid) {
			t.Fatalf("step %d: got %v, want ErrAlreadyPaid", step, err)
		}
		if e.rowCount() != before {
			t.Fatalf("step %d: a row was written", step)
		}
	}
}

// Catches: a second payment of an invoice whose first payment the ledger marked
// failed, but which the node later paid. The node is the source of truth.
func TestLateSuccessBlocksANewPayment(t *testing.T) {
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
		t.Fatalf("got %v, want ErrAlreadyPaid", err)
	}
}
