package ledger

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"math"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/2rdzy/nwc4ln/internal/node"
	"github.com/2rdzy/nwc4ln/internal/node/fake"
	"github.com/2rdzy/nwc4ln/internal/store"
)

const sat = int64(1_000)

// testFunds is more than any test spends, unless it sets its own figure.
const testFunds = 1_000_000_000 * sat

// env is one ledger on a real SQLite file and the fake. The loops are slowed down,
// so a test decides when a sweep runs by calling it.
type env struct {
	t      *testing.T
	store  *store.Store
	path   string // Of the database.
	node   *fake.Node
	ledger *Ledger
	now    *atomic.Int64 // Unix seconds, read by the ledger's clock.
	acct   int64
}

func newEnv(t *testing.T) *env {
	t.Helper()
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "test.db")
	db, err := store.Create(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	e := &env{t: t, store: db, path: path, node: fake.New(), now: &atomic.Int64{}}
	e.node.SetSpendable(node.Spendable{ActiveMsat: testFunds, AllMsat: testFunds})
	e.now.Store(time.Now().Unix())
	result, err := db.DB.ExecContext(ctx, `INSERT INTO accounts (type, name, can_pay, can_invoice, created_at)
		VALUES ('app', 'test', 1, 1, 0)`)
	if err != nil {
		t.Fatal(err)
	}
	if e.acct, err = result.LastInsertId(); err != nil {
		t.Fatal(err)
	}
	e.ledger = e.fresh(e.node)
	return e
}

// fresh makes a ledger on the same database, as a restarted process would.
func (e *env) fresh(n node.Node) *Ledger {
	l := New(e.store, n, slog.New(slog.NewTextHandler(io.Discard, nil)))
	l.now = func() time.Time { return time.Unix(e.now.Load(), 0) }
	l.sweepEvery, l.retryEvery = time.Hour, time.Millisecond
	return l
}

func (e *env) start() {
	e.t.Helper()
	if err := e.ledger.Start(context.Background()); err != nil {
		e.t.Fatal(err)
	}
	e.t.Cleanup(e.ledger.Close)
	e.waitUntil("the stream opens", func() bool { return e.node.Streams() > 0 })
}

func (e *env) waitUntil(what string, done func() bool) {
	e.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !done() {
		if time.Now().After(deadline) {
			e.t.Fatalf("timed out waiting until %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

// restart stops the ledger and starts a new one on the same database and node.
func (e *env) restart() {
	e.t.Helper()
	e.ledger.Close()
	e.ledger = e.fresh(e.node)
	e.start()
}

func (e *env) advance(d time.Duration) { e.now.Add(int64(d / time.Second)) }

func (e *env) sweep() {
	e.t.Helper()
	if err := e.ledger.sweep(context.Background()); err != nil {
		e.t.Fatal(err)
	}
}

func (e *env) pay(invoice string) Transaction {
	e.t.Helper()
	tx, err := e.ledger.Pay(context.Background(), PayRequest{AccountID: e.acct, Invoice: invoice})
	if err != nil {
		e.t.Fatal(err)
	}
	return tx
}

func (e *env) invoice(amount int64) Transaction {
	e.t.Helper()
	tx, err := e.ledger.MakeInvoice(context.Background(), InvoiceRequest{AccountID: e.acct, AmountMsat: amount})
	if err != nil {
		e.t.Fatal(err)
	}
	return tx
}

func (e *env) get(id int64) Transaction {
	e.t.Helper()
	tx, err := e.ledger.transaction(context.Background(), id)
	if err != nil {
		e.t.Fatal(err)
	}
	return tx
}

func (e *env) wantState(id int64, state string) Transaction {
	e.t.Helper()
	tx := e.get(id)
	if tx.State != state {
		e.t.Fatalf("transaction %d is %s, want %s", id, tx.State, state)
	}
	return tx
}

func (e *env) waitState(id int64, state string) {
	e.t.Helper()
	e.waitUntil("transaction is "+state, func() bool { return e.get(id).State == state })
}

func (e *env) rowCount() int {
	e.t.Helper()
	var count int
	err := e.store.DB.QueryRow("SELECT count(*) FROM transactions").Scan(&count)
	if err != nil {
		e.t.Fatal(err)
	}
	return count
}

func (e *env) index() uint64 {
	e.t.Helper()
	index, err := settleIndex(context.Background(), e.store.DB)
	if err != nil {
		e.t.Fatal(err)
	}
	return index
}

// Catches: rounding error or overflow in the reserve.
func TestReserve(t *testing.T) {
	cases := []struct{ amount, want int64 }{
		{0, 10_000},
		{999 * sat, 10_000},
		{1_000 * sat, 10_000},
		{1_000*sat + 1, 10_001},
		{1_001 * sat, 10_010},
		{MaxAmountMsat, MaxAmountMsat / 100},
		{MaxAmountMsat - 1, MaxAmountMsat / 100},
		{math.MaxInt64, math.MaxInt64/100 + 1},
	}
	for _, c := range cases {
		if got := Reserve(c.amount); got != c.want {
			t.Errorf("Reserve(%d) = %d, want %d", c.amount, got, c.want)
		}
	}
	if total := MaxAmountMsat + Reserve(MaxAmountMsat); total < MaxAmountMsat {
		t.Errorf("amount plus reserve overflows: %d", int64(total))
	}
}

// Catches: a fee above the hold, and the reserve charged in place of the fee.
func TestFeeLimitIsReserveAndRealFeeIsCharged(t *testing.T) {
	e := newEnv(t)
	e.start()
	e.node.SetRouteFee(1_234)
	tx := e.pay(e.node.OutsideInvoice(1_001 * sat))
	if requests := e.node.PayRequests(); len(requests) != 1 || requests[0].FeeLimitMsat != 10_010 {
		t.Fatalf("pay requests: %+v", requests)
	}
	if tx.State != StateSettled || tx.FeeMsat != 1_234 || tx.ReserveMsat != 10_010 || tx.Preimage == "" {
		t.Fatalf("transaction: %+v", tx)
	}
}

// Catches: funds released for a paid invoice.
func TestTimeoutThenSuccess(t *testing.T) {
	e := newEnv(t)
	e.start()
	invoice := e.node.OutsideInvoice(5 * sat)
	e.node.ScriptPay(fake.PayTimeout)
	tx := e.pay(invoice)
	if tx.State != StatePending {
		t.Fatalf("state after a timeout: %s", tx.State)
	}
	e.advance(time.Hour)
	e.sweep()
	e.wantState(tx.ID, StatePending) // Still in flight, however long it takes.
	if err := e.node.Finish(tx.PaymentHash, true); err != nil {
		t.Fatal(err)
	}
	e.sweep()
	if got := e.wantState(tx.ID, StateSettled); got.Preimage == "" {
		t.Fatal("no preimage")
	}
}

// Catches: funds stuck forever.
func TestTimeoutThenFailure(t *testing.T) {
	e := newEnv(t)
	e.start()
	invoice := e.node.OutsideInvoice(5 * sat)
	e.node.ScriptPay(fake.PayTimeout)
	tx := e.pay(invoice)
	if err := e.node.Finish(tx.PaymentHash, false); err != nil {
		t.Fatal(err)
	}
	e.advance(noRecordGrace)
	e.sweep()
	e.wantState(tx.ID, StateFailed)
}

// Catches: funds released for a request still in transit, and a hold stuck until
// the next restart.
func TestNoRecordAtRunTimeWaitsForGrace(t *testing.T) {
	e := newEnv(t)
	e.start()
	e.node.ScriptPay(fake.PayLost)
	tx := e.pay(e.node.OutsideInvoice(5 * sat))
	e.wantState(tx.ID, StatePending)
	e.advance(noRecordGrace - time.Second)
	e.sweep()
	e.wantState(tx.ID, StatePending)
	e.advance(time.Second)
	e.sweep()
	e.wantState(tx.ID, StateFailed)
}

// Catches: paying twice, and a retry blocked after a real failure.
func TestOneAttemptPerInvoice(t *testing.T) {
	e := newEnv(t)
	e.start()
	ctx := context.Background()

	pending := e.node.OutsideInvoice(5 * sat)
	e.node.ScriptPay(fake.PayTimeout)
	e.pay(pending)
	_, err := e.ledger.Pay(ctx, PayRequest{AccountID: e.acct, Invoice: pending})
	if !errors.Is(err, ErrAlreadyPaid) {
		t.Fatalf("while pending: %v", err)
	}

	settled := e.node.OutsideInvoice(5 * sat)
	e.pay(settled)
	_, err = e.ledger.Pay(ctx, PayRequest{AccountID: e.acct, Invoice: settled})
	if !errors.Is(err, ErrAlreadyPaid) {
		t.Fatalf("after settling: %v", err)
	}

	failed := e.node.OutsideInvoice(5 * sat)
	e.node.ScriptPay(fake.PayFail)
	if tx := e.pay(failed); tx.State != StateFailed {
		t.Fatalf("first try: %s", tx.State)
	}
	if tx := e.pay(failed); tx.State != StateSettled {
		t.Fatalf("retry: %s", tx.State)
	}
}

// Catches: two payments of one invoice both passing the one-attempt check.
func TestConcurrentPaymentsOfOneInvoice(t *testing.T) {
	e := newEnv(t)
	e.start()
	invoice := e.node.OutsideInvoice(5 * sat)
	var wg sync.WaitGroup
	var accepted atomic.Int32
	for range 10 {
		wg.Go(func() {
			if _, err := e.ledger.Pay(context.Background(), PayRequest{AccountID: e.acct, Invoice: invoice}); err == nil {
				accepted.Add(1)
			} else if !errors.Is(err, ErrAlreadyPaid) {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	if accepted.Load() != 1 || len(e.node.PayRequests()) != 1 {
		t.Fatalf("accepted %d, node saw %d", accepted.Load(), len(e.node.PayRequests()))
	}
}

// Catches: a hold never released after a crash between the insert and the node call.
func TestCrashBeforeNodeCall(t *testing.T) {
	e := newEnv(t)
	e.start()
	e.ledger.beforePay = func() { e.ledger.stop() } // The process dies here.
	tx := e.pay(e.node.OutsideInvoice(5 * sat))
	e.wantState(tx.ID, StatePending)
	if requests := e.node.PayRequests(); len(requests) != 0 {
		t.Fatalf("the node saw the payment: %+v", requests)
	}
	e.restart()
	e.wantState(tx.ID, StatePending) // Too young to believe "no record".
	e.advance(noRecordGrace)
	e.sweep()
	e.wantState(tx.ID, StateFailed)
}

// Catches: funds released for a payment that may settle.
func TestNodeUnreachableDuringRestart(t *testing.T) {
	e := newEnv(t)
	e.start()
	e.node.ScriptPay(fake.PayTimeout)
	tx := e.pay(e.node.OutsideInvoice(5 * sat))
	e.ledger.Close()

	e.node.SetUnreachable(true)
	e.ledger = e.fresh(e.node)
	if err := e.ledger.Start(context.Background()); err == nil {
		t.Fatal("started with the node unreachable")
	}
	e.wantState(tx.ID, StatePending)

	e.node.SetUnreachable(false)
	if err := e.node.Finish(tx.PaymentHash, true); err != nil {
		t.Fatal(err)
	}
	e.start()
	e.wantState(tx.ID, StateSettled)
}

// blockedPay holds Pay until released, so a test can act during a live node call.
type blockedPay struct {
	*fake.Node
	entered, release chan struct{}
}

func (b *blockedPay) Pay(ctx context.Context, request node.PayRequest) (node.Payment, error) {
	close(b.entered)
	<-b.release
	return b.Node.Pay(context.Background(), request)
}

// Catches: the end of a caller's wait abandoning the payment, and a sweep failing
// a payment the node has not seen yet.
func TestCallerStopsWaitingDuringLiveCall(t *testing.T) {
	e := newEnv(t)
	blocked := &blockedPay{Node: e.node, entered: make(chan struct{}), release: make(chan struct{})}
	e.ledger = e.fresh(blocked)
	e.start()
	release := sync.OnceFunc(func() { close(blocked.release) })
	t.Cleanup(release) // Runs before Close, which waits for the call to end.
	invoice := e.node.OutsideInvoice(5 * sat)

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan Transaction)
	go func() {
		tx, err := e.ledger.Pay(ctx, PayRequest{AccountID: e.acct, Invoice: invoice})
		if err != nil {
			t.Error(err)
		}
		result <- tx
	}()
	<-blocked.entered
	cancel()
	tx := <-result
	if tx.State != StatePending {
		t.Fatalf("state: %s", tx.State)
	}

	// The fake has no record yet, as the request is still on its way.
	e.advance(time.Hour)
	e.sweep()
	e.wantState(tx.ID, StatePending)

	release()
	e.waitState(tx.ID, StateSettled)
}

// Catches: a wrong internal path: a fee, a reserve, no preimage, or the node used.
func TestInternalPayment(t *testing.T) {
	e := newEnv(t)
	e.start()
	in := e.invoice(5 * sat)
	out := e.pay(in.Invoice)
	if out.State != StateSettled || !out.Internal || out.FeeMsat != 0 || out.ReserveMsat != 0 {
		t.Fatalf("outgoing: %+v", out)
	}
	if out.Preimage == "" || out.Preimage != in.Preimage {
		t.Fatalf("preimage %q, invoice has %q", out.Preimage, in.Preimage)
	}
	if got := e.wantState(in.ID, StateSettled); !got.Internal {
		t.Fatalf("incoming: %+v", got)
	}
	if requests := e.node.PayRequests(); len(requests) != 0 {
		t.Fatalf("the node saw the payment: %+v", requests)
	}
	stored, err := e.node.LookupInvoice(context.Background(), in.PaymentHash)
	if err != nil || stored.State != node.InvoiceCanceled {
		t.Fatalf("invoice at the node: %+v, %v", stored, err)
	}
}

// Catches: a double credit when the invoice may already be paid from outside.
func TestInternalPaymentWhenCancelFails(t *testing.T) {
	e := newEnv(t)
	e.start()
	in := e.invoice(5 * sat)
	e.node.FailNextCancel(errors.New("invoice already settled"))
	out := e.pay(in.Invoice)
	if out.State != StateFailed {
		t.Fatalf("outgoing: %+v", out)
	}
	e.wantState(in.ID, StatePending)
}

// Catches: a half-settled transfer. A trigger aborts the second side, which is
// what a crash does to an unfinished database transaction.
func TestCrashBetweenTwoSides(t *testing.T) {
	e := newEnv(t)
	e.start()
	in := e.invoice(5 * sat)
	_, err := e.store.DB.Exec(`CREATE TRIGGER crash BEFORE UPDATE ON transactions
		WHEN NEW.type = 'incoming' AND NEW.state = 'settled'
		BEGIN SELECT RAISE(ABORT, 'crash'); END`)
	if err != nil {
		t.Fatal(err)
	}
	out := e.pay(in.Invoice)
	e.wantState(out.ID, StatePending)
	e.wantState(in.ID, StatePending)

	if _, err := e.store.DB.Exec("DROP TRIGGER crash"); err != nil {
		t.Fatal(err)
	}
	e.restart()
	e.advance(noRecordGrace)
	e.sweep()
	e.wantState(out.ID, StateFailed)
	e.wantState(in.ID, StateFailed)
}

// sweepDuringCancel runs a sweep right after the cancel, inside an internal payment.
type sweepDuringCancel struct {
	*fake.Node
	sweep func()
}

func (s *sweepDuringCancel) CancelInvoice(ctx context.Context, hash string) error {
	err := s.Node.CancelInvoice(ctx, hash)
	s.sweep()
	return err
}

// Catches: a sweep at index 0 seeing the canceled invoice and failing its row,
// so that a running internal payment cannot settle.
func TestSweepLeavesRunningInternalPayment(t *testing.T) {
	e := newEnv(t)
	e.ledger = e.fresh(&sweepDuringCancel{Node: e.node, sweep: func() {
		if err := e.ledger.sweep(context.Background()); err != nil {
			t.Error(err) // This runs off the test goroutine, so it cannot be Fatal.
		}
	}})
	e.start()
	in := e.invoice(5 * sat)
	if out := e.pay(in.Invoice); out.State != StateSettled {
		t.Fatalf("outgoing: %+v", out)
	}
	e.wantState(in.ID, StateSettled)
}

// Catches: income credited twice.
func TestSameInvoiceSettledTwice(t *testing.T) {
	e := newEnv(t)
	e.start()
	in := e.invoice(5 * sat)
	if err := e.node.Settle(in.PaymentHash); err != nil {
		t.Fatal(err)
	}
	e.waitState(in.ID, StateSettled)
	first := e.get(in.ID)
	settled, err := e.node.LookupInvoice(context.Background(), in.PaymentHash)
	if err != nil {
		t.Fatal(err)
	}
	settled.PaidMsat = 999 * sat
	if err := e.ledger.onSettled(context.Background(), settled); err != nil {
		t.Fatal(err)
	}
	if got := e.get(in.ID); got.AmountMsat != first.AmountMsat {
		t.Fatalf("credited again: %d", got.AmountMsat)
	}
}

// Catches: crediting the invoice amount, not what arrived.
func TestOverpaidInvoice(t *testing.T) {
	e := newEnv(t)
	e.start()
	in := e.invoice(5 * sat)
	if err := e.node.SettlePaid(in.PaymentHash, 7*sat); err != nil {
		t.Fatal(err)
	}
	e.waitState(in.ID, StateSettled)
	if got := e.get(in.ID); got.AmountMsat != 7*sat {
		t.Fatalf("credited %d", got.AmountMsat)
	}
}

// Catches: income missed while stopped, with an index stored and without one.
func TestInvoicePaidWhileDown(t *testing.T) {
	for _, stored := range []bool{false, true} {
		e := newEnv(t)
		t.Logf("index stored: %v", stored)
		e.start()
		if stored {
			earlier := e.invoice(5 * sat)
			if err := e.node.Settle(earlier.PaymentHash); err != nil {
				t.Fatal(err)
			}
			e.waitUntil("an index is stored", func() bool { return e.index() != 0 })
		}
		in := e.invoice(5 * sat)
		e.ledger.Close()
		if err := e.node.Settle(in.PaymentHash); err != nil {
			t.Fatal(err)
		}
		e.ledger = e.fresh(e.node)
		e.start()
		// Start itself must have finished this: no waiting here.
		e.wantState(in.ID, StateSettled)
	}
}

// lateStream runs a hook just before the stream opens.
type lateStream struct {
	*fake.Node
	once   sync.Once
	before func()
}

func (s *lateStream) SubscribeInvoices(ctx context.Context, from uint64, onSettled func(node.Invoice) error) error {
	s.once.Do(s.before)
	return s.Node.SubscribeInvoices(ctx, from, onSettled)
}

// Catches: an invoice lost for good. It settles after the start check and before
// the stream opens, so index 0 never sends it, and a later invoice then stores an
// index past it.
func TestInvoiceInTheIndexGap(t *testing.T) {
	e := newEnv(t)
	e.start()
	missed, later := e.invoice(5*sat), e.invoice(6*sat)
	e.ledger.Close()

	settled := make(chan struct{})
	e.ledger = e.fresh(&lateStream{Node: e.node, before: func() {
		if err := e.node.Settle(missed.PaymentHash); err != nil {
			t.Error(err)
		}
		close(settled)
	}})
	e.start()
	<-settled
	if err := e.node.Settle(later.PaymentHash); err != nil {
		t.Fatal(err)
	}
	e.waitState(later.ID, StateSettled)
	e.wantState(missed.ID, StateSettled)
	e.waitUntil("the index passes both", func() bool { return e.index() == 2 })
}

// Catches: the same gap with no later invoice: the sweep must find it.
func TestSweepFindsInvoiceAtIndexZero(t *testing.T) {
	e := newEnv(t)
	in := e.invoice(5 * sat)
	e.start()
	e.ledger.Close() // No stream, so only the sweep can find it.
	if err := e.node.Settle(in.PaymentHash); err != nil {
		t.Fatal(err)
	}
	e.sweep()
	e.wantState(in.ID, StateSettled)
}

// Catches: the stream stuck on income made outside nwc4ln.
func TestOutsideIncomeAdvancesIndex(t *testing.T) {
	e := newEnv(t)
	e.start()
	outside, err := e.node.MakeInvoice(context.Background(), node.InvoiceRequest{AmountMsat: 5 * sat})
	if err != nil {
		t.Fatal(err)
	}
	in := e.invoice(5 * sat)
	if err := e.node.Settle(outside.Hash); err != nil {
		t.Fatal(err)
	}
	if err := e.node.Settle(in.PaymentHash); err != nil {
		t.Fatal(err)
	}
	e.waitState(in.ID, StateSettled)
	e.waitUntil("the index passes both", func() bool { return e.index() == 2 })
	if e.rowCount() != 1 {
		t.Fatalf("%d rows", e.rowCount())
	}
}

// Catches: the service going deaf after the stream drops, or income missed while
// it was down.
func TestStreamReconnects(t *testing.T) {
	e := newEnv(t)
	e.start()
	first, second := e.invoice(5*sat), e.invoice(6*sat)
	if err := e.node.Settle(first.PaymentHash); err != nil {
		t.Fatal(err)
	}
	e.waitUntil("an index is stored", func() bool { return e.index() == 1 })

	e.node.SetUnreachable(true)
	e.waitUntil("the stream drops", func() bool { return e.node.Streams() == 0 })
	if err := e.node.Settle(second.PaymentHash); err != nil {
		t.Fatal(err)
	}
	e.node.SetUnreachable(false)
	e.waitState(second.ID, StateSettled)
}

// Catches: income missing from the account's history.
func TestIncomeInHistory(t *testing.T) {
	e := newEnv(t)
	e.start()
	in := e.invoice(5 * sat)
	if err := e.node.Settle(in.PaymentHash); err != nil {
		t.Fatal(err)
	}
	e.waitState(in.ID, StateSettled)
	history, _, err := e.ledger.History(context.Background(), HistoryQuery{AccountID: e.acct, Unpaid: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 1 || history[0].ID != in.ID || history[0].State != StateSettled {
		t.Fatalf("history: %+v", history)
	}
}

// Catches: a bad request leaving a row, and so a hold.
func TestRefusedRequestsWriteNothing(t *testing.T) {
	e := newEnv(t)
	e.start()
	ctx := context.Background()
	amountless := e.node.OutsideInvoice(0)
	tooBig := e.node.OutsideInvoice(MaxAmountMsat + 1)
	normal := e.node.OutsideInvoice(5 * sat)

	cases := []struct {
		name    string
		request PayRequest
		want    error
	}{
		{"amountless with no amount", PayRequest{AccountID: e.acct, Invoice: amountless}, ErrBadAmount},
		{"negative amount", PayRequest{AccountID: e.acct, Invoice: amountless, AmountMsat: -1}, ErrBadAmount},
		{"above the cap", PayRequest{AccountID: e.acct, Invoice: tooBig}, ErrBadAmount},
		{"differs from the invoice", PayRequest{AccountID: e.acct, Invoice: normal, AmountMsat: 6 * sat}, ErrBadAmount},
		{"unknown account", PayRequest{AccountID: 99, Invoice: normal}, ErrUnknownAccount},
	}
	for _, c := range cases {
		if _, err := e.ledger.Pay(ctx, c.request); !errors.Is(err, c.want) {
			t.Errorf("%s: %v", c.name, err)
		}
	}
	if _, err := e.ledger.MakeInvoice(ctx, InvoiceRequest{AccountID: e.acct}); !errors.Is(err, ErrBadAmount) {
		t.Errorf("zero invoice: %v", err)
	}
	if _, err := e.ledger.MakeInvoice(ctx, InvoiceRequest{AccountID: 99, AmountMsat: 1}); !errors.Is(err, ErrUnknownAccount) {
		t.Errorf("invoice for unknown account: %v", err)
	}
	if _, err := e.store.DB.Exec("UPDATE accounts SET deleted_at = 1"); err != nil {
		t.Fatal(err)
	}
	if _, err := e.ledger.Pay(ctx, PayRequest{AccountID: e.acct, Invoice: normal}); !errors.Is(err, ErrAccountDeleted) {
		t.Errorf("deleted account: %v", err)
	}
	if _, err := e.store.DB.Exec("UPDATE accounts SET deleted_at = NULL"); err != nil {
		t.Fatal(err)
	}
	e.advance(2 * time.Hour)
	if _, err := e.ledger.Pay(ctx, PayRequest{AccountID: e.acct, Invoice: normal}); !errors.Is(err, ErrInvoiceExpired) {
		t.Errorf("expired: %v", err)
	}
	if e.rowCount() != 0 {
		t.Fatalf("%d rows written", e.rowCount())
	}
}

// Catches: paying an own invoice that is settled or closed, and an amountless
// invoice that loses its amount on the way to the node.
func TestOwnClosedInvoiceAndAmountless(t *testing.T) {
	e := newEnv(t)
	e.start()
	in := e.invoice(5 * sat)
	if err := e.node.Settle(in.PaymentHash); err != nil {
		t.Fatal(err)
	}
	e.waitState(in.ID, StateSettled)
	_, err := e.ledger.Pay(context.Background(), PayRequest{AccountID: e.acct, Invoice: in.Invoice})
	if !errors.Is(err, ErrInvoiceClosed) {
		t.Fatalf("own settled invoice: %v", err)
	}

	amountless := e.node.OutsideInvoice(0)
	tx, err := e.ledger.Pay(context.Background(), PayRequest{AccountID: e.acct, Invoice: amountless, AmountMsat: 3 * sat})
	if err != nil || tx.State != StateSettled || tx.AmountMsat != 3*sat {
		t.Fatalf("amountless: %+v, %v", tx, err)
	}
	if got := e.node.PayRequests(); len(got) != 1 || got[0].AmountMsat != 3*sat || got[0].FeeLimitMsat != minReserveMsat {
		t.Fatalf("node request: %+v", got)
	}
}

// Catches: requests answered before recovery has run.
func TestPayBeforeStartIsRefused(t *testing.T) {
	e := newEnv(t)
	_, err := e.ledger.Pay(context.Background(), PayRequest{AccountID: e.acct, Invoice: e.node.OutsideInvoice(5 * sat)})
	if !errors.Is(err, ErrNotRunning) {
		t.Fatalf("%v", err)
	}
}

// Catches: a fast restart failing a payment whose request is still in transit. A
// second process would do the same, but the data folder lock prevents that.
func TestYoungRowWithNoAttemptIsNotFailed(t *testing.T) {
	e := newEnv(t)
	e.start()
	other := e.fresh(e.node) // Knows nothing of this payment, like a restarted process.
	e.ledger.beforePay = func() {
		if err := other.trackPending(context.Background()); err != nil {
			t.Error(err)
		}
	}
	if tx := e.pay(e.node.OutsideInvoice(5 * sat)); tx.State != StateSettled {
		t.Fatalf("state: %s", tx.State)
	}
}

// closedAfterPay closes the database once the node has the payment, so that
// reading the result fails.
type closedAfterPay struct {
	*fake.Node
	after func()
}

func (c *closedAfterPay) Pay(ctx context.Context, request node.PayRequest) (node.Payment, error) {
	payment, err := c.Node.Pay(ctx, request)
	c.after()
	return payment, err
}

// Catches: an error after the hold, which a client would read as "not paid".
func TestReadFailureAfterHoldIsPending(t *testing.T) {
	e := newEnv(t)
	e.ledger = e.fresh(&closedAfterPay{Node: e.node, after: func() { _ = e.store.DB.Close() }})
	e.start()
	tx, err := e.ledger.Pay(context.Background(), PayRequest{AccountID: e.acct, Invoice: e.node.OutsideInvoice(5 * sat)})
	if err != nil {
		t.Fatalf("error after the hold: %v", err)
	}
	if tx.State != StatePending || tx.ID == 0 {
		t.Fatalf("transaction: %+v", tx)
	}
}

// Catches: a success lost in silence when the row has failed already.
func TestSuccessOnFailedRowRaisesAlert(t *testing.T) {
	e := newEnv(t)
	e.start()
	invoice := e.node.OutsideInvoice(5 * sat)
	e.ledger.beforePay = func() {
		if _, err := e.store.DB.Exec("UPDATE transactions SET state = 'failed'"); err != nil {
			t.Error(err)
		}
	}
	tx := e.pay(invoice)
	if tx.State != StateFailed {
		t.Fatalf("state: %s", tx.State)
	}
	alerts, err := e.ledger.Alerts(context.Background())
	if err != nil || len(alerts) != 1 || alerts[0].Kind != AlertPaidAfterFailed {
		t.Fatalf("alerts %+v, %v", alerts, err)
	}
}

// otherNode has a different identity.
type otherNode struct{ *fake.Node }

func (o otherNode) Info(ctx context.Context) (node.Info, error) {
	info, err := o.Node.Info(ctx)
	info.PubKey = "03other"
	return info, err
}

// Catches: a swapped node's "no record" answers releasing every hold.
func TestSwappedNodeIsRefused(t *testing.T) {
	e := newEnv(t)
	e.start()
	e.node.ScriptPay(fake.PayTimeout)
	tx := e.pay(e.node.OutsideInvoice(5 * sat))
	e.ledger.Close()
	e.advance(time.Hour)

	swapped := e.fresh(otherNode{fake.New()})
	if err := swapped.Start(context.Background()); !errors.Is(err, ErrNodeChanged) {
		t.Fatalf("start: %v", err)
	}
	e.wantState(tx.ID, StatePending)
}

// Catches: charging the row's amount when the node sent another: the hash was
// paid by a different tool in the moment after the check before the hold, which
// TestInvoicePaidOutsideIsRefused covers.
func TestChargesWhatTheNodeSent(t *testing.T) {
	e := newEnv(t)
	e.start()
	invoice := e.node.OutsideInvoice(0)
	e.ledger.beforePay = func() {
		_, err := e.node.Pay(context.Background(), node.PayRequest{
			Invoice: invoice, AmountMsat: 1 * sat, FeeLimitMsat: minReserveMsat, Timeout: time.Second,
		})
		if err != nil {
			t.Error(err)
		}
	}
	tx, err := e.ledger.Pay(context.Background(), PayRequest{AccountID: e.acct, Invoice: invoice, AmountMsat: 900 * sat})
	if err != nil {
		t.Fatal(err)
	}
	if got := e.get(tx.ID); got.State != StateSettled || got.AmountMsat != 1*sat {
		t.Fatalf("transaction: %+v", got)
	}
}

// Catches: a payment the node would refuse holding funds for the whole grace.
func TestOwnInvoiceNotInLedgerIsRefused(t *testing.T) {
	e := newEnv(t)
	e.start()
	own, err := e.node.MakeInvoice(context.Background(), node.InvoiceRequest{AmountMsat: 5 * sat})
	if err != nil {
		t.Fatal(err)
	}
	_, err = e.ledger.Pay(context.Background(), PayRequest{AccountID: e.acct, Invoice: own.PaymentRequest})
	if !errors.Is(err, ErrOwnInvoice) {
		t.Fatalf("%v", err)
	}
	if e.rowCount() != 0 {
		t.Fatalf("%d rows written", e.rowCount())
	}
}

// removedInvoices answers "not found", like --gc-canceled-invoices-on-the-fly.
type removedInvoices struct {
	*fake.Node
	removed map[string]bool
}

func (r *removedInvoices) LookupInvoice(ctx context.Context, hash string) (node.Invoice, error) {
	if r.removed[hash] {
		return node.Invoice{}, node.ErrNotFound
	}
	return r.Node.LookupInvoice(ctx, hash)
}

// Catches: rows that stay pending for ever once an index is stored, each one looked
// up at every start.
func TestSlowSweepClosesInvoices(t *testing.T) {
	e := newEnv(t)
	removed := &removedInvoices{Node: e.node, removed: map[string]bool{}}
	e.ledger = e.fresh(removed)
	e.start()
	first := e.invoice(5 * sat)
	if err := e.node.Settle(first.PaymentHash); err != nil {
		t.Fatal(err)
	}
	e.waitUntil("an index is stored", func() bool { return e.index() != 0 })

	canceled, gone, open := e.invoice(5*sat), e.invoice(5*sat), e.invoice(5*sat)
	if err := e.node.CancelInvoice(context.Background(), canceled.PaymentHash); err != nil {
		t.Fatal(err)
	}
	removed.removed[gone.PaymentHash] = true
	e.sweep()
	e.wantState(canceled.ID, StatePending) // Not due yet.
	e.advance(catchUpEvery)
	e.sweep()
	e.wantState(canceled.ID, StateFailed)
	e.wantState(gone.ID, StateFailed)
	e.wantState(open.ID, StatePending)
}

// settledLog records what OnSettled reports.
type settledLog struct {
	mu   sync.Mutex
	rows []Transaction
}

func (s *settledLog) add(row Transaction) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rows = append(s.rows, row)
}

func (s *settledLog) list() []Transaction {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Transaction(nil), s.rows...)
}

// Catches: a notification that fires only for payments the client waited on. Income
// and anything found by a sweep or a restart must report too, and each row once.
func TestOnSettledCoversEveryPath(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t)
	log := &settledLog{}
	e.ledger.OnSettled(log.add)
	e.start()
	wantLast := func(path string, count int, kind, hash string) {
		t.Helper()
		e.waitUntil(path, func() bool { return len(log.list()) >= count })
		rows := log.list()
		last := rows[len(rows)-1]
		if len(rows) != count || last.Type != kind || last.PaymentHash != hash || last.State != StateSettled {
			t.Fatalf("%s: got %+v", path, rows)
		}
	}

	paid := e.pay(e.node.OutsideInvoice(1_000 * sat))
	wantLast("Pay's own result", 1, TypeOutgoing, paid.PaymentHash)

	e.node.ScriptPay(fake.PayTimeout)
	slow := e.pay(e.node.OutsideInvoice(1_000 * sat))
	if slow.State != StatePending {
		t.Fatalf("got %+v", slow)
	}
	if err := e.node.Finish(slow.PaymentHash, true); err != nil {
		t.Fatal(err)
	}
	e.sweep()
	e.sweep() // A second look at the same payment must not report it again.
	wantLast("the sweep", 2, TypeOutgoing, slow.PaymentHash)

	income, err := e.ledger.MakeInvoice(ctx, InvoiceRequest{AccountID: e.acct, AmountMsat: 1_000 * sat})
	if err != nil {
		t.Fatal(err)
	}
	if err := e.node.Settle(income.PaymentHash); err != nil {
		t.Fatal(err)
	}
	wantLast("the stream", 3, TypeIncoming, income.PaymentHash)

	own, err := e.ledger.MakeInvoice(ctx, InvoiceRequest{AccountID: e.acct, AmountMsat: 1_000 * sat})
	if err != nil {
		t.Fatal(err)
	}
	e.pay(own.Invoice)
	wantLast("the internal payment", 5, TypeIncoming, own.PaymentHash)
	if rows := log.list(); rows[3].Type != TypeOutgoing || rows[3].PaymentHash != own.PaymentHash {
		t.Fatalf("payer side: %+v", rows[3])
	}

	down, err := e.ledger.MakeInvoice(ctx, InvoiceRequest{AccountID: e.acct, AmountMsat: 1_000 * sat})
	if err != nil {
		t.Fatal(err)
	}
	e.ledger.Close()
	if err := e.node.Settle(down.PaymentHash); err != nil {
		t.Fatal(err)
	}
	e.ledger = e.fresh(e.node)
	e.ledger.OnSettled(log.add)
	e.start()
	wantLast("the catch-up", 6, TypeIncoming, down.PaymentHash)
}

// Catches: reporting a failure as sent.
func TestOnSettledSkipsFailedPayment(t *testing.T) {
	e := newEnv(t)
	log := &settledLog{}
	e.ledger.OnSettled(log.add)
	e.start()
	e.node.ScriptPay(fake.PayFail)
	if tx := e.pay(e.node.OutsideInvoice(1_000 * sat)); tx.State != StateFailed {
		t.Fatalf("got %+v", tx)
	}
	if rows := log.list(); len(rows) != 0 {
		t.Fatalf("got %+v", rows)
	}
}

// Catches: the callback running inside a database transaction. Only one writer can
// exist, so anything it writes would wait for itself.
func TestOnSettledRunsAfterCommit(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t)
	var failures atomic.Int64
	var calls atomic.Int64
	e.ledger.OnSettled(func(Transaction) {
		write, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		if _, err := e.store.DB.ExecContext(write, `INSERT INTO settings (key, value) VALUES ('probe', '1')
			ON CONFLICT (key) DO NOTHING`); err != nil {
			failures.Add(1)
		}
		calls.Add(1)
	})
	e.start()
	own, err := e.ledger.MakeInvoice(ctx, InvoiceRequest{AccountID: e.acct, AmountMsat: 1_000 * sat})
	if err != nil {
		t.Fatal(err)
	}
	e.pay(own.Invoice)
	income, err := e.ledger.MakeInvoice(ctx, InvoiceRequest{AccountID: e.acct, AmountMsat: 1_000 * sat})
	if err != nil {
		t.Fatal(err)
	}
	if err := e.node.Settle(income.PaymentHash); err != nil {
		t.Fatal(err)
	}
	e.pay(e.node.OutsideInvoice(1_000 * sat))
	e.waitUntil("each row is reported", func() bool { return calls.Load() == 4 })
	if failures.Load() != 0 {
		t.Fatalf("%d writes failed inside the callback", failures.Load())
	}
}

// Catches: a start that gives up while the node boots, and one that keeps retrying a
// swapped node or ignores a stop signal.
func TestStartRetrying(t *testing.T) {
	t.Run("NodeComesUp", func(t *testing.T) {
		e := newEnv(t)
		e.node.SetUnreachable(true)
		done := make(chan error, 1)
		go func() { done <- e.ledger.StartRetrying(context.Background()) }()
		select {
		case err := <-done:
			t.Fatalf("returned with the node down: %v", err)
		case <-time.After(20 * time.Millisecond):
		}
		e.node.SetUnreachable(false)
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		e.ledger.Close()
	})
	t.Run("SwappedNode", func(t *testing.T) {
		e := newEnv(t)
		if _, err := e.store.DB.Exec(`INSERT INTO settings (key, value) VALUES ('node_pubkey', '03other')`); err != nil {
			t.Fatal(err)
		}
		if err := e.ledger.StartRetrying(context.Background()); !errors.Is(err, ErrNodeChanged) {
			t.Fatalf("got %v", err)
		}
	})
	t.Run("Stopped", func(t *testing.T) {
		e := newEnv(t)
		e.node.SetUnreachable(true)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := e.ledger.StartRetrying(ctx); !errors.Is(err, context.Canceled) {
			t.Fatalf("got %v", err)
		}
	})
}

// Catches: an off-by-one at either end of a time window, and rows of equal time in
// no fixed order, which would repeat or lose rows between pages.
func TestHistoryWindow(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t)
	e.start()
	first := e.now.Load()
	var hashes []string
	for range 4 {
		row, err := e.ledger.MakeInvoice(ctx, InvoiceRequest{AccountID: e.acct, AmountMsat: sat})
		if err != nil {
			t.Fatal(err)
		}
		hashes = append(hashes, row.PaymentHash)
		e.advance(10 * time.Second)
	}
	rows, total, err := e.ledger.History(ctx, HistoryQuery{
		AccountID: e.acct, From: first + 10, Until: first + 20, Unpaid: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if total != 2 || len(rows) != 2 || rows[0].PaymentHash != hashes[2] || rows[1].PaymentHash != hashes[1] {
		t.Fatalf("total %d, rows %+v", total, rows)
	}
}
