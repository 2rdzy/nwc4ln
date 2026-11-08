package ledger

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/2rdzy/nwc4ln/internal/node"
	"github.com/2rdzy/nwc4ln/internal/node/fake"
)

func (e *env) move(call func(context.Context, int64, int64) (Transaction, error), id, amount int64) Transaction {
	e.t.Helper()
	row, err := call(context.Background(), id, amount)
	if err != nil {
		e.t.Fatal(err)
	}
	if row.State != StateSettled {
		e.t.Fatalf("transfer is %s", row.State)
	}
	return row
}

func (e *env) deleteSubWallet(id int64) {
	e.t.Helper()
	if err := e.ledger.DeleteSubWallet(context.Background(), id); err != nil {
		e.t.Fatal(err)
	}
}

// sides returns the two rows of one hash.
func (e *env) sides(hash string) (outgoing, incoming Transaction) {
	e.t.Helper()
	rows, err := listRows(context.Background(), e.store.DB, "payment_hash = ? ORDER BY type", hash)
	if err != nil {
		e.t.Fatal(err)
	}
	if len(rows) != 2 {
		e.t.Fatalf("%d rows for %s", len(rows), hash)
	}
	return rows[1], rows[0]
}

// wantBalances checks a sub-wallet and the main wallet together.
func (e *env) wantBalances(id, balance, available int64) {
	e.t.Helper()
	if got := e.balance(id); got != balance {
		e.t.Fatalf("balance %d, want %d", got, balance)
	}
	if got := e.available(); got != available {
		e.t.Fatalf("available %d, want %d", got, available)
	}
}

// Catches: a fund that spends sub-wallet money or holds, a transfer that is not an
// internal pair with the main wallet side empty, or a cap off by one msat.
func TestFundUpToTheMainWallet(t *testing.T) {
	e := newEnv(t)
	e.start()
	e.node.SetSpendable(node.Spendable{ActiveMsat: 10_000 * sat})
	other, wallet := e.addSubWallet(), e.addSubWallet()
	e.fund(other, 4_000*sat)
	e.node.ScriptPay(fake.PayTimeout)
	if _, err := e.ledger.Pay(context.Background(), PayRequest{AccountID: other, Invoice: e.node.OutsideInvoice(1_000 * sat)}); err != nil {
		t.Fatal(err)
	}
	available := e.available()
	if available != 10_000*sat { // Node 14,000, less 2,990 of balance and a 1,010 hold.
		t.Fatalf("available %d", available)
	}
	if _, err := e.ledger.Fund(context.Background(), wallet, available+1); !errors.Is(err, ErrInsufficientBalance) {
		t.Fatalf("one msat over: %v", err)
	}
	rows := e.rowCount()
	row := e.move(e.ledger.Fund, wallet, available)
	e.wantBalances(wallet, available, 0)
	if e.rowCount() != rows+2 {
		t.Fatalf("%d rows, was %d", e.rowCount(), rows)
	}
	outgoing, incoming := e.sides(row.PaymentHash)
	if outgoing.AccountID != 0 || incoming.AccountID != wallet || !outgoing.Internal || !incoming.Internal {
		t.Fatalf("outgoing %+v, incoming %+v", outgoing, incoming)
	}
	if outgoing.ReserveMsat != 0 || outgoing.FeeMsat != 0 || incoming.AmountMsat != available {
		t.Fatalf("outgoing %+v, incoming %+v", outgoing, incoming)
	}
	if outgoing.Preimage == "" || outgoing.Preimage != incoming.Preimage {
		t.Fatalf("preimages %q and %q", outgoing.Preimage, incoming.Preimage)
	}
	if len(e.node.PayRequests()) != 1 {
		t.Fatal("a transfer went out through the node")
	}
	if _, err := e.ledger.Fund(context.Background(), wallet, 1); !errors.Is(err, ErrInsufficientBalance) {
		t.Fatalf("main wallet at 0: %v", err)
	}
	e.node.SetSpendable(node.Spendable{})
	if _, err := e.ledger.Fund(context.Background(), wallet, 1); !errors.Is(err, ErrInsufficientBalance) {
		t.Fatalf("main wallet below 0: %v", err)
	}
}

// Catches: a fund run on a guess of the main wallet, or a withdraw that waits for a
// figure it does not use. R14 needs withdraw to work when that figure is no help.
func TestOnlyFundNeedsTheNodeFigure(t *testing.T) {
	e := newEnv(t)
	spendable := &failingSpendable{Node: e.node}
	e.ledger = e.fresh(spendable)
	e.start()
	wallet := e.addSubWallet()
	e.fund(wallet, 1_000*sat)
	spendable.failing.Store(true)
	rows := e.rowCount()
	if _, err := e.ledger.Fund(context.Background(), wallet, sat); err == nil {
		t.Fatal("funded with no node figure")
	}
	if e.rowCount() != rows {
		t.Fatal("rows written")
	}
	e.move(e.ledger.Withdraw, wallet, 1_000*sat)
	if got := e.balance(wallet); got != 0 {
		t.Fatalf("balance %d", got)
	}
}

type failingSpendable struct {
	*fake.Node
	failing atomic.Bool
}

func (n *failingSpendable) Spendable(ctx context.Context) (node.Spendable, error) {
	if n.failing.Load() {
		return node.Spendable{}, errors.New("no figure")
	}
	return n.Node.Spendable(ctx)
}

// Catches: a fund reading the node figure before the lock. The same money would count
// in the figure and be free in the rows, as in TestSubWalletSettlementWaitsForAppCheck.
func TestSettlementWaitsForFundCheck(t *testing.T) {
	e := newEnv(t)
	e.start()
	e.node.SetSpendable(node.Spendable{ActiveMsat: 0})
	wallet, target := e.addSubWallet(), e.addSubWallet()
	e.fund(wallet, 1_000*sat)
	e.node.ScriptPay(fake.PayTimeout)
	row, err := e.ledger.Pay(context.Background(), PayRequest{AccountID: wallet, Invoice: e.node.OutsideInvoice(500 * sat)})
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
	if _, err := e.ledger.Fund(context.Background(), target, 400*sat); !errors.Is(err, ErrInsufficientBalance) {
		t.Fatalf("funded with the sub-wallet's money: %v", err)
	}
	e.waitUntil("the sub-wallet payment settles", func() bool { return e.get(row.ID).State == StateSettled })
}

// Catches: held money withdrawn, a cap off by one msat, or a withdraw that needs the
// main wallet and so fails in a shortfall (R14).
func TestWithdrawUpToTheBalance(t *testing.T) {
	e := newEnv(t)
	e.start()
	wallet := e.addSubWallet()
	e.fund(wallet, 1_000*sat)
	e.node.ScriptPay(fake.PayTimeout)
	if _, err := e.ledger.Pay(context.Background(), PayRequest{AccountID: wallet, Invoice: e.node.OutsideInvoice(100 * sat)}); err != nil {
		t.Fatal(err)
	}
	free := e.balance(wallet)
	if free != 890*sat {
		t.Fatalf("balance %d", free)
	}
	if _, err := e.ledger.Withdraw(context.Background(), wallet, free+1); !errors.Is(err, ErrInsufficientBalance) {
		t.Fatalf("one msat over: %v", err)
	}
	before := e.available()
	funds, err := e.node.Spendable(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	e.node.SetSpendable(node.Spendable{}) // More is owed than the node holds.
	row := e.move(e.ledger.Withdraw, wallet, free)
	if got := e.balance(wallet); got != 0 {
		t.Fatalf("balance %d", got)
	}
	outgoing, incoming := e.sides(row.PaymentHash)
	if outgoing.AccountID != wallet || incoming.AccountID != 0 || outgoing.ReserveMsat != 0 {
		t.Fatalf("outgoing %+v, incoming %+v", outgoing, incoming)
	}
	e.node.SetSpendable(funds)
	if got := e.available(); got != before+free {
		t.Fatalf("available %d, was %d", got, before)
	}
}

// Catches: money moved for an account that has no business with it, or a call that
// leaves rows behind.
func TestTransferRefused(t *testing.T) {
	e := newEnv(t)
	e.start()
	wallet, deleted := e.addSubWallet(), e.addSubWallet()
	e.deleteSubWallet(deleted)
	cases := []struct {
		name   string
		id     int64
		amount int64
		want   error
	}{
		{"App", e.acct, sat, ErrUnknownAccount},
		{"Main", 0, sat, ErrUnknownAccount},
		{"Unknown", 9_999, sat, ErrUnknownAccount},
		{"Deleted", deleted, sat, ErrAccountDeleted},
		{"Zero", wallet, 0, ErrBadAmount},
		{"Negative", wallet, -1, ErrBadAmount},
		{"TooBig", wallet, MaxAmountMsat + 1, ErrBadAmount},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, err := e.ledger.Fund(context.Background(), c.id, c.amount); !errors.Is(err, c.want) {
				t.Errorf("fund: %v", err)
			}
			if _, err := e.ledger.Withdraw(context.Background(), c.id, c.amount); !errors.Is(err, c.want) {
				t.Errorf("withdraw: %v", err)
			}
			if got := e.rowCount(); got != 0 {
				t.Errorf("%d rows", got)
			}
		})
	}
}

// canceled records the invoices a node was told to cancel.
type canceled struct {
	*fake.Node
	mu     sync.Mutex
	hashes []string
	made   []string
}

func (n *canceled) MakeInvoice(ctx context.Context, request node.InvoiceRequest) (node.Invoice, error) {
	invoice, err := n.Node.MakeInvoice(ctx, request)
	n.mu.Lock()
	defer n.mu.Unlock()
	n.made = append(n.made, invoice.Hash)
	return invoice, err
}

func (n *canceled) CancelInvoice(ctx context.Context, hash string) error {
	n.mu.Lock()
	n.hashes = append(n.hashes, hash)
	n.mu.Unlock()
	return n.Node.CancelInvoice(ctx, hash)
}

// Catches: a refused transfer leaving an open invoice behind at the node.
func TestRefusedTransferCancelsItsInvoice(t *testing.T) {
	e := newEnv(t)
	recorder := &canceled{Node: e.node}
	e.ledger = e.fresh(recorder)
	e.start()
	wallet := e.addSubWallet()
	if _, err := e.ledger.Withdraw(context.Background(), wallet, sat); !errors.Is(err, ErrInsufficientBalance) {
		t.Fatal(err)
	}
	if len(recorder.made) != 1 || len(recorder.hashes) != 1 || recorder.made[0] != recorder.hashes[0] {
		t.Fatalf("made %v, canceled %v", recorder.made, recorder.hashes)
	}
}

// Catches: a transfer that makes an invoice before the ledger has recovered.
func TestTransferBeforeStart(t *testing.T) {
	e := newEnv(t)
	wallet := e.addSubWallet()
	if _, err := e.ledger.Fund(context.Background(), wallet, sat); !errors.Is(err, ErrNotRunning) {
		t.Fatalf("fund: %v", err)
	}
	if _, err := e.ledger.Withdraw(context.Background(), wallet, sat); !errors.Is(err, ErrNotRunning) {
		t.Fatalf("withdraw: %v", err)
	}
}

// Catches: two transfers both passing the check, which spends one balance twice.
func TestTransfersAtOnce(t *testing.T) {
	for _, c := range []struct {
		name string
		call func(*env) func(context.Context, int64, int64) (Transaction, error)
	}{
		{"Withdraw", func(e *env) func(context.Context, int64, int64) (Transaction, error) { return e.ledger.Withdraw }},
		{"Fund", func(e *env) func(context.Context, int64, int64) (Transaction, error) { return e.ledger.Fund }},
	} {
		t.Run(c.name, func(t *testing.T) {
			e := newEnv(t)
			e.start()
			e.node.SetSpendable(node.Spendable{})
			wallet := e.addSubWallet()
			e.fund(wallet, 1_000*sat)
			if c.name == "Fund" {
				e.node.SetSpendable(node.Spendable{ActiveMsat: 2_000 * sat}) // 1,000 sat left for the main wallet.
			}
			var (
				running sync.WaitGroup
				settled atomic.Int64
			)
			call := c.call(e)
			for range 20 {
				running.Go(func() {
					row, err := call(context.Background(), wallet, 100*sat)
					switch {
					case err == nil && row.State == StateSettled:
						settled.Add(1)
					case !errors.Is(err, ErrInsufficientBalance):
						t.Errorf("row %+v, %v", row, err)
					}
				})
			}
			running.Wait()
			if settled.Load() != 10 {
				t.Fatalf("%d transfers passed", settled.Load())
			}
		})
	}
}

// Catches: a credit after a failed cancel, when the invoice may be paid. R10 applies
// to a transfer as to any internal payment.
func TestTransferWhenCancelFails(t *testing.T) {
	e := newEnv(t)
	e.start()
	wallet := e.addSubWallet()
	e.fund(wallet, 1_000*sat)
	before := e.available()
	for name, call := range map[string]func(context.Context, int64, int64) (Transaction, error){
		"fund": e.ledger.Fund, "withdraw": e.ledger.Withdraw,
	} {
		e.node.FailNextCancel(errors.New("no answer"))
		row, err := call(context.Background(), wallet, 100*sat)
		if err != nil || row.State != StateFailed {
			t.Fatalf("%s: %+v, %v", name, row, err)
		}
		e.wantBalances(wallet, 1_000*sat, before)
	}
}

// Catches: a stop between the hold and the settle, which must release the hold and
// credit nobody.
func TestCrashInATransfer(t *testing.T) {
	e := newEnv(t)
	e.start()
	wallet := e.addSubWallet()
	e.fund(wallet, 1_000*sat)
	before := e.available()
	_, err := e.store.DB.Exec(`CREATE TRIGGER crash BEFORE UPDATE ON transactions
		WHEN NEW.type = 'incoming' AND NEW.state = 'settled'
		BEGIN SELECT RAISE(ABORT, 'crash'); END`)
	if err != nil {
		t.Fatal(err)
	}
	fund, err := e.ledger.Fund(context.Background(), wallet, 100*sat)
	if err != nil || fund.State != StatePending {
		t.Fatalf("fund %+v, %v", fund, err)
	}
	withdraw, err := e.ledger.Withdraw(context.Background(), wallet, 100*sat)
	if err != nil || withdraw.State != StatePending {
		t.Fatalf("withdraw %+v, %v", withdraw, err)
	}
	e.wantBalances(wallet, 900*sat, before-100*sat) // Each hold still counts.
	if _, err := e.store.DB.Exec("DROP TRIGGER crash"); err != nil {
		t.Fatal(err)
	}
	e.restart()
	e.advance(noRecordGrace)
	e.sweep()
	for _, row := range []Transaction{fund, withdraw} {
		e.wantState(row.ID, StateFailed)
		_, incoming := e.sides(row.PaymentHash)
		e.wantState(incoming.ID, StateFailed)
	}
	e.wantBalances(wallet, 1_000*sat, before)
}

// Catches: a hold never released after a stop before the cancel, when the invoice is
// still open at the node and neither side has moved.
func TestCrashBeforeTheCancel(t *testing.T) {
	for _, toSubWallet := range []bool{true, false} {
		e := newEnv(t)
		e.start()
		wallet := e.addSubWallet()
		e.fund(wallet, 1_000*sat)
		before := e.available()
		invoice, err := e.node.MakeInvoice(context.Background(), node.InvoiceRequest{AmountMsat: 100 * sat})
		if err != nil {
			t.Fatal(err)
		}
		move := transfer{accountID: wallet, amountMsat: 100 * sat, toSubWallet: toSubWallet}
		row, err := e.ledger.holdTransfer(context.Background(), move, invoice)
		if err != nil {
			t.Fatal(err)
		}
		e.restart()
		e.sweep() // Too young to believe "no record": the hold stays.
		e.wantState(row.ID, StatePending)
		e.advance(noRecordGrace)
		e.sweep()
		e.wantState(row.ID, StateFailed)
		e.wantBalances(wallet, 1_000*sat, before)
		e.ledger.Close()
	}
}

// Catches: money stranded in a deleted sub-wallet, moved twice, or moved with no
// record of it. Until the sweep the money stays locked away from apps.
func TestDeleteMovesTheBalance(t *testing.T) {
	e := newEnv(t)
	e.start()
	wallet := e.addSubWallet()
	e.fund(wallet, 1_000*sat)
	before := e.available()
	e.deleteSubWallet(wallet)
	if _, err := e.ledger.Access(context.Background(), wallet); !errors.Is(err, ErrAccountDeleted) {
		t.Fatalf("access: %v", err)
	}
	if wallets, err := e.ledger.SubWallets(context.Background()); err != nil || len(wallets) != 0 {
		t.Fatalf("list %+v, %v", wallets, err)
	}
	e.wantBalances(wallet, 1_000*sat, before)
	e.sweep()
	e.wantBalances(wallet, 0, before+1_000*sat)
	rows := e.rowCount()
	e.sweep()
	if e.rowCount() != rows {
		t.Fatal("a second sweep moved money")
	}
	history, total, err := e.ledger.History(context.Background(), HistoryQuery{AccountID: wallet})
	if err != nil || total != 2 {
		t.Fatalf("history %+v, %v", history, err)
	}
	if history[0].Type != TypeOutgoing || history[0].AmountMsat != 1_000*sat || !history[0].Internal {
		t.Fatalf("no recorded withdraw: %+v", history[0])
	}
}

// Catches: a release after the delete staying in the deleted sub-wallet, whichever way
// the payment ends.
func TestDeleteWithAPendingPayment(t *testing.T) {
	const fee = 3 * sat
	for _, c := range []struct {
		name      string
		succeeded bool
		paid      int64 // What left the node.
		state     string
	}{
		{"SettlesBelowTheReserve", true, 100*sat + fee, StateSettled},
		{"Fails", false, 0, StateFailed},
	} {
		t.Run(c.name, func(t *testing.T) {
			e := newEnv(t)
			e.start()
			wallet := e.addSubWallet()
			e.fund(wallet, 1_000*sat)
			e.node.SetRouteFee(fee)
			e.node.ScriptPay(fake.PayTimeout)
			row, err := e.ledger.Pay(context.Background(), PayRequest{AccountID: wallet, Invoice: e.node.OutsideInvoice(100 * sat)})
			if err != nil {
				t.Fatal(err)
			}
			before := e.available()
			e.deleteSubWallet(wallet)
			e.sweep()
			e.wantBalances(wallet, 0, before+890*sat)
			if err := e.node.Finish(row.PaymentHash, c.succeeded); err != nil {
				t.Fatal(err)
			}
			e.advance(noRecordGrace + time.Second)
			e.sweep()
			e.wantState(row.ID, c.state)
			e.wantBalances(wallet, 0, testFunds+1_000*sat-c.paid)
		})
	}
}

// Catches: a delete that needs the node, or money stranded after a node outage. The
// key must die at once, and the balance must stay away from apps until it has moved.
func TestDeleteWithTheNodeDown(t *testing.T) {
	e := newEnv(t)
	e.start()
	wallet := e.addSubWallet()
	e.fund(wallet, 1_000*sat)
	e.node.SetUnreachable(true)
	e.deleteSubWallet(wallet)
	if _, err := e.ledger.Access(context.Background(), wallet); !errors.Is(err, ErrAccountDeleted) {
		t.Fatalf("access: %v", err)
	}
	if err := e.ledger.sweep(context.Background()); err == nil {
		t.Fatal("sweep reported nothing")
	}
	e.node.SetUnreachable(false)
	e.wantBalances(wallet, 1_000*sat, testFunds)
	e.sweep()
	e.wantBalances(wallet, 0, testFunds+1_000*sat)
}

// Catches: the move kept in memory, so a stop after the delete strands the money.
func TestRestartAfterDelete(t *testing.T) {
	e := newEnv(t)
	e.start()
	wallet := e.addSubWallet()
	e.fund(wallet, 1_000*sat)
	e.deleteSubWallet(wallet)
	e.restart()
	e.sweep()
	e.wantBalances(wallet, 0, testFunds+1_000*sat)
}

// Catches: money stranded in a deleted sub-wallet, or left unrecorded, when income
// arrives on an invoice the sweep could not cancel.
func TestIncomeAfterDelete(t *testing.T) {
	e := newEnv(t)
	e.start()
	wallet := e.addSubWallet()
	e.fund(wallet, 300*sat)
	in, err := e.ledger.MakeInvoice(context.Background(), InvoiceRequest{AccountID: wallet, AmountMsat: 700 * sat})
	if err != nil {
		t.Fatal(err)
	}
	e.deleteSubWallet(wallet)
	e.node.FailNextCancel(errors.New("no answer"))
	if err := e.ledger.sweep(context.Background()); err == nil {
		t.Fatal("sweep reported nothing")
	}
	e.wantState(in.ID, StatePending)
	e.wantBalances(wallet, 0, testFunds+300*sat) // One failed cancel must not hold up the move.
	if err := e.node.Settle(in.PaymentHash); err != nil {
		t.Fatal(err)
	}
	e.waitState(in.ID, StateSettled)
	e.sweep()
	e.wantBalances(wallet, 0, testFunds+1_000*sat)
	history, total, err := e.ledger.History(context.Background(), HistoryQuery{AccountID: wallet, Type: TypeOutgoing})
	if err != nil || total != 2 || history[0].AmountMsat != 700*sat {
		t.Fatalf("history %+v, %v", history, err)
	}
}

// Catches: an invoice left payable after its sub-wallet is deleted, or its row left
// pending for ever.
func TestDeleteCancelsOpenInvoices(t *testing.T) {
	e := newEnv(t)
	e.start()
	wallet := e.addSubWallet()
	in, err := e.ledger.MakeInvoice(context.Background(), InvoiceRequest{AccountID: wallet, AmountMsat: 700 * sat})
	if err != nil {
		t.Fatal(err)
	}
	e.deleteSubWallet(wallet)
	e.sweep()
	e.wantState(in.ID, StateFailed)
	invoice, err := e.node.LookupInvoice(context.Background(), in.PaymentHash)
	if err != nil || invoice.State != node.InvoiceCanceled {
		t.Fatalf("invoice %+v, %v", invoice, err)
	}
}

// Catches: a payer losing money to the main wallet, and getting a preimage, for paying
// an invoice whose owner has gone. It holds in the moment before the sweep cancels it.
func TestDeletedAccountsInvoiceIsClosed(t *testing.T) {
	for _, c := range []struct {
		name   string
		delete func(*env, int64) error
		add    func(*env) int64
	}{
		{"SubWallet", func(e *env, id int64) error { return e.ledger.DeleteSubWallet(context.Background(), id) },
			func(e *env) int64 { return e.addSubWallet() }},
		{"App", func(e *env, id int64) error { return e.ledger.DeleteApp(context.Background(), id) },
			func(e *env) int64 { return e.addLimitedApp(0) }},
	} {
		t.Run(c.name, func(t *testing.T) {
			e := newEnv(t)
			e.start()
			owner := c.add(e)
			if c.name == "App" {
				if _, err := e.store.DB.Exec(`UPDATE accounts SET can_invoice = 1 WHERE id = ?`, owner); err != nil {
					t.Fatal(err)
				}
			}
			in, err := e.ledger.MakeInvoice(context.Background(), InvoiceRequest{AccountID: owner, AmountMsat: 100 * sat})
			if err != nil {
				t.Fatal(err)
			}
			if err := c.delete(e, owner); err != nil {
				t.Fatal(err)
			}
			rows := e.rowCount()
			if _, err := e.tryPayInvoice(in.Invoice); !errors.Is(err, ErrInvoiceClosed) {
				t.Fatalf("paid: %v", err)
			}
			if e.rowCount() != rows {
				t.Fatal("a row was stored")
			}
		})
	}
}

func (e *env) tryPayInvoice(invoice string) (Transaction, error) {
	return e.ledger.Pay(context.Background(), PayRequest{AccountID: e.acct, Invoice: invoice})
}

func (e *env) addLimitedApp(limit int64) int64 {
	e.t.Helper()
	result, err := e.store.DB.Exec(`INSERT INTO accounts (type, name, limit_msat, can_pay, can_invoice, created_at)
		VALUES ('app', 'limited', ?, 1, 1, 0)`, limit)
	if err != nil {
		e.t.Fatal(err)
	}
	id, err := result.LastInsertId()
	if err != nil {
		e.t.Fatal(err)
	}
	return id
}

// cancelGate stops the first cancel until the test lets it go. Later cancels pass.
type cancelGate struct {
	*fake.Node
	used    atomic.Bool
	reached chan struct{}
	release chan struct{}
}

func (n *cancelGate) CancelInvoice(ctx context.Context, hash string) error {
	if n.used.CompareAndSwap(false, true) {
		close(n.reached)
		<-n.release
	}
	return n.Node.CancelInvoice(ctx, hash)
}

// Catches: a delete cutting across a payment already held. The sweep must leave its
// row to it, or the payer's money stays held for nothing, and the money must end up in
// the main wallet.
func TestDeleteDuringATransfer(t *testing.T) {
	cases := []struct {
		name string
		run  func(e *env, wallet int64, invoice string) (Transaction, error)
	}{
		{"Fund", func(e *env, wallet int64, _ string) (Transaction, error) {
			return e.ledger.Fund(context.Background(), wallet, 100*sat)
		}},
		{"InternalPayment", func(e *env, _ int64, invoice string) (Transaction, error) {
			return e.tryPayInvoice(invoice)
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			e := newEnv(t)
			gate := &cancelGate{Node: e.node, reached: make(chan struct{}), release: make(chan struct{})}
			e.ledger = e.fresh(gate)
			e.start()
			wallet := e.addSubWallet()
			in, err := e.ledger.MakeInvoice(context.Background(), InvoiceRequest{AccountID: wallet, AmountMsat: 100 * sat})
			if err != nil {
				t.Fatal(err)
			}
			var running sync.WaitGroup
			var row Transaction
			running.Go(func() {
				var err error
				if row, err = c.run(e, wallet, in.Invoice); err != nil {
					t.Error(err)
				}
			})
			<-gate.reached
			e.deleteSubWallet(wallet)
			e.sweep()
			close(gate.release)
			running.Wait()
			e.wantState(row.ID, StateSettled)
			e.sweep()
			e.wantBalances(wallet, 0, testFunds)
		})
	}
}

// Catches: a second delete moving money, or a delete of the wrong kind of account.
func TestDeleteRefused(t *testing.T) {
	e := newEnv(t)
	e.start()
	wallet := e.addSubWallet()
	for name, call := range map[string]func() error{
		"app as sub-wallet": func() error { return e.ledger.DeleteSubWallet(context.Background(), e.acct) },
		"sub-wallet as app": func() error { return e.ledger.DeleteApp(context.Background(), wallet) },
		"unknown":           func() error { return e.ledger.DeleteSubWallet(context.Background(), 9_999) },
	} {
		if err := call(); !errors.Is(err, ErrUnknownAccount) {
			t.Errorf("%s: %v", name, err)
		}
	}
	e.deleteSubWallet(wallet)
	if err := e.ledger.DeleteSubWallet(context.Background(), wallet); !errors.Is(err, ErrUnknownAccount) {
		t.Fatalf("second delete: %v", err)
	}
}

// Catches: a wrong credit, a wrong limit, or a move that counts against an app. Main
// has no connection and no invoices of its own, so it pairs only with sub-wallets.
func TestEachPair(t *testing.T) {
	const amount = 100 * sat
	type accounts struct{ app, otherApp, wallet, otherWallet int64 }
	cases := []struct {
		name        string
		payer       func(accounts) int64 // 0 means the main wallet.
		payee       func(accounts) int64 // 0 means the main wallet.
		mainChange  int64
		usedByPayer int64
	}{
		{"AppToSubWallet", func(a accounts) int64 { return a.app }, func(a accounts) int64 { return a.wallet }, -amount, amount},
		{"AppToApp", func(a accounts) int64 { return a.app }, func(a accounts) int64 { return a.otherApp }, 0, amount},
		{"SubWalletToApp", func(a accounts) int64 { return a.wallet }, func(a accounts) int64 { return a.app }, amount, 0},
		{"SubWalletToSubWallet", func(a accounts) int64 { return a.wallet }, func(a accounts) int64 { return a.otherWallet }, 0, 0},
		{"MainToSubWallet", func(accounts) int64 { return 0 }, func(a accounts) int64 { return a.wallet }, -amount, 0},
		{"SubWalletToMain", func(a accounts) int64 { return a.wallet }, func(accounts) int64 { return 0 }, amount, 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			e := newEnv(t)
			e.start()
			all := accounts{e.addLimitedApp(1_000 * sat), e.addLimitedApp(1_000 * sat), e.addSubWallet(), e.addSubWallet()}
			e.fund(all.wallet, 1_000*sat)
			payer, payee := c.payer(all), c.payee(all)
			balances := map[int64]int64{all.wallet: e.balance(all.wallet), all.otherWallet: e.balance(all.otherWallet)}
			main := e.available()
			var hash string
			switch {
			case payer == 0:
				hash = e.move(e.ledger.Fund, payee, amount).PaymentHash
			case payee == 0:
				hash = e.move(e.ledger.Withdraw, payer, amount).PaymentHash
			default:
				in, err := e.ledger.MakeInvoice(context.Background(), InvoiceRequest{AccountID: payee, AmountMsat: amount})
				if err != nil {
					t.Fatal(err)
				}
				row, err := e.ledger.Pay(context.Background(), PayRequest{AccountID: payer, Invoice: in.Invoice})
				if err != nil || row.State != StateSettled {
					t.Fatalf("pay %+v, %v", row, err)
				}
				hash = row.PaymentHash
			}
			if _, isWallet := balances[payer]; isWallet {
				balances[payer] -= amount
			}
			if _, isWallet := balances[payee]; isWallet {
				balances[payee] += amount
			}
			for id, want := range balances {
				if got := e.balance(id); got != want {
					t.Errorf("account %d has %d, want %d", id, got, want)
				}
			}
			if got := e.available(); got != main+c.mainChange {
				t.Errorf("main wallet %d, want %d", got, main+c.mainChange)
			}
			for _, app := range []int64{all.app, all.otherApp} {
				want := int64(0)
				if app == payer {
					want = c.usedByPayer
				}
				budget, err := e.ledger.AppBudget(context.Background(), app)
				if err != nil || budget.UsedMsat != want {
					t.Errorf("app %d: budget %+v, %v, want used %d", app, budget, err, want)
				}
			}
			outgoing, incoming := e.sides(hash)
			if outgoing.AccountID != payer || incoming.AccountID != payee {
				t.Errorf("rows on %d and %d", outgoing.AccountID, incoming.AccountID)
			}
		})
	}
}
