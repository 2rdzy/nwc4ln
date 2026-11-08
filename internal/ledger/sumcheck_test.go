package ledger

import (
	"context"
	"errors"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/2rdzy/nwc4ln/internal/node/fake"
)

// book is the test's own record of where the money is. It is kept by hand from what
// each operation should do, so it shares no code with the ledger. Comparing R5 with
// its own definition would prove nothing.
type book struct {
	e       *env
	rng     *rand.Rand
	node    int64
	main    int64
	held    int64
	wallets map[int64]int64 // Every sub-wallet, deleted or not.
	live    []int64         // Sub-wallets not deleted.
	open    map[int64][]Transaction
	flying  []flight
}

// flight is a payment the node has not finished.
type flight struct {
	hash    string
	payer   int64 // The app is e.acct.
	cost    int64 // Amount plus reserve.
	reserve int64
	fee     int64
	amount  int64
}

func (b *book) check(step int, what string) {
	t := b.e.t
	t.Helper()
	sum := b.main + b.held
	for id, want := range b.wallets {
		if got := b.e.balance(id); got != want {
			t.Fatalf("step %d, %s: account %d has %d, want %d", step, what, id, got, want)
		}
		sum += want
	}
	spendable, err := b.e.node.Spendable(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	// A payment in flight has already left the node's figure, as on LND.
	flying := int64(0)
	for _, f := range b.flying {
		flying += f.amount + f.fee
	}
	if spendable.ActiveMsat != b.node-flying {
		t.Fatalf("step %d, %s: node has %d, want %d less %d in flight", step, what, spendable.ActiveMsat, b.node, flying)
	}
	if sum != b.node {
		t.Fatalf("step %d, %s: accounts, holds and main add up to %d, node has %d", step, what, sum, b.node)
	}
	if got := b.e.available(); got != b.main {
		t.Fatalf("step %d, %s: main wallet %d, want %d", step, what, got, b.main)
	}
}

func (b *book) amount() int64 { return 1 + b.rng.Int64N(2_000*sat) }

func (b *book) wallet() (int64, bool) {
	if len(b.live) == 0 {
		return 0, false
	}
	return b.live[b.rng.IntN(len(b.live))], true
}

// payer picks the app or a live sub-wallet.
func (b *book) payer() int64 {
	if id, ok := b.wallet(); ok && b.rng.IntN(3) > 0 {
		return id
	}
	return b.e.acct
}

// funds returns what a payer can spend.
func (b *book) funds(id int64) int64 {
	if id == b.e.acct {
		return b.main
	}
	return b.wallets[id]
}

func (b *book) spend(id, msat int64) {
	if id == b.e.acct {
		b.main -= msat
		return
	}
	b.wallets[id] -= msat
}

// sweep runs a pass, and moves in the book what a pass moves.
func (b *book) sweep() {
	b.e.sweep()
	b.sweptDeleted()
}

func (b *book) sweptDeleted() {
	for id, balance := range b.wallets {
		if balance > 0 && !contains(b.live, id) {
			b.main += balance
			b.wallets[id] = 0
		}
	}
}

func contains(ids []int64, id int64) bool {
	for _, each := range ids {
		if each == id {
			return true
		}
	}
	return false
}

func (b *book) addWallet() {
	id := b.e.addSubWallet()
	b.wallets[id] = 0
	b.live = append(b.live, id)
}

func (b *book) income() {
	amount := b.amount()
	b.node += amount
	if id, ok := b.wallet(); ok && b.rng.IntN(2) == 0 {
		b.e.fund(id, amount)
		b.wallets[id] += amount
		return
	}
	row, err := b.e.ledger.MakeInvoice(context.Background(), InvoiceRequest{AccountID: b.e.acct, AmountMsat: amount})
	if err != nil {
		b.e.t.Fatal(err)
	}
	if err := b.e.node.Settle(row.PaymentHash); err != nil {
		b.e.t.Fatal(err)
	}
	b.e.waitState(row.ID, StateSettled)
	b.main += amount
}

// openInvoice leaves an invoice for a later delete to deal with.
func (b *book) openInvoice() {
	id, ok := b.wallet()
	if !ok {
		return
	}
	row, err := b.e.ledger.MakeInvoice(context.Background(), InvoiceRequest{AccountID: id, AmountMsat: b.amount()})
	if err != nil {
		b.e.t.Fatal(err)
	}
	b.open[id] = append(b.open[id], row)
}

func (b *book) payOutside() {
	e, payer, amount := b.e, b.payer(), b.amount()
	reserve := Reserve(amount)
	fee := b.rng.Int64N(reserve + 1)
	e.node.SetRouteFee(fee)
	invoice := e.node.OutsideInvoice(amount)
	if amount+reserve > b.funds(payer) {
		row, err := e.ledger.Pay(context.Background(), PayRequest{AccountID: payer, Invoice: invoice})
		if !errors.Is(err, ErrInsufficientBalance) {
			e.t.Fatalf("over the funds: %+v, %v", row, err)
		}
		return
	}
	step := []fake.PayStep{fake.PaySucceed, fake.PaySucceed, fake.PayFail, fake.PayTimeout}[b.rng.IntN(4)]
	e.node.ScriptPay(step)
	row, err := e.ledger.Pay(context.Background(), PayRequest{AccountID: payer, Invoice: invoice})
	if err != nil {
		e.t.Fatal(err)
	}
	switch step {
	case fake.PaySucceed:
		e.wantState(row.ID, StateSettled)
		b.node -= amount + fee
		b.spend(payer, amount+fee)
	case fake.PayFail:
		e.wantState(row.ID, StateFailed)
	default:
		e.wantState(row.ID, StatePending)
		b.spend(payer, amount+reserve)
		b.held += amount + reserve
		b.flying = append(b.flying, flight{
			hash: row.PaymentHash, payer: payer, cost: amount + reserve, reserve: reserve, fee: fee, amount: amount,
		})
	}
}

func (b *book) payInternal() {
	e, payer, amount := b.e, b.payer(), b.amount()
	payee := b.payer()
	if payee == payer {
		return
	}
	row, err := e.ledger.MakeInvoice(context.Background(), InvoiceRequest{AccountID: payee, AmountMsat: amount})
	if err != nil {
		e.t.Fatal(err)
	}
	paid, err := e.ledger.Pay(context.Background(), PayRequest{AccountID: payer, Invoice: row.Invoice})
	if amount > b.funds(payer) {
		if !errors.Is(err, ErrInsufficientBalance) {
			e.t.Fatalf("over the funds: %+v, %v", paid, err)
		}
		b.open[payee] = append(b.open[payee], row) // Still open, and a delete must close it.
		return
	}
	if err != nil || paid.State != StateSettled {
		e.t.Fatalf("internal payment %+v, %v", paid, err)
	}
	b.spend(payer, amount)
	b.spend(payee, -amount)
}

func (b *book) transfer(toSubWallet bool) {
	id, ok := b.wallet()
	if !ok {
		return
	}
	e, amount := b.e, b.amount()
	call, most := e.ledger.Withdraw, b.wallets[id]
	if toSubWallet {
		call, most = e.ledger.Fund, b.main
	}
	row, err := call(context.Background(), id, amount)
	if amount > most {
		if !errors.Is(err, ErrInsufficientBalance) {
			e.t.Fatalf("over the cap: %+v, %v", row, err)
		}
		return
	}
	if err != nil || row.State != StateSettled {
		e.t.Fatalf("transfer %+v, %v", row, err)
	}
	if toSubWallet {
		amount = -amount
	}
	b.wallets[id] -= amount
	b.main += amount
}

// finish ends one payment in flight. A release on a deleted sub-wallet moves in the
// same pass.
func (b *book) finish() {
	if len(b.flying) == 0 {
		return
	}
	i := b.rng.IntN(len(b.flying))
	f := b.flying[i]
	b.flying = append(b.flying[:i], b.flying[i+1:]...)
	succeeded := b.rng.IntN(2) == 0
	if err := b.e.node.Finish(f.hash, succeeded); err != nil {
		b.e.t.Fatal(err)
	}
	b.held -= f.cost
	if succeeded {
		b.node -= f.amount + f.fee
		b.spend(f.payer, -(f.reserve - f.fee))
	} else {
		b.spend(f.payer, -f.cost)
	}
	b.e.advance(noRecordGrace + time.Second)
	b.sweep()
}

// remove deletes a sub-wallet. If it has an open invoice, the cancel of the first one
// fails and the invoice is then paid.
func (b *book) remove() {
	id, ok := b.wallet()
	if !ok {
		return
	}
	e := b.e
	e.deleteSubWallet(id)
	for i, each := range b.live {
		if each == id {
			b.live = append(b.live[:i], b.live[i+1:]...)
			break
		}
	}
	open := b.open[id]
	delete(b.open, id)
	if len(open) > 0 {
		e.node.FailNextCancel(errors.New("no answer"))
		if err := e.ledger.sweep(context.Background()); err == nil {
			e.t.Fatal("sweep reported nothing")
		}
		b.sweptDeleted()
		if err := e.node.Settle(open[0].PaymentHash); err != nil {
			e.t.Fatal(err)
		}
		e.waitState(open[0].ID, StateSettled)
		b.node += open[0].AmountMsat
		b.wallets[id] += open[0].AmountMsat
	}
	b.sweep()
}

// Catches: money lost or invented by any mix of operations, and any balance change with
// no matching transaction.
func TestSumCheck(t *testing.T) {
	for _, seed := range []uint64{1, 2, 3, 4} {
		e := newEnv(t)
		e.node.SetClock(func() time.Time { return time.Unix(e.now.Load(), 0) })
		e.start()
		b := &book{
			e: e, rng: rand.New(rand.NewPCG(seed, 0)), node: testFunds, main: testFunds,
			wallets: map[int64]int64{}, open: map[int64][]Transaction{},
		}
		operations := []struct {
			name   string
			weight int
			run    func()
		}{
			{"add", 2, b.addWallet},
			{"income", 6, b.income},
			{"open invoice", 2, b.openInvoice},
			{"pay outside", 6, b.payOutside},
			{"pay internal", 4, b.payInternal},
			{"fund", 4, func() { b.transfer(true) }},
			{"withdraw", 4, func() { b.transfer(false) }},
			{"finish", 3, b.finish},
			{"delete", 1, b.remove},
		}
		var deck []int
		for i, operation := range operations {
			for range operation.weight {
				deck = append(deck, i)
			}
		}
		b.addWallet()
		for step := range 300 {
			operation := operations[deck[b.rng.IntN(len(deck))]]
			operation.run()
			b.check(step, operation.name)
		}
		for len(b.flying) > 0 {
			b.finish()
		}
		for len(b.live) > 0 {
			b.remove()
		}
		b.sweep()
		b.check(300, "end")
		if b.main != b.node || b.held != 0 {
			t.Fatalf("seed %d: main %d, node %d, held %d", seed, b.main, b.node, b.held)
		}
		var unpaired int
		err := e.store.DB.QueryRow(`SELECT count(*) FROM transactions o WHERE o.internal = 1 AND o.state = 'settled'
			AND NOT EXISTS (SELECT 1 FROM transactions p WHERE p.payment_hash = o.payment_hash AND p.id != o.id
				AND p.type != o.type AND p.state = 'settled' AND p.internal = 1
				AND p.amount_msat = o.amount_msat AND p.fee_msat = 0 AND o.fee_msat = 0)`).Scan(&unpaired)
		if err != nil || unpaired != 0 {
			t.Fatalf("seed %d: %d internal rows with no matching side, %v", seed, unpaired, err)
		}
		var moves int
		err = e.store.DB.QueryRow(`SELECT count(*) FROM transactions WHERE account_id IS NULL AND state = 'settled'`).Scan(&moves)
		if err != nil || moves == 0 {
			t.Fatalf("seed %d: no transfers ran: %d, %v", seed, moves, err)
		}
		e.ledger.Close()
	}
}
