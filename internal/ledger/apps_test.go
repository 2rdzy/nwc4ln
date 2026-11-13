package ledger

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/2rdzy/nwc4ln/internal/node"
	"github.com/2rdzy/nwc4ln/internal/node/fake"
)

func limit(msat int64) *int64 { return &msat }

// followClock makes the fake stamp invoices with the test's time, for a test that
// moves far from the real date.
func (e *env) followClock() {
	e.node.SetClock(func() time.Time { return time.Unix(e.now.Load(), 0) })
}

func (e *env) settings(change func(*AppSettings)) {
	e.t.Helper()
	settings := DefaultApp()
	settings.CanInvoice = true
	change(&settings)
	if err := e.ledger.EditApp(context.Background(), e.acct, settings); err != nil {
		e.t.Fatal(err)
	}
}

func (e *env) tryPay(amountMsat int64) (Transaction, error) {
	return e.ledger.Pay(context.Background(), PayRequest{AccountID: e.acct, Invoice: e.node.OutsideInvoice(amountMsat)})
}

func (e *env) used() int64 {
	e.t.Helper()
	budget, err := e.ledger.AppBudget(context.Background(), e.acct)
	if err != nil || budget == nil {
		e.t.Fatalf("budget %v: %v", budget, err)
	}
	return budget.UsedMsat
}

// payMany sends payments at once and counts those that were held and those refused
// with the given error. Any other error fails the test.
func (e *env) payMany(count int, amountMsat int64, refusal error) (held, refused int) {
	e.t.Helper()
	invoices := make([]string, count)
	for i := range invoices {
		invoices[i] = e.node.OutsideInvoice(amountMsat)
	}
	var (
		mu sync.Mutex
		wg sync.WaitGroup
	)
	for _, invoice := range invoices {
		wg.Go(func() {
			_, err := e.ledger.Pay(context.Background(), PayRequest{AccountID: e.acct, Invoice: invoice})
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				held++
			case errors.Is(err, refusal):
				refused++
			default:
				e.t.Errorf("pay: %v", err)
			}
		})
	}
	wg.Wait()
	return held, refused
}

// Catches: a limit overspent under load. Each payment stays pending, so every hold
// counts against the next check, and the count is exact.
func TestConcurrentPaymentsStopAtLimit(t *testing.T) {
	e := newEnv(t)
	e.start()
	e.settings(func(s *AppSettings) { s.LimitMsat = limit(5 * 110 * sat) }) // 100 sat plus the 10 sat reserve.
	steps := make([]fake.PayStep, 30)
	for i := range steps {
		steps[i] = fake.PayTimeout
	}
	e.node.ScriptPay(steps...)
	held, refused := e.payMany(30, 100*sat, ErrQuotaExceeded)
	if held != 5 || refused != 25 {
		t.Fatalf("held %d, refused %d", held, refused)
	}
}

// Catches: main wallet funds spent twice under load (R5).
func TestConcurrentPaymentsStopAtMainWallet(t *testing.T) {
	e := newEnv(t)
	e.start()
	e.node.SetSpendable(node.Spendable{ActiveMsat: 3 * 110 * sat})
	steps := make([]fake.PayStep, 20)
	for i := range steps {
		steps[i] = fake.PayTimeout
	}
	e.node.ScriptPay(steps...)
	held, refused := e.payMany(20, 100*sat, ErrInsufficientBalance)
	if held != 3 || refused != 17 {
		t.Fatalf("held %d, refused %d", held, refused)
	}
}

// Catches: a comparison in sats or one msat off, and the reserve left out of it.
func TestLimitIsExactInMsat(t *testing.T) {
	e := newEnv(t)
	e.start()
	cost := 100*sat + 10*sat
	e.settings(func(s *AppSettings) { s.LimitMsat = limit(cost - 1) })
	if _, err := e.tryPay(100 * sat); !errors.Is(err, ErrQuotaExceeded) {
		t.Fatalf("one msat short: %v", err)
	}
	e.settings(func(s *AppSettings) { s.LimitMsat = limit(cost) })
	if _, err := e.tryPay(100 * sat); err != nil {
		t.Fatalf("exact fit: %v", err)
	}
	e.node.SetSpendable(node.Spendable{ActiveMsat: cost - 1})
	e.settings(func(s *AppSettings) {})
	if _, err := e.tryPay(100 * sat); !errors.Is(err, ErrInsufficientBalance) {
		t.Fatalf("one msat short of funds: %v", err)
	}
	e.node.SetSpendable(node.Spendable{ActiveMsat: cost})
	if _, err := e.tryPay(100 * sat); err != nil {
		t.Fatalf("exact fit in funds: %v", err)
	}
}

// Catches: a stale node figure. The first payment settles while the second is
// between its node read and its check. The node figure is from before the payment
// left, and if the hold is gone by the check, the same money counts twice. Removing
// holdLock from hold or from the settle write makes this test fail.
func TestSettleCannotPassNodeRead(t *testing.T) {
	e := newEnv(t)
	e.start()
	e.node.SetSpendable(node.Spendable{ActiveMsat: 1_000 * sat})
	e.node.ScriptPay(fake.PayTimeout)
	first, err := e.tryPay(600 * sat)
	if err != nil {
		t.Fatal(err)
	}
	var settle sync.WaitGroup
	e.ledger.afterNodeRead = func() {
		e.ledger.afterNodeRead = nil
		if err := e.node.Finish(first.PaymentHash, true); err != nil {
			t.Error(err)
		}
		e.node.SetSpendable(node.Spendable{ActiveMsat: 400 * sat})
		done := make(chan struct{})
		settle.Go(func() {
			defer close(done)
			if err := e.ledger.trackPending(context.Background()); err != nil {
				t.Error(err)
			}
		})
		select { // The settle must wait for the lock. If it cannot, it ends here.
		case <-done:
		case <-time.After(200 * time.Millisecond):
		}
	}
	if _, err := e.tryPay(500 * sat); !errors.Is(err, ErrInsufficientBalance) {
		t.Fatalf("got %v: the second payment used money that has left", err)
	}
	settle.Wait()
	e.wantState(first.ID, StateSettled)
}

// Catches: a settle write that does not wait for a check in progress. The node
// figure counts income once it arrives, so a credit or internal settlement between
// the read and the check would change what the figure means.
func TestSettleWritesTakeTheLock(t *testing.T) {
	// waits runs a write with the lock held by the test. The row must stay pending
	// until the lock is free.
	waits := func(e *env, id int64, write func()) {
		t.Helper()
		e.ledger.holdLock.Lock()
		var running sync.WaitGroup
		running.Go(write)
		time.Sleep(100 * time.Millisecond)
		state := e.get(id).State
		e.ledger.holdLock.Unlock()
		running.Wait()
		if state != StatePending {
			t.Errorf("state %s with the lock held", state)
		}
		e.waitState(id, StateSettled)
	}
	t.Run("Credit", func(t *testing.T) {
		e := newEnv(t)
		e.start()
		invoice := e.invoice(100 * sat)
		waits(e, invoice.ID, func() {
			if err := e.node.Settle(invoice.PaymentHash); err != nil {
				t.Error(err)
			}
			time.Sleep(150 * time.Millisecond) // The stream delivers it in this time.
		})
	})
	t.Run("Internal", func(t *testing.T) {
		e := newEnv(t)
		e.start()
		invoice := e.invoice(100 * sat)
		row := Transaction{
			AccountID: e.acct, Type: TypeOutgoing, State: StatePending, AmountMsat: 100 * sat, Invoice: invoice.Invoice,
			PaymentHash: invoice.PaymentHash, Internal: true, CreatedAt: time.Unix(e.now.Load(), 0),
		}
		var err error
		if row.ID, err = insert(t.Context(), e.store.DB, row); err != nil {
			t.Fatal(err)
		}
		waits(e, row.ID, func() { e.ledger.settleInternal(context.Background(), row) })
	})
}

// Catches: a credit that waits on holdLock while holding the database, or that
// fires its callback under the lock, which would stall every payment.
func TestSettleCallbackRunsOutsideLock(t *testing.T) {
	e := newEnv(t)
	seen := make(chan struct{}, 2)
	e.ledger.OnSettled(func(Transaction) {
		// Another part of the ledger may hold the lock for a moment, which is normal, so
		// TryLock alone gives false alarms. Instead, wait for the lock from another
		// goroutine: if the callback runs under it, the lock cannot come free while the
		// callback waits, and the wait times out.
		free := make(chan struct{})
		go func() {
			e.ledger.holdLock.Lock()
			e.ledger.holdLock.Unlock()
			close(free)
		}()
		select {
		case <-free:
		case <-time.After(2 * time.Second):
			t.Error("holdLock is held during the callback")
		}
		seen <- struct{}{}
	})
	e.start()
	invoice := e.invoice(1_000 * sat)
	if err := e.node.Settle(invoice.PaymentHash); err != nil {
		t.Fatal(err)
	}
	<-seen
	if tx := e.pay(e.node.OutsideInvoice(100 * sat)); tx.State != StateSettled {
		t.Fatalf("state %s", tx.State)
	}
	<-seen
}

type noSpendable struct{ *fake.Node }

func (noSpendable) Spendable(context.Context) (node.Spendable, error) {
	return node.Spendable{}, errors.New("node away")
}

// Catches: a payment on a guessed figure when only the balance read fails.
func TestNoNodeFigureMeansNoPayment(t *testing.T) {
	e := newEnv(t)
	e.ledger = e.fresh(noSpendable{e.node})
	e.start()
	if _, err := e.tryPay(100 * sat); err == nil {
		t.Fatal("paid with no node figure")
	}
	if got := e.rowCount(); got != 0 {
		t.Fatalf("%d rows", got)
	}
}

// Catches: usage that is wrong for any state of a row (R6).
func TestLimitUsage(t *testing.T) {
	e := newEnv(t)
	e.start()
	e.settings(func(s *AppSettings) { s.LimitMsat = limit(10_000 * sat) })

	e.node.SetRouteFee(7)
	if tx := e.pay(e.node.OutsideInvoice(100 * sat)); tx.State != StateSettled {
		t.Fatalf("state %s", tx.State)
	}
	e.node.SetRouteFee(0)
	want := 100*sat + 7
	if got := e.used(); got != want {
		t.Fatalf("settled: used %d, want amount plus real fee %d", got, want)
	}

	e.node.ScriptPay(fake.PayFail)
	if tx := e.pay(e.node.OutsideInvoice(100 * sat)); tx.State != StateFailed {
		t.Fatalf("state %s", tx.State)
	}
	if got := e.used(); got != want {
		t.Fatalf("failed: used %d, want %d", got, want)
	}

	e.node.ScriptPay(fake.PayTimeout)
	pending := e.pay(e.node.OutsideInvoice(100 * sat))
	if got, hold := e.used(), want+100*sat+10*sat; got != hold {
		t.Fatalf("pending: used %d, want the whole hold %d", got, hold)
	}
	if err := e.node.Finish(pending.PaymentHash, false); err != nil {
		t.Fatal(err)
	}
	e.advance(noRecordGrace)
	e.sweep()
	e.waitState(pending.ID, StateFailed)
	if got := e.used(); got != want {
		t.Fatalf("hold released: used %d, want %d", got, want)
	}

	invoice := e.invoice(5_000 * sat)
	if err := e.node.Settle(invoice.PaymentHash); err != nil {
		t.Fatal(err)
	}
	e.waitState(invoice.ID, StateSettled)
	if got := e.used(); got != want {
		t.Fatalf("income: used %d, want %d", got, want)
	}

	own := e.invoice(200 * sat)
	if tx := e.pay(own.Invoice); tx.State != StateSettled || !tx.Internal {
		t.Fatalf("got %+v", tx)
	}
	if got := e.used(); got != want+200*sat {
		t.Fatalf("internal: used %d, want no reserve in %d", got, want+200*sat)
	}
}

// Catches: a period that starts or ends a second off, in local time, on the wrong
// weekday, or wrong at a month or year end.
func TestPeriods(t *testing.T) {
	utc := func(year int, month time.Month, day, hour, minute, second int) time.Time {
		return time.Date(year, month, day, hour, minute, second, 0, time.UTC)
	}
	cases := []struct {
		name, renewal string
		at            time.Time
		start, end    time.Time
	}{
		{"day", RenewDaily, utc(2026, 3, 4, 12, 0, 0), utc(2026, 3, 4, 0, 0, 0), utc(2026, 3, 5, 0, 0, 0)},
		{"day, last second", RenewDaily, utc(2026, 3, 4, 23, 59, 59), utc(2026, 3, 4, 0, 0, 0), utc(2026, 3, 5, 0, 0, 0)},
		{"day, first second", RenewDaily, utc(2026, 3, 5, 0, 0, 0), utc(2026, 3, 5, 0, 0, 0), utc(2026, 3, 6, 0, 0, 0)},
		{"day at year end", RenewDaily, utc(2026, 12, 31, 5, 0, 0), utc(2026, 12, 31, 0, 0, 0), utc(2027, 1, 1, 0, 0, 0)},
		{"week on Monday", RenewWeekly, utc(2026, 11, 2, 0, 0, 0), utc(2026, 11, 2, 0, 0, 0), utc(2026, 11, 9, 0, 0, 0)},
		{"week on Sunday", RenewWeekly, utc(2026, 11, 8, 23, 59, 59), utc(2026, 11, 2, 0, 0, 0), utc(2026, 11, 9, 0, 0, 0)},
		{"week across months", RenewWeekly, utc(2026, 11, 1, 8, 0, 0), utc(2026, 10, 26, 0, 0, 0), utc(2026, 11, 2, 0, 0, 0)},
		{"week across years", RenewWeekly, utc(2027, 1, 1, 8, 0, 0), utc(2026, 12, 28, 0, 0, 0), utc(2027, 1, 4, 0, 0, 0)},
		{"month, 31 days", RenewMonthly, utc(2026, 1, 31, 23, 59, 59), utc(2026, 1, 1, 0, 0, 0), utc(2026, 2, 1, 0, 0, 0)},
		{"February", RenewMonthly, utc(2026, 2, 28, 23, 59, 59), utc(2026, 2, 1, 0, 0, 0), utc(2026, 3, 1, 0, 0, 0)},
		{"leap February", RenewMonthly, utc(2028, 2, 29, 12, 0, 0), utc(2028, 2, 1, 0, 0, 0), utc(2028, 3, 1, 0, 0, 0)},
		{"December", RenewMonthly, utc(2026, 12, 15, 0, 0, 0), utc(2026, 12, 1, 0, 0, 0), utc(2027, 1, 1, 0, 0, 0)},
		{"year", RenewYearly, utc(2028, 2, 29, 12, 0, 0), utc(2028, 1, 1, 0, 0, 0), utc(2029, 1, 1, 0, 0, 0)},
		{"year, last second", RenewYearly, utc(2026, 12, 31, 23, 59, 59), utc(2026, 1, 1, 0, 0, 0), utc(2027, 1, 1, 0, 0, 0)},
		{"never", RenewNever, utc(2026, 3, 4, 12, 0, 0), time.Unix(0, 0).UTC(), time.Time{}},
		{"local input", RenewDaily, time.Date(2026, 3, 5, 1, 0, 0, 0, time.FixedZone("east", 2*3600)),
			utc(2026, 3, 4, 0, 0, 0), utc(2026, 3, 5, 0, 0, 0)},
	}
	for _, c := range cases {
		if got := periodStart(c.renewal, c.at); !got.Equal(c.start) {
			t.Errorf("%s: start %v, want %v", c.name, got, c.start)
		}
		if got := periodEnd(c.renewal, c.at); !got.Equal(c.end) {
			t.Errorf("%s: end %v, want %v", c.name, got, c.end)
		}
	}
}

// Catches: a limit that does not reset, or resets a second early, in the real path
// from rows to refusal. A payment stays in the period of its created_at.
func TestLimitResets(t *testing.T) {
	boundary := time.Date(2028, 3, 1, 0, 0, 0, 0, time.UTC) // After a leap February.
	for _, renewal := range []string{RenewDaily, RenewWeekly, RenewMonthly} {
		t.Run(renewal, func(t *testing.T) {
			e := newEnv(t)
			e.followClock()
			e.start()
			e.settings(func(s *AppSettings) { s.LimitMsat = limit(110 * sat); s.Renewal = renewal })
			end := periodEnd(renewal, boundary.Add(-time.Second))
			e.now.Store(end.Unix() - 1)
			e.node.ScriptPay(fake.PayTimeout)
			pending, err := e.tryPay(100 * sat)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := e.tryPay(1 * sat); !errors.Is(err, ErrQuotaExceeded) {
				t.Fatalf("last second of the period: %v", err)
			}
			e.now.Store(end.Unix())
			if got := e.used(); got != 0 {
				t.Fatalf("first second of the next period: used %d", got)
			}
			if err := e.node.Finish(pending.PaymentHash, true); err != nil {
				t.Fatal(err)
			}
			e.sweep()
			e.waitState(pending.ID, StateSettled)
			if got := e.used(); got != 0 {
				t.Fatalf("settled in the next period: used %d", got)
			}
			if _, err := e.tryPay(100 * sat); err != nil {
				t.Fatalf("after the reset: %v", err)
			}
			if got := e.used(); got != 100*sat {
				t.Fatalf("a payment in the period's first second: used %d", got)
			}
		})
	}
	t.Run(RenewNever, func(t *testing.T) {
		e := newEnv(t)
		e.followClock()
		e.start()
		e.settings(func(s *AppSettings) { s.LimitMsat = limit(110 * sat) })
		if _, err := e.tryPay(100 * sat); err != nil {
			t.Fatal(err)
		}
		e.advance(10 * 365 * 24 * time.Hour)
		if _, err := e.tryPay(100 * sat); !errors.Is(err, ErrQuotaExceeded) {
			t.Fatalf("a one-time limit came back: %v", err)
		}
	})
}

// Catches: a zero limit read as no limit, and a limit cut below usage going wrong.
func TestLimitValues(t *testing.T) {
	e := newEnv(t)
	e.start()
	e.settings(func(s *AppSettings) { s.LimitMsat = limit(0) })
	if _, err := e.tryPay(1 * sat); !errors.Is(err, ErrQuotaExceeded) {
		t.Fatalf("limit 0: %v", err)
	}
	if budget, err := e.ledger.AppBudget(t.Context(), e.acct); err != nil || budget == nil || budget.TotalMsat != 0 {
		t.Fatalf("limit 0: budget %+v, %v", budget, err)
	}
	e.settings(func(s *AppSettings) {})
	if budget, err := e.ledger.AppBudget(t.Context(), e.acct); err != nil || budget != nil {
		t.Fatalf("no limit: budget %+v, %v", budget, err)
	}

	e.settings(func(s *AppSettings) { s.LimitMsat = limit(1_000 * sat) })
	e.pay(e.node.OutsideInvoice(500 * sat))
	e.settings(func(s *AppSettings) { s.LimitMsat = limit(100 * sat) })
	balance, err := e.ledger.AppBalance(t.Context(), e.acct)
	if err != nil || balance != -400*sat {
		t.Fatalf("cut limit: balance %d, %v", balance, err)
	}
	if _, err := e.tryPay(1 * sat); !errors.Is(err, ErrQuotaExceeded) {
		t.Fatalf("cut limit: %v", err)
	}
}

// Catches: settings the database would refuse or misread.
func TestBadSettings(t *testing.T) {
	e := newEnv(t)
	cases := []struct {
		settings AppSettings
		want     error
	}{
		{AppSettings{Renewal: "hourly"}, ErrBadRenewal},
		{AppSettings{Renewal: RenewNever, LimitMsat: limit(-1)}, ErrBadAmount},
		{AppSettings{Renewal: RenewNever, LimitMsat: limit(MaxAmountMsat + 1)}, ErrBadAmount},
	}
	for _, c := range cases {
		if err := e.ledger.EditApp(t.Context(), e.acct, c.settings); !errors.Is(err, c.want) {
			t.Errorf("%+v: got %v, want %v", c.settings, err, c.want)
		}
	}
}

// Catches: an expired, deleted or restricted app that can still spend, and a pending
// payment that stops finishing.
func TestAccessInTheLedger(t *testing.T) {
	refused := func(e *env, want error) {
		t.Helper()
		if _, err := e.tryPay(1 * sat); !errors.Is(err, want) {
			t.Errorf("pay: got %v, want %v", err, want)
		}
	}
	t.Run("PayOff", func(t *testing.T) {
		e := newEnv(t)
		e.start()
		e.settings(func(s *AppSettings) { s.CanPay = false })
		refused(e, ErrRestricted)
		e.invoice(1 * sat)
	})
	t.Run("InvoiceOff", func(t *testing.T) {
		e := newEnv(t)
		e.start()
		e.settings(func(s *AppSettings) { s.CanInvoice = false })
		if _, err := e.ledger.MakeInvoice(t.Context(), InvoiceRequest{AccountID: e.acct, AmountMsat: sat}); !errors.Is(err, ErrRestricted) {
			t.Fatalf("got %v", err)
		}
	})
	t.Run("Expired", func(t *testing.T) {
		e := newEnv(t)
		e.start()
		at := time.Unix(e.now.Load()+60, 0)
		e.node.ScriptPay(fake.PayTimeout)
		e.settings(func(s *AppSettings) { s.ExpiresAt = at })
		pending, err := e.tryPay(1 * sat)
		if err != nil {
			t.Fatalf("before expiry: %v", err)
		}
		e.now.Store(at.Unix() - 1)
		if _, err := e.ledger.Access(t.Context(), e.acct); err != nil {
			t.Fatalf("last second: %v", err)
		}
		e.now.Store(at.Unix())
		refused(e, ErrAccountExpired)
		if _, err := e.ledger.MakeInvoice(t.Context(), InvoiceRequest{AccountID: e.acct, AmountMsat: sat}); !errors.Is(err, ErrAccountExpired) {
			t.Fatalf("invoice: %v", err)
		}
		if err := e.node.Finish(pending.PaymentHash, true); err != nil {
			t.Fatal(err)
		}
		e.sweep()
		e.waitState(pending.ID, StateSettled)
	})
	t.Run("Deleted", func(t *testing.T) {
		e := newEnv(t)
		e.start()
		e.node.ScriptPay(fake.PayTimeout)
		pending := e.pay(e.node.OutsideInvoice(1 * sat))
		if err := e.ledger.DeleteApp(t.Context(), e.acct); err != nil {
			t.Fatal(err)
		}
		refused(e, ErrAccountDeleted)
		if err := e.node.Finish(pending.PaymentHash, true); err != nil {
			t.Fatal(err)
		}
		e.sweep()
		e.waitState(pending.ID, StateSettled)
		if err := e.ledger.DeleteApp(t.Context(), e.acct); !errors.Is(err, ErrUnknownAccount) {
			t.Fatalf("second delete: %v", err)
		}
		if err := e.ledger.EditApp(t.Context(), e.acct, DefaultApp()); !errors.Is(err, ErrUnknownAccount) {
			t.Fatalf("edit after delete: %v", err)
		}
	})
}

// Catches: a list that misses usage or settings, or still shows a deleted app.
func TestApps(t *testing.T) {
	e := newEnv(t)
	e.start()
	expires := time.Unix(e.now.Load()+3600, 0).UTC()
	e.settings(func(s *AppSettings) {
		s.LimitMsat, s.Renewal, s.ExpiresAt = limit(1_000*sat), RenewMonthly, expires
	})
	e.pay(e.node.OutsideInvoice(100 * sat))
	apps, err := e.ledger.Apps(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(apps) != 1 {
		t.Fatalf("got %+v", apps)
	}
	got := apps[0]
	if got.ID != e.acct || got.Name != "test" || got.UsedMsat != 100*sat || got.LimitMsat == nil ||
		*got.LimitMsat != 1_000*sat || got.Renewal != RenewMonthly || !got.ExpiresAt.Equal(expires) || !got.CanPay {
		t.Fatalf("got %+v", got)
	}
	if err := e.ledger.DeleteApp(t.Context(), e.acct); err != nil {
		t.Fatal(err)
	}
	if apps, err = e.ledger.Apps(t.Context()); err != nil || len(apps) != 0 {
		t.Fatalf("got %+v, %v", apps, err)
	}
}
