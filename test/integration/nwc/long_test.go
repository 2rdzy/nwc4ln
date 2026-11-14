//go:build integration

package nwc_test

// Long tests: a random soak, a crash loop and node and relay outages. Each runs a
// short version by default, as CI does, and reads its length from the environment
// for longer runs (see scripts/longtests.sh):
//
//	NWC4LN_SOAK_FOR       how long the soak runs, such as 90s, 6h or 24h
//	NWC4LN_SOAK_ROUND     how often the soak checks the ledger against the node
//	NWC4LN_SOAK_APPS      how many apps take part
//	NWC4LN_SOAK_WALLETS   how many sub-wallets take part
//	NWC4LN_CRASH_FOR      how long the crash loop runs; at least NWC4LN_CRASH_KILLS kills
//	NWC4LN_CRASH_KILLS    the fewest kills
//	NWC4LN_OUTAGE_ROUNDS  how many node and relay outages
//	NWC4LN_LONG_SEED      the random seed, to replay a run
//	NWC4LN_LONG_REPORT    a file the soak writes its counts to, as JSON
//	NWC4LN_IT_WAIT        how long the harness waits for a node after a restart (default 2m)

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"os"
	"os/exec"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/time/rate"

	"github.com/2rdzy/nwc4ln/internal/ledger"
	"github.com/2rdzy/nwc4ln/internal/node"
	"github.com/2rdzy/nwc4ln/internal/nwc/nwctest"
	"github.com/2rdzy/nwc4ln/test/integration"
)

func envDuration(t *testing.T, name string, fallback time.Duration) time.Duration {
	t.Helper()
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}
	d, err := time.ParseDuration(value)
	if err != nil || d <= 0 {
		t.Fatalf("%s=%q: want a duration such as 90s or 6h", name, value)
	}
	return d
}

func envInt(t *testing.T, name string, fallback int) int {
	t.Helper()
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}
	i, err := strconv.Atoi(value)
	if err != nil || i < 1 {
		t.Fatalf("%s=%q: want a whole number from 1", name, value)
	}
	return i
}

// seed returns the run's random seed, and logs it so that a failure can be replayed.
func seed(t *testing.T) uint64 {
	t.Helper()
	s := uint64(time.Now().UnixNano())
	if value := os.Getenv("NWC4LN_LONG_SEED"); value != "" {
		var err error
		if s, err = strconv.ParseUint(value, 10, 64); err != nil {
			t.Fatalf("NWC4LN_LONG_SEED=%q: %v", value, err)
		}
	}
	t.Logf("seed %d (replay with NWC4LN_LONG_SEED=%d)", s, s)
	return s
}

// tally counts what happened, by operation and outcome.
type tally struct {
	mu     sync.Mutex
	counts map[string]int
}

func (c *tally) add(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.counts == nil {
		c.counts = map[string]int{}
	}
	c.counts[key]++
}

func (c *tally) total() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, v := range c.counts {
		n += v
	}
	return n
}

func (c *tally) report(t *testing.T) {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	keys := slices.Sorted(func(yield func(string) bool) {
		for k := range c.counts {
			if !yield(k) {
				return
			}
		}
	})
	for _, k := range keys {
		t.Logf("%-40s %d", k, c.counts[k])
	}
	if path := os.Getenv("NWC4LN_LONG_REPORT"); path != "" {
		data, err := json.MarshalIndent(c.counts, "", "  ")
		if err == nil {
			err = os.WriteFile(path, data, 0o600)
		}
		if err != nil {
			t.Errorf("write report: %v", err)
		}
	}
}

// bad lists the error codes that mean a bug in nwc4ln, never a normal outcome under
// load: a refusal for funds, rate, limit or a pending payment is normal.
var bad = []string{"INTERNAL", "NOT_IMPLEMENTED", "RESTRICTED", "UNSUPPORTED_ENCRYPTION", "UNAUTHORIZED"}

// outcome names a reply for the tally, and reports one that means a bug.
func outcome(op string, response *nwctest.Response, err error) (string, bool) {
	switch {
	case err != nil:
		return op + " no reply", true
	case response.Error == nil:
		return op + " ok", true
	default:
		return op + " " + response.Error.Code, !slices.Contains(bad, response.Error.Code)
	}
}

// soakAccount is one account a worker drives over NWC. Its pace stays under the
// service's own limits (5 requests a second, 10 invoices a minute), so that the soak
// measures work done, not refusals.
//
// An account that reaches the daily invoice cap (1,000 in 24 hours) is replaced by a
// fresh one of the same kind, so that a run of hours keeps moving money.
type soakAccount struct {
	name     string
	wallet   bool
	requests *rate.Limiter
	invoices *rate.Limiter

	mu      sync.Mutex
	id      int64
	client  *nwctest.Client
	renewed int
}

func (a *soakAccount) current() (*nwctest.Client, int64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.client, a.id
}

// call sends one request, with the wait a client allows.
func (a *soakAccount) call(ctx context.Context, method string, params any) (*nwctest.Response, error) {
	if err := a.requests.Wait(ctx); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, 65*time.Second)
	defer cancel()
	client, _ := a.current()
	return client.CallWith(ctx, nwctest.NIP44, method, params)
}

// atCap reports a refusal for the daily invoice cap, not for the rate.
func atCap(response *nwctest.Response) bool {
	return response != nil && response.Error != nil && response.Error.Code == "RATE_LIMITED" &&
		strings.Contains(response.Error.Message, "expire")
}

// invoice asks the account for an invoice, and returns it, or "" if refused.
func (a *soakAccount) invoice(ctx context.Context, amountMsat int64) (string, string, *nwctest.Response, error) {
	response, err := a.call(ctx, "make_invoice", map[string]int64{"amount": amountMsat})
	if err != nil || response.Error != nil {
		return "", "", response, err
	}
	var made struct {
		Invoice string `json:"invoice"`
		Hash    string `json:"payment_hash"`
	}
	if err := json.Unmarshal(response.Result, &made); err != nil {
		return "", "", response, err
	}
	return made.Invoice, made.Hash, response, nil
}

// steadyAll reads the node's figure over all channels until two reads agree.
func steadyAll(t *testing.T, lightning node.Node) int64 {
	t.Helper()
	last := int64(-1)
	for deadline := time.Now().Add(60 * time.Second); time.Now().Before(deadline); time.Sleep(time.Second) {
		spendable, err := lightning.Spendable(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if len(spendable.Outgoing) == 0 && spendable.AllMsat == last {
			return last
		}
		last = spendable.AllMsat
	}
	t.Fatal("the node's figure kept moving")
	return 0
}

// waitNoPendingPayments waits until no payment is pending in the ledger. Unpaid
// invoices may stay open, and do not count.
// On a timeout it reports each stuck row and what the node and the payee say about it.
func waitNoPendingPayments(t *testing.T, n *integration.Network, money *ledger.Ledger, lightning node.Node,
	limit time.Duration) {
	t.Helper()
	for deadline := time.Now().Add(limit); ; time.Sleep(2 * time.Second) {
		rows, _, err := money.AllHistory(t.Context(), ledger.AllHistoryQuery{AccountID: ledger.AllAccounts})
		if err != nil {
			t.Fatal(err)
		}
		var pending []ledger.Transaction
		for _, row := range rows {
			if row.Type == ledger.TypeOutgoing && row.State == ledger.StatePending {
				pending = append(pending, row)
			}
		}
		if len(pending) == 0 {
			return
		}
		if time.Now().After(deadline) {
			for _, row := range pending {
				payment, err := lightning.TrackPayment(t.Context(), row.PaymentHash)
				payee, _ := n.LNCLI(t.Context(), other, "lookupinvoice", row.PaymentHash)
				t.Logf("stuck %s: created %s, internal %v, amount %d; node says state %d (%v); payee says %s",
					row.PaymentHash, row.CreatedAt.Format(time.TimeOnly), row.Internal, row.AmountMsat, payment.State, err,
					strings.Join(strings.Fields(string(payee)), " "))
			}
			t.Fatalf("%d payments still pending after %s", len(pending), limit)
		}
	}
}

// ledgerFlow is what the ledger says left or reached the node over the rows: settled
// income less settled payments and their fees, internal rows left out.
func ledgerFlow(t *testing.T, money *ledger.Ledger) int64 {
	t.Helper()
	rows, _, err := money.AllHistory(t.Context(), ledger.AllHistoryQuery{AccountID: ledger.AllAccounts})
	if err != nil {
		t.Fatal(err)
	}
	var flow int64
	for _, row := range rows {
		if row.State != ledger.StateSettled || row.Internal {
			continue
		}
		if row.Type == ledger.TypeIncoming {
			flow += row.AmountMsat
		} else {
			flow -= row.AmountMsat + row.FeeMsat
		}
	}
	return flow
}

func checkClean(t *testing.T, money *ledger.Ledger, since time.Time) {
	t.Helper()
	mismatches, err := money.Check(t.Context(), since)
	if err != nil {
		t.Fatalf("check: %v", err)
	}
	for _, m := range mismatches {
		t.Errorf("mismatch: %s", m)
	}
	if len(mismatches) > 0 {
		t.FailNow()
	}
}

// Catches: money lost or invented under real node timing, with many accounts paying,
// receiving and moving money at once, which the fake node cannot show. Each round, the
// ledger must agree with the node. At the end, nothing may be pending, and the node's
// balance must have changed by exactly what the ledger says came in and went out.
func TestSoak(t *testing.T) {
	n, err := integration.Load(integration.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	if err := waitReady(ctx, n); err != nil {
		t.Fatal(err)
	}
	runFor := envDuration(t, "NWC4LN_SOAK_FOR", 90*time.Second)
	round := envDuration(t, "NWC4LN_SOAK_ROUND", 30*time.Second)
	random := rand.New(rand.NewPCG(seed(t), 1))
	money, service := inProcess(t, ctx, n)
	lightning := nodeClient(t, n, t.TempDir())
	before := steadyAll(t, lightning)
	flowBefore := ledgerFlow(t, money)

	var counts tally
	var (
		closeMu sync.Mutex
		toClose []*nwctest.Client
	)
	defer func() {
		closeMu.Lock()
		defer closeMu.Unlock()
		for _, c := range toClose {
			c.Close()
		}
	}()
	// open makes an account of one kind, and returns its connected client and, for a
	// sub-wallet, its ID.
	open := func(name string, wallet bool) (*nwctest.Client, int64, error) {
		var connectionString string
		var err error
		if wallet {
			connectionString, err = service.AddSubWallet(ctx, name)
		} else {
			settings := ledger.DefaultApp()
			settings.CanInvoice = true
			connectionString, err = service.AddApp(ctx, name, settings)
		}
		if err != nil {
			return nil, 0, err
		}
		client, err := nwctest.Parse(connectionString)
		if err != nil {
			return nil, 0, err
		}
		if err := client.Connect(ctx); err != nil {
			return nil, 0, err
		}
		closeMu.Lock()
		toClose = append(toClose, client)
		closeMu.Unlock()
		var id int64
		if wallet {
			wallets, err := money.SubWallets(ctx)
			if err != nil {
				return nil, 0, err
			}
			for _, w := range wallets {
				if w.Name == name {
					id = w.ID
				}
			}
		}
		return client, id, nil
	}
	var accounts []*soakAccount
	add := func(name string, wallet bool) {
		t.Helper()
		client, id, err := open(name, wallet)
		if err != nil {
			t.Fatal(err)
		}
		waitForInfo(t, client)
		accounts = append(accounts, &soakAccount{name: name, wallet: wallet, client: client, id: id,
			requests: rate.NewLimiter(4, 4), invoices: rate.NewLimiter(rate.Every(7*time.Second), 9)})
	}
	for i := range envInt(t, "NWC4LN_SOAK_APPS", 2) {
		add(fmt.Sprintf("app-%d", i), false)
	}
	for i := range envInt(t, "NWC4LN_SOAK_WALLETS", 3) {
		add(fmt.Sprintf("wallet-%d", i), true)
	}
	// renew replaces an account that reached the daily invoice cap. A sub-wallet's money
	// stays in the old one, and the cleanup at the end moves it back.
	renew := func(a *soakAccount) {
		a.mu.Lock()
		defer a.mu.Unlock()
		a.renewed++
		client, id, err := open(fmt.Sprintf("%s-%d", a.name, a.renewed), a.wallet)
		if err != nil {
			counts.add("renew failed")
			return
		}
		a.client, a.id = client, id
		counts.add("account renewed at the daily invoice cap")
	}

	var bugs atomic.Int64
	note := func(op string, response *nwctest.Response, err error) {
		key, ok := outcome(op, response, err)
		counts.add(key)
		if !ok {
			bugs.Add(1)
			t.Errorf("%s: %+v, %v", key, response, err)
		}
	}
	deadline := time.Now().Add(runFor)
	// net is money in from outside less money out, so far. Payments out are cheaper to
	// make than invoices, so without this a long run drains the node's channels and then
	// only tests refusals. Each direction pauses while it is ahead by more than drift.
	var net atomic.Int64
	const drift = 100_000_000
	var work sync.WaitGroup
	for i, a := range accounts {
		r := rand.New(rand.NewPCG(random.Uint64(), uint64(i)))
		work.Go(func() {
			for time.Now().Before(deadline) && ctx.Err() == nil && bugs.Load() == 0 {
				amount := int64(1+r.IntN(500)) * 1000
				switch pick := r.IntN(100); {
				case (pick < 30 || net.Load() < -drift) && net.Load() < drift && a.invoices.Allow():
					// Money in from outside.
					invoice, _, response, err := a.invoice(ctx, amount)
					if atCap(response) {
						renew(a)
						continue
					}
					if invoice == "" {
						note("make_invoice", response, err)
						continue
					}
					counts.add("make_invoice ok")
					if err := n.PayInvoice(ctx, other, invoice); err != nil {
						counts.add("outside payer failed")
					} else {
						counts.add("received from outside")
						net.Add(amount)
					}
				case pick >= 30 && pick < 50 && net.Load() > -drift: // Money out, directly or one hop on.
					payee := other
					if r.IntN(4) == 0 {
						payee = integration.NodeLNDC
					}
					invoice, _, err := n.Invoice(ctx, payee, amount)
					if err != nil {
						counts.add("outside invoice failed")
						continue
					}
					response, err := a.call(ctx, "pay_invoice", map[string]string{"invoice": invoice})
					note("pay_invoice out", response, err)
					if err == nil && response.Error == nil {
						net.Add(-amount)
					}
				case pick >= 50 && pick < 65 && accounts[pick%len(accounts)].invoices.Allow():
					// Money to another account on this node.
					to := accounts[pick%len(accounts)]
					invoice, _, response, err := to.invoice(ctx, amount)
					if atCap(response) {
						renew(to)
						continue
					}
					if invoice == "" {
						note("make_invoice", response, err)
						continue
					}
					response, err = a.call(ctx, "pay_invoice", map[string]string{"invoice": invoice})
					note("pay_invoice internal", response, err)
				case pick >= 65 && pick < 85 || pick < 30:
					response, err := a.call(ctx, "get_balance", nil)
					note("get_balance", response, err)
				default:
					response, err := a.call(ctx, "list_transactions", map[string]any{"limit": 5, "unpaid": true})
					note("list_transactions", response, err)
				}
			}
		})
	}
	// The admin moves money between the main wallet and sub-wallets meanwhile.
	work.Go(func() {
		r := rand.New(rand.NewPCG(random.Uint64(), 99))
		for time.Now().Before(deadline) && ctx.Err() == nil && bugs.Load() == 0 {
			time.Sleep(time.Duration(2+r.IntN(4)) * time.Second)
			var walletAccounts []*soakAccount
			for _, a := range accounts {
				if a.wallet {
					walletAccounts = append(walletAccounts, a)
				}
			}
			if len(walletAccounts) == 0 {
				continue
			}
			a := walletAccounts[r.IntN(len(walletAccounts))]
			amount := int64(1+r.IntN(200)) * 1000
			var err error
			_, id := a.current()
			if r.IntN(2) == 0 {
				_, err = money.Fund(ctx, id, amount)
				counts.add(fmt.Sprintf("fund %v", err == nil))
			} else {
				_, err = money.Withdraw(ctx, id, amount)
				counts.add(fmt.Sprintf("withdraw %v", err == nil))
			}
		}
	})
	// The ledger and the node must agree after every round.
	roundStart := time.Now().Add(-time.Minute)
	for time.Now().Before(deadline) && bugs.Load() == 0 {
		time.Sleep(min(round, time.Until(deadline)+time.Second))
		next := time.Now().Add(-time.Minute) // Rows from the round on; overlap is harmless.
		checkClean(t, money, roundStart)
		roundStart = next
		t.Logf("%s left, %d operations so far", time.Until(deadline).Round(time.Second), counts.total())
	}
	work.Wait()
	counts.report(t)
	if bugs.Load() > 0 {
		t.FailNow()
	}
	waitNoPendingPayments(t, n, money, lightning, 5*time.Minute)
	checkClean(t, money, time.Unix(0, 0))
	nodeDelta := steadyAll(t, lightning) - before
	ledgerDelta := ledgerFlow(t, money) - flowBefore
	// The node reports some figures in whole sats, so each channel can read up to a sat
	// low. Anything more is money the ledger did not record.
	if diff := nodeDelta - ledgerDelta; diff > 4_000 || diff < -4_000 {
		t.Fatalf("node changed by %d msat, the ledger by %d msat: %d msat unaccounted", nodeDelta, ledgerDelta, diff)
	}
	t.Logf("node changed by %d msat, the ledger by %d msat", nodeDelta, ledgerDelta)
	// Leave the main wallet with the money, so that a test after this one can pay from an
	// app: sub-wallet money is set aside from it.
	wallets, err := money.SubWallets(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, w := range wallets {
		if w.BalanceMsat <= 0 {
			continue
		}
		if row, err := money.Withdraw(ctx, w.ID, w.BalanceMsat); err != nil || row.State != ledger.StateSettled {
			t.Fatalf("withdraw %s: %+v, %v", w.Name, row, err)
		}
	}
}

// needFunds stops a test whose app has too little to spend, with the reason, rather
// than let it fail later in a way that reads like a bug in nwc4ln. An app spends from
// the main wallet, which an earlier test may have moved into sub-wallets.
func needFunds(t *testing.T, client *nwctest.Client, msat int64) {
	t.Helper()
	var balance struct {
		Balance int64 `json:"balance"`
	}
	for deadline := time.Now().Add(30 * time.Second); ; time.Sleep(time.Second) {
		ctx, cancel := context.WithTimeout(t.Context(), callTimeout)
		response, err := client.Call(ctx, nwctest.NIP44, "get_balance")
		cancel()
		if err == nil && response.Error == nil && json.Unmarshal(response.Result, &balance) == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("get_balance: %+v, %v", response, err)
		}
	}
	if balance.Balance < msat {
		t.Fatalf("the app can spend %d msat, and this test needs %d: an earlier test left the node's money "+
			"elsewhere, such as in sub-wallets", balance.Balance, msat)
	}
}

// waitNodeAgrees waits until every pending payment in the ledger is one the node still
// has in flight. That is the rule after an outage: a payment between two nodes can stay
// in flight for as long as a channel needs to settle it, and on regtest no block ends it,
// so the ledger rightly keeps its hold. A row the node has finished must not stay pending.
func waitNodeAgrees(t *testing.T, money *ledger.Ledger, lightning node.Node, limit time.Duration) {
	t.Helper()
	for deadline := time.Now().Add(limit); ; time.Sleep(2 * time.Second) {
		rows, _, err := money.AllHistory(t.Context(), ledger.AllHistoryQuery{AccountID: ledger.AllAccounts})
		if err != nil {
			t.Fatal(err)
		}
		var wrong, inFlight []string
		for _, row := range rows {
			if row.Type != ledger.TypeOutgoing || row.State != ledger.StatePending {
				continue
			}
			payment, err := lightning.TrackPayment(t.Context(), row.PaymentHash)
			switch {
			case err != nil:
				wrong = append(wrong, fmt.Sprintf("%s: %v", row.PaymentHash, err))
			case payment.State == node.PaymentInFlight:
				inFlight = append(inFlight, row.PaymentHash)
			default:
				wrong = append(wrong, fmt.Sprintf("%s: pending, node says state %d", row.PaymentHash, payment.State))
			}
		}
		if len(wrong) == 0 {
			for _, hash := range inFlight {
				t.Logf("still in flight at the node, hold kept: %s", hash)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("after %s, rows the node has finished are still pending:\n%s", limit, strings.Join(wrong, "\n"))
		}
	}
}

// lookupWithin is lookupUntil with a longer wait.
func lookupWithin(t *testing.T, c *nwctest.Client, hash, want string, limit time.Duration) {
	t.Helper()
	params := map[string]string{"payment_hash": hash}
	var last found
	for deadline := time.Now().Add(limit); time.Now().Before(deadline); time.Sleep(2 * time.Second) {
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		response, err := c.CallWith(ctx, nwctest.NIP44, "lookup_invoice", params)
		cancel()
		if err != nil || response.Error != nil {
			continue
		}
		if err := json.Unmarshal(response.Result, &last); err == nil && last.State == want {
			return
		}
	}
	t.Fatalf("state stayed %+v, want %s", last, want)
}

// Catches: a hold stuck, or released for a payment that went out, after the real
// binary is killed at a random moment with payments in flight (R12), and a check
// command that does not work in the real binary.
func TestCrashLoop(t *testing.T) {
	n, err := integration.Load(integration.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	if err := waitReady(t.Context(), n); err != nil {
		t.Fatal(err)
	}
	runFor := envDuration(t, "NWC4LN_CRASH_FOR", time.Second)
	kills := envInt(t, "NWC4LN_CRASH_KILLS", 3)
	random := rand.New(rand.NewPCG(seed(t), 2))
	s := startService(t, n)
	client, err := nwctest.Parse(s.connectionString)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Connect(t.Context()); err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	waitForInfo(t, client)
	needFunds(t, client, 100_000_000)

	deadline := time.Now().Add(runFor)
	for round := 0; round < kills || time.Now().Before(deadline); round++ {
		request, hash, preimage := holdInvoice(t, n)
		go func() { // The reply is lost when the process dies.
			ctx, cancel := context.WithTimeout(t.Context(), 65*time.Second)
			defer cancel()
			_, _ = client.CallWith(ctx, nwctest.NIP44, "pay_invoice", map[string]string{"invoice": request})
		}()
		waitHeld(t, n, hash)
		for range random.IntN(3) { // A few quick payments that may be cut off.
			invoice, _, err := n.Invoice(t.Context(), other, int64(1+random.IntN(50))*1000)
			if err != nil {
				t.Fatal(err)
			}
			go func() {
				ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
				defer cancel()
				_, _ = client.CallWith(ctx, nwctest.NIP44, "pay_invoice", map[string]string{"invoice": invoice})
			}()
		}
		time.Sleep(time.Duration(random.IntN(2000)) * time.Millisecond)
		s.kill()
		s.start()
		if err := client.Connect(t.Context()); err != nil {
			t.Fatal(err)
		}
		waitForInfo(t, client)
		if random.IntN(2) == 0 {
			settleHeld(t, n, preimage)
			lookupUntil(t, client, hash, "settled")
		} else {
			if out, err := n.LNCLI(t.Context(), other, "cancelinvoice", hash); err != nil {
				t.Fatalf("cancelinvoice: %v: %s", err, out)
			}
			// A row made before the restart is believed failed only once 2 minutes old (R12).
			lookupWithin(t, client, hash, "failed", 4*time.Minute)
		}
		t.Logf("round %d done", round+1)
	}
	// The real check command, beside the running service, must find nothing.
	out, err := exec.CommandContext(t.Context(), s.binary, "check", "--all", "--config", s.config).CombinedOutput()
	if err != nil || !strings.Contains(string(out), "agree") {
		t.Fatalf("check: %v: %s", err, out)
	}
}

// Catches: a service that goes deaf, a payment marked failed that later went out, a
// hold that never clears, or a payment in flight across the node's restart that the
// ledger does not follow up, after the node and then the relay go away under load.
func TestOutages(t *testing.T) {
	n, err := integration.Load(integration.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	if err := waitReady(ctx, n); err != nil {
		t.Fatal(err)
	}
	rounds := envInt(t, "NWC4LN_OUTAGE_ROUNDS", 1)
	money, service := inProcess(t, ctx, n)
	settings := ledger.DefaultApp()
	connectionString, err := service.AddApp(ctx, "outage", settings)
	if err != nil {
		t.Fatal(err)
	}
	client, err := nwctest.Parse(connectionString)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	waitForInfo(t, client)
	needFunds(t, client, 100_000_000)

	var counts tally
	stop := make(chan struct{})
	var load sync.WaitGroup
	load.Go(func() {
		for {
			select {
			case <-stop:
				return
			case <-time.After(500 * time.Millisecond):
			}
			invoice, _, err := n.Invoice(ctx, other, 1000)
			if err != nil {
				counts.add("outside invoice failed")
				continue
			}
			callCtx, cancelCall := context.WithTimeout(ctx, 65*time.Second)
			response, err := client.CallWith(callCtx, nwctest.NIP44, "pay_invoice", map[string]string{"invoice": invoice})
			cancelCall()
			key, _ := outcome("pay_invoice", response, err)
			counts.add(key) // An outage may give any answer, or none.
			if err != nil {
				_ = client.Connect(ctx) // The relay may have gone away.
			}
		}
	})
	for range rounds {
		for _, down := range []string{self, integration.NodeRelay} {
			time.Sleep(3 * time.Second)
			// Across the node's restart, a payment the payee holds: the ledger must follow
			// it up after the restart, and settle it once the payee does.
			var preimage string
			if down == self {
				var request, hash string
				request, hash, preimage = holdInvoice(t, n)
				go func() {
					callCtx, cancelCall := context.WithTimeout(ctx, 65*time.Second)
					defer cancelCall()
					_, _ = client.CallWith(callCtx, nwctest.NIP44, "pay_invoice", map[string]string{"invoice": request})
				}()
				waitHeld(t, n, hash)
			}
			if err := n.StopNode(down); err != nil {
				t.Fatal(err)
			}
			time.Sleep(10 * time.Second)
			if err := n.StartNode(ctx, down); err != nil {
				t.Fatal(err)
			}
			if down == self {
				if err := waitReady(ctx, n); err != nil {
					t.Fatal(err)
				}
				settleHeld(t, n, preimage)
			}
		}
	}
	close(stop)
	load.Wait()
	counts.report(t)
	// The service reconnects to the relay with a backoff, so it may take a moment.
	var last error
	for deadline := time.Now().Add(90 * time.Second); ; time.Sleep(2 * time.Second) {
		_ = client.Connect(ctx)
		callCtx, cancelCall := context.WithTimeout(ctx, callTimeout)
		response, err := client.Call(callCtx, nwctest.NIP44, "get_info")
		cancelCall()
		if err == nil && response.Error == nil {
			break
		}
		if last = err; err == nil {
			last = fmt.Errorf("%+v", response.Error)
		}
		if time.Now().After(deadline) {
			t.Fatalf("service deaf after the outages: %v", last)
		}
	}
	waitNodeAgrees(t, money, nodeClient(t, n, t.TempDir()), 5*time.Minute)
	checkClean(t, money, time.Unix(0, 0))
}
