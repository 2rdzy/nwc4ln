package ledger

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/2rdzy/nwc4ln/wallet/internal/node"
	"github.com/2rdzy/nwc4ln/wallet/internal/node/fake"
)

// Catches: a payment admitted for a connection that may not pay, or for no
// connection at all, because the hold's transaction does not check it (S04).
func TestPayRefusesConnectionThatMayNotPay(t *testing.T) {
	cases := map[string]func(e *env) int64{
		"none": func(*env) int64 { return 0 },
		"revoked": func(e *env) int64 {
			id := e.connection(e.acct)
			if _, err := e.store.DB.Exec(`UPDATE connections SET revoked_at = 1 WHERE id = ?`, id); err != nil {
				e.t.Fatal(err)
			}
			return id
		},
		"receive only": func(e *env) int64 {
			id := e.connection(e.acct)
			if _, err := e.store.DB.Exec(`UPDATE connections SET role = 'receive_only' WHERE id = ?`, id); err != nil {
				e.t.Fatal(err)
			}
			return id
		},
		"another account's": func(e *env) int64 { return e.connection(e.addSubWallet()) },
	}
	for name, connection := range cases {
		t.Run(name, func(t *testing.T) {
			e := newEnv(t)
			e.start()
			_, err := e.ledger.Pay(context.Background(), PayRequest{
				AccountID: e.acct, ConnectionID: connection(e), Invoice: e.node.OutsideInvoice(sat),
			})
			if !errors.Is(err, ErrConnectionRevoked) {
				t.Fatalf("got %v, want ErrConnectionRevoked", err)
			}
			if rows, sent := e.rowCount(), len(e.node.PayRequests()); rows != 0 || sent != 0 {
				t.Errorf("%d rows stored and %d payments sent to the node", rows, sent)
			}
		})
	}
}

// bentNode changes a success from the node, as a faulty or hostile node might.
type bentNode struct {
	node.Node
	bend func(*node.Payment)
}

func (n *bentNode) Pay(ctx context.Context, request node.PayRequest) (node.Payment, error) {
	payment, err := n.Node.Pay(ctx, request)
	if err == nil && payment.State == node.PaymentSucceeded {
		n.bend(&payment)
	}
	return payment, err
}

func (n *bentNode) TrackPayment(ctx context.Context, hash string) (node.Payment, error) {
	payment, err := n.Node.TrackPayment(ctx, hash)
	if err == nil && payment.State == node.PaymentSucceeded {
		n.bend(&payment)
	}
	return payment, err
}

// Catches: the ledger recording a payment that cannot have happened: a preimage that
// is not the proof for the hash, or an amount below zero. Checked on the answer to
// Pay and on the sweep's answer, which take different paths into applySuccess.
func TestImpossibleNodeReplyIsNotApplied(t *testing.T) {
	cases := map[string]func(*node.Payment){
		"wrong preimage":   func(p *node.Payment) { p.Preimage = strings.Repeat("ab", 32) },
		"empty preimage":   func(p *node.Payment) { p.Preimage = "" },
		"preimage not hex": func(p *node.Payment) { p.Preimage = "zz" + p.Preimage[2:] },
		"negative fee":     func(p *node.Payment) { p.FeeMsat = -5_000 },
		"negative amount":  func(p *node.Payment) { p.AmountMsat = -1 },
	}
	for name, bend := range cases {
		for _, path := range []string{"pay", "sweep"} {
			t.Run(name+" on "+path, func(t *testing.T) {
				e := newEnv(t)
				e.ledger = e.fresh(&bentNode{Node: e.node, bend: bend})
				e.start()
				if path == "sweep" {
					e.node.ScriptPay(fake.PayTimeout)
				}
				row := e.pay(e.node.OutsideInvoice(5 * sat))
				if path == "sweep" {
					e.wantState(row.ID, StatePending)
					if err := e.node.Finish(row.PaymentHash, true); err != nil {
						t.Fatal(err)
					}
					e.sweep()
				}
				got := e.wantState(row.ID, StatePending)
				if got.FeeMsat != 0 || got.Preimage != "" {
					t.Errorf("pending row changed: fee %d, preimage %q", got.FeeMsat, got.Preimage)
				}
				alert := e.wantAlert(AlertImpossibleReply)
				if !strings.Contains(alert.Message, row.PaymentHash) {
					t.Errorf("alert does not name the payment: %q", alert.Message)
				}
			})
		}
	}
}

// shortfallFor makes a lasting shortfall: a funded sub-wallet, and a node that can
// send nothing.
func (e *env) shortfallFor() {
	e.t.Helper()
	wallet := e.addSubWallet()
	e.fund(wallet, 100*sat)
	e.node.SetSpendable(node.Spendable{})
	e.watch()
	e.advance(shortfallAlertAfter + time.Minute)
}

func (e *env) watch() {
	e.t.Helper()
	if err := e.ledger.watchShortfall(context.Background()); err != nil {
		e.t.Fatal(err)
	}
}

func (e *env) paused() bool {
	e.t.Helper()
	paused, err := e.ledger.SpendingPaused(context.Background())
	if err != nil {
		e.t.Fatal(err)
	}
	return paused
}

func (e *env) openShortfallAlerts() int {
	e.t.Helper()
	var open int
	if err := e.store.DB.QueryRow(`SELECT count(*) FROM alerts WHERE kind = ? AND closed_at IS NULL`,
		AlertShortfall).Scan(&open); err != nil {
		e.t.Fatal(err)
	}
	return open
}

// failWrites makes inserts into a table fail until the returned function is called.
func (e *env) failWrites(table string) (restore func()) {
	e.t.Helper()
	if _, err := e.store.DB.Exec(`CREATE TRIGGER fail_` + table + ` BEFORE INSERT ON ` + table +
		` BEGIN SELECT RAISE(ABORT, 'injected failure'); END`); err != nil {
		e.t.Fatal(err)
	}
	return func() {
		if _, err := e.store.DB.Exec(`DROP TRIGGER fail_` + table); err != nil {
			e.t.Fatal(err)
		}
	}
}

// Catches: an alert that says spending is paused while it is not, after one failed
// write, or a pause with no alert to explain it (S06). Alert and pause are stored
// together, and a later pass tries again.
func TestShortfallAlertAndPauseStoredTogether(t *testing.T) {
	for _, table := range []string{"settings", "alerts"} {
		t.Run("write to "+table+" fails", func(t *testing.T) {
			e := newEnv(t)
			e.start()
			e.shortfallFor()
			restore := e.failWrites(table)
			if err := e.ledger.watchShortfall(context.Background()); err == nil {
				t.Fatal("a failed write was not reported")
			}
			if e.paused() || e.openShortfallAlerts() != 0 {
				t.Fatalf("half stored: paused %t, %d open alerts", e.paused(), e.openShortfallAlerts())
			}
			restore()
			e.advance(shortfallCheckEvery)
			e.watch()
			if !e.paused() || e.openShortfallAlerts() != 1 {
				t.Errorf("after the retry: paused %t, %d open alerts", e.paused(), e.openShortfallAlerts())
			}
		})
	}
}

// Catches: the automatic pause turned on again over the admin's resume during the
// same shortfall, by a later pass or by a restart, which also opened a second alert.
// A new shortfall after the old one ended must pause again.
func TestAdminResumeStandsForTheShortfall(t *testing.T) {
	e := newEnv(t)
	e.start()
	e.shortfallFor()
	e.watch()
	if !e.paused() {
		t.Fatal("not paused after the shortfall lasted")
	}
	e.setPaused(false) // The admin resumes.
	for _, restart := range []bool{false, true} {
		if restart {
			e.restart()
		}
		for range 4 {
			e.advance(shortfallCheckEvery)
			e.watch()
		}
		if e.paused() || e.openShortfallAlerts() != 1 {
			t.Fatalf("restart %t: paused %t, %d open alerts", restart, e.paused(), e.openShortfallAlerts())
		}
	}

	e.node.SetSpendable(node.Spendable{ActiveMsat: testFunds, AllMsat: testFunds}) // It ends.
	e.watch()
	if e.openShortfallAlerts() != 0 {
		t.Fatal("alert still open after the shortfall ended")
	}
	e.node.SetSpendable(node.Spendable{}) // A new one.
	e.watch()
	e.advance(shortfallAlertAfter + time.Minute)
	e.watch()
	if !e.paused() || e.openShortfallAlerts() != 1 {
		t.Errorf("new shortfall: paused %t, %d open alerts", e.paused(), e.openShortfallAlerts())
	}
}

// Catches: a shortfall alert whose text says spending is paused after the admin
// resumed it, or the reverse.
func TestShortfallAlertTextFollowsTheSwitch(t *testing.T) {
	e := newEnv(t)
	e.start()
	e.shortfallFor()
	e.watch()
	if text := e.wantAlert(AlertShortfall).Message; !strings.Contains(text, "spending is paused") {
		t.Errorf("paused, but the alert says: %q", text)
	}
	e.setPaused(false)
	text := e.wantAlert(AlertShortfall).Message
	if strings.Contains(text, "is paused") || !strings.Contains(text, "resumed by the admin") {
		t.Errorf("resumed, but the alert says: %q", text)
	}
}
