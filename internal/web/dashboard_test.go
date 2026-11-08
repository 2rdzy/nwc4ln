package web

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"path/filepath"
	"strings"
	"testing"

	"github.com/2rdzy/nwc4ln/internal/ledger"
	"github.com/2rdzy/nwc4ln/internal/node"
	"github.com/2rdzy/nwc4ln/internal/node/fake"
	"github.com/2rdzy/nwc4ln/internal/store"
)

const sat = 1_000

// Catches: the dashboard showing other figures than the ledger's, on the real ledger
// with a funded sub-wallet, a payment in flight, a drain and a node that is down.
func TestDashboardMatchesLedger(t *testing.T) {
	ctx := context.Background()
	db, err := store.Create(ctx, filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	n := fake.New()
	n.SetSpendable(node.Spendable{ActiveMsat: 5_000 * sat, AllMsat: 5_000 * sat})
	money := ledger.New(db, n, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err := money.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(money.Close)
	result, err := db.DB.Exec(`INSERT INTO accounts (type, name, can_pay, can_invoice, created_at)
		VALUES ('sub_wallet', 'holder', 1, 1, 0)`)
	if err != nil {
		t.Fatal(err)
	}
	wallet, err := result.LastInsertId()
	if err != nil {
		t.Fatal(err)
	}
	if row, err := money.Fund(ctx, wallet, 1_000*sat); err != nil || row.State != ledger.StateSettled {
		t.Fatalf("fund: %+v, %v", row, err)
	}
	n.ScriptPay(fake.PayTimeout)
	held, err := money.Pay(ctx, ledger.PayRequest{AccountID: wallet, Invoice: n.OutsideInvoice(100 * sat)})
	if err != nil || held.State != ledger.StatePending {
		t.Fatalf("pay: %+v, %v", held, err)
	}

	e := newEnv(t, money)
	cookie := e.login()
	show := func(want map[string]string, warnings ...string) {
		t.Helper()
		page := body(t, e.do(http.MethodGet, "/", withCookie(cookie)))
		for id, value := range want {
			if got := figure(t, page, id); got != value {
				t.Errorf("%s: %q, want %q", id, got, value)
			}
		}
		for _, id := range []string{"shortfall", "negative", "unknown"} {
			shown := strings.Contains(page, `id="`+id+`"`)
			if wanted := strings.Contains(strings.Join(warnings, " "), id); shown != wanted {
				t.Errorf("warning %s shown: %t", id, shown)
			}
		}
	}
	// 110 sat held: 100 sat and the 10 sat minimum reserve.
	show(map[string]string{"node": "5,000 sat", "subwallets": "890 sat", "holds": "110 sat", "main": "4,000 sat"})

	// Drained from outside. The fake's figure already leaves out the 100 sat in
	// flight, as LND's does, so the ledger adds that back: the node shows 600 sat.
	n.SetSpendable(node.Spendable{ActiveMsat: 500 * sat, AllMsat: 500 * sat})
	show(map[string]string{"node": "600 sat", "subwallets": "890 sat", "holds": "110 sat", "main": "-400 sat"},
		"shortfall", "negative")
	page := body(t, e.do(http.MethodGet, "/", withCookie(cookie)))
	if !strings.Contains(page, "Shortfall: 400 sat.") {
		t.Errorf("shortfall amount missing:\n%s", page)
	}

	n.SetUnreachable(true)
	show(map[string]string{"node": "unknown", "subwallets": "unknown", "holds": "unknown", "main": "unknown"}, "unknown")
}
