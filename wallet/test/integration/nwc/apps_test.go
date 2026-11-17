//go:build integration

package nwc_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/2rdzy/nwc4ln/internal/ledger"
	"github.com/2rdzy/nwc4ln/internal/node"
	"github.com/2rdzy/nwc4ln/internal/node/fake"
	"github.com/2rdzy/nwc4ln/internal/nwc"
	"github.com/2rdzy/nwc4ln/internal/nwc/nwctest"
	"github.com/2rdzy/nwc4ln/internal/store"
	"github.com/2rdzy/nwc4ln/test/integration"
)

// setLimit gives an app a limit, using the same Go function the UI will use. There
// is no command for it, so the service stops while this test holds the database.
func (s *service) setLimit(name string, limitMsat int64, renewal string) {
	s.t.Helper()
	ctx := s.t.Context()
	if err := s.cmd.Process.Signal(os.Interrupt); err != nil {
		s.t.Fatal(err)
	}
	_ = s.cmd.Wait()
	s.cmd = nil
	db, err := store.Open(ctx, filepath.Join(s.dir, "nwc4ln.db"))
	if err != nil {
		s.t.Fatal(err)
	}
	money := ledger.New(db, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	apps, err := money.Apps(ctx)
	if err != nil {
		s.t.Fatal(err)
	}
	for _, app := range apps {
		if app.Name != name {
			continue
		}
		settings := app.AppSettings
		settings.LimitMsat, settings.Renewal = &limitMsat, renewal
		if err := money.EditApp(ctx, app.ID, settings); err != nil {
			s.t.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		s.t.Fatal(err)
	}
	s.start()
}

// Catches: a limit overspent on a real node, where payments really take time. How
// many get through depends on timing, so the test holds to what must always be true.
func TestLimitUnderLoad(t *testing.T) {
	n, err := integration.Load(integration.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	s := startService(t, n)
	const (
		payments = 8
		amount   = 1_000 // Sats. The reserve is 10 sats, so three fit and a fourth does not.
		limit    = 3_030_000
	)
	s.setLimit("first", limit, ledger.RenewDaily)
	client, err := nwctest.Parse(s.connectionString)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	waitForInfo(t, client)

	invoices := make([]string, payments)
	for i := range invoices {
		out, err := n.LNCLI(t.Context(), other, "addinvoice", "--amt", "1000")
		if err != nil {
			t.Fatalf("addinvoice: %v: %s", err, out)
		}
		var reply struct {
			Request string `json:"payment_request"`
		}
		if err := json.Unmarshal(out, &reply); err != nil {
			t.Fatal(err)
		}
		invoices[i] = reply.Request
	}
	var (
		mu         sync.Mutex
		wg         sync.WaitGroup
		paid, over int
	)
	for _, invoice := range invoices {
		wg.Go(func() {
			ctx, cancel := context.WithTimeout(t.Context(), replyWithin)
			defer cancel()
			response, err := client.CallWith(ctx, nwctest.NIP44, "pay_invoice", map[string]any{"invoice": invoice})
			if err != nil {
				t.Error(err)
				return
			}
			mu.Lock()
			defer mu.Unlock()
			switch {
			case response.Error == nil:
				paid++
			case response.Error.Code == "QUOTA_EXCEEDED":
				over++
			default:
				t.Errorf("code %s: %s", response.Error.Code, response.Error.Message)
			}
		})
	}
	wg.Wait()
	if paid+over != payments || paid == 0 || paid > 3 {
		t.Fatalf("paid %d, refused %d, of %d", paid, over, payments)
	}
	var budget struct {
		Used  int64 `json:"used_budget"`
		Total int64 `json:"total_budget"`
	}
	response := call(t, client, nwctest.NIP44, "get_budget")
	if err := json.Unmarshal(response.Result, &budget); err != nil {
		t.Fatal(err)
	}
	if budget.Total != limit || budget.Used > limit || budget.Used < int64(paid)*amount*1_000 {
		t.Fatalf("budget %+v after %d payments", budget, paid)
	}
}

// Catches: a service that goes on serving only the keys it started with. It starts
// with no connections at all, and the relay is real, so the new subscription and the
// new info event are real too. The node does not matter here, so it is the fake.
func TestAppsChangeWhileRunning(t *testing.T) {
	n, err := integration.Load(integration.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	db, err := store.Create(ctx, filepath.Join(t.TempDir(), "nwc4ln.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	lightning := fake.New()
	lightning.SetSpendable(node.Spendable{ActiveMsat: 1_000_000})
	money := ledger.New(db, lightning, log)
	service, err := nwc.New(ctx, db, lightning, money, n.RelayURL(), log)
	if err != nil {
		t.Fatal(err)
	}
	if err := money.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer money.Close()
	var running sync.WaitGroup
	running.Go(func() { service.Run(ctx) })
	defer running.Wait()
	defer cancel()

	add := func(name string) *nwctest.Client {
		t.Helper()
		connectionString, err := service.AddApp(ctx, name, ledger.DefaultApp())
		if err != nil {
			t.Fatal(err)
		}
		client, err := nwctest.Parse(connectionString)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(client.Close)
		return client
	}
	first := add("first")
	waitForInfo(t, first)
	if response := call(t, first, nwctest.NIP44, "get_info"); response.Error != nil {
		t.Fatalf("first: %+v", response.Error)
	}
	second := add("second")
	waitForInfo(t, second)
	for _, client := range []*nwctest.Client{first, second} {
		if response := call(t, client, nwctest.NIP44, "get_balance"); response.Error != nil {
			t.Fatalf("after adding: %+v", response.Error)
		}
	}

	apps, err := money.Apps(ctx)
	if err != nil {
		t.Fatal(err)
	}
	settings := ledger.DefaultApp()
	settings.CanPay = false
	if err := service.EditApp(ctx, apps[1].ID, settings); err != nil {
		t.Fatal(err)
	}
	for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(100 * time.Millisecond) {
		info, err := second.ReadInfo(ctx)
		if err == nil && !slices.Contains(info.Methods, "pay_invoice") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the info event did not change: %+v, %v", info, err)
		}
	}
	if response := call(t, second, nwctest.NIP44, "get_balance"); response.Error == nil ||
		response.Error.Code != "RESTRICTED" {
		t.Fatalf("after the edit: %+v", response.Error)
	}
}
