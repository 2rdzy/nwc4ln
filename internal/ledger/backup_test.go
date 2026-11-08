package ledger

import (
	"context"
	"io"
	"log/slog"
	"path/filepath"
	"testing"

	"github.com/2rdzy/nwc4ln/internal/node/fake"
	"github.com/2rdzy/nwc4ln/internal/store"
)

// Catches: a backup that cannot be restored, or that is not one consistent snapshot of
// committed rows. It is taken while another write is open, as when the service runs.
func TestBackupRestoresBalances(t *testing.T) {
	e := newEnv(t)
	e.start()
	first, second := e.addSubWallet(), e.addSubWallet()
	e.fund(first, 1_000*sat)
	e.fund(second, 500*sat)
	e.pay(e.node.OutsideInvoice(70 * sat))
	e.node.ScriptPay(fake.PayTimeout)
	if row, err := e.payFrom(first, 100*sat); err != nil || row.State != StatePending {
		t.Fatalf("payment %+v, %v", row, err)
	}
	want := map[int64]int64{first: e.balance(first), second: e.balance(second)}
	available := e.available()

	ctx := context.Background()
	open, err := e.store.DB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = open.Rollback() }()
	if _, err := insert(ctx, open, Transaction{
		AccountID: second, Type: TypeIncoming, State: StateSettled, AmountMsat: 5 * sat, PaymentHash: "ab", CreatedAt: e.ledger.now(),
	}); err != nil {
		t.Fatal(err)
	}
	dest := filepath.Join(t.TempDir(), "backup.db")
	if err := store.Backup(ctx, e.path, dest); err != nil {
		t.Fatal(err)
	}
	if err := open.Commit(); err != nil {
		t.Fatal(err)
	}
	if got := e.balance(second); got != want[second]+5*sat {
		t.Fatalf("live balance %d after the commit", got)
	}

	restored, err := store.Open(ctx, dest)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = restored.Close() }()
	from := New(restored, e.node, slog.New(slog.NewTextHandler(io.Discard, nil)))
	for id, balance := range want {
		if got, err := from.Balance(ctx, id); err != nil || got != balance {
			t.Errorf("sub-wallet %d restored with %d, %v, want %d", id, got, err, balance)
		}
	}
	if got, err := from.MainAvailable(ctx); err != nil || got != available {
		t.Errorf("main wallet restored as %d, %v, want %d", got, err, available)
	}
}
