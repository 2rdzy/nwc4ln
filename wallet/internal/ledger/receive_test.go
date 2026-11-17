package ledger

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/2rdzy/nwc4ln/wallet/internal/node"
	"github.com/2rdzy/nwc4ln/wallet/internal/node/fake"
)

// gatedInvoices holds every MakeInvoice at the node until released, so that many
// requests are inside the node call at once.
type gatedInvoices struct {
	*fake.Node
	entered chan struct{}
	release chan struct{}
}

func (g *gatedInvoices) MakeInvoice(ctx context.Context, request node.InvoiceRequest) (node.Invoice, error) {
	g.entered <- struct{}{}
	<-g.release
	return g.Node.MakeInvoice(ctx, request)
}

// Catches: requests running at once all passing the open-invoice cap, because the
// count was read only before the node call.
func TestInvoiceCapHoldsForRequestsAtOnce(t *testing.T) {
	e := newEnv(t)
	gated := &gatedInvoices{Node: e.node, entered: make(chan struct{}), release: make(chan struct{})}
	e.ledger = e.fresh(gated)
	e.ledger.maxOpenInvoices = 1
	connection := e.connection(e.acct)
	const requests = 8
	errs := make(chan error, requests)
	var wg sync.WaitGroup
	for range requests {
		wg.Go(func() {
			_, err := e.ledger.MakeInvoice(context.Background(), InvoiceRequest{
				AccountID: e.acct, ConnectionID: connection, AmountMsat: sat,
			})
			errs <- err
		})
	}
	for range requests {
		<-gated.entered
	}
	close(gated.release)
	wg.Wait()
	close(errs)
	made := 0
	for err := range errs {
		switch {
		case err == nil:
			made++
		case !errors.Is(err, ErrTooManyInvoices):
			t.Fatalf("unexpected error: %v", err)
		}
	}
	var rows int
	if err := e.store.DB.QueryRow(`SELECT count(*) FROM transactions WHERE type = 'incoming'`).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if made != 1 || rows != 1 {
		t.Fatalf("cap of 1: %d invoices given, %d rows stored", made, rows)
	}
}

// Catches: an invoice made for a connection of another account, or with none;
// and a receive-only connection refused, which WP19 needs.
func TestInvoiceConnectionCheck(t *testing.T) {
	e := newEnv(t)
	receiveOnly, err := e.store.DB.Exec(`INSERT INTO connections (account_id, client_pubkey, service_key, role, created_at)
		VALUES (?, 'receiver', 'key', 'receive_only', 0)`, e.acct)
	if err != nil {
		t.Fatal(err)
	}
	receiver, err := receiveOnly.LastInsertId()
	if err != nil {
		t.Fatal(err)
	}
	other, err := e.store.DB.Exec(`INSERT INTO accounts (type, name, can_pay, can_invoice, created_at)
		VALUES ('app', 'other', 1, 1, 0)`)
	if err != nil {
		t.Fatal(err)
	}
	otherAccount, err := other.LastInsertId()
	if err != nil {
		t.Fatal(err)
	}
	for name, c := range map[string]struct {
		connection int64
		want       error
	}{
		"full":              {e.connection(e.acct), nil},
		"receive-only":      {receiver, nil},
		"none":              {0, ErrConnectionRevoked},
		"another account's": {e.connection(otherAccount), ErrConnectionRevoked},
	} {
		_, err := e.ledger.MakeInvoice(context.Background(), InvoiceRequest{
			AccountID: e.acct, ConnectionID: c.connection, AmountMsat: sat,
		})
		if !errors.Is(err, c.want) {
			t.Errorf("%s: %v", name, err)
		}
	}
}
