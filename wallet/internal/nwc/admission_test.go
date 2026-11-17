package nwc

import (
	"context"
	"sync"
	"testing"

	"github.com/2rdzy/nwc4ln/internal/node"
)

// replacingNode replaces the connection once, inside the node call named by at. It
// lets a test put the replace at an exact point in a payment.
type replacingNode struct {
	node.Node
	at      string
	replace func()
	once    sync.Once
}

func (n *replacingNode) hook(call string) {
	if call == n.at {
		n.once.Do(n.replace)
	}
}

func (n *replacingNode) TrackPayment(ctx context.Context, hash string) (node.Payment, error) {
	n.hook("TrackPayment")
	return n.Node.TrackPayment(ctx, hash)
}

func (n *replacingNode) Spendable(ctx context.Context) (node.Spendable, error) {
	n.hook("Spendable")
	return n.Node.Spendable(ctx)
}

func (n *replacingNode) Pay(ctx context.Context, request node.PayRequest) (node.Payment, error) {
	n.hook("Pay")
	return n.Node.Pay(ctx, request)
}

// Catches: a connection replaced while its payment waits still pays (S04). Before the
// hold: in TrackPayment, which runs before the lock, and in Spendable, which an app
// reads under the lock but before the hold's transaction, so a check placed anywhere
// but in that transaction misses it. After the hold, in Pay: a payment already
// promised must not be dropped.
func TestReplaceDuringPayment(t *testing.T) {
	cases := []struct {
		at   string
		pays bool
	}{{"TrackPayment", false}, {"Spendable", false}, {"Pay", true}}
	for _, c := range cases {
		t.Run("replace in "+c.at, func(t *testing.T) {
			e := newEnv(t)
			wrapped := &replacingNode{Node: e.node, at: c.at}
			e.restartOn(wrapped)
			account := e.accountOf(e.client)
			replaced := false
			wrapped.replace = func() {
				if _, err := e.service.ReplaceConnection(t.Context(), account); err != nil {
					t.Errorf("replace: %v", err)
				}
				replaced = true
			}
			reply := e.call(e.client, "pay_invoice", map[string]any{"invoice": e.node.OutsideInvoice(sat)})
			if !replaced {
				t.Fatalf("the payment never called %s", c.at)
			}
			var rows int
			if err := e.store.DB.QueryRow(`SELECT count(*) FROM transactions WHERE type = 'outgoing'`).Scan(&rows); err != nil {
				t.Fatal(err)
			}
			sent := len(e.node.PayRequests())
			if !c.pays {
				if reply.Error == nil || reply.Error.Code != codeUnauthorized || rows != 0 || sent != 0 {
					t.Errorf("reply %+v, %d outgoing rows, %d payments sent to the node", reply.Error, rows, sent)
				}
				return
			}
			if reply.Error != nil || rows != 1 || sent != 1 {
				t.Errorf("reply %+v, %d outgoing rows, %d payments sent to the node", reply.Error, rows, sent)
			}
		})
	}
}
