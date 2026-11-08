//go:build integration

package nwc_test

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"testing"
	"time"

	"github.com/2rdzy/nwc4ln/internal/ledger"
	"github.com/2rdzy/nwc4ln/test/integration"
)

// heldHTLCs counts the node's outgoing HTLCs for a hash.
func heldHTLCs(t *testing.T, n *integration.Network, hash string) int {
	t.Helper()
	if integration.IsCLN(self) {
		return clnHeldHTLCs(t, n, hash)
	}
	out, err := n.LNCLI(t.Context(), self, "listchannels")
	if err != nil {
		t.Fatalf("listchannels: %v: %s", err, out)
	}
	var list struct {
		Channels []struct {
			HTLCs []struct {
				Incoming bool   `json:"incoming"`
				HashLock string `json:"hash_lock"`
			} `json:"pending_htlcs"`
		} `json:"channels"`
	}
	if err := json.Unmarshal(out, &list); err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, channel := range list.Channels {
		for _, htlc := range channel.HTLCs {
			if !htlc.Incoming && htlc.HashLock == hash {
				count++
			}
		}
	}
	return count
}

// steadyAvailable reads the main wallet once the node's figure has stopped moving.
// The payments waitReady makes can still be settling into the channel balance after
// they return: on Core Lightning a figure read then rose by 1 sat a moment later.
func steadyAvailable(t *testing.T, money *ledger.Ledger) int64 {
	t.Helper()
	last := int64(-1)
	for deadline := time.Now().Add(20 * time.Second); time.Now().Before(deadline); time.Sleep(500 * time.Millisecond) {
		got, err := money.MainAvailable(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if got == last {
			return got
		}
		last = got
	}
	t.Fatal("the node's figure kept moving")
	return 0
}

func shortfall(t *testing.T, money *ledger.Ledger) int64 {
	t.Helper()
	got, err := money.Shortfall(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	return got
}

// Catches: a payment in flight counted twice on a real node, once by LND's local
// balance and once as its hold, which shows a false shortfall (R14) on normal traffic.
// The node is fully allocated, so any error shows: all of the main wallet is moved into
// a sub-wallet, which then pays an invoice that lnd-b holds.
func TestInFlightRaisesNoShortfall(t *testing.T) {
	n, err := integration.Load(integration.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	if err := waitReady(ctx, n); err != nil {
		t.Fatal(err)
	}
	money, service := inProcess(t, ctx, n)
	if _, err := service.AddSubWallet(ctx, "holder"); err != nil {
		t.Fatal(err)
	}
	wallets, err := money.SubWallets(ctx)
	if err != nil || len(wallets) != 1 {
		t.Fatalf("sub-wallets %+v, %v", wallets, err)
	}
	wallet := wallets[0].ID
	available := steadyAvailable(t, money)
	if row, err := money.Fund(ctx, wallet, available); err != nil || row.State != ledger.StateSettled {
		t.Fatalf("fund %+v, %v", row, err)
	}
	if got := shortfall(t, money); got != 0 {
		t.Fatalf("shortfall %d with the node fully allocated", got)
	}

	secret := make([]byte, 32)
	if _, err := rand.Read(secret); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(secret)
	hash := hex.EncodeToString(sum[:])
	var invoice struct {
		Request string `json:"payment_request"`
	}
	out, err := n.LNCLI(ctx, other, "addholdinvoice", hash, "--amt", "20000")
	if err != nil || json.Unmarshal(out, &invoice) != nil {
		t.Fatalf("addholdinvoice: %v: %s", err, out)
	}
	paid := make(chan ledger.Transaction, 1)
	go func() {
		row, err := money.Pay(ctx, ledger.PayRequest{AccountID: wallet, Invoice: invoice.Request})
		if err != nil {
			t.Error(err)
		}
		paid <- row
	}()
	for deadline := time.Now().Add(30 * time.Second); heldHTLCs(t, n, hash) == 0; time.Sleep(100 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("the payment never reached lnd-b")
		}
	}
	if got := shortfall(t, money); got != 0 {
		t.Errorf("shortfall %d with the payment in flight", got)
	}
	if got, err := money.MainAvailable(ctx); err != nil || got != 0 {
		t.Errorf("main wallet %d, %v with the payment in flight", got, err)
	}

	if out, err := n.LNCLI(ctx, other, "cancelinvoice", hash); err != nil {
		t.Fatalf("cancelinvoice: %v: %s", err, out)
	}
	select {
	case row := <-paid:
		if row.State != ledger.StateFailed {
			t.Fatalf("payment %+v", row)
		}
	case <-time.After(60 * time.Second):
		t.Fatal("the payment did not end")
	}
	if got := shortfall(t, money); got != 0 {
		t.Fatalf("shortfall %d after the payment failed", got)
	}
}
