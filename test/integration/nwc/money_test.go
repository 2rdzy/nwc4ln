//go:build integration

package nwc_test

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/2rdzy/nwc4ln/internal/nwc/nwctest"
	"github.com/2rdzy/nwc4ln/test/integration"
)

const other = integration.NodeLNDB

// replyWithin is a little over what the service waits, and under what clients wait.
const replyWithin = 59 * time.Second

// holdInvoice makes an invoice on lnd-b that holds a payment until told otherwise.
// A payment to it is a real payment in flight.
func holdInvoice(t *testing.T, n *integration.Network) (request, hash, preimage string) {
	t.Helper()
	secret := make([]byte, 32)
	if _, err := rand.Read(secret); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(secret)
	hash = hex.EncodeToString(sum[:])
	var reply struct {
		Request string `json:"payment_request"`
	}
	out, err := n.LNCLI(t.Context(), other, "addholdinvoice", hash, "--amt", "2000")
	if err != nil {
		t.Fatalf("addholdinvoice: %v: %s", err, out)
	}
	if err := json.Unmarshal(out, &reply); err != nil {
		t.Fatal(err)
	}
	return reply.Request, hash, hex.EncodeToString(secret)
}

// waitHeld waits until the payment has reached lnd-b. Before that, a stop tests only
// a crash before the node call.
func waitHeld(t *testing.T, n *integration.Network, hash string) {
	t.Helper()
	for deadline := time.Now().Add(30 * time.Second); time.Now().Before(deadline); time.Sleep(100 * time.Millisecond) {
		out, err := n.LNCLI(t.Context(), other, "lookupinvoice", hash)
		if err != nil {
			t.Fatalf("lookupinvoice: %v: %s", err, out)
		}
		var invoice struct {
			State string `json:"state"`
		}
		if err := json.Unmarshal(out, &invoice); err != nil {
			t.Fatal(err)
		}
		if invoice.State == "ACCEPTED" {
			return
		}
	}
	t.Fatal("the payment never reached lnd-b")
}

func settleHeld(t *testing.T, n *integration.Network, preimage string) {
	t.Helper()
	if out, err := n.LNCLI(t.Context(), other, "settleinvoice", preimage); err != nil {
		t.Fatalf("settleinvoice: %v: %s", err, out)
	}
}

type found struct {
	Type     string `json:"type"`
	State    string `json:"state"`
	Preimage string `json:"preimage"`
	Amount   int64  `json:"amount"`
}

// lookupUntil asks until the state arrives. A service just restarted may not be
// listening yet, and then the relay refuses the request or nothing answers.
func lookupUntil(t *testing.T, c *nwctest.Client, hash, want string) found {
	t.Helper()
	var last found
	var lastErr error
	params := map[string]string{"payment_hash": hash}
	for deadline := time.Now().Add(90 * time.Second); time.Now().Before(deadline); time.Sleep(500 * time.Millisecond) {
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
		response, err := c.CallWith(ctx, nwctest.NIP44, "lookup_invoice", params)
		cancel()
		if lastErr = err; err != nil {
			continue
		}
		if response.Error != nil {
			t.Fatalf("lookup_invoice: %+v", response.Error)
		}
		if err := json.Unmarshal(response.Result, &last); err != nil {
			t.Fatal(err)
		}
		if last.State == want {
			return last
		}
	}
	t.Fatalf("state stayed %+v (%v), want %s", last, lastErr, want)
	return last
}

func TestMoney(t *testing.T) {
	n, err := integration.Load(integration.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	s := startService(t, n)
	client, err := nwctest.Parse(s.connectionString)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	waitForInfo(t, client)

	// Catches: R12 failing on a real node. The payment goes on at the node while
	// nwc4ln is gone, and only the node knows how it ended.
	t.Run("KilledWithPaymentInFlight", func(t *testing.T) {
		request, hash, preimage := holdInvoice(t, n)
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
		defer cancel()
		_, err := client.CallWith(ctx, nwctest.NIP44, "pay_invoice", map[string]string{"invoice": request})
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("got a reply while the payment was held: %v", err)
		}
		waitHeld(t, n, hash)
		s.kill()
		settleHeld(t, n, preimage)
		s.start()
		got := lookupUntil(t, client, hash, "settled")
		if got.Type != "outgoing" || got.Preimage != preimage || got.Amount != 2_000_000 {
			t.Fatalf("got %+v", got)
		}
	})

	// Catches: missed income, for an invoice paid while nwc4ln is gone.
	t.Run("InvoicePaidWhileDown", func(t *testing.T) {
		response := call(t, client, nwctest.NIP44, "get_info")
		if response.Error != nil {
			t.Fatalf("get_info: %+v", response.Error)
		}
		ctx, cancel := context.WithTimeout(t.Context(), callTimeout)
		defer cancel()
		response, err := client.CallWith(ctx, nwctest.NIP44, "make_invoice", map[string]int{"amount": 3_000_000})
		if err != nil || response.Error != nil {
			t.Fatalf("make_invoice: %v %+v", err, response)
		}
		var invoice struct {
			Invoice string `json:"invoice"`
			Hash    string `json:"payment_hash"`
		}
		if err := json.Unmarshal(response.Result, &invoice); err != nil {
			t.Fatal(err)
		}
		s.kill()
		if out, err := n.LNCLI(t.Context(), other, "payinvoice", "--force", "--json", invoice.Invoice); err != nil {
			t.Fatalf("payinvoice: %v: %s", err, out)
		}
		s.start()
		if got := lookupUntil(t, client, invoice.Hash, "settled"); got.Type != "incoming" || got.Amount != 3_000_000 {
			t.Fatalf("got %+v", got)
		}
	})

	// Catches: a real payment still in flight reading as failed when the wait ends,
	// which makes a client pay again. Also a reply too late for clients to hear it.
	t.Run("SlowPayment", func(t *testing.T) {
		request, hash, preimage := holdInvoice(t, n)
		ready := call(t, client, nwctest.NIP44, "get_info")
		if ready.Error != nil {
			t.Fatalf("get_info: %+v", ready.Error)
		}
		ctx, cancel := context.WithTimeout(t.Context(), replyWithin)
		defer cancel()
		began := time.Now()
		response, err := client.CallWith(ctx, nwctest.NIP44, "pay_invoice", map[string]string{"invoice": request})
		if err != nil {
			t.Fatalf("no reply in %s: %v", time.Since(began), err)
		}
		if response.Error == nil || response.Error.Code != "OTHER" || response.Error.Message != "payment pending" {
			t.Fatalf("got %+v", response)
		}
		if got := lookupUntil(t, client, hash, "pending"); got.Preimage != "" {
			t.Fatalf("got %+v", got)
		}
		settleHeld(t, n, preimage)
		if got := lookupUntil(t, client, hash, "settled"); got.Preimage != preimage {
			t.Fatalf("got %+v", got)
		}
	})
}
