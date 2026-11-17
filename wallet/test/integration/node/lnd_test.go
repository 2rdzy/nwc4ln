//go:build integration

// Package node_test runs the LND client against the regtest network, with a
// macaroon baked from lnd.Methods and never admin.macaroon.
package node_test

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/2rdzy/nwc4ln/internal/node"
	"github.com/2rdzy/nwc4ln/internal/node/lnd"
	"github.com/2rdzy/nwc4ln/internal/node/nodetest"
	"github.com/2rdzy/nwc4ln/test/integration"
)

const (
	self  = integration.NodeLNDA
	other = integration.NodeLNDB
)

// calls records the gRPC methods a client uses.
type calls struct {
	mu   sync.Mutex
	seen map[string]bool
}

func (c *calls) add(method string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.seen[method] = true
}

func (c *calls) options() []grpc.DialOption {
	return []grpc.DialOption{
		grpc.WithUnaryInterceptor(func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn,
			invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
			c.add(method)
			return invoker(ctx, method, req, reply, cc, opts...)
		}),
		grpc.WithStreamInterceptor(func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string,
			streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
			c.add(method)
			return streamer(ctx, desc, cc, method, opts...)
		}),
	}
}

func bake(t *testing.T, n *integration.Network, name string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "nwc4ln.macaroon")
	args := []string{"bakemacaroon", "--save_to", path}
	for _, method := range lnd.Methods {
		args = append(args, "uri:"+method)
	}
	if _, err := n.LNCLI(t.Context(), name, args...); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0o600); err != nil { // lncli writes it readable by all.
		t.Fatal(err)
	}
	return path
}

func client(t *testing.T, n *integration.Network, name, macaroon string, options ...grpc.DialOption) *lnd.Client {
	t.Helper()
	c, err := lnd.New(lnd.Config{
		Host:         n.LNDHost(name),
		TLSCertPath:  filepath.Join(n.LNDDir(name), "tls.cert"),
		MacaroonPath: macaroon,
	}, options...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func network(t *testing.T) *integration.Network {
	t.Helper()
	n, err := integration.Load(integration.DefaultConfig())
	if err != nil {
		t.Fatal(err) // A skip here would look like a pass.
	}
	return n
}

func lncliJSON(t *testing.T, n *integration.Network, name string, out any, args ...string) {
	t.Helper()
	output, err := n.LNCLI(t.Context(), name, args...)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(output, out); err != nil {
		t.Fatal(err)
	}
}

func outsideInvoice(t *testing.T, n *integration.Network, amountMsat int64) (request, hash string) {
	t.Helper()
	var reply struct {
		Request string `json:"payment_request"`
		Hash    string `json:"r_hash"`
	}
	lncliJSON(t, n, other, &reply, "addinvoice", "--amt_msat", strconv.FormatInt(amountMsat, 10))
	return reply.Request, reply.Hash
}

func harness(n *integration.Network, c node.Node) nodetest.Harness {
	return nodetest.Harness{
		Node: c,
		OutsideInvoice: func(t *testing.T, amountMsat int64) string {
			request, _ := outsideInvoice(t, n, amountMsat)
			return request
		},
		StartHeld: func(t *testing.T, amountMsat int64) (string, func()) {
			hash, _ := startHeldSats(t, n, c, int(amountMsat/1000))
			return hash, func() {
				if _, err := n.LNCLI(t.Context(), other, "cancelinvoice", hash); err != nil {
					t.Error(err)
				}
			}
		},
		PayFromOutside: func(t *testing.T, invoice node.Invoice) {
			// lncli reports a failed payment in its output and also exits non-zero.
			if _, err := n.LNCLI(t.Context(), other, "payinvoice", "--force", "--json", "--timeout", "30s",
				invoice.PaymentRequest); err != nil {
				t.Fatal(err)
			}
		},
	}
}

func holdInvoice(t *testing.T, n *integration.Network, sats int) (request, hash, preimage string) {
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
	lncliJSON(t, n, other, &reply, "addholdinvoice", hash, "--amt", strconv.Itoa(sats))
	return reply.Request, hash, hex.EncodeToString(secret)
}

// startHeld pays a hold invoice, gives up waiting while the payment is held at
// the payee, and returns its hash. This is a timeout on a real node.
func startHeld(t *testing.T, n *integration.Network, c node.Node) (hash, preimage string) {
	t.Helper()
	return startHeldSats(t, n, c, 2_000)
}

func startHeldSats(t *testing.T, n *integration.Network, c node.Node, sats int) (hash, preimage string) {
	t.Helper()
	request, hash, preimage := holdInvoice(t, n, sats)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	payment, err := c.Pay(ctx, node.PayRequest{Invoice: request, FeeLimitMsat: 10_000, Timeout: 30 * time.Second})
	if err == nil {
		t.Fatalf("Pay finished while the payee held it: %+v", payment)
	}
	return hash, preimage
}

func waitState(t *testing.T, c node.Node, hash string, want node.PaymentState) node.Payment {
	t.Helper()
	var last node.Payment
	for deadline := time.Now().Add(30 * time.Second); time.Now().Before(deadline); time.Sleep(200 * time.Millisecond) {
		payment, err := c.TrackPayment(t.Context(), hash)
		if err != nil {
			t.Fatal(err)
		}
		if payment.State == want {
			return payment
		}
		last = payment
	}
	t.Fatalf("payment stayed %+v, want state %d", last, want)
	return last
}

// Catches: funds released for a paid invoice (R8): after a timeout the payment
// must read in flight, then succeed.
func testTimeoutThenSuccess(t *testing.T, n *integration.Network, c node.Node) {
	hash, preimage := startHeld(t, n, c)
	if got := waitState(t, c, hash, node.PaymentInFlight); got.Hash != hash {
		t.Fatalf("got %+v", got)
	}
	if _, err := n.LNCLI(t.Context(), other, "settleinvoice", preimage); err != nil {
		t.Fatal(err)
	}
	if got := waitState(t, c, hash, node.PaymentSucceeded); got.Preimage != preimage {
		t.Fatalf("preimage %q, want %q", got.Preimage, preimage)
	}
}

// Catches: funds stuck forever (R8): after a timeout the failure must arrive.
func testTimeoutThenFailure(t *testing.T, n *integration.Network, c node.Node) {
	hash, _ := startHeld(t, n, c)
	waitState(t, c, hash, node.PaymentInFlight)
	if _, err := n.LNCLI(t.Context(), other, "cancelinvoice", hash); err != nil {
		t.Fatal(err)
	}
	if got := waitState(t, c, hash, node.PaymentFailed); got.FailureReason == "" {
		t.Fatalf("got %+v", got)
	}
}

// Catches: R10 double credit. One part of a multi-part payment is held on an open
// invoice. Cancel must succeed, fail the part back, and leave nothing received.
func testCancelWithAcceptedPart(t *testing.T, n *integration.Network, c node.Node) {
	ctx := t.Context()
	invoice, err := c.MakeInvoice(ctx, node.InvoiceRequest{AmountMsat: 10_000_000, Description: "parts"})
	if err != nil {
		t.Fatal(err)
	}
	var added struct {
		Address string `json:"payment_addr"`
	}
	lncliJSON(t, n, self, &added, "lookupinvoice", invoice.Hash)
	info, err := c.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var built map[string]any
	lncliJSON(t, n, other, &built, "buildroute", "--amt", "4000", "--hops", info.PubKey,
		"--payment_addr", added.Address)
	hops := built["route"].(map[string]any)["hops"].([]any)
	hops[len(hops)-1].(map[string]any)["mpp_record"].(map[string]any)["total_amt_msat"] = "10000000"
	route, err := json.Marshal(built)
	if err != nil {
		t.Fatal(err)
	}
	sent := make(chan []byte, 1)
	go func() {
		// A background context: this returns only when the part is failed back.
		output, _ := n.LNCLI(context.Background(), other, "sendtoroute", "--payment_hash", invoice.Hash,
			"-r", string(route))
		sent <- output
	}()
	var held bool
	for deadline := time.Now().Add(20 * time.Second); !held && time.Now().Before(deadline); {
		var state struct {
			HTLCs []struct {
				State string `json:"state"`
			} `json:"htlcs"`
		}
		lncliJSON(t, n, self, &state, "lookupinvoice", invoice.Hash)
		held = len(state.HTLCs) == 1 && state.HTLCs[0].State == "ACCEPTED"
		time.Sleep(100 * time.Millisecond)
	}
	if !held {
		t.Fatal("the part was never held")
	}
	if err := c.CancelInvoice(ctx, invoice.Hash); err != nil {
		t.Fatalf("cancel with an accepted part: %v", err)
	}
	found, err := c.LookupInvoice(ctx, invoice.Hash)
	if err != nil {
		t.Fatal(err)
	}
	if found.State != node.InvoiceCanceled || found.PaidMsat != 0 {
		t.Fatalf("after cancel: %+v", found)
	}
	select {
	case output := <-sent:
		if !strings.Contains(string(output), `"FAILED"`) {
			t.Fatalf("sender did not see a failure: %s", output)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("the part did not come back to its sender")
	}
}

// Catches: missing permission (a method below fails) or excess permission.
func testExcessDenied(t *testing.T, n *integration.Network, macaroon string) {
	for _, args := range [][]string{
		{"walletbalance"}, {"newaddress", "p2wkh"}, {"listpayments"}, {"listinvoices"}, {"listpeers"},
		{"sendcoins", "--addr", "bcrt1qw508d6qejxtdg4y5r3zarvary0c5xw7kygt080", "--amt", "1000", "--force"},
		{"bakemacaroon", "info:read"},
	} {
		_, err := n.LNCLI(t.Context(), self, append([]string{"--macaroonpath", macaroon}, args...)...)
		if err == nil || !strings.Contains(err.Error(), "permission denied") {
			t.Errorf("%s: %v, want permission denied", args[0], err)
		}
	}
}

// Catches: a connection error read as "no record", which releases funds wrongly
// (R12); offline funds counted as sendable (R5); offline funds dropped from the
// shortfall figure (R14); a subscription that goes quiet when the node stops; a
// client that never reconnects.
func testNodeStopped(t *testing.T, n *integration.Network, c node.Node) {
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	peer := client(t, n, other, bake(t, n, other))
	if _, err := peer.Info(ctx); err != nil {
		t.Fatal(err)
	}
	before, err := c.Spendable(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if before.ActiveMsat <= 0 || before.ActiveMsat != before.AllMsat {
		t.Fatalf("before stop: %+v", before)
	}
	stream := make(chan error, 1)
	go func() {
		stream <- peer.SubscribeInvoices(ctx, 0, func(node.Invoice) error { return nil })
	}()
	time.Sleep(500 * time.Millisecond) // Let the stream open, so its end is the stop.

	if err := n.StopNode(other); err != nil {
		t.Fatal(err)
	}
	started := false
	t.Cleanup(func() {
		ctx := context.Background() // The test's own context has ended here.
		if !started {
			if err := n.StartNode(ctx, other); err != nil {
				t.Error(err)
			}
		}
		if err := n.WaitChannels(ctx); err != nil {
			t.Error(err)
		}
	})
	select {
	case err := <-stream:
		if err == nil {
			t.Error("subscription ended with no error")
		}
	case <-time.After(30 * time.Second):
		t.Error("subscription stayed open with the node stopped")
	}
	payment, err := peer.TrackPayment(ctx, strings.Repeat("ab", 32))
	if err == nil {
		t.Errorf("track with the node stopped gave %+v, want an error", payment)
	}
	if payment.State == node.PaymentNoRecord {
		t.Error("a connection error came with PaymentNoRecord")
	}
	var after node.Spendable
	for deadline := time.Now().Add(30 * time.Second); time.Now().Before(deadline); time.Sleep(200 * time.Millisecond) {
		if after, err = c.Spendable(ctx); err != nil {
			t.Fatal(err)
		}
		if after.ActiveMsat == 0 {
			break
		}
	}
	if after.ActiveMsat != 0 || after.AllMsat != before.AllMsat {
		t.Errorf("with the peer stopped: %+v, before %+v", after, before)
	}
	if err := n.StartNode(ctx, other); err != nil {
		t.Fatal(err)
	}
	started = true
	// gRPC waits between attempts, so the first calls can still fail.
	for deadline := time.Now().Add(30 * time.Second); ; time.Sleep(200 * time.Millisecond) {
		_, err := peer.Info(ctx)
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("client did not reconnect: %v", err)
		}
	}
}

func TestLND(t *testing.T) {
	n := network(t)
	macaroon := bake(t, n, self)
	recorded := &calls{seen: map[string]bool{}}
	c := client(t, n, self, macaroon, recorded.options()...)

	t.Run("Behaviour", func(t *testing.T) { nodetest.Run(t, harness(n, c)) })
	t.Run("TimeoutThenSuccess", func(t *testing.T) { testTimeoutThenSuccess(t, n, c) })
	t.Run("TimeoutThenFailure", func(t *testing.T) { testTimeoutThenFailure(t, n, c) })
	t.Run("CancelWithAcceptedPart", func(t *testing.T) { testCancelWithAcceptedPart(t, n, c) })
	t.Run("ExcessDenied", func(t *testing.T) { testExcessDenied(t, n, macaroon) })
	t.Run("NodeStopped", func(t *testing.T) { testNodeStopped(t, n, c) })

	// Catches: a method in the list that nothing uses, or a call that only the
	// list's completeness let through.
	t.Run("MethodsMatchCalls", func(t *testing.T) {
		var used []string
		for method := range recorded.seen {
			used = append(used, method)
		}
		slices.Sort(used)
		want := slices.Sorted(slices.Values(lnd.Methods))
		if !slices.Equal(used, want) {
			t.Fatalf("called %v\nlisted %v", used, want)
		}
	})
}
