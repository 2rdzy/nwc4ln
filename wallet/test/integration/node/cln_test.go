//go:build integration

package node_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/2rdzy/nwc4ln/internal/node"
	"github.com/2rdzy/nwc4ln/internal/node/cln"
	"github.com/2rdzy/nwc4ln/internal/node/nodetest"
	"github.com/2rdzy/nwc4ln/test/integration"
)

const selfCLN = integration.NodeCLNA

// makeRune creates a rune on cln-a that allows only cln.Methods, and saves it
// with mode 0600, as an operator would.
func makeRune(t *testing.T, n *integration.Network) string {
	t.Helper()
	alternatives := make([]string, len(cln.Methods))
	for i, method := range cln.Methods {
		alternatives[i] = "method=" + method
	}
	restrictions, err := json.Marshal([][]string{alternatives})
	if err != nil {
		t.Fatal(err)
	}
	output, err := n.CLNCLI(t.Context(), selfCLN, "-k", "createrune", "restrictions="+string(restrictions))
	if err != nil {
		t.Fatal(err)
	}
	var created struct {
		Rune string `json:"rune"`
	}
	if err := json.Unmarshal(output, &created); err != nil || created.Rune == "" {
		t.Fatalf("createrune: %v: %s", err, output)
	}
	path := filepath.Join(t.TempDir(), "nwc4ln.rune")
	if err := os.WriteFile(path, []byte(created.Rune), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

type methodLog struct {
	mu   sync.Mutex
	seen map[string]bool
}

func (m *methodLog) add(method string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.seen[method] = true
}

func clnClient(t *testing.T, n *integration.Network, runePath string, options ...cln.Option) *cln.Client {
	t.Helper()
	c, err := cln.New(cln.Config{URL: n.CLNURL(selfCLN), CACertPath: n.CLNCACert(selfCLN), RunePath: runePath}, options...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func clnHarness(n *integration.Network, c node.Node) nodetest.Harness {
	h := harness(n, c)
	h.CancelDeletes, h.LookupHidesPreimage, h.ReplaysFromZero, h.NoBlockHash = true, true, true, true
	h.PayFailureNotFinal = true
	return h
}

// Catches: a commitment fee, anchor or in-flight amount counted differently from
// LND (R5, R14). The peer's view of the same channels is the reference.
func testSpendableMatchesPeer(t *testing.T, n *integration.Network, c node.Node) {
	info, err := c.Info(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	for deadline := time.Now().Add(20 * time.Second); ; time.Sleep(200 * time.Millisecond) {
		var channels struct {
			Channels []struct {
				Remote      string `json:"remote_pubkey"`
				Balance     int64  `json:"remote_balance,string"`
				Constraints struct {
					Reserve int64 `json:"chan_reserve_sat,string"`
				} `json:"remote_constraints"`
			} `json:"channels"`
		}
		lncliJSON(t, n, other, &channels, "listchannels")
		var wantSat int64
		for _, ch := range channels.Channels {
			if ch.Remote == info.PubKey {
				wantSat += max(ch.Balance-ch.Constraints.Reserve, 0)
			}
		}
		got, err := c.Spendable(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		// lnd-b shows whole sats per channel, and rounds its fee its own way, so
		// allow under 1 sat per channel either side.
		if diff := got.ActiveMsat - wantSat*1000; wantSat > 0 && diff > -2000 && diff < 2000 && got.AllMsat == got.ActiveMsat {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("cln-a figure %+v, lnd-b's view %d sat", got, wantSat)
		}
	}
}

// Catches: an HTLC cost that differs from what left the figure on a channel cln-a
// opened, where the commitment fee applies, which shows as a false shortfall (R14).
// 100,000 sat is more than cln-a has on lnd-b's channel, so part of it must go over
// its own. nodetest's flight test may not.
func testFlightOverOwnChannel(t *testing.T, n *integration.Network, c node.Node) {
	before := steadySpendable(t, c)
	hash, _ := startHeldSats(t, n, c, 100_000)
	t.Cleanup(func() { _, _ = n.LNCLI(context.Background(), other, "cancelinvoice", hash) })
	for deadline := time.Now().Add(20 * time.Second); ; time.Sleep(200 * time.Millisecond) {
		during, err := c.Spendable(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		var cost, amount int64
		for _, htlc := range during.Outgoing {
			if htlc.Hash == hash {
				cost += htlc.CostMsat
				amount += htlc.AmountMsat
			}
		}
		// The figures settle once both sides have signed the HTLC, as in nodetest.
		drop := before.ActiveMsat - during.ActiveMsat
		if amount == 100_000_000 && cost > amount && drop == cost && ownChannelHolds(t, n, hash) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("figure dropped %d, HTLCs cost %d for %d: %+v", drop, cost, amount, during)
		}
	}
}

// steadySpendable reads the figure once it has stopped moving. Income from the
// tests before can still be settling into a channel: a 21 sat payment once moved it
// between two reads.
func steadySpendable(t *testing.T, c node.Node) node.Spendable {
	t.Helper()
	var last node.Spendable
	for deadline := time.Now().Add(20 * time.Second); time.Now().Before(deadline); time.Sleep(500 * time.Millisecond) {
		got, err := c.Spendable(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if got.ActiveMsat == last.ActiveMsat && got.AllMsat == last.AllMsat && len(got.Outgoing) == 0 {
			return got
		}
		last = got
	}
	t.Fatalf("the figure kept moving: %+v", last)
	return last
}

// ownChannelHolds says whether a channel cln-a opened has an outgoing HTLC for hash.
func ownChannelHolds(t *testing.T, n *integration.Network, hash string) bool {
	t.Helper()
	output, err := n.CLNCLI(t.Context(), selfCLN, "listpeerchannels")
	if err != nil {
		t.Fatal(err)
	}
	var list struct {
		Channels []struct {
			Opener string `json:"opener"`
			HTLCs  []struct {
				Direction string `json:"direction"`
				Hash      string `json:"payment_hash"`
			} `json:"htlcs"`
		} `json:"channels"`
	}
	if err := json.Unmarshal(output, &list); err != nil {
		t.Fatal(err)
	}
	for _, ch := range list.Channels {
		for _, htlc := range ch.HTLCs {
			if ch.Opener == "local" && htlc.Direction == "out" && htlc.Hash == hash {
				return true
			}
		}
	}
	return false
}

// Catches: R8 on Core Lightning. A held payment must stay in flight, then succeed
// with the payee's preimage.
func testCLNTimeoutThenSuccess(t *testing.T, n *integration.Network, c node.Node) {
	hash, preimage := startHeld(t, n, c)
	waitState(t, c, hash, node.PaymentInFlight)
	if _, err := n.LNCLI(t.Context(), other, "settleinvoice", preimage); err != nil {
		t.Fatal(err)
	}
	if got := waitState(t, c, hash, node.PaymentSucceeded); got.Preimage != preimage {
		t.Fatalf("preimage %q, want %q", got.Preimage, preimage)
	}
}

// Catches: a hold released while xpay may still start a new attempt, and one
// stuck forever. After the payee fails it, TrackPayment reads in flight until no
// attempt can come, and failed after that.
func testCLNTimeoutThenFailure(t *testing.T, n *integration.Network, c node.Node, runePath string) {
	hash, _ := startHeld(t, n, c)
	waitState(t, c, hash, node.PaymentInFlight)
	if _, err := n.LNCLI(t.Context(), other, "cancelinvoice", hash); err != nil {
		t.Fatal(err)
	}
	time.Sleep(3 * time.Second) // The failure reaches cln-a.
	if got, err := c.TrackPayment(t.Context(), hash); err != nil || got.State != node.PaymentInFlight {
		t.Fatalf("right after the failure: %+v, %v; want in flight", got, err)
	}
	later := clnClient(t, n, runePath, cln.WithClock(func() time.Time { return time.Now().Add(3 * time.Minute) }))
	if got := waitState(t, later, hash, node.PaymentFailed); got.FailureReason == "" {
		t.Fatalf("got %+v", got)
	}
}

// Catches: a hold released while xpay may still record a part, and one stuck for
// ever. When the payee fails the payment while Pay still waits, Pay gives an error,
// never a final failure: TrackPayment reads in flight until no attempt can come, and
// failed after that (SR1).
func testCLNPayFails(t *testing.T, n *integration.Network, c node.Node, runePath string) {
	request, hash, _ := holdInvoice(t, n, 2_000)
	done := make(chan struct{})
	var payment node.Payment
	var err error
	go func() {
		defer close(done)
		payment, err = c.Pay(t.Context(), node.PayRequest{Invoice: request, FeeLimitMsat: 10_000, Timeout: 5 * time.Second})
	}()
	waitState(t, c, hash, node.PaymentInFlight)
	if _, err := n.LNCLI(t.Context(), other, "cancelinvoice", hash); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(time.Minute):
		t.Fatal("Pay did not return")
	}
	if err == nil {
		t.Fatalf("got %+v with no error; want an error", payment)
	}
	if got, err := c.TrackPayment(t.Context(), hash); err != nil || got.State != node.PaymentInFlight {
		t.Fatalf("right after the failure: %+v, %v; want in flight", got, err)
	}
	later := clnClient(t, n, runePath, cln.WithClock(func() time.Time { return time.Now().Add(3 * time.Minute) }))
	waitState(t, later, hash, node.PaymentFailed)
}

// Catches: R10 double credit with part of a payment held on the invoice. The
// cancel must delete the invoice, nothing may settle it, and the part must go
// back to its sender.
func testCLNCancelWithHeldPart(t *testing.T, n *integration.Network, c node.Node) {
	ctx := t.Context()
	invoice, err := c.MakeInvoice(ctx, node.InvoiceRequest{AmountMsat: 10_000_000, Description: "parts"})
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := n.CLNCLI(ctx, selfCLN, "decode", invoice.PaymentRequest)
	if err != nil {
		t.Fatal(err)
	}
	var secret struct {
		Secret string `json:"payment_secret"`
	}
	if err := json.Unmarshal(decoded, &secret); err != nil || secret.Secret == "" {
		t.Fatalf("decode: %v: %s", err, decoded)
	}
	info, err := c.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var built map[string]any
	lncliJSON(t, n, other, &built, "buildroute", "--amt", "4000", "--hops", info.PubKey, "--payment_addr", secret.Secret)
	hops := built["route"].(map[string]any)["hops"].([]any)
	hops[len(hops)-1].(map[string]any)["mpp_record"].(map[string]any)["total_amt_msat"] = "10000000"
	route, err := json.Marshal(built)
	if err != nil {
		t.Fatal(err)
	}
	before := toUs(t, n)
	go func() {
		// Its answer comes after lncli's time limit, so the channels are read instead.
		_, _ = n.LNCLI(context.Background(), other, "sendtoroute", "--payment_hash", invoice.Hash, "-r", string(route))
	}()
	var held bool
	for deadline := time.Now().Add(20 * time.Second); !held && time.Now().Before(deadline); time.Sleep(100 * time.Millisecond) {
		output, err := n.CLNCLI(ctx, selfCLN, "listpeerchannels")
		if err != nil {
			t.Fatal(err)
		}
		held = strings.Contains(string(output), invoice.Hash)
	}
	if !held {
		t.Fatal("the part was never held")
	}
	if err := c.CancelInvoice(ctx, invoice.Hash); err != nil {
		t.Fatalf("cancel with a held part: %v", err)
	}
	// Core Lightning holds a part for up to 60 seconds, then fails it back.
	for deadline := time.Now().Add(2 * time.Minute); ; time.Sleep(time.Second) {
		output, err := n.CLNCLI(ctx, selfCLN, "listpeerchannels")
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(output), invoice.Hash) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the part was never failed back")
		}
	}
	if after := toUs(t, n); after != before {
		t.Fatalf("cln-a holds %d msat, %d before: the part was settled", after, before)
	}
	if found, err := c.LookupInvoice(ctx, invoice.Hash); err == nil {
		t.Fatalf("invoice still there after cancel: %+v", found)
	}
}

// Catches: excess permission: the rune must refuse every method not on the list.
func testRuneRefusesOthers(t *testing.T, n *integration.Network, runePath string) {
	rune, err := os.ReadFile(runePath)
	if err != nil {
		t.Fatal(err)
	}
	pem, err := os.ReadFile(n.CLNCACert(selfCLN))
	if err != nil {
		t.Fatal(err)
	}
	client := clnTestHTTP(t, pem)
	for _, method := range []string{"listfunds", "withdraw", "newaddr", "pay", "keysend", "createrune", "listpeers",
		"close", "fundchannel", "sendpay", "injectpaymentonion", "listpays", "stop"} {
		request, err := http.NewRequestWithContext(t.Context(), http.MethodPost, n.CLNURL(selfCLN)+"/v1/"+method,
			strings.NewReader("{}"))
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Rune", string(rune))
		response, err := client.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		var reply struct {
			Code int `json:"code"`
		}
		_ = json.NewDecoder(response.Body).Decode(&reply)
		response.Body.Close()
		if reply.Code != 1502 {
			t.Errorf("%s: code %d, HTTP %d, want 1502 Not permitted", method, reply.Code, response.StatusCode)
		}
	}
}

// Catches: a connection error read as "no record" (R12), offline funds counted as
// sendable (R5) or dropped from the shortfall figure (R14), and a stream that goes
// quiet when the node stops.
func testCLNStopped(t *testing.T, n *integration.Network, c node.Node) {
	ctx, cancel := context.WithTimeout(t.Context(), 4*time.Minute)
	defer cancel()
	t.Cleanup(func() {
		ctx := context.Background()
		for _, name := range []string{other, selfCLN} {
			if !n.Running(name) {
				if err := n.StartNode(ctx, name); err != nil {
					t.Error(err)
				}
			}
		}
		if err := n.WaitCLN(ctx); err != nil {
			t.Error(err)
		}
		if err := n.WaitChannels(ctx); err != nil {
			t.Error(err)
		}
	})
	before, err := c.Spendable(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := n.StopNode(other); err != nil {
		t.Fatal(err)
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

	stream := make(chan error, 1)
	go func() { stream <- c.SubscribeInvoices(ctx, 1<<40, func(node.Invoice) error { return nil }) }()
	time.Sleep(500 * time.Millisecond)
	if err := n.StopNode(selfCLN); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-stream:
		if err == nil {
			t.Error("stream ended with no error")
		}
	case <-time.After(30 * time.Second):
		t.Error("stream stayed open with the node stopped")
	}
	payment, err := c.TrackPayment(ctx, strings.Repeat("ab", 32))
	if err == nil || payment.State == node.PaymentNoRecord {
		t.Errorf("track with the node stopped: %+v, %v; want an error", payment, err)
	}
	if err := n.StartNode(ctx, selfCLN); err != nil {
		t.Fatal(err)
	}
	for deadline := time.Now().Add(30 * time.Second); ; time.Sleep(200 * time.Millisecond) {
		if _, err := c.Info(ctx); err == nil {
			break
		} else if time.Now().After(deadline) {
			t.Fatalf("client did not reconnect: %v", err)
		}
	}
}

func TestCLN(t *testing.T) {
	n := network(t)
	runePath := makeRune(t, n)
	recorded := &methodLog{seen: map[string]bool{}}
	c := clnClient(t, n, runePath, cln.OnCall(recorded.add))

	t.Run("SpendableMatchesPeer", func(t *testing.T) { testSpendableMatchesPeer(t, n, c) })
	t.Run("Behaviour", func(t *testing.T) { nodetest.Run(t, clnHarness(n, c)) })
	t.Run("FlightOverOwnChannel", func(t *testing.T) { testFlightOverOwnChannel(t, n, c) })
	t.Run("TimeoutThenSuccess", func(t *testing.T) { testCLNTimeoutThenSuccess(t, n, c) })
	t.Run("TimeoutThenFailure", func(t *testing.T) { testCLNTimeoutThenFailure(t, n, c, runePath) })
	t.Run("PayFails", func(t *testing.T) { testCLNPayFails(t, n, c, runePath) })
	t.Run("CancelWithHeldPart", func(t *testing.T) { testCLNCancelWithHeldPart(t, n, c) })
	t.Run("RuneRefusesOthers", func(t *testing.T) { testRuneRefusesOthers(t, n, runePath) })
	t.Run("Stopped", func(t *testing.T) { testCLNStopped(t, n, c) })

	// Catches: a method in the rune that nothing uses, or a call it would refuse.
	t.Run("MethodsMatchCalls", func(t *testing.T) {
		recorded.mu.Lock()
		used := slices.Sorted(func(yield func(string) bool) {
			for method := range recorded.seen {
				if !yield(method) {
					return
				}
			}
		})
		recorded.mu.Unlock()
		if want := slices.Sorted(slices.Values(cln.Methods)); !slices.Equal(used, want) {
			t.Fatalf("called %v\nlisted %v", used, want)
		}
	})
}

func clnTestHTTP(t *testing.T, pem []byte) *http.Client {
	t.Helper()
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		t.Fatal("no certificate in clnrest's ca.pem")
	}
	return &http.Client{Timeout: 30 * time.Second, Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool}}}
}

// toUs sums cln-a's own balance over its channels.
func toUs(t *testing.T, n *integration.Network) int64 {
	t.Helper()
	output, err := n.CLNCLI(t.Context(), selfCLN, "listpeerchannels")
	if err != nil {
		t.Fatal(err)
	}
	var list struct {
		Channels []struct {
			ToUs int64 `json:"to_us_msat"`
		} `json:"channels"`
	}
	if err := json.Unmarshal(output, &list); err != nil {
		t.Fatal(err)
	}
	var total int64
	for _, ch := range list.Channels {
		total += ch.ToUs
	}
	return total
}
