package cln

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/2rdzy/nwc4ln/internal/node"
)

const testRune = "test-rune"

// reply is what the fake clnrest sends: a result, or a JSON-RPC error.
type reply struct {
	result any
	code   int // Above 0 sends a JSON-RPC error with this code.
	data   any
	broken bool // Sends a reply that is not JSON-RPC, as a proxy or a crash would.
}

func ok(result any) reply { return reply{result: result} }

func rpcErr(code int) reply { return reply{code: code} }

// server is a fake clnrest. Each method answers from its handler.
type server struct {
	t        *testing.T
	mu       sync.Mutex
	handlers map[string]func(params map[string]any) reply
	calls    []string
	params   map[string][]map[string]any
}

func (s *server) on(method string, handler func(params map[string]any) reply) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.handlers[method] = handler
}

func (s *server) called(method string) []map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.params[method]
}

func (s *server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	method := strings.TrimPrefix(r.URL.Path, "/v1/")
	if r.Header.Get("Rune") != testRune || r.Method != http.MethodPost {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"code":1501,"message":"Not authorized: Missing rune"}`))
		return
	}
	var params map[string]any
	if err := json.NewDecoder(r.Body).Decode(&params); err != nil {
		s.t.Errorf("%s: bad body: %v", method, err)
	}
	s.mu.Lock()
	handler := s.handlers[method]
	s.calls = append(s.calls, method)
	s.params[method] = append(s.params[method], params)
	s.mu.Unlock()
	if handler == nil {
		s.t.Errorf("unexpected call to %s", method)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	answer := handler(params)
	switch {
	case answer.broken:
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write([]byte("<html>bad gateway</html>"))
	case answer.code != 0:
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]any{"code": answer.code, "message": "fake error", "data": answer.data})
	default:
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(answer.result)
	}
}

func newServer(t *testing.T) (*server, *Client) {
	t.Helper()
	s := &server{t: t, handlers: map[string]func(map[string]any) reply{}, params: map[string][]map[string]any{}}
	httpServer := httptest.NewTLSServer(s)
	t.Cleanup(httpServer.Close)
	dir := t.TempDir()
	ca := filepath.Join(dir, "ca.pem")
	block := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: httpServer.Certificate().Raw})
	if err := os.WriteFile(ca, block, 0o600); err != nil {
		t.Fatal(err)
	}
	runePath := filepath.Join(dir, "rune")
	if err := os.WriteFile(runePath, []byte(testRune+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	c, err := New(Config{URL: httpServer.URL, CACertPath: ca, RunePath: runePath})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return s, c
}

const (
	ownID   = "02aa"
	otherID = "03bb"
	payHash = "1111111111111111111111111111111111111111111111111111111111111111"
)

// payable sets up decode and getinfo for an invoice from another node.
func payable(s *server) {
	s.on("decode", func(map[string]any) reply {
		return ok(map[string]any{"type": "bolt11 invoice", "valid": true, "payment_hash": payHash, "payee": otherID,
			"amount_msat": 21000, "created_at": 1700000000, "expiry": 3600})
	})
	s.on("getinfo", func(map[string]any) reply { return ok(map[string]any{"id": ownID, "network": "regtest"}) })
}

func sendPays(parts ...part) func(map[string]any) reply {
	return func(map[string]any) reply {
		list := []part{}
		return ok(map[string]any{"payments": append(list, parts...)})
	}
}

var request = node.PayRequest{Invoice: "lnbcrt1", FeeLimitMsat: 10_000, Timeout: 50 * time.Second}

// Catches: funds released for a payment that may still settle. Whether xpay can
// still start or record a part after it answers depends on its internals, so no
// error from it is final, not even a refusal before anything was sent: the ledger
// asks TrackPayment, which waits until no new attempt can come.
func TestPayErrorIsNeverFinal(t *testing.T) {
	tests := []struct {
		name string
		xpay reply
	}{
		{"Failed", rpcErr(203)},
		{"RefusedBeforeSending", rpcErr(205)},
		{"AlreadyPaid", rpcErr(codeAlreadyPaid)},
		{"NotAnRPCAnswer", reply{broken: true}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			s, c := newServer(t)
			payable(s)
			s.on("xpay", func(map[string]any) reply { return test.xpay })
			if payment, err := c.Pay(t.Context(), request); err == nil {
				t.Fatalf("got %+v with no error", payment)
			}
		})
	}
}

// Catches: a fee above the hold (R4), a payment that waits past what the ledger
// allows, and a wrong fee charged to the account.
func TestPayPassesLimitsAndReadsFee(t *testing.T) {
	s, c := newServer(t)
	payable(s)
	s.on("xpay", func(map[string]any) reply {
		return ok(map[string]any{"payment_preimage": "ab", "amount_msat": 21000, "amount_sent_msat": 21007})
	})
	long := request
	long.Timeout = 5 * time.Minute
	payment, err := c.Pay(t.Context(), long)
	if err != nil {
		t.Fatal(err)
	}
	want := node.Payment{State: node.PaymentSucceeded, Hash: payHash, Preimage: "ab", AmountMsat: 21000, FeeMsat: 7}
	if payment != want {
		t.Fatalf("got %+v, want %+v", payment, want)
	}
	params := s.called("xpay")[0]
	if params["maxfee"] != float64(10_000) || params["retry_for"] != float64(60) || params["amount_msat"] != nil {
		t.Fatalf("xpay params %v", params)
	}
}

// Catches: the node paying its own invoice, which LND refuses. The ledger settles
// those itself (R10), and a node payment would credit twice.
func TestPayRefusesOwnInvoice(t *testing.T) {
	s, c := newServer(t)
	payable(s)
	s.on("getinfo", func(map[string]any) reply { return ok(map[string]any{"id": otherID}) })
	if _, err := c.Pay(t.Context(), request); err == nil {
		t.Fatal("paid an invoice from this node")
	}
	if len(s.called("xpay")) != 0 {
		t.Fatal("xpay was called")
	}
}

// Catches: a hold released while xpay can still start a new attempt, a paid
// invoice read as failed, and a wrong amount or fee from the parts.
func TestTrackFromParts(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	at := func(ago time.Duration) int64 { return now.Add(-ago).Unix() }
	tests := []struct {
		name  string
		parts []part
		want  node.Payment
	}{
		{"NoParts", nil, node.Payment{State: node.PaymentNoRecord}},
		{"Pending", []part{{Status: "pending", CreatedAt: at(time.Hour)}}, node.Payment{State: node.PaymentInFlight}},
		{"UnknownStatus", []part{{Status: "new", CreatedAt: at(time.Hour)}}, node.Payment{State: node.PaymentInFlight}},
		{"FailedJustNow", []part{{Status: "failed", CreatedAt: at(time.Second)}}, node.Payment{State: node.PaymentInFlight}},
		{"FailedWithinRetry", []part{
			{Status: "failed", CreatedAt: at(time.Hour)}, {Status: "failed", CreatedAt: at(quietAfterRetry - time.Second)},
		}, node.Payment{State: node.PaymentInFlight}},
		{"FailedLongAgo", []part{{Status: "failed", CreatedAt: at(quietAfterRetry + time.Second)}},
			node.Payment{State: node.PaymentFailed, FailureReason: "every attempt failed"}},
		{"SucceededAfterFailedGroup", []part{
			{Status: "failed", GroupID: 1, AmountMsat: 5000, SentMsat: 5100},
			{Status: "complete", GroupID: 2, AmountMsat: 3000, SentMsat: 3004, Preimage: "cd"},
			{Status: "complete", GroupID: 2, AmountMsat: 2000, SentMsat: 2001, Preimage: "cd"},
		}, node.Payment{State: node.PaymentSucceeded, Preimage: "cd", AmountMsat: 5000, FeeMsat: 5}},
		{"SucceededWithPartPending", []part{
			{Status: "pending", GroupID: 1}, {Status: "complete", GroupID: 1, AmountMsat: 1000, SentMsat: 1000, Preimage: "cd"},
		}, node.Payment{State: node.PaymentSucceeded, Preimage: "cd", AmountMsat: 1000}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			s, c := newServer(t)
			c.now = func() time.Time { return now }
			s.on("listsendpays", sendPays(test.parts...))
			got, err := c.TrackPayment(t.Context(), payHash)
			if err != nil {
				t.Fatal(err)
			}
			test.want.Hash = payHash
			if got != test.want {
				t.Fatalf("got %+v, want %+v", got, test.want)
			}
		})
	}
}

// Catches: a connection or proxy error read as "no record", which would release
// the hold of a payment that may settle (R12).
func TestTrackErrorIsNeverNoRecord(t *testing.T) {
	for _, answer := range []reply{{broken: true}, rpcErr(-32602), ok(map[string]any{})} {
		s, c := newServer(t)
		s.on("listsendpays", func(map[string]any) reply { return answer })
		if payment, err := c.TrackPayment(t.Context(), payHash); err == nil {
			t.Fatalf("answer %+v gave %+v with no error", answer, payment)
		}
	}
}

func invoiceList(status string) func(map[string]any) reply {
	return func(map[string]any) reply {
		return ok(map[string]any{"invoices": []map[string]any{
			{"label": "nwc4ln-x", "payment_hash": payHash, "status": status, "bolt11": "lnbcrt1"},
		}})
	}
}

// Catches: R10 settling inside the ledger an invoice that was paid at the node, or
// trusting a cancel that did not delete the invoice.
func TestCancelInvoice(t *testing.T) {
	t.Run("Unpaid", func(t *testing.T) {
		s, c := newServer(t)
		s.on("listinvoices", invoiceList("unpaid"))
		s.on("delinvoice", func(map[string]any) reply { return ok(map[string]any{}) })
		if err := c.CancelInvoice(t.Context(), payHash); err != nil {
			t.Fatal(err)
		}
		if got := s.called("delinvoice")[0]; got["label"] != "nwc4ln-x" || got["status"] != "unpaid" {
			t.Fatalf("delinvoice %v", got)
		}
	})
	t.Run("Paid", func(t *testing.T) {
		s, c := newServer(t)
		s.on("listinvoices", invoiceList("paid"))
		if err := c.CancelInvoice(t.Context(), payHash); err == nil {
			t.Fatal("canceled a paid invoice")
		}
		if len(s.called("delinvoice")) != 0 {
			t.Fatal("delinvoice was called")
		}
	})
	t.Run("PaidMeanwhile", func(t *testing.T) {
		s, c := newServer(t)
		s.on("listinvoices", invoiceList("unpaid"))
		s.on("delinvoice", func(map[string]any) reply {
			return reply{code: codeInvoiceStatus, data: map[string]any{"current_status": "paid"}}
		})
		if err := c.CancelInvoice(t.Context(), payHash); err == nil {
			t.Fatal("canceled an invoice paid after the lookup")
		}
		if n := len(s.called("delinvoice")); n != 1 {
			t.Fatalf("delinvoice called %d times", n)
		}
	})
	t.Run("ExpiredMeanwhile", func(t *testing.T) {
		s, c := newServer(t)
		s.on("listinvoices", invoiceList("unpaid"))
		s.on("delinvoice", func(params map[string]any) reply {
			if params["status"] == "expired" {
				return ok(map[string]any{})
			}
			return reply{code: codeInvoiceStatus, data: map[string]any{"current_status": "expired"}}
		})
		if err := c.CancelInvoice(t.Context(), payHash); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("AlreadyDeleted", func(t *testing.T) {
		s, c := newServer(t)
		s.on("listinvoices", invoiceList("unpaid"))
		s.on("delinvoice", func(map[string]any) reply { return rpcErr(codeInvoiceNotFound) })
		if err := c.CancelInvoice(t.Context(), payHash); err == nil {
			t.Fatal("cancel passed on a deleted invoice")
		}
	})
	t.Run("Unknown", func(t *testing.T) {
		s, c := newServer(t)
		s.on("listinvoices", func(map[string]any) reply { return ok(map[string]any{"invoices": []any{}}) })
		if err := c.CancelInvoice(t.Context(), payHash); !errors.Is(err, node.ErrNotFound) {
			t.Fatalf("got %v, want ErrNotFound", err)
		}
	})
}

// Catches: an expired invoice read as open, so a deleted sub-wallet's sweep never
// closes it, and a lookup that hides a lost connection as "not found".
func TestLookupInvoice(t *testing.T) {
	s, c := newServer(t)
	s.on("decode", func(map[string]any) reply {
		return ok(map[string]any{"type": "bolt11 invoice", "valid": true, "payment_hash": payHash, "created_at": 1700000000})
	})
	s.on("listinvoices", invoiceList("expired"))
	invoice, err := c.LookupInvoice(t.Context(), payHash)
	if err != nil || invoice.State != node.InvoiceCanceled {
		t.Fatalf("got %+v, %v; want canceled", invoice, err)
	}
	s.on("listinvoices", func(map[string]any) reply { return reply{broken: true} })
	if _, err := c.LookupInvoice(t.Context(), payHash); err == nil || errors.Is(err, node.ErrNotFound) {
		t.Fatalf("got %v, want an error that is not ErrNotFound", err)
	}
}

// Catches: a preimage that does not match the invoice (R10 hands it to the payer),
// and a description hash taken without the text it hashes.
func TestMakeInvoice(t *testing.T) {
	s, c := newServer(t)
	s.on("invoice", func(params map[string]any) reply {
		raw, _ := hex.DecodeString(params["preimage"].(string))
		sum := sha256.Sum256(raw)
		return ok(map[string]any{"payment_hash": hex.EncodeToString(sum[:])})
	})
	s.on("listinvoices", func(params map[string]any) reply {
		return ok(map[string]any{"invoices": []map[string]any{
			{"label": "l", "payment_hash": params["payment_hash"], "status": "unpaid", "bolt11": "lnbcrt1"},
		}})
	})
	s.on("decode", func(map[string]any) reply {
		return ok(map[string]any{"type": "bolt11 invoice", "valid": true, "payment_hash": payHash, "created_at": 1700000000})
	})
	invoice, err := c.MakeInvoice(t.Context(), node.InvoiceRequest{AmountMsat: 1500, Description: "d"})
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := hex.DecodeString(invoice.Preimage)
	if sum := sha256.Sum256(raw); len(raw) != 32 || hex.EncodeToString(sum[:]) != invoice.Hash {
		t.Fatalf("preimage %q does not match hash %s", invoice.Preimage, invoice.Hash)
	}
	params := s.called("invoice")[0]
	if params["label"] != labelPrefix+invoice.Hash || params["amount_msat"] != float64(1500) || params["deschashonly"] != nil {
		t.Fatalf("invoice params %v", params)
	}
	_, err = c.MakeInvoice(t.Context(), node.InvoiceRequest{DescriptionHash: strings.Repeat("ef", 32)})
	if !errors.Is(err, node.ErrNeedsDescription) {
		t.Fatalf("bare description hash: got %v, want ErrNeedsDescription", err)
	}
	sum := sha256.Sum256([]byte("metadata"))
	_, err = c.MakeInvoice(t.Context(), node.InvoiceRequest{Description: "metadata", DescriptionHash: hex.EncodeToString(sum[:])})
	if err != nil || s.called("invoice")[1]["deschashonly"] != true {
		t.Fatalf("hash with its text: %v, params %v", err, s.called("invoice")[1])
	}
}

func channelJSON(fields map[string]any) map[string]any {
	ch := map[string]any{
		"state": "CHANNELD_NORMAL", "peer_connected": true, "opener": "local", "to_us_msat": 700_000_000,
		"total_msat":       1_000_000_000,
		"our_reserve_msat": 10_000_000, "last_tx_fee_msat": 1_626_000, "dust_limit_msat": 546_000,
		"feerate": map[string]any{"perkw": 1250}, "channel_type": map[string]any{"bits": []int{12, 22}},
		"htlcs": []map[string]any{
			{"direction": "out", "amount_msat": 20_000_000, "payment_hash": "aa", "state": "SENT_ADD_ACK_REVOCATION"},
			{"direction": "in", "amount_msat": 5_000_000, "payment_hash": "bb", "state": "RCVD_ADD_ACK_REVOCATION"},
		},
	}
	for key, value := range fields {
		ch[key] = value
	}
	return ch
}

// Catches: a main wallet overstated (R5) by counting what LND leaves out, a
// closing channel, or a channel below its reserve; and an in-flight cost that
// differs from what left the figure, which shows as a false shortfall (R14).
func TestSpendable(t *testing.T) {
	// Measured on a real channel: LND's local_balance for it was to_us less the
	// HTLC, the commitment fee and the 660 sat anchors.
	opened := int64(700_000_000 - 20_000_000 - 1_626_000 - 660_000 - 10_000_000)
	htlc := node.HTLC{Hash: "aa", AmountMsat: 20_000_000, CostMsat: 20_000_000 + 172*1250/1000*1000, Active: true}
	tests := []struct {
		name     string
		channels []map[string]any
		active   int64
		all      int64
		outgoing []node.HTLC
	}{
		{"OpenedHere", []map[string]any{channelJSON(nil)}, opened, opened, []node.HTLC{htlc}},
		{"OpenedByPeer", []map[string]any{channelJSON(map[string]any{"opener": "remote"})},
			700_000_000 - 20_000_000 - 10_000_000, 700_000_000 - 20_000_000 - 10_000_000,
			[]node.HTLC{{Hash: "aa", AmountMsat: 20_000_000, CostMsat: 20_000_000, Active: true}}},
		{"PeerOffline", []map[string]any{channelJSON(map[string]any{"peer_connected": false})}, 0, opened,
			[]node.HTLC{{Hash: "aa", AmountMsat: 20_000_000, CostMsat: htlc.CostMsat}}},
		{"Closing", []map[string]any{channelJSON(map[string]any{"state": "CHANNELD_SHUTTING_DOWN"}),
			channelJSON(map[string]any{"state": "ONCHAIN"})}, 0, 0, nil},
		{"BelowReserve", []map[string]any{channelJSON(map[string]any{"to_us_msat": 1_000_000}), channelJSON(nil)},
			opened, opened, []node.HTLC{htlc, htlc}},
		// Measured: the peer had 1 sat, so it had no output and no anchor, and the
		// opener paid for one anchor only.
		{"PeerHasNoOutput", []map[string]any{channelJSON(map[string]any{"to_us_msat": 999_999_000, "htlcs": nil,
			"last_tx_fee_msat": 3_146_000})},
			999_999_000 - 3_146_000 - 330_000 - 10_000_000, 999_999_000 - 3_146_000 - 330_000 - 10_000_000, nil},
		// Any HTLC with an output brings both anchors back.
		{"PeerHasNoOutputButHTLC", []map[string]any{channelJSON(map[string]any{"to_us_msat": 999_999_000,
			"last_tx_fee_msat": 3_146_000, "htlcs": []map[string]any{
				{"direction": "out", "amount_msat": 20_000_000, "payment_hash": "aa", "state": "SENT_ADD_ACK_REVOCATION"}}})},
			999_999_000 - 20_000_000 - 3_146_000 - 660_000 - 10_000_000, 999_999_000 - 20_000_000 - 3_146_000 - 660_000 - 10_000_000,
			[]node.HTLC{htlc}},
		// Found in CI after a soak: an HTLC is listed from the moment it is offered, but
		// last_tx_fee_msat covers it only once it is in our commitment. Before that, and
		// while it is being removed, its fee must not be added back, or the node reads
		// 430 sat richer than it is. Nor does it bring the peer's anchor back yet.
		{"HTLCNotYetInOurCommitment", []map[string]any{channelJSON(map[string]any{"htlcs": []map[string]any{
			{"direction": "out", "amount_msat": 20_000_000, "payment_hash": "aa", "state": "SENT_ADD_HTLC"}}})},
			opened, opened, []node.HTLC{{Hash: "aa", AmountMsat: 20_000_000, CostMsat: 20_000_000, Active: true}}},
		{"HTLCLeavingOurCommitment", []map[string]any{channelJSON(map[string]any{"htlcs": []map[string]any{
			{"direction": "out", "amount_msat": 20_000_000, "payment_hash": "aa", "state": "RCVD_REMOVE_COMMIT"}}})},
			opened, opened, []node.HTLC{{Hash: "aa", AmountMsat: 20_000_000, CostMsat: 20_000_000, Active: true}}},
		{"PeerHasNoOutputHTLCNotYetCommitted", []map[string]any{channelJSON(map[string]any{"to_us_msat": 999_999_000,
			"last_tx_fee_msat": 3_146_000, "htlcs": []map[string]any{
				{"direction": "out", "amount_msat": 20_000_000, "payment_hash": "aa", "state": "SENT_ADD_COMMIT"}}})},
			999_999_000 - 20_000_000 - 3_146_000 - 330_000 - 10_000_000, 999_999_000 - 20_000_000 - 3_146_000 - 330_000 - 10_000_000,
			[]node.HTLC{{Hash: "aa", AmountMsat: 20_000_000, CostMsat: 20_000_000, Active: true}}},
		{"PeerNearDust", []map[string]any{channelJSON(map[string]any{"to_us_msat": 999_500_000, "htlcs": nil,
			"last_tx_fee_msat": 3_146_000})},
			999_500_000 - 3_146_000 - 660_000 - 10_000_000, 999_500_000 - 3_146_000 - 660_000 - 10_000_000, nil},
		{"DustHTLC", []map[string]any{channelJSON(map[string]any{"htlcs": []map[string]any{
			{"direction": "out", "amount_msat": 545_999, "payment_hash": "aa", "state": "SENT_ADD_ACK_REVOCATION"}}})},
			700_000_000 - 545_999 - 1_626_000 - 660_000 - 10_000_000, 700_000_000 - 545_999 - 1_626_000 - 660_000 - 10_000_000,
			[]node.HTLC{{Hash: "aa", AmountMsat: 545_999, CostMsat: 545_999, Active: true}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			s, c := newServer(t)
			s.on("listpeerchannels", func(map[string]any) reply { return ok(map[string]any{"channels": test.channels}) })
			got, err := c.Spendable(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			if got.ActiveMsat != test.active || got.AllMsat != test.all || len(got.Outgoing) != len(test.outgoing) {
				t.Fatalf("got %+v, want active %d, all %d, outgoing %+v", got, test.active, test.all, test.outgoing)
			}
			for i := range got.Outgoing {
				if got.Outgoing[i] != test.outgoing[i] {
					t.Fatalf("HTLC %d: got %+v, want %+v", i, got.Outgoing[i], test.outgoing[i])
				}
			}
		})
	}
	s, c := newServer(t)
	noReserve := channelJSON(nil)
	delete(noReserve, "our_reserve_msat")
	s.on("listpeerchannels", func(map[string]any) reply { return ok(map[string]any{"channels": []any{noReserve}}) })
	if _, err := c.Spendable(t.Context()); err == nil {
		t.Fatal("a channel with no reserve counted as reserve 0")
	}
}

// Catches: a timed-out wait or a BOLT 12 invoice ending the stream for good, income
// credited with the wrong index, and a failed credit skipped (R11).
func TestSubscribeInvoices(t *testing.T) {
	s, c := newServer(t)
	s.on("decode", func(params map[string]any) reply {
		if params["string"] == "" {
			return rpcErr(-32602)
		}
		return ok(map[string]any{"type": "bolt11 invoice", "valid": true, "payment_hash": payHash, "created_at": 1700000000})
	})
	s.on("waitanyinvoice", func(params map[string]any) reply {
		switch len(s.called("waitanyinvoice")) {
		case 1:
			return rpcErr(codeWaitTimedOut)
		case 2: // BOLT 12, which has no BOLT 11 string to decode.
			return ok(map[string]any{"payment_hash": payHash, "status": "paid", "bolt12": "lni1", "pay_index": 8,
				"amount_received_msat": 5000, "paid_at": 1700000050})
		}
		index := int(params["lastpay_index"].(float64)) + 1
		return ok(map[string]any{"payment_hash": payHash, "status": "paid", "bolt11": "lnbcrt1", "pay_index": index,
			"amount_received_msat": 21000, "paid_at": 1700000100, "payment_preimage": "ab"})
	})
	failure := errors.New("database is locked")
	var got []node.Invoice
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	err := c.SubscribeInvoices(ctx, 7, func(invoice node.Invoice) error {
		got = append(got, invoice)
		if len(got) == 3 {
			return failure
		}
		return nil
	})
	if !errors.Is(err, failure) || len(got) != 3 {
		t.Fatalf("err %v after %d invoices", err, len(got))
	}
	if got[0].SettleIndex != 8 || got[0].PaidMsat != 5000 || got[1].SettleIndex != 9 || got[2].SettleIndex != 10 ||
		got[1].PaidMsat != 21000 || got[1].State != node.InvoiceSettled {
		t.Fatalf("got %+v", got)
	}
	calls := s.called("waitanyinvoice")
	if calls[0]["lastpay_index"] != float64(7) || calls[1]["lastpay_index"] != float64(7) ||
		calls[2]["lastpay_index"] != float64(8) || calls[3]["lastpay_index"] != float64(9) {
		t.Fatalf("waitanyinvoice calls %v", calls)
	}
}

// Catches: a rune that other users can read, and a rune sent over plain HTTP.
func TestNewRefusesUnsafeSetup(t *testing.T) {
	dir := t.TempDir()
	runePath := filepath.Join(dir, "rune")
	if err := os.WriteFile(runePath, []byte(testRune), 0o644); err != nil {
		t.Fatal(err)
	}
	ca := filepath.Join(dir, "ca.pem")
	if err := os.WriteFile(ca, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := New(Config{URL: "https://127.0.0.1:3010", CACertPath: ca, RunePath: runePath}); err == nil ||
		!strings.Contains(err.Error(), "chmod 600") {
		t.Fatalf("readable rune: %v", err)
	}
	if err := os.Chmod(runePath, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := New(Config{URL: "http://127.0.0.1:3010", CACertPath: ca, RunePath: runePath}); err == nil ||
		!strings.Contains(err.Error(), "https") {
		t.Fatalf("plain HTTP: %v", err)
	}
}
