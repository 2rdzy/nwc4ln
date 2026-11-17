package lnd

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lightningnetwork/lnd/lnrpc"
	"github.com/lightningnetwork/lnd/lnrpc/routerrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/2rdzy/nwc4ln/internal/node"
)

const testHash = "abababababababababababababababababababababababababababababababab"

func channel(active bool, local int64, ownReserve, peerReserve, oldField uint64) *lnrpc.Channel {
	return &lnrpc.Channel{
		Active:              active,
		LocalBalance:        local,
		LocalChanReserveSat: int64(oldField),
		LocalConstraints:    &lnrpc.ChannelConstraints{ChanReserveSat: ownReserve},
		RemoteConstraints:   &lnrpc.ChannelConstraints{ChanReserveSat: peerReserve},
	}
}

// Catches: main wallet overstated, by reading the deprecated field or the peer's
// constraints. The harness cannot show this, as all three values are equal there.
func TestSpendableUsesOwnReserveFromConstraints(t *testing.T) {
	got, err := spendable([]*lnrpc.Channel{channel(true, 100_000, 1_000, 2_000, 3_000)})
	if err != nil {
		t.Fatal(err)
	}
	if want := int64(99_000_000); got.ActiveMsat != want || got.AllMsat != want {
		t.Fatalf("got %+v, want %d in both", got, want)
	}
}

// Catches: offline funds counted as sendable, or dropped from the shortfall
// figure.
func TestSpendableKeepsInactiveOutOfActiveFigure(t *testing.T) {
	got, err := spendable([]*lnrpc.Channel{channel(true, 5_000, 1_000, 0, 0), channel(false, 3_000, 1_000, 0, 0)})
	if err != nil {
		t.Fatal(err)
	}
	if got.ActiveMsat != 4_000_000 || got.AllMsat != 6_000_000 {
		t.Fatalf("got %+v", got)
	}
}

// Catches: a channel below its reserve taking from what the others can send.
func TestSpendableClampsEachChannel(t *testing.T) {
	got, err := spendable([]*lnrpc.Channel{channel(true, 1_001, 10_000, 0, 0), channel(true, 20_000, 10_000, 0, 0)})
	if err != nil {
		t.Fatal(err)
	}
	if got.ActiveMsat != 10_000_000 {
		t.Fatalf("got %+v", got)
	}
}

// Catches: an outgoing HTLC listed at a cost other than what LND took from the local
// balance, an incoming one listed, or a commitment fee counted for a channel we did not
// open, an older channel type, or a dust HTLC. The integration test checks the anchor
// case on a real node; the other cases do not occur in the harness.
func TestSpendableListsOutgoingHTLCs(t *testing.T) {
	hash := []byte{0xab, 0xcd}
	with := func(initiator bool, kind lnrpc.CommitmentType, htlcs ...*lnrpc.HTLC) *lnrpc.Channel {
		c := channel(true, 50_000, 1_000, 0, 0)
		c.Initiator, c.CommitmentType, c.FeePerKw, c.PendingHtlcs = initiator, kind, 2_500, htlcs
		c.LocalConstraints.DustLimitSat = 354
		return c
	}
	inactive := func(c *lnrpc.Channel) *lnrpc.Channel { c.Active = false; return c }
	out := func(sats int64) *lnrpc.HTLC { return &lnrpc.HTLC{Amount: sats, HashLock: hash} }
	cases := []struct {
		name     string
		channel  *lnrpc.Channel
		wantCost []int64
	}{
		{"AnchorWeOpened", with(true, lnrpc.CommitmentType_ANCHORS, out(20_000)), []int64{20_430_000}},
		{"TaprootWeOpened", with(true, lnrpc.CommitmentType_SIMPLE_TAPROOT, out(20_000)), []int64{20_430_000}},
		{"PeerOpened", with(false, lnrpc.CommitmentType_ANCHORS, out(20_000)), []int64{20_000_000}},
		{"Legacy", with(true, lnrpc.CommitmentType_STATIC_REMOTE_KEY, out(20_000)), []int64{20_000_000}},
		{"Dust", with(true, lnrpc.CommitmentType_ANCHORS, out(353)), []int64{353_000}},
		{"AtDustLimit", with(true, lnrpc.CommitmentType_ANCHORS, out(354)), []int64{784_000}},
		{"IncomingLeftOut", with(true, lnrpc.CommitmentType_ANCHORS, &lnrpc.HTLC{Incoming: true, Amount: 5_000}), nil},
		{"EachPart", with(true, lnrpc.CommitmentType_ANCHORS, out(1_000), out(2_000)), []int64{1_430_000, 2_430_000}},
		{"Inactive", inactive(with(true, lnrpc.CommitmentType_ANCHORS, out(20_000))), []int64{20_430_000}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := spendable([]*lnrpc.Channel{c.channel})
			if err != nil {
				t.Fatal(err)
			}
			if got.AllMsat != 49_000_000 || len(got.Outgoing) != len(c.wantCost) {
				t.Fatalf("got %+v", got)
			}
			for i, htlc := range got.Outgoing {
				if htlc.CostMsat != c.wantCost[i] || htlc.Hash != "abcd" || htlc.Active != c.channel.Active ||
					htlc.AmountMsat != c.channel.PendingHtlcs[i].Amount*1000 {
					t.Errorf("HTLC %d: %+v, want cost %d", i, htlc, c.wantCost[i])
				}
			}
		})
	}
}

// Catches: a reserve read as 0 when LND leaves the constraints out.
func TestSpendableRefusesMissingConstraints(t *testing.T) {
	if _, err := spendable([]*lnrpc.Channel{{Active: true, LocalBalance: 1}}); err == nil {
		t.Fatal("no error for a channel without constraints")
	}
}

// Catches: a status that is not final releasing or finishing a payment.
func TestPaymentStateOnlyFinalStatusesAreFinal(t *testing.T) {
	for status, want := range map[lnrpc.Payment_PaymentStatus]node.PaymentState{
		lnrpc.Payment_UNKNOWN:   node.PaymentInFlight,
		lnrpc.Payment_INITIATED: node.PaymentInFlight,
		lnrpc.Payment_IN_FLIGHT: node.PaymentInFlight,
		lnrpc.Payment_SUCCEEDED: node.PaymentSucceeded,
		lnrpc.Payment_FAILED:    node.PaymentFailed,
		99:                      node.PaymentInFlight,
	} {
		if got := paymentFrom(&lnrpc.Payment{Status: status}).State; got != want {
			t.Errorf("status %d: got %d, want %d", status, got, want)
		}
	}
}

// Catches: a zero preimage on a failed payment being treated as proof of payment.
func TestPaymentPreimageOnlyOnSuccess(t *testing.T) {
	zero := strings.Repeat("0", 64)
	failed := paymentFrom(&lnrpc.Payment{Status: lnrpc.Payment_FAILED, PaymentPreimage: zero})
	if failed.Preimage != "" {
		t.Fatalf("failed payment has preimage %q", failed.Preimage)
	}
}

// Catches: an invoice state that is not final crediting an account.
func TestInvoiceStateOnlyFinalStatesAreFinal(t *testing.T) {
	for state, want := range map[lnrpc.Invoice_InvoiceState]node.InvoiceState{
		lnrpc.Invoice_OPEN:     node.InvoiceOpen,
		lnrpc.Invoice_ACCEPTED: node.InvoiceOpen,
		lnrpc.Invoice_SETTLED:  node.InvoiceSettled,
		lnrpc.Invoice_CANCELED: node.InvoiceCanceled,
		99:                     node.InvoiceOpen,
	} {
		if got := invoiceState(state); got != want {
			t.Errorf("state %d: got %d, want %d", state, got, want)
		}
	}
}

// stubRouter answers with scripted payment updates, then an error. Other methods
// stay nil, as the embedded interface is empty.
type stubRouter struct {
	routerrpc.RouterClient
	sent    *routerrpc.SendPaymentRequest
	updates []*lnrpc.Payment
	err     error
	openErr error
}

type stubStream struct {
	grpc.ClientStream
	router *stubRouter
}

func (s stubStream) Recv() (*lnrpc.Payment, error) {
	if len(s.router.updates) == 0 {
		return nil, s.router.err
	}
	update := s.router.updates[0]
	s.router.updates = s.router.updates[1:]
	return update, nil
}

func (r *stubRouter) SendPaymentV2(_ context.Context, request *routerrpc.SendPaymentRequest,
	_ ...grpc.CallOption) (routerrpc.Router_SendPaymentV2Client, error) {
	r.sent = request
	return stubStream{router: r}, r.openErr
}

func (r *stubRouter) TrackPaymentV2(context.Context, *routerrpc.TrackPaymentRequest,
	...grpc.CallOption) (routerrpc.Router_TrackPaymentV2Client, error) {
	return stubStream{router: r}, r.openErr
}

func goodPay() node.PayRequest {
	return node.PayRequest{Invoice: "lnbcrt1x", FeeLimitMsat: 12_345, Timeout: 40 * time.Second}
}

// Catches: fee above the hold (R4). This checks the request, because a harness
// route costs no fee and passes with any limit.
func TestPaySendsExactFeeLimitInMsat(t *testing.T) {
	router := &stubRouter{updates: []*lnrpc.Payment{{Status: lnrpc.Payment_SUCCEEDED}}}
	client := &Client{router: router}
	if _, err := client.Pay(t.Context(), goodPay()); err != nil {
		t.Fatal(err)
	}
	if router.sent.FeeLimitMsat != 12_345 || router.sent.FeeLimitSat != 0 {
		t.Errorf("fee limit: msat %d, sat %d", router.sent.FeeLimitMsat, router.sent.FeeLimitSat)
	}
	if router.sent.TimeoutSeconds != 40 {
		t.Errorf("timeout: %d", router.sent.TimeoutSeconds)
	}
}

// Catches: an in-flight update returned as the result, or a broken stream
// returned as a state rather than an error.
func TestPayWaitsForFinalUpdate(t *testing.T) {
	router := &stubRouter{updates: []*lnrpc.Payment{
		{Status: lnrpc.Payment_INITIATED}, {Status: lnrpc.Payment_IN_FLIGHT}, {Status: lnrpc.Payment_FAILED},
	}}
	payment, err := (&Client{router: router}).Pay(t.Context(), goodPay())
	if err != nil || payment.State != node.PaymentFailed {
		t.Fatalf("got %+v, %v", payment, err)
	}
	for _, cause := range []error{io.EOF, status.Error(codes.Unavailable, "gone")} {
		router = &stubRouter{updates: []*lnrpc.Payment{{Status: lnrpc.Payment_IN_FLIGHT}}, err: cause}
		if payment, err := (&Client{router: router}).Pay(t.Context(), goodPay()); err == nil {
			t.Errorf("%v: no error, got %+v", cause, payment)
		}
	}
}

// Catches: a request that LND would read differently from what was meant.
func TestPayRefusesBadRequests(t *testing.T) {
	for name, change := range map[string]func(*node.PayRequest){
		"zero fee limit": func(r *node.PayRequest) { r.FeeLimitMsat = 0 },
		"negative fee":   func(r *node.PayRequest) { r.FeeLimitMsat = -1 },
		"no timeout":     func(r *node.PayRequest) { r.Timeout = 0 },
		"no invoice":     func(r *node.PayRequest) { r.Invoice = "" },
		"bad amount":     func(r *node.PayRequest) { r.AmountMsat = -1 },
	} {
		router := &stubRouter{}
		request := goodPay()
		change(&request)
		if _, err := (&Client{router: router}).Pay(t.Context(), request); err == nil || router.sent != nil {
			t.Errorf("%s: err %v, sent %v", name, err, router.sent)
		}
	}
}

// Catches: a hold released for a payment the node may still finish. Only the
// node's exact answer may become PaymentNoRecord.
func TestTrackPaymentNoRecordOnlyFromNodeAnswer(t *testing.T) {
	noRecord := status.Error(codes.NotFound, "payment isn't initiated")
	cases := map[string]struct {
		openErr, recvErr error
		wantRecord       bool
	}{
		"answer on first read":  {recvErr: noRecord, wantRecord: true},
		"answer on open":        {openErr: noRecord, wantRecord: true},
		"connection lost":       {recvErr: status.Error(codes.Unavailable, "connection refused")},
		"timeout":               {recvErr: status.Error(codes.DeadlineExceeded, "deadline exceeded")},
		"node still starting":   {recvErr: status.Error(codes.Unknown, "the RPC server is in the process of starting up")},
		"other not found":       {recvErr: status.Error(codes.NotFound, "unknown service routerrpc.Router")},
		"plain error":           {recvErr: errors.New("payment isn't initiated")},
		"stream ended, no data": {recvErr: io.EOF},
	}
	for name, test := range cases {
		t.Run(name, func(t *testing.T) {
			client := &Client{router: &stubRouter{openErr: test.openErr, err: test.recvErr}}
			payment, err := client.TrackPayment(t.Context(), testHash)
			if test.wantRecord {
				if err != nil || payment.State != node.PaymentNoRecord || payment.Hash != testHash {
					t.Fatalf("got %+v, %v", payment, err)
				}
				return
			}
			if err == nil {
				t.Fatalf("no error, got %+v", payment)
			}
			if payment.State == node.PaymentNoRecord {
				t.Fatal("error came with PaymentNoRecord")
			}
		})
	}
}

// Catches: a macaroon sent over plain text, or an empty one that fails later
// with a confusing permission error.
func TestMacaroon(t *testing.T) {
	path := filepath.Join(t.TempDir(), "m")
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := readMacaroon(path); err == nil {
		t.Error("empty macaroon accepted")
	}
	if err := os.WriteFile(path, []byte{0xab, 0xcd}, 0o600); err != nil {
		t.Fatal(err)
	}
	mac, err := readMacaroon(path)
	if err != nil {
		t.Fatal(err)
	}
	metadata, err := mac.GetRequestMetadata(t.Context())
	if err != nil || metadata["macaroon"] != "abcd" {
		t.Errorf("metadata %v, %v", metadata, err)
	}
	if !mac.RequireTransportSecurity() {
		t.Error("macaroon allowed without TLS")
	}
}

// Catches: a macaroon other users on the machine can read, and so pay with. lncli
// bakemacaroon writes mode 0644.
func TestMacaroonReadableByOthersIsRefused(t *testing.T) {
	path := filepath.Join(t.TempDir(), "m")
	for _, mode := range []os.FileMode{0o644, 0o640, 0o604} {
		if err := os.WriteFile(path, []byte{0xab}, 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(path, mode); err != nil {
			t.Fatal(err)
		}
		if _, err := readMacaroon(path); err == nil || !strings.Contains(err.Error(), "chmod 600") {
			t.Errorf("mode %#o: got %v", mode, err)
		}
	}
}
