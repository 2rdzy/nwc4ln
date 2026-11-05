// Package lnd is the node.Node for LND, over gRPC.
package lnd

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/lightningnetwork/lnd/lnrpc"
	"github.com/lightningnetwork/lnd/lnrpc/invoicesrpc"
	"github.com/lightningnetwork/lnd/lnrpc/routerrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"

	"github.com/2rdzy/nwc4ln/internal/node"
)

// Methods lists every gRPC method the client calls, and nothing else. The
// operator bakes the macaroon with these, each with a "uri:" prefix.
var Methods = []string{
	"/lnrpc.Lightning/GetInfo",
	"/lnrpc.Lightning/ListChannels",
	"/lnrpc.Lightning/AddInvoice",
	"/lnrpc.Lightning/LookupInvoice",
	"/lnrpc.Lightning/SubscribeInvoices",
	"/lnrpc.Lightning/DecodePayReq",
	"/routerrpc.Router/SendPaymentV2",
	"/routerrpc.Router/TrackPaymentV2",
	"/invoicesrpc.Invoices/CancelInvoice",
}

// callTimeout limits a call that should answer at once, so a hung node gives an
// error rather than a stuck caller. Pay and SubscribeInvoices run until ctx ends.
const callTimeout = 30 * time.Second

// noPaymentRecord is LND's answer for a payment hash it has never seen. Matching
// the text as well as the code is deliberate: if a later LND words it
// differently, the payment stays pending, which is the safe direction.
const noPaymentRecord = "payment isn't initiated"

// Config says how to reach the node.
type Config struct {
	Host         string
	TLSCertPath  string
	MacaroonPath string
}

// Client is a node.Node on LND. It holds the macaroon only in memory.
type Client struct {
	conn      *grpc.ClientConn
	lightning lnrpc.LightningClient
	router    routerrpc.RouterClient
	invoices  invoicesrpc.InvoicesClient
}

var _ node.Node = (*Client)(nil)

// New prepares a client. It does not connect: the first call does, and a node
// that is down gives errors until it returns. Options are for tests.
func New(cfg Config, options ...grpc.DialOption) (*Client, error) {
	if cfg.Host == "" || cfg.TLSCertPath == "" || cfg.MacaroonPath == "" {
		return nil, errors.New("lnd: host, TLS cert path and macaroon path are all required")
	}
	tls, err := credentials.NewClientTLSFromFile(cfg.TLSCertPath, "")
	if err != nil {
		return nil, fmt.Errorf("lnd: read TLS cert: %w", err)
	}
	mac, err := readMacaroon(cfg.MacaroonPath)
	if err != nil {
		return nil, err
	}
	all := append([]grpc.DialOption{
		grpc.WithTransportCredentials(tls),
		grpc.WithPerRPCCredentials(mac),
		// Without pings, a dead connection leaves a stream waiting forever, and
		// income stops with no error. Pings run only while a stream is open: by
		// default LND drops a client that pings without one (too_many_pings).
		grpc.WithKeepaliveParams(keepalive.ClientParameters{Time: time.Minute, Timeout: 20 * time.Second}),
	}, options...)
	conn, err := grpc.NewClient("passthrough:///"+cfg.Host, all...)
	if err != nil {
		return nil, fmt.Errorf("lnd: %w", err)
	}
	return &Client{
		conn:      conn,
		lightning: lnrpc.NewLightningClient(conn),
		router:    routerrpc.NewRouterClient(conn),
		invoices:  invoicesrpc.NewInvoicesClient(conn),
	}, nil
}

// Close drops the connection.
func (c *Client) Close() error { return c.conn.Close() }

// macaroon sends the macaroon with each call, and only over TLS.
type macaroon string

func readMacaroon(path string) (macaroon, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("lnd: read macaroon: %w", err)
	}
	if len(data) == 0 {
		return "", fmt.Errorf("lnd: macaroon file %s is empty", path)
	}
	return macaroon(hex.EncodeToString(data)), nil
}

func (m macaroon) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return map[string]string{"macaroon": string(m)}, nil
}

func (macaroon) RequireTransportSecurity() bool { return true }

func decodeHash(hash string) ([]byte, error) {
	data, err := hex.DecodeString(hash)
	if err != nil || len(data) != 32 {
		return nil, fmt.Errorf("lnd: bad payment hash %q", hash)
	}
	return data, nil
}

// Info implements node.Node.
func (c *Client) Info(ctx context.Context) (node.Info, error) {
	ctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	reply, err := c.lightning.GetInfo(ctx, &lnrpc.GetInfoRequest{})
	if err != nil {
		return node.Info{}, fmt.Errorf("lnd: get info: %w", err)
	}
	info := node.Info{
		PubKey:      reply.IdentityPubkey,
		Alias:       reply.Alias,
		Color:       reply.Color,
		BlockHeight: reply.BlockHeight,
		BlockHash:   reply.BlockHash,
		Synced:      reply.SyncedToChain,
	}
	if len(reply.Chains) > 0 {
		info.Network = reply.Chains[0].Network
	}
	return info, nil
}

// Spendable implements node.Node.
func (c *Client) Spendable(ctx context.Context) (node.Spendable, error) {
	ctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	reply, err := c.lightning.ListChannels(ctx, &lnrpc.ListChannelsRequest{})
	if err != nil {
		return node.Spendable{}, fmt.Errorf("lnd: list channels: %w", err)
	}
	return spendable(reply.Channels)
}

// spendable sums local balance less our channel reserve. A channel with no
// constraints is an error, because treating its reserve as 0 would overstate
// the wallet. Fractions of a sat are not reported, which undercounts.
func spendable(channels []*lnrpc.Channel) (node.Spendable, error) {
	const msatPerSat = 1000
	var total node.Spendable
	for _, channel := range channels {
		if channel.LocalConstraints == nil {
			return node.Spendable{}, fmt.Errorf("lnd: channel %s has no local constraints", channel.ChannelPoint)
		}
		free := max(channel.LocalBalance-int64(channel.LocalConstraints.ChanReserveSat), 0) * msatPerSat
		total.AllMsat += free
		if channel.Active {
			total.ActiveMsat += free
		}
	}
	return total, nil
}

// MakeInvoice implements node.Node.
func (c *Client) MakeInvoice(ctx context.Context, request node.InvoiceRequest) (node.Invoice, error) {
	if request.AmountMsat < 0 {
		return node.Invoice{}, fmt.Errorf("lnd: negative invoice amount %d", request.AmountMsat)
	}
	invoice := &lnrpc.Invoice{
		ValueMsat: request.AmountMsat,
		Expiry:    request.ExpirySeconds,
		// Adds route hints for private channels. A node with only those could
		// not be paid without them.
		Private: true,
	}
	if request.DescriptionHash != "" {
		hash, err := hex.DecodeString(request.DescriptionHash)
		if err != nil {
			return node.Invoice{}, fmt.Errorf("lnd: bad description hash: %w", err)
		}
		invoice.DescriptionHash = hash
	} else {
		invoice.Memo = request.Description
	}
	addCtx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	reply, err := c.lightning.AddInvoice(addCtx, invoice)
	if err != nil {
		return node.Invoice{}, fmt.Errorf("lnd: add invoice: %w", err)
	}
	// The reply lacks the expiry and creation time, so read the invoice back.
	return c.LookupInvoice(ctx, hex.EncodeToString(reply.RHash))
}

// LookupInvoice implements node.Node.
func (c *Client) LookupInvoice(ctx context.Context, hash string) (node.Invoice, error) {
	raw, err := decodeHash(hash)
	if err != nil {
		return node.Invoice{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	reply, err := c.lightning.LookupInvoice(ctx, &lnrpc.PaymentHash{RHash: raw})
	if status.Code(err) == codes.NotFound {
		return node.Invoice{}, fmt.Errorf("lnd: invoice %s: %w", hash, node.ErrNotFound)
	}
	if err != nil {
		return node.Invoice{}, fmt.Errorf("lnd: lookup invoice: %w", err)
	}
	return invoiceFrom(reply), nil
}

func invoiceFrom(invoice *lnrpc.Invoice) node.Invoice {
	result := node.Invoice{
		Hash:            hex.EncodeToString(invoice.RHash),
		Preimage:        hex.EncodeToString(invoice.RPreimage),
		PaymentRequest:  invoice.PaymentRequest,
		Description:     invoice.Memo,
		DescriptionHash: hex.EncodeToString(invoice.DescriptionHash),
		AmountMsat:      invoice.ValueMsat,
		PaidMsat:        invoice.AmtPaidMsat,
		State:           invoiceState(invoice.State),
		SettleIndex:     invoice.SettleIndex,
		CreatedAt:       time.Unix(invoice.CreationDate, 0).UTC(),
		ExpiresAt:       time.Unix(invoice.CreationDate+invoice.Expiry, 0).UTC(),
	}
	if invoice.SettleDate != 0 {
		result.SettledAt = time.Unix(invoice.SettleDate, 0).UTC()
	}
	return result
}

// invoiceState reads only the two final states. Anything else, including a state
// a later LND adds, counts as open, so it can never credit an account.
func invoiceState(state lnrpc.Invoice_InvoiceState) node.InvoiceState {
	switch state {
	case lnrpc.Invoice_SETTLED:
		return node.InvoiceSettled
	case lnrpc.Invoice_CANCELED:
		return node.InvoiceCanceled
	default:
		return node.InvoiceOpen
	}
}

// CancelInvoice implements node.Node.
func (c *Client) CancelInvoice(ctx context.Context, hash string) error {
	raw, err := decodeHash(hash)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	if _, err := c.invoices.CancelInvoice(ctx, &invoicesrpc.CancelInvoiceMsg{PaymentHash: raw}); err != nil {
		return fmt.Errorf("lnd: cancel invoice: %w", err)
	}
	return nil
}

// DecodeInvoice implements node.Node.
func (c *Client) DecodeInvoice(ctx context.Context, invoice string) (node.DecodedInvoice, error) {
	ctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	reply, err := c.lightning.DecodePayReq(ctx, &lnrpc.PayReqString{PayReq: invoice})
	if err != nil {
		return node.DecodedInvoice{}, fmt.Errorf("lnd: decode invoice: %w", err)
	}
	return node.DecodedInvoice{
		Hash:            reply.PaymentHash,
		Destination:     reply.Destination,
		Description:     reply.Description,
		DescriptionHash: reply.DescriptionHash,
		AmountMsat:      reply.NumMsat,
		CreatedAt:       time.Unix(reply.Timestamp, 0).UTC(),
		ExpiresAt:       time.Unix(reply.Timestamp+reply.Expiry, 0).UTC(),
	}, nil
}

// Pay implements node.Node.
func (c *Client) Pay(ctx context.Context, request node.PayRequest) (node.Payment, error) {
	if err := request.Check(); err != nil {
		return node.Payment{}, err
	}
	stream, err := c.router.SendPaymentV2(ctx, &routerrpc.SendPaymentRequest{
		PaymentRequest:    request.Invoice,
		AmtMsat:           request.AmountMsat,
		FeeLimitMsat:      request.FeeLimitMsat,
		TimeoutSeconds:    int32(request.Timeout / time.Second),
		NoInflightUpdates: true,
	})
	if err != nil {
		return node.Payment{}, fmt.Errorf("lnd: send payment: %w", err)
	}
	for {
		update, err := stream.Recv()
		if err != nil {
			return node.Payment{}, fmt.Errorf("lnd: send payment: %w", err)
		}
		if payment := paymentFrom(update); payment.State != node.PaymentInFlight {
			return payment, nil
		}
	}
}

// TrackPayment implements node.Node.
func (c *Client) TrackPayment(ctx context.Context, hash string) (node.Payment, error) {
	raw, err := decodeHash(hash)
	if err != nil {
		return node.Payment{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel() // Ends the stream: only its first update is wanted.
	stream, err := c.router.TrackPaymentV2(ctx, &routerrpc.TrackPaymentRequest{PaymentHash: raw})
	if err == nil {
		var update *lnrpc.Payment
		if update, err = stream.Recv(); err == nil {
			return paymentFrom(update), nil
		}
	}
	if isNoRecord(err) {
		return node.Payment{State: node.PaymentNoRecord, Hash: hash}, nil
	}
	return node.Payment{}, fmt.Errorf("lnd: track payment: %w", err)
}

// isNoRecord is true only for the node's own answer. This is the one place that
// lets a hold go without a payment result, so everything else must fail it.
func isNoRecord(err error) bool {
	return status.Code(err) == codes.NotFound && strings.Contains(status.Convert(err).Message(), noPaymentRecord)
}

// paymentFrom reads a payment update. Only the two final statuses are final:
// anything else, including a status a later LND adds, stays in flight.
func paymentFrom(update *lnrpc.Payment) node.Payment {
	payment := node.Payment{
		Hash:       update.PaymentHash,
		AmountMsat: update.ValueMsat,
		FeeMsat:    update.FeeMsat,
	}
	switch update.Status {
	case lnrpc.Payment_SUCCEEDED:
		payment.State = node.PaymentSucceeded
		payment.Preimage = update.PaymentPreimage
	case lnrpc.Payment_FAILED:
		payment.State = node.PaymentFailed
		payment.FailureReason = update.FailureReason.String()
	default:
		payment.State = node.PaymentInFlight
	}
	return payment
}

// SubscribeInvoices implements node.Node.
func (c *Client) SubscribeInvoices(ctx context.Context, fromSettleIndex uint64, onSettled func(node.Invoice) error) error {
	stream, err := c.lightning.SubscribeInvoices(ctx, &lnrpc.InvoiceSubscription{SettleIndex: fromSettleIndex})
	if err != nil {
		return fmt.Errorf("lnd: subscribe invoices: %w", err)
	}
	for {
		update, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return errors.New("lnd: invoice stream ended")
		}
		if err != nil {
			return fmt.Errorf("lnd: invoice stream: %w", err)
		}
		// The stream also carries each new invoice, which is not income.
		if update.State != lnrpc.Invoice_SETTLED {
			continue
		}
		if err := onSettled(invoiceFrom(update)); err != nil {
			return err
		}
	}
}
