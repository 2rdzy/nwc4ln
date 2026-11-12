// Package cln is the node.Node for Core Lightning, over its REST plugin
// (clnrest) with a rune that allows only the methods in Methods.
//
// Core Lightning differs from LND in ways the rest of nwc4ln must allow for:
//
//   - CancelInvoice deletes the invoice, so a second cancel fails, and a lookup
//     afterwards gives node.ErrNotFound.
//   - An unpaid invoice does not show its preimage, so this client makes the
//     preimage itself, and only MakeInvoice returns it before payment.
//   - SubscribeInvoices from index 0 replays every invoice ever paid.
//   - Info has no block hash.
//   - An invoice with a description hash needs the description itself.
package cln

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/2rdzy/nwc4ln/internal/node"
)

// Methods lists every method the client calls, and nothing else. The operator
// makes the rune with these, each as a "method=" alternative in one restriction.
var Methods = []string{
	"getinfo",
	"listpeerchannels",
	"invoice",
	"listinvoices",
	"delinvoice",
	"decode",
	"xpay",
	"listsendpays",
	"waitanyinvoice",
}

const (
	// callTimeout limits a call that should answer at once. Pay and
	// SubscribeInvoices run until their context ends.
	callTimeout = 30 * time.Second
	// maxRetry caps how long xpay may start new attempts. xpay starts none after
	// it, except a first one.
	maxRetry = 60 * time.Second
	// quietAfterRetry is how long after the last attempt began a payment whose
	// attempts all failed still counts as in flight. xpay can start a new attempt
	// after the last one failed, so "all failed" is final only once no new attempt
	// can come.
	quietAfterRetry = maxRetry + time.Minute
	// waitSeconds is how long one waitanyinvoice call waits before it times out.
	waitSeconds = 60
	// labelPrefix marks invoices made by nwc4ln, so their labels never clash
	// with the operator's.
	labelPrefix = "nwc4ln-"
	// anchorMsat is one anchor output, which the opener of an anchor channel pays
	// for (BOLT 3).
	anchorMsat = 330_000
	// minDustMsat is the lowest dust limit a channel may have (BOLT 2). An output
	// below it never exists, whoever's dust limit applies.
	minDustMsat = 354_000
	// htlcWeight is what one HTLC output adds to a commitment, in weight units.
	htlcWeight = 172
	msatPerSat = 1000
)

// Error codes from Core Lightning's JSON-RPC (common/jsonrpc_errors.h).
const (
	codeInvoiceNotFound   = 905
	codeInvoiceStatus     = 906
	codeWaitTimedOut      = 904
	codeAlreadyPaid       = 219
	statusPaid            = "paid"
	statusUnpaid          = "unpaid"
	statusExpired         = "expired"
	sendPayComplete       = "complete"
	sendPayPending        = "pending"
	sendPayFailed         = "failed"
	stateNormal           = "CHANNELD_NORMAL"
	stateAwaitingSplice   = "CHANNELD_AWAITING_SPLICE"
	featureAnchors        = 20
	featureAnchorsZeroFee = 22
)

// RPCError is an answer from the node: a JSON-RPC error with its code. Anything
// else that goes wrong in a call, such as a lost connection, is a plain error.
type RPCError struct {
	Method  string
	Code    int
	Message string
	Data    json.RawMessage
}

func (e *RPCError) Error() string {
	return fmt.Sprintf("cln: %s: %s (code %d)", e.Method, e.Message, e.Code)
}

func rpcCode(err error) (int, bool) {
	var rpc *RPCError
	if errors.As(err, &rpc) {
		return rpc.Code, true
	}
	return 0, false
}

// Config says how to reach the node.
type Config struct {
	// URL is the clnrest address, such as https://127.0.0.1:3010.
	URL string
	// CACertPath is clnrest's ca.pem, which signed its server certificate.
	CACertPath string
	// RunePath is a file holding the rune, readable by the service user only.
	RunePath string
}

// Option changes a client. Options are for tests.
type Option func(*Client)

// OnCall has the client report each method before it calls it.
func OnCall(observe func(method string)) Option {
	return func(c *Client) { c.observe = observe }
}

// WithClock sets the clock that TrackPayment reads.
func WithClock(now func() time.Time) Option {
	return func(c *Client) { c.now = now }
}

// Client is a node.Node on Core Lightning. It holds the rune only in memory.
type Client struct {
	base    string
	http    *http.Client
	rune    string
	observe func(string)
	now     func() time.Time

	mu    sync.Mutex
	ownID string // The node's public key, once read.
}

var _ node.Node = (*Client)(nil)

// New prepares a client. It does not connect: the first call does.
func New(cfg Config, options ...Option) (*Client, error) {
	if cfg.URL == "" || cfg.CACertPath == "" || cfg.RunePath == "" {
		return nil, errors.New("cln: URL, CA cert path and rune path are all required")
	}
	parsed, err := url.Parse(cfg.URL)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" {
		return nil, fmt.Errorf("cln: URL must be https://host:port, got %q", cfg.URL)
	}
	rune, err := readRune(cfg.RunePath)
	if err != nil {
		return nil, err
	}
	pem, err := os.ReadFile(cfg.CACertPath)
	if err != nil {
		return nil, fmt.Errorf("cln: read CA cert: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("cln: no certificate in %s", cfg.CACertPath)
	}
	c := &Client{
		base: strings.TrimSuffix(cfg.URL, "/"),
		http: &http.Client{Transport: &http.Transport{
			TLSClientConfig:     &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12},
			TLSHandshakeTimeout: 15 * time.Second,
			MaxIdleConnsPerHost: 4,
		}},
		rune: rune,
		now:  time.Now,
	}
	for _, option := range options {
		option(c)
	}
	return c, nil
}

// readRune reads the rune, and refuses a file that others can read, because the
// rune can pay from the node.
func readRune(path string) (string, error) {
	info, err := os.Stat(path)
	if err != nil {
		return "", fmt.Errorf("cln: rune file: %w", err)
	}
	if mode := info.Mode().Perm(); mode&0o077 != 0 {
		return "", fmt.Errorf("cln: rune file %s has mode %#o: allow the service user only (chmod 600)", path, mode)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("cln: read rune: %w", err)
	}
	rune := strings.TrimSpace(string(data))
	if rune == "" {
		return "", fmt.Errorf("cln: rune file %s is empty", path)
	}
	return rune, nil
}

// Close drops idle connections.
func (c *Client) Close() error {
	c.http.CloseIdleConnections()
	return nil
}

// call runs one method. A JSON-RPC error comes back as *RPCError.
func (c *Client) call(ctx context.Context, method string, params, out any) error {
	if c.observe != nil {
		c.observe(method)
	}
	if params == nil {
		params = struct{}{}
	}
	body, err := json.Marshal(params)
	if err != nil {
		return fmt.Errorf("cln: %s: %w", method, err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, c.base+"/v1/"+method, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("cln: %s: %w", method, err)
	}
	request.Header.Set("Rune", c.rune)
	request.Header.Set("Content-Type", "application/json")
	response, err := c.http.Do(request)
	if err != nil {
		return fmt.Errorf("cln: %s: %w", method, err)
	}
	defer response.Body.Close()
	data, err := io.ReadAll(io.LimitReader(response.Body, 64<<20))
	if err != nil {
		return fmt.Errorf("cln: %s: %w", method, err)
	}
	if response.StatusCode != http.StatusOK && response.StatusCode != http.StatusCreated {
		var rpc struct {
			Code    *int            `json:"code"`
			Message string          `json:"message"`
			Data    json.RawMessage `json:"data"`
		}
		if json.Unmarshal(data, &rpc) == nil && rpc.Code != nil {
			return &RPCError{Method: method, Code: *rpc.Code, Message: rpc.Message, Data: rpc.Data}
		}
		return fmt.Errorf("cln: %s: HTTP %s: %.200s", method, response.Status, data)
	}
	if err := json.Unmarshal(data, out); err != nil {
		return fmt.Errorf("cln: %s: bad reply: %w", method, err)
	}
	return nil
}

// quick runs a call that should answer at once.
func (c *Client) quick(ctx context.Context, method string, params, out any) error {
	ctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	return c.call(ctx, method, params, out)
}

func checkHash(hash string) error {
	data, err := hex.DecodeString(hash)
	if err != nil || len(data) != 32 {
		return fmt.Errorf("cln: bad payment hash %q", hash)
	}
	return nil
}

// Info implements node.Node. Core Lightning does not give a block hash.
func (c *Client) Info(ctx context.Context) (node.Info, error) {
	var reply struct {
		ID              string `json:"id"`
		Alias           string `json:"alias"`
		Color           string `json:"color"`
		Network         string `json:"network"`
		BlockHeight     uint32 `json:"blockheight"`
		WarningBitcoind string `json:"warning_bitcoind_sync"`
		WarningLNSync   string `json:"warning_lightningd_sync"`
	}
	if err := c.quick(ctx, "getinfo", nil, &reply); err != nil {
		return node.Info{}, err
	}
	if reply.ID == "" {
		return node.Info{}, errors.New("cln: getinfo gave no node id")
	}
	c.mu.Lock()
	c.ownID = reply.ID
	c.mu.Unlock()
	network := reply.Network
	if network == "bitcoin" { // As LND and NIP-47 name it.
		network = "mainnet"
	}
	return node.Info{
		PubKey:      reply.ID,
		Alias:       reply.Alias,
		Color:       reply.Color,
		Network:     network,
		BlockHeight: reply.BlockHeight,
		Synced:      reply.WarningBitcoind == "" && reply.WarningLNSync == "",
	}, nil
}

// nodeID returns the node's public key, reading it once.
func (c *Client) nodeID(ctx context.Context) (string, error) {
	c.mu.Lock()
	id := c.ownID
	c.mu.Unlock()
	if id != "" {
		return id, nil
	}
	info, err := c.Info(ctx)
	return info.PubKey, err
}

type channel struct {
	State         string `json:"state"`
	PeerConnected bool   `json:"peer_connected"`
	Opener        string `json:"opener"`
	ChannelID     string `json:"channel_id"`
	ToUsMsat      *int64 `json:"to_us_msat"`
	TotalMsat     int64  `json:"total_msat"`
	ReserveMsat   *int64 `json:"our_reserve_msat"`
	LastFeeMsat   int64  `json:"last_tx_fee_msat"`
	DustMsat      int64  `json:"dust_limit_msat"`
	Feerate       struct {
		PerKW int64 `json:"perkw"`
	} `json:"feerate"`
	Type struct {
		Bits []int `json:"bits"`
	} `json:"channel_type"`
	HTLCs []struct {
		Direction  string `json:"direction"`
		AmountMsat int64  `json:"amount_msat"`
		Hash       string `json:"payment_hash"`
	} `json:"htlcs"`
}

// Spendable implements node.Node.
func (c *Client) Spendable(ctx context.Context) (node.Spendable, error) {
	var reply struct {
		Channels []channel `json:"channels"`
	}
	if err := c.quick(ctx, "listpeerchannels", nil, &reply); err != nil {
		return node.Spendable{}, err
	}
	return spendable(reply.Channels)
}

// spendable gives the figure LND would give for the same channels, so that the
// money rules read the same on both.
//
// Core Lightning's to_us_msat still holds outgoing HTLCs, and on a channel we
// opened it also holds the commitment fee and the anchors, all of which LND leaves
// out of local_balance (measured on the same channel from both sides). So those
// are taken off here, then the reserve, and each channel counts for at least 0.
// Only open channels count: a closing channel's balance is not spendable.
func spendable(channels []channel) (node.Spendable, error) {
	var total node.Spendable
	for _, ch := range channels {
		if ch.State != stateNormal && ch.State != stateAwaitingSplice {
			continue
		}
		if ch.ToUsMsat == nil || ch.ReserveMsat == nil {
			return node.Spendable{}, fmt.Errorf("cln: channel %s has no balance or reserve", ch.ChannelID)
		}
		active := ch.State == stateNormal && ch.PeerConnected
		local := *ch.ToUsMsat
		opener := ch.Opener == "local"
		anchors := slices.Contains(ch.Type.Bits, featureAnchorsZeroFee) || slices.Contains(ch.Type.Bits, featureAnchors)
		var outgoing int64
		for _, htlc := range ch.HTLCs {
			if htlc.Direction == "out" {
				outgoing += htlc.AmountMsat
			}
		}
		if opener {
			local -= ch.LastFeeMsat
			if anchors {
				local -= anchorMsat * anchorCount(ch, outgoing)
			}
		}
		for _, htlc := range ch.HTLCs {
			if htlc.Direction != "out" {
				continue
			}
			local -= htlc.AmountMsat
			total.Outgoing = append(total.Outgoing, node.HTLC{
				Hash:       htlc.Hash,
				AmountMsat: htlc.AmountMsat,
				CostMsat:   htlc.AmountMsat + htlcFeeMsat(opener, anchors, ch.Feerate.PerKW, ch.DustMsat, htlc.AmountMsat),
				Active:     active,
			})
		}
		free := max(local-*ch.ReserveMsat, 0)
		total.AllMsat += free
		if active {
			total.ActiveMsat += free
		}
	}
	return total, nil
}

// anchorCount gives how many anchor outputs a commitment has. Each side's anchor
// exists only while that side has an output or any HTLC has one (BOLT 3): with a
// new channel, the peer often has none. When it is unclear, because an output is
// near a dust limit, the anchor counts, so the figure errs low.
func anchorCount(ch channel, outgoing int64) int64 {
	for _, htlc := range ch.HTLCs {
		if htlc.AmountMsat >= minDustMsat {
			return 2
		}
	}
	var count int64
	if *ch.ToUsMsat-outgoing-ch.LastFeeMsat >= minDustMsat {
		count++
	}
	if ch.TotalMsat-*ch.ToUsMsat >= minDustMsat {
		count++
	}
	return count
}

// htlcFeeMsat is the commitment fee one outgoing HTLC takes, as in the LND
// client: only the opener pays it, and only anchor channels are counted, where
// an HTLC has an output exactly when it is not below the dust limit. Older
// channel types give 0, which undercounts.
func htlcFeeMsat(opener, anchors bool, perKW, dustMsat, amountMsat int64) int64 {
	if !opener || !anchors || amountMsat/msatPerSat < dustMsat/msatPerSat {
		return 0
	}
	return htlcWeight * perKW / 1000 * msatPerSat
}

// MakeInvoice implements node.Node. The client makes the preimage, because Core
// Lightning shows it only after payment, and the ledger needs it before (R10).
//
// A description hash needs the description itself, which Core Lightning hashes:
// it cannot take a bare hash.
func (c *Client) MakeInvoice(ctx context.Context, request node.InvoiceRequest) (node.Invoice, error) {
	if request.AmountMsat < 0 {
		return node.Invoice{}, fmt.Errorf("cln: negative invoice amount %d", request.AmountMsat)
	}
	var preimage [32]byte
	if _, err := rand.Read(preimage[:]); err != nil {
		return node.Invoice{}, fmt.Errorf("cln: make preimage: %w", err)
	}
	sum := sha256.Sum256(preimage[:])
	hash := hex.EncodeToString(sum[:])
	params := map[string]any{
		"amount_msat": "any",
		"label":       labelPrefix + hash,
		"description": request.Description,
		"preimage":    hex.EncodeToString(preimage[:]),
	}
	if request.AmountMsat > 0 {
		params["amount_msat"] = request.AmountMsat
	}
	if request.ExpirySeconds > 0 {
		params["expiry"] = request.ExpirySeconds
	}
	if request.DescriptionHash != "" {
		text := sha256.Sum256([]byte(request.Description))
		if !strings.EqualFold(hex.EncodeToString(text[:]), request.DescriptionHash) {
			return node.Invoice{}, fmt.Errorf("cln: %w", node.ErrNeedsDescription)
		}
		params["deschashonly"] = true
	}
	var reply struct {
		Hash string `json:"payment_hash"`
	}
	if err := c.quick(ctx, "invoice", params, &reply); err != nil {
		return node.Invoice{}, err
	}
	if reply.Hash != hash {
		return node.Invoice{}, fmt.Errorf("cln: invoice hash %s, want %s", reply.Hash, hash)
	}
	invoice, err := c.LookupInvoice(ctx, hash)
	if err != nil {
		return node.Invoice{}, err
	}
	invoice.Preimage = params["preimage"].(string)
	return invoice, nil
}

type invoiceReply struct {
	Label        string `json:"label"`
	Hash         string `json:"payment_hash"`
	Status       string `json:"status"`
	Bolt11       string `json:"bolt11"`
	AmountMsat   int64  `json:"amount_msat"`
	ReceivedMsat int64  `json:"amount_received_msat"`
	PayIndex     uint64 `json:"pay_index"`
	PaidAt       int64  `json:"paid_at"`
	ExpiresAt    int64  `json:"expires_at"`
	Preimage     string `json:"payment_preimage"`
}

// LookupInvoice implements node.Node. An unpaid invoice comes back with no
// preimage.
func (c *Client) LookupInvoice(ctx context.Context, hash string) (node.Invoice, error) {
	found, err := c.findInvoice(ctx, hash)
	if err != nil {
		return node.Invoice{}, err
	}
	return c.invoiceFrom(ctx, found)
}

func (c *Client) findInvoice(ctx context.Context, hash string) (invoiceReply, error) {
	if err := checkHash(hash); err != nil {
		return invoiceReply{}, err
	}
	var reply struct {
		Invoices []invoiceReply `json:"invoices"`
	}
	if err := c.quick(ctx, "listinvoices", map[string]any{"payment_hash": hash}, &reply); err != nil {
		return invoiceReply{}, err
	}
	for _, invoice := range reply.Invoices {
		if invoice.Hash == hash && invoice.Bolt11 != "" {
			return invoice, nil
		}
	}
	return invoiceReply{}, fmt.Errorf("cln: invoice %s: %w", hash, node.ErrNotFound)
}

// invoiceFrom reads an invoice. Its creation time and description are read from
// the invoice itself, because the list gives neither in full.
//
// A BOLT 12 invoice has no BOLT 11 string to read. nwc4ln never makes one, so it
// only ever arrives from the stream, and counts as main wallet: it is returned
// without those fields, rather than stopping the stream for good.
func (c *Client) invoiceFrom(ctx context.Context, found invoiceReply) (node.Invoice, error) {
	var decoded node.DecodedInvoice
	if found.Bolt11 != "" {
		var err error
		if decoded, err = c.DecodeInvoice(ctx, found.Bolt11); err != nil {
			return node.Invoice{}, err
		}
	}
	invoice := node.Invoice{
		Hash:            found.Hash,
		PaymentRequest:  found.Bolt11,
		Description:     decoded.Description,
		DescriptionHash: decoded.DescriptionHash,
		AmountMsat:      found.AmountMsat,
		State:           invoiceState(found.Status),
		CreatedAt:       decoded.CreatedAt,
		ExpiresAt:       time.Unix(found.ExpiresAt, 0).UTC(),
	}
	if invoice.State == node.InvoiceSettled {
		invoice.Preimage = found.Preimage
		invoice.PaidMsat = found.ReceivedMsat
		invoice.SettleIndex = found.PayIndex
		invoice.SettledAt = time.Unix(found.PaidAt, 0).UTC()
	}
	return invoice, nil
}

// invoiceState reads only the two final states. An expired invoice can never be
// paid on Core Lightning, so it counts as canceled. Anything else is open, so it
// can never credit an account.
func invoiceState(status string) node.InvoiceState {
	switch status {
	case statusPaid:
		return node.InvoiceSettled
	case statusExpired:
		return node.InvoiceCanceled
	default:
		return node.InvoiceOpen
	}
}

// CancelInvoice implements node.Node by deleting the invoice. Core Lightning
// checks the status in the same step, so a paid invoice is never deleted, and an
// invoice it no longer has can never settle. A second cancel finds no invoice
// and fails, as does a cancel of an unknown one.
func (c *Client) CancelInvoice(ctx context.Context, hash string) error {
	found, err := c.findInvoice(ctx, hash)
	if err != nil {
		return fmt.Errorf("cln: cancel invoice: %w", err)
	}
	status := found.Status
	for range 2 {
		if status != statusUnpaid && status != statusExpired {
			return fmt.Errorf("cln: cancel invoice %s: status is %s", hash, status)
		}
		var reply json.RawMessage
		err = c.quick(ctx, "delinvoice", map[string]any{"label": found.Label, "status": status}, &reply)
		if code, ok := rpcCode(err); ok && code == codeInvoiceStatus {
			// The status changed after the lookup: try once more with the new one.
			var data struct {
				Current string `json:"current_status"`
			}
			var rpc *RPCError
			errors.As(err, &rpc)
			if json.Unmarshal(rpc.Data, &data) == nil && data.Current != "" && data.Current != status {
				status = data.Current
				continue
			}
		}
		break
	}
	if err != nil {
		return fmt.Errorf("cln: cancel invoice: %w", err)
	}
	return nil
}

// DecodeInvoice implements node.Node.
func (c *Client) DecodeInvoice(ctx context.Context, invoice string) (node.DecodedInvoice, error) {
	var reply struct {
		Type            string `json:"type"`
		Valid           bool   `json:"valid"`
		Hash            string `json:"payment_hash"`
		Payee           string `json:"payee"`
		Description     string `json:"description"`
		DescriptionHash string `json:"description_hash"`
		AmountMsat      int64  `json:"amount_msat"`
		CreatedAt       int64  `json:"created_at"`
		Expiry          int64  `json:"expiry"`
	}
	if err := c.quick(ctx, "decode", map[string]any{"string": invoice}, &reply); err != nil {
		return node.DecodedInvoice{}, err
	}
	if !reply.Valid || reply.Type != "bolt11 invoice" || reply.Hash == "" {
		return node.DecodedInvoice{}, fmt.Errorf("cln: not a valid BOLT 11 invoice (type %q)", reply.Type)
	}
	return node.DecodedInvoice{
		Hash:            reply.Hash,
		Destination:     reply.Payee,
		Description:     reply.Description,
		DescriptionHash: reply.DescriptionHash,
		AmountMsat:      reply.AmountMsat,
		CreatedAt:       time.Unix(reply.CreatedAt, 0).UTC(),
		ExpiresAt:       time.Unix(reply.CreatedAt+reply.Expiry, 0).UTC(),
	}, nil
}

// Pay implements node.Node with xpay.
//
// Only success is final here. Any error from xpay, a failure included, says
// nothing the ledger may act on: whether xpay can still start or record a part
// after it answers depends on how it works inside, which can change with a
// version. So the caller asks TrackPayment, which counts every part failed as
// final only once no new attempt can come. A payment with no route therefore
// reads as pending for a few minutes on Core Lightning, where LND fails it at once
// (owner, SR1 STOP 1).
//
// An invoice from this node is refused before xpay, which would pay it: LND
// refuses, and the ledger settles those itself (R10).
func (c *Client) Pay(ctx context.Context, request node.PayRequest) (node.Payment, error) {
	if err := request.Check(); err != nil {
		return node.Payment{}, err
	}
	decoded, err := c.DecodeInvoice(ctx, request.Invoice)
	if err != nil {
		return node.Payment{}, err
	}
	own, err := c.nodeID(ctx)
	if err != nil {
		return node.Payment{}, err
	}
	if decoded.Destination == own {
		return node.Payment{}, errors.New("cln: refusing to pay an invoice from this node")
	}
	retry := min(request.Timeout, maxRetry)
	params := map[string]any{
		"invstring": request.Invoice,
		"maxfee":    request.FeeLimitMsat,
		"retry_for": int64(retry / time.Second),
	}
	if request.AmountMsat > 0 {
		params["amount_msat"] = request.AmountMsat
	}
	var reply struct {
		Preimage   string `json:"payment_preimage"`
		AmountMsat int64  `json:"amount_msat"`
		SentMsat   int64  `json:"amount_sent_msat"`
	}
	if err := c.call(ctx, "xpay", params, &reply); err != nil {
		return node.Payment{}, err
	}
	return node.Payment{
		State: node.PaymentSucceeded, Hash: decoded.Hash, Preimage: reply.Preimage,
		AmountMsat: reply.AmountMsat, FeeMsat: reply.SentMsat - reply.AmountMsat,
	}, nil
}

type part struct {
	Status     string `json:"status"`
	GroupID    uint64 `json:"groupid"`
	AmountMsat int64  `json:"amount_msat"`
	SentMsat   int64  `json:"amount_sent_msat"`
	Preimage   string `json:"payment_preimage"`
	CreatedAt  int64  `json:"created_at"`
}

func (c *Client) parts(ctx context.Context, hash string) ([]part, error) {
	var reply struct {
		Payments *[]part `json:"payments"`
	}
	if err := c.quick(ctx, "listsendpays", map[string]any{"payment_hash": hash}, &reply); err != nil {
		return nil, err
	}
	if reply.Payments == nil {
		return nil, errors.New("cln: listsendpays: reply has no payments list")
	}
	return *reply.Payments, nil
}

// TrackPayment implements node.Node from the payment's parts. Any part that
// succeeded means the invoice is paid. Any part in flight keeps the payment in
// flight. When every part failed, xpay may still start another attempt, so the
// payment counts as failed only once the last part began longer ago than xpay
// can keep trying. No parts at all is the node's own answer: no record.
func (c *Client) TrackPayment(ctx context.Context, hash string) (node.Payment, error) {
	if err := checkHash(hash); err != nil {
		return node.Payment{}, err
	}
	parts, err := c.parts(ctx, hash)
	if err != nil {
		return node.Payment{}, err
	}
	return track(hash, parts, c.now()), nil
}

func track(hash string, parts []part, now time.Time) node.Payment {
	if len(parts) == 0 {
		return node.Payment{State: node.PaymentNoRecord, Hash: hash}
	}
	for _, p := range parts {
		if p.Status != sendPayComplete {
			continue
		}
		payment := node.Payment{State: node.PaymentSucceeded, Hash: hash, Preimage: p.Preimage}
		for _, q := range parts {
			if q.GroupID == p.GroupID && q.Status == sendPayComplete {
				payment.AmountMsat += q.AmountMsat
				payment.FeeMsat += q.SentMsat - q.AmountMsat
			}
		}
		return payment
	}
	var last int64
	for _, p := range parts {
		if p.Status != sendPayFailed {
			// Pending, or a status a later version adds: the hold stays.
			return node.Payment{State: node.PaymentInFlight, Hash: hash}
		}
		last = max(last, p.CreatedAt)
	}
	if now.Before(time.Unix(last, 0).Add(quietAfterRetry)) {
		return node.Payment{State: node.PaymentInFlight, Hash: hash}
	}
	return node.Payment{State: node.PaymentFailed, Hash: hash, FailureReason: "every attempt failed"}
}

// SubscribeInvoices implements node.Node with waitanyinvoice, which waits for
// the next invoice paid after an index. From index 0 it starts at the first
// invoice ever paid, so it replays them all. Crediting is idempotent, and an
// invoice nwc4ln did not make counts as main wallet, so a replay is safe.
func (c *Client) SubscribeInvoices(ctx context.Context, fromSettleIndex uint64, onSettled func(node.Invoice) error) error {
	index := fromSettleIndex
	for {
		var paid invoiceReply
		wait, cancel := context.WithTimeout(ctx, (waitSeconds+30)*time.Second)
		err := c.call(wait, "waitanyinvoice", map[string]any{"lastpay_index": index, "timeout": waitSeconds}, &paid)
		cancel()
		if code, ok := rpcCode(err); ok && code == codeWaitTimedOut {
			continue
		}
		if err != nil {
			return fmt.Errorf("cln: invoice stream: %w", err)
		}
		if paid.Status != statusPaid || paid.PayIndex <= index {
			return fmt.Errorf("cln: invoice stream: invoice %s has status %s and pay index %d after %d",
				paid.Hash, paid.Status, paid.PayIndex, index)
		}
		invoice, err := c.invoiceFrom(ctx, paid)
		if err != nil {
			return fmt.Errorf("cln: invoice stream: %w", err)
		}
		if err := onSettled(invoice); err != nil {
			return err
		}
		index = paid.PayIndex
	}
}
