// Package fake is a scripted node.Node for unit tests. It runs in memory, and it
// keeps LND's awkward behaviour, so that a test passing here means something:
// index 0 replays nothing, a refused payment leaves no record, and a timeout
// says nothing about the payment. internal/node/nodetest runs the same tests on
// this fake and on LND.
package fake

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/2rdzy/nwc4ln/internal/node"
)

// ErrUnreachable is what every call returns while the node is unreachable.
var ErrUnreachable = errors.New("fake: node unreachable")

// ErrPayTimeout is what Pay returns for a PayTimeout step.
var ErrPayTimeout = errors.New("fake: payment timed out")

// PayStep says what the next Pay does. With no step queued, Pay succeeds.
type PayStep int

// Pay steps.
const (
	// PaySucceed finishes the payment at once.
	PaySucceed PayStep = iota
	// PayFail fails the payment at once.
	PayFail
	// PayTimeout returns ErrPayTimeout and leaves the payment in flight. Call
	// Finish to end it.
	PayTimeout
	// PayLost returns ErrPayTimeout and the node keeps no record, like a crash
	// before the request reached the node.
	PayLost
)

const pubKey = "02fake"

// Node is the fake. The zero value is not usable: call New.
type Node struct {
	mu          sync.Mutex
	changed     chan struct{} // Closed and replaced on each settlement or outage.
	unreachable bool
	spendable   node.Spendable
	routeFee    int64
	nextIndex   uint64
	steps       []PayStep
	cancelErrs  []error
	requests    []node.PayRequest
	invoices    map[string]*node.Invoice        // Made on this node, by hash.
	outside     map[string]*node.DecodedInvoice // Payable, by invoice string.
	payments    map[string]*node.Payment        // By hash.
	preimages   map[string]string               // Of outside invoices, by hash.
}

var _ node.Node = (*Node)(nil)

// New returns a reachable fake with no money and no invoices.
func New() *Node {
	return &Node{
		changed:   make(chan struct{}),
		invoices:  map[string]*node.Invoice{},
		outside:   map[string]*node.DecodedInvoice{},
		payments:  map[string]*node.Payment{},
		preimages: map[string]string{},
	}
}

// SetUnreachable makes every call fail, and ends subscriptions, until cleared.
func (n *Node) SetUnreachable(unreachable bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.unreachable = unreachable
	n.wake()
}

// SetSpendable sets what Spendable reports.
func (n *Node) SetSpendable(spendable node.Spendable) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.spendable = spendable
}

// SetRouteFee sets the fee of the next payments. One above its fee limit fails.
func (n *Node) SetRouteFee(msat int64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.routeFee = msat
}

// ScriptPay queues what the next calls to Pay do.
func (n *Node) ScriptPay(steps ...PayStep) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.steps = append(n.steps, steps...)
}

// FailNextCancel makes the next CancelInvoice return err and change nothing.
func (n *Node) FailNextCancel(err error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.cancelErrs = append(n.cancelErrs, err)
}

// PayRequests returns each request Pay received, in order.
func (n *Node) PayRequests() []node.PayRequest {
	n.mu.Lock()
	defer n.mu.Unlock()
	return slices.Clone(n.requests)
}

// OutsideInvoice makes an invoice from someone else, which Pay can pay.
func (n *Node) OutsideInvoice(amountMsat int64) string {
	n.mu.Lock()
	defer n.mu.Unlock()
	preimage, hash := newSecret()
	n.preimages[hash] = preimage
	now := time.Now().UTC().Truncate(time.Second)
	invoice := "lnfake-outside-" + hash
	n.outside[invoice] = &node.DecodedInvoice{
		Hash: hash, AmountMsat: amountMsat, Destination: "03outside",
		CreatedAt: now, ExpiresAt: now.Add(time.Hour),
	}
	return invoice
}

// Finish ends a payment left in flight by PayTimeout.
func (n *Node) Finish(hash string, succeeded bool) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	payment, ok := n.payments[hash]
	if !ok || payment.State != node.PaymentInFlight {
		return fmt.Errorf("fake: no payment in flight for %s", hash)
	}
	n.finish(payment, succeeded)
	return nil
}

func (n *Node) finish(payment *node.Payment, succeeded bool) {
	if succeeded {
		payment.State = node.PaymentSucceeded
		payment.Preimage = n.preimages[payment.Hash]
		return
	}
	payment.State = node.PaymentFailed
	payment.FailureReason = "FAILURE_REASON_NO_ROUTE"
	payment.FeeMsat = 0
}

// Settle pays an invoice from outside, as a payer on the network would.
func (n *Node) Settle(hash string) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	invoice, ok := n.invoices[hash]
	if !ok {
		return fmt.Errorf("fake: no invoice %s", hash)
	}
	if invoice.State != node.InvoiceOpen {
		return fmt.Errorf("fake: invoice %s is not open", hash)
	}
	n.nextIndex++
	invoice.State = node.InvoiceSettled
	invoice.PaidMsat = invoice.AmountMsat
	invoice.SettleIndex = n.nextIndex
	invoice.SettledAt = time.Now().UTC().Truncate(time.Second)
	n.wake()
	return nil
}

func (n *Node) wake() {
	close(n.changed)
	n.changed = make(chan struct{})
}

func newSecret() (preimage, hash string) {
	secret := make([]byte, 32)
	_, _ = rand.Read(secret) // Never fails: see crypto/rand.
	sum := sha256.Sum256(secret)
	return hex.EncodeToString(secret), hex.EncodeToString(sum[:])
}

// enter takes the lock for one call, and fails if the node is unreachable or ctx
// has ended. On success the caller must unlock.
func (n *Node) enter(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	n.mu.Lock()
	if n.unreachable {
		n.mu.Unlock()
		return ErrUnreachable
	}
	return nil
}

// Info implements node.Node.
func (n *Node) Info(ctx context.Context) (node.Info, error) {
	if err := n.enter(ctx); err != nil {
		return node.Info{}, err
	}
	defer n.mu.Unlock()
	return node.Info{
		PubKey: pubKey, Alias: "fake", Color: "#000000", Network: "regtest",
		BlockHeight: 1, BlockHash: "00", Synced: true,
	}, nil
}

// Spendable implements node.Node.
func (n *Node) Spendable(ctx context.Context) (node.Spendable, error) {
	if err := n.enter(ctx); err != nil {
		return node.Spendable{}, err
	}
	defer n.mu.Unlock()
	return n.spendable, nil
}

// MakeInvoice implements node.Node.
func (n *Node) MakeInvoice(ctx context.Context, request node.InvoiceRequest) (node.Invoice, error) {
	if err := n.enter(ctx); err != nil {
		return node.Invoice{}, err
	}
	defer n.mu.Unlock()
	preimage, hash := newSecret()
	expiry := request.ExpirySeconds
	if expiry == 0 {
		expiry = 86400 // LND's default for a new invoice.
	}
	now := time.Now().UTC().Truncate(time.Second)
	invoice := &node.Invoice{
		Hash: hash, Preimage: preimage, PaymentRequest: "lnfake-" + hash, AmountMsat: request.AmountMsat,
		Description: request.Description, DescriptionHash: request.DescriptionHash,
		CreatedAt: now, ExpiresAt: now.Add(time.Duration(expiry) * time.Second),
	}
	if request.DescriptionHash != "" {
		invoice.Description = ""
	}
	n.invoices[hash] = invoice
	return *invoice, nil
}

// LookupInvoice implements node.Node.
func (n *Node) LookupInvoice(ctx context.Context, hash string) (node.Invoice, error) {
	if err := n.enter(ctx); err != nil {
		return node.Invoice{}, err
	}
	defer n.mu.Unlock()
	invoice, ok := n.invoices[hash]
	if !ok {
		return node.Invoice{}, fmt.Errorf("fake: invoice %s: %w", hash, node.ErrNotFound)
	}
	return *invoice, nil
}

// CancelInvoice implements node.Node.
func (n *Node) CancelInvoice(ctx context.Context, hash string) error {
	if err := n.enter(ctx); err != nil {
		return err
	}
	defer n.mu.Unlock()
	if len(n.cancelErrs) > 0 {
		err := n.cancelErrs[0]
		n.cancelErrs = n.cancelErrs[1:]
		return err
	}
	invoice, ok := n.invoices[hash]
	switch {
	case !ok:
		return errors.New("fake: unable to locate invoice")
	case invoice.State == node.InvoiceSettled:
		return errors.New("fake: invoice already settled")
	}
	invoice.State = node.InvoiceCanceled
	return nil
}

// DecodeInvoice implements node.Node.
func (n *Node) DecodeInvoice(ctx context.Context, invoice string) (node.DecodedInvoice, error) {
	if err := n.enter(ctx); err != nil {
		return node.DecodedInvoice{}, err
	}
	defer n.mu.Unlock()
	if decoded, ok := n.outside[invoice]; ok {
		return *decoded, nil
	}
	for _, own := range n.invoices {
		if own.PaymentRequest == invoice {
			return node.DecodedInvoice{
				Hash: own.Hash, Destination: pubKey, Description: own.Description,
				DescriptionHash: own.DescriptionHash, AmountMsat: own.AmountMsat,
				CreatedAt: own.CreatedAt, ExpiresAt: own.ExpiresAt,
			}, nil
		}
	}
	return node.DecodedInvoice{}, fmt.Errorf("fake: cannot decode invoice %q", invoice)
}

// Pay implements node.Node.
func (n *Node) Pay(ctx context.Context, request node.PayRequest) (node.Payment, error) {
	if err := request.Check(); err != nil {
		return node.Payment{}, err
	}
	if err := n.enter(ctx); err != nil {
		return node.Payment{}, err
	}
	defer n.mu.Unlock()
	n.requests = append(n.requests, request)
	for _, own := range n.invoices {
		if own.PaymentRequest == request.Invoice {
			return node.Payment{}, errors.New("fake: self-payments not allowed")
		}
	}
	decoded, ok := n.outside[request.Invoice]
	if !ok {
		return node.Payment{}, errors.New("fake: unknown invoice")
	}
	if payment, ok := n.payments[decoded.Hash]; ok && payment.State != node.PaymentFailed {
		return node.Payment{}, errors.New("fake: invoice is already paid")
	}
	step := PaySucceed
	if len(n.steps) > 0 {
		step = n.steps[0]
		n.steps = n.steps[1:]
	}
	if step == PayLost {
		return node.Payment{}, ErrPayTimeout
	}
	amount := decoded.AmountMsat
	if amount == 0 {
		amount = request.AmountMsat
	}
	payment := &node.Payment{Hash: decoded.Hash, AmountMsat: amount, FeeMsat: n.routeFee}
	n.payments[decoded.Hash] = payment
	switch {
	case step == PayTimeout:
		return node.Payment{}, ErrPayTimeout
	case step == PayFail, n.routeFee > request.FeeLimitMsat:
		n.finish(payment, false)
	default:
		n.finish(payment, true)
	}
	return *payment, nil
}

// TrackPayment implements node.Node.
func (n *Node) TrackPayment(ctx context.Context, hash string) (node.Payment, error) {
	if err := n.enter(ctx); err != nil {
		return node.Payment{}, err
	}
	defer n.mu.Unlock()
	if payment, ok := n.payments[hash]; ok {
		return *payment, nil
	}
	return node.Payment{State: node.PaymentNoRecord, Hash: hash}, nil
}

// SubscribeInvoices implements node.Node.
func (n *Node) SubscribeInvoices(ctx context.Context, fromSettleIndex uint64, onSettled func(node.Invoice) error) error {
	if err := n.enter(ctx); err != nil {
		return err
	}
	if fromSettleIndex == 0 {
		fromSettleIndex = n.nextIndex // Index 0 replays nothing, as on LND.
	}
	n.mu.Unlock()
	for {
		if err := n.enter(ctx); err != nil {
			return err
		}
		due := n.settledAfter(fromSettleIndex)
		changed := n.changed
		n.mu.Unlock()
		for _, invoice := range due {
			if err := onSettled(invoice); err != nil {
				return err
			}
			fromSettleIndex = invoice.SettleIndex
		}
		if len(due) > 0 {
			continue
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
		}
	}
}

func (n *Node) settledAfter(index uint64) []node.Invoice {
	var due []node.Invoice
	for invoice := range maps.Values(n.invoices) {
		if invoice.SettleIndex > index {
			due = append(due, *invoice)
		}
	}
	slices.SortFunc(due, func(a, b node.Invoice) int {
		if a.SettleIndex < b.SettleIndex {
			return -1
		}
		return 1
	})
	return due
}
