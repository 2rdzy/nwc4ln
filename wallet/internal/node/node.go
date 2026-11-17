// Package node is the one way nwc4ln talks to a Lightning node. Everything above
// it works with this interface, so that LND, Core Lightning and the test fake
// can stand in for each other.
//
// Amounts are integers in millisatoshis. Payment hashes and preimages are hex.
package node

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// ErrNotFound means the node answered that it does not know the invoice. It is
// never returned for a connection error.
var ErrNotFound = errors.New("not found on the node")

// ErrNeedsDescription means the node cannot make an invoice from a bare description
// hash: Core Lightning hashes the description itself, so it needs the text too.
var ErrNeedsDescription = errors.New("a description hash needs the description it hashes")

// Node is what nwc4ln needs from a Lightning node.
type Node interface {
	// Info describes the node.
	Info(ctx context.Context) (Info, error)
	// Spendable gives what the node can send now, for the money rules.
	Spendable(ctx context.Context) (Spendable, error)
	// MakeInvoice creates an invoice on the node.
	MakeInvoice(ctx context.Context, request InvoiceRequest) (Invoice, error)
	// LookupInvoice returns an invoice made on this node, or ErrNotFound. Before
	// payment the preimage may be missing (Core Lightning): only MakeInvoice must
	// return it.
	LookupInvoice(ctx context.Context, hash string) (Invoice, error)
	// CancelInvoice cancels an invoice. On success the invoice can never settle,
	// and any part already held is failed back to its payer. It also succeeds on
	// an invoice already canceled. It fails on a settled invoice, and on any
	// connection error, so after a failure the invoice may already be paid: never
	// settle it inside the ledger, and refuse the payment.
	//
	// Core Lightning deletes the invoice instead. A lookup then gives ErrNotFound,
	// and a second cancel fails like a cancel of an unknown invoice. So no caller
	// may need a repeated cancel to succeed.
	CancelInvoice(ctx context.Context, hash string) error
	// DecodeInvoice reads a BOLT 11 invoice. The node does this, so that nwc4ln
	// never rejects an invoice the node accepts.
	DecodeInvoice(ctx context.Context, invoice string) (DecodedInvoice, error)
	// Pay sends a payment and waits for its final state. An error says nothing
	// about the payment: it may be refused, in flight or already paid. After any
	// error, ask TrackPayment, and never mark the payment failed from the error.
	Pay(ctx context.Context, request PayRequest) (Payment, error)
	// TrackPayment returns what the node says about a payment now. It does not
	// wait for a final state. PaymentNoRecord is an answer from the node, so a
	// connection error is always an error and never PaymentNoRecord.
	TrackPayment(ctx context.Context, hash string) (Payment, error)
	// SubscribeInvoices calls onSettled in order for each invoice settled after
	// fromSettleIndex, then for each new one, until the stream ends or onSettled
	// returns an error. It always returns an error, because the stream has no
	// normal end. To carry on, call it again from the last SettleIndex seen.
	//
	// From index 0, LND sends nothing from the past, only new settlements, so an
	// invoice settled before the first index was stored is not sent here. Find
	// those with LookupInvoice. Core Lightning replays every invoice ever settled
	// instead. Crediting is idempotent, so the caller must handle both.
	SubscribeInvoices(ctx context.Context, fromSettleIndex uint64, onSettled func(Invoice) error) error
}

// Info describes a node.
type Info struct {
	PubKey      string
	Alias       string
	Color       string
	Network     string
	BlockHeight uint32
	// BlockHash is empty on Core Lightning, which does not give it.
	BlockHash string
	Synced    bool
}

// Spendable is what a node can send, after channel reserves. Each channel counts
// for at least 0, so one below its reserve takes nothing from the others.
//
// A payment in flight has already left both figures, as on LND. Outgoing lists
// what each such payment took, so that the ledger can add back the ones it also
// holds, and only those.
type Spendable struct {
	// ActiveMsat covers active channels: what can go out now.
	ActiveMsat int64
	// AllMsat covers all open channels, so a peer that is offline still counts.
	AllMsat int64
	// Outgoing lists the HTLCs in flight out of this node, on all open channels.
	Outgoing []HTLC
}

// HTLC is one part of an outgoing payment in flight. A payment split into parts
// has one HTLC per part, all with the same hash.
type HTLC struct {
	Hash       string
	AmountMsat int64
	// CostMsat is what the HTLC took from the figures: its amount, plus on LND the
	// commitment fee for its output when this node opened the channel. It returns
	// if the HTLC fails.
	CostMsat int64
	// Active says whether its channel counts in ActiveMsat.
	Active bool
}

// InvoiceState is the state of an invoice. The zero value is the only one that
// cannot credit anything.
type InvoiceState int

// Invoice states.
const (
	InvoiceOpen InvoiceState = iota
	InvoiceSettled
	InvoiceCanceled
)

// InvoiceRequest asks for a new invoice.
type InvoiceRequest struct {
	AmountMsat int64
	// Description goes in the invoice, unless DescriptionHash is set.
	Description string
	// DescriptionHash is hex.
	DescriptionHash string
	// ExpirySeconds of 0 uses the node's default.
	ExpirySeconds int64
}

// Invoice is an invoice made on this node.
type Invoice struct {
	Hash            string
	Preimage        string
	PaymentRequest  string
	Description     string
	DescriptionHash string
	AmountMsat      int64
	// PaidMsat is what was received. It can differ from AmountMsat.
	PaidMsat    int64
	State       InvoiceState
	SettleIndex uint64
	CreatedAt   time.Time
	ExpiresAt   time.Time
	SettledAt   time.Time
}

// DecodedInvoice is what a BOLT 11 invoice says.
type DecodedInvoice struct {
	Hash            string
	Destination     string
	Description     string
	DescriptionHash string
	// AmountMsat is 0 on an invoice with no amount.
	AmountMsat int64
	CreatedAt  time.Time
	ExpiresAt  time.Time
}

// PayRequest asks for a payment.
type PayRequest struct {
	Invoice string
	// AmountMsat is only for an invoice with no amount. Otherwise leave it 0.
	AmountMsat int64
	// FeeLimitMsat is the most the payment may cost in fees. It must be above 0,
	// because LND reads 0 as "zero-fee routes only".
	FeeLimitMsat int64
	// Timeout is how long the node keeps trying new routes. A payment can stay
	// in flight past it.
	Timeout time.Duration
}

// Check reports a request that must not reach a node.
func (r PayRequest) Check() error {
	switch {
	case r.Invoice == "":
		return errors.New("pay: no invoice")
	case r.AmountMsat < 0:
		return fmt.Errorf("pay: negative amount %d", r.AmountMsat)
	case r.FeeLimitMsat <= 0:
		return fmt.Errorf("pay: fee limit must be above 0, got %d", r.FeeLimitMsat)
	case r.Timeout < time.Second:
		return fmt.Errorf("pay: timeout must be at least a second, got %s", r.Timeout)
	}
	return nil
}

// PaymentState is what a node says about a payment. The zero value is the safe
// one: anything unrecognised stays in flight, and the hold stays.
type PaymentState int

// Payment states.
const (
	PaymentInFlight PaymentState = iota
	PaymentSucceeded
	PaymentFailed
	// PaymentNoRecord means the node answered, and it has never seen this hash.
	PaymentNoRecord
)

// Payment is a payment as the node sees it.
type Payment struct {
	State PaymentState
	Hash  string
	// Preimage is set only when State is PaymentSucceeded.
	Preimage   string
	AmountMsat int64
	FeeMsat    int64
	// FailureReason is set only when State is PaymentFailed.
	FailureReason string
}
