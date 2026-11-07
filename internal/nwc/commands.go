package nwc

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/2rdzy/nwc4ln/internal/ledger"
)

const (
	// payWait is how long pay_invoice waits for a final state. Clients stop waiting
	// at 60 seconds (the @getalby/sdk default), so the reply must arrive before
	// that. The ledger's own node call ends near 55 seconds.
	payWait = 57 * time.Second

	// maxPage caps list_transactions, as NWC-05 advises. A larger page could pass
	// NIP-44's limit of 65,535 bytes, and then no reply could be sent at all.
	maxPage = 20
)

// What this service offers beyond the core commands, for clients to read.
var (
	notificationTypes = []string{"payment_received", "payment_sent"}
	extensions        = []string{"02", "05", "13"}
)

// errPaymentPending is a payment with no final state yet. It may still settle, so
// it must never read as a failure: the client would pay again. answer compares
// this exact value.
var errPaymentPending = &Error{Code: codeOther, Message: "payment pending"}

// readParams reads a command's params. A request with no params is an empty one.
func readParams(raw json.RawMessage, into any) *Error {
	if len(raw) == 0 {
		return nil
	}
	if err := json.Unmarshal(raw, into); err != nil {
		return &Error{Code: codeOther, Message: "cannot read the params"}
	}
	return nil
}

// transaction is the shape NIP-47 uses wherever a transaction appears.
type transaction struct {
	Type        string `json:"type"`
	State       string `json:"state"`
	Invoice     string `json:"invoice,omitempty"`
	Description string `json:"description,omitempty"`
	Preimage    string `json:"preimage,omitempty"`
	PaymentHash string `json:"payment_hash"`
	Amount      int64  `json:"amount"`
	FeesPaid    int64  `json:"fees_paid"`
	CreatedAt   int64  `json:"created_at"`
	ExpiresAt   int64  `json:"expires_at,omitempty"`
	SettledAt   int64  `json:"settled_at,omitempty"`
}

// present turns a row into what a client sees. Every reply and notification goes
// through here. The rows of an unpaid invoice hold its preimage, and showing that
// would let a client claim the payment, so only a settled row shows one.
func (s *Service) present(row ledger.Transaction) transaction {
	out := transaction{
		Type: row.Type, State: row.State, Invoice: row.Invoice, Description: row.Description,
		PaymentHash: row.PaymentHash, Amount: row.AmountMsat, FeesPaid: row.FeeMsat, CreatedAt: row.CreatedAt.Unix(),
	}
	if row.State == ledger.StateSettled {
		out.Preimage = row.Preimage
		out.SettledAt = row.SettledAt.Unix()
	}
	if !row.ExpiresAt.IsZero() {
		out.ExpiresAt = row.ExpiresAt.Unix()
		// The ledger closes such a row only now and then, so work it out here.
		if row.Type == ledger.TypeIncoming && row.State != ledger.StateSettled && !s.now().Before(row.ExpiresAt) {
			out.State = "expired"
		}
	}
	return out
}

type getInfoResult struct {
	Alias         string   `json:"alias"`
	Color         string   `json:"color"`
	PubKey        string   `json:"pubkey"`
	Network       string   `json:"network"`
	BlockHeight   uint32   `json:"block_height"`
	BlockHash     string   `json:"block_hash"`
	Methods       []string `json:"methods"`
	Extensions    []string `json:"extensions"`
	Notifications []string `json:"notifications"`
}

func (s *Service) getInfo(ctx context.Context, a *accepted, _ json.RawMessage) (any, *Error) {
	info, err := s.node.Info(ctx)
	if err != nil {
		s.log.Error("get_info: node call failed", "error", err)
		return nil, &Error{Code: codeInternal, Message: "the node cannot be reached"}
	}
	return getInfoResult{
		Alias: info.Alias, Color: info.Color, PubKey: info.PubKey, Network: info.Network,
		BlockHeight: info.BlockHeight, BlockHash: info.BlockHash, Methods: s.methodNames(a.settings.CanPay, a.settings.CanInvoice),
		Extensions: extensions, Notifications: notificationTypes,
	}, nil
}

type balanceResult struct {
	Balance int64 `json:"balance"`
}

func (s *Service) getBalance(ctx context.Context, a *accepted, _ json.RawMessage) (any, *Error) {
	balance, err := s.ledger.Balance(ctx, a.connection.accountID)
	if err != nil {
		s.log.Error("get_balance failed", "error", err)
		return nil, &Error{Code: codeInternal, Message: "the balance cannot be read"}
	}
	return balanceResult{Balance: max(balance, 0)}, nil
}

// budgetResult is NWC-13. used_budget can pass total_budget after a limit is cut.
type budgetResult struct {
	UsedBudget    int64  `json:"used_budget"`
	TotalBudget   int64  `json:"total_budget"`
	RenewsAt      int64  `json:"renews_at,omitempty"`
	RenewalPeriod string `json:"renewal_period"`
}

// getBudget answers with an empty object if there is no limit. A limit of 0 is a
// limit, and clients must be able to tell the two apart.
func (s *Service) getBudget(ctx context.Context, a *accepted, _ json.RawMessage) (any, *Error) {
	budget, err := s.ledger.AppBudget(ctx, a.connection.accountID)
	if err != nil {
		return nil, s.refused("get_budget", err)
	}
	if budget == nil {
		return struct{}{}, nil
	}
	result := budgetResult{UsedBudget: budget.UsedMsat, TotalBudget: budget.TotalMsat, RenewalPeriod: budget.Renewal}
	if !budget.RenewsAt.IsZero() {
		result.RenewsAt = budget.RenewsAt.Unix()
	}
	return result, nil
}

type payParams struct {
	Invoice string `json:"invoice"`
	Amount  int64  `json:"amount"`
}

type payResult struct {
	Preimage string `json:"preimage"`
	FeesPaid int64  `json:"fees_paid"`
}

func (s *Service) payInvoice(ctx context.Context, a *accepted, raw json.RawMessage) (any, *Error) {
	var params payParams
	if failed := readParams(raw, &params); failed != nil {
		return nil, failed
	}
	if params.Invoice == "" {
		return nil, &Error{Code: codeOther, Message: "invoice is required"}
	}
	ctx, cancel := context.WithTimeout(ctx, s.payWait)
	defer cancel()
	row, err := s.ledger.Pay(ctx, ledger.PayRequest{
		AccountID: a.connection.accountID, Invoice: params.Invoice, AmountMsat: params.Amount, RequestID: a.rowID,
	})
	if err != nil {
		return nil, s.refused("pay_invoice", err)
	}
	switch row.State {
	case ledger.StateSettled:
		return payResult{Preimage: row.Preimage, FeesPaid: row.FeeMsat}, nil
	case ledger.StateFailed:
		return nil, &Error{Code: codePaymentFailed, Message: "the payment failed"}
	default:
		return nil, errPaymentPending
	}
}

// refused turns a ledger error into a reply. Every error here means nothing was
// held. PAYMENT_FAILED is kept for the node's final answer, and never used here:
// an invoice that is already paid must not look like a payment that failed.
func (s *Service) refused(command string, err error) *Error {
	var message string
	switch {
	case errors.Is(err, ledger.ErrAccountDeleted):
		return &Error{Code: codeUnauthorized, Message: "this account is deleted"}
	case errors.Is(err, ledger.ErrAccountExpired):
		return &Error{Code: codeUnauthorized, Message: "this app has expired"}
	case errors.Is(err, ledger.ErrRestricted):
		return &Error{Code: codeRestricted, Message: "this app may not do that"}
	case errors.Is(err, ledger.ErrQuotaExceeded):
		return &Error{Code: codeQuotaExceeded, Message: "over the spending limit"}
	case errors.Is(err, ledger.ErrInsufficientBalance):
		return &Error{Code: codeInsufficientBalance, Message: "not enough funds, including the fee reserve"}
	case errors.Is(err, ledger.ErrTooManyInvoices):
		return &Error{Code: codeRateLimited, Message: "too many open invoices, wait for some to expire"}
	case errors.Is(err, ledger.ErrDescriptionTooLong):
		message = "description is too long"
	case errors.Is(err, ledger.ErrAlreadyPaid):
		message = "this invoice is already paid or being paid"
	case errors.Is(err, ledger.ErrInvoiceClosed):
		message = "this invoice is closed"
	case errors.Is(err, ledger.ErrInvoiceExpired):
		message = "this invoice has expired"
	case errors.Is(err, ledger.ErrOwnInvoice):
		message = "this invoice is from this node, and nwc4ln did not make it"
	case errors.Is(err, ledger.ErrBadAmount):
		message = "bad amount"
	default:
		s.log.Error("command failed", "command", command, "error", err)
		return &Error{Code: codeInternal, Message: "the request could not be done"}
	}
	return &Error{Code: codeOther, Message: message}
}

type makeInvoiceParams struct {
	Amount          int64  `json:"amount"`
	Description     string `json:"description"`
	DescriptionHash string `json:"description_hash"`
	Expiry          int64  `json:"expiry"`
}

func (s *Service) makeInvoice(ctx context.Context, a *accepted, raw json.RawMessage) (any, *Error) {
	var params makeInvoiceParams
	if failed := readParams(raw, &params); failed != nil {
		return nil, failed
	}
	if params.Expiry < 0 {
		return nil, &Error{Code: codeOther, Message: "bad expiry"}
	}
	row, err := s.ledger.MakeInvoice(ctx, ledger.InvoiceRequest{
		AccountID: a.connection.accountID, AmountMsat: params.Amount, Description: params.Description,
		DescriptionHash: params.DescriptionHash, ExpirySeconds: params.Expiry, RequestID: a.rowID,
	})
	if err != nil {
		return nil, s.refused("make_invoice", err)
	}
	return s.present(row), nil
}

type lookupParams struct {
	PaymentHash string `json:"payment_hash"`
	Invoice     string `json:"invoice"`
}

func (s *Service) lookupInvoice(ctx context.Context, a *accepted, raw json.RawMessage) (any, *Error) {
	var params lookupParams
	if failed := readParams(raw, &params); failed != nil {
		return nil, failed
	}
	if params.PaymentHash == "" && params.Invoice == "" {
		return nil, &Error{Code: codeOther, Message: "payment_hash or invoice is required"}
	}
	row, err := s.ledger.Lookup(ctx, a.connection.accountID, params.PaymentHash, params.Invoice)
	if errors.Is(err, ledger.ErrNotFound) {
		return nil, &Error{Code: codeNotFound, Message: "invoice not found"}
	}
	if err != nil {
		return nil, s.refused("lookup_invoice", err)
	}
	return s.present(row), nil
}

type listParams struct {
	From   int64  `json:"from"`
	Until  int64  `json:"until"`
	Limit  int64  `json:"limit"`
	Offset int64  `json:"offset"`
	Unpaid bool   `json:"unpaid"`
	Type   string `json:"type"`
}

type listResult struct {
	Transactions []transaction `json:"transactions"`
	TotalCount   int64         `json:"total_count"`
}

func (s *Service) listTransactions(ctx context.Context, a *accepted, raw json.RawMessage) (any, *Error) {
	var params listParams
	if failed := readParams(raw, &params); failed != nil {
		return nil, failed
	}
	if params.From < 0 || params.Until < 0 || params.Limit < 0 || params.Offset < 0 {
		return nil, &Error{Code: codeOther, Message: "from, until, limit and offset cannot be negative"}
	}
	if params.Type != "" && params.Type != ledger.TypeIncoming && params.Type != ledger.TypeOutgoing {
		return nil, &Error{Code: codeOther, Message: "type must be incoming or outgoing"}
	}
	if params.Until == 0 {
		params.Until = s.now().Unix()
	}
	limit := params.Limit
	if limit == 0 || limit > maxPage {
		limit = maxPage
	}
	rows, total, err := s.ledger.History(ctx, ledger.HistoryQuery{
		AccountID: a.connection.accountID, From: params.From, Until: params.Until,
		Limit: limit, Offset: params.Offset, Unpaid: params.Unpaid, Type: params.Type,
	})
	if err != nil {
		return nil, s.refused("list_transactions", err)
	}
	result := listResult{Transactions: make([]transaction, 0, len(rows)), TotalCount: total}
	for _, row := range rows {
		result.Transactions = append(result.Transactions, s.present(row))
	}
	return result, nil
}
