//go:build integration

package nwc_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"path/filepath"
	"strconv"
	"sync"
	"testing"

	"github.com/2rdzy/nwc4ln/internal/ledger"
	"github.com/2rdzy/nwc4ln/internal/node/lnd"
	"github.com/2rdzy/nwc4ln/internal/nwc"
	"github.com/2rdzy/nwc4ln/internal/nwc/nwctest"
	"github.com/2rdzy/nwc4ln/internal/store"
	"github.com/2rdzy/nwc4ln/test/integration"
)

// routedInvoice makes an invoice on lnd-c, one hop past lnd-b, so paying it from
// lnd-a costs a real routing fee.
func routedInvoice(t *testing.T, n *integration.Network, sats int) string {
	t.Helper()
	out, err := n.LNCLI(t.Context(), integration.NodeLNDC, "addinvoice", "--amt", strconv.Itoa(sats))
	if err != nil {
		t.Fatalf("addinvoice: %v: %s", err, out)
	}
	var reply struct {
		Request string `json:"payment_request"`
	}
	if err := json.Unmarshal(out, &reply); err != nil {
		t.Fatal(err)
	}
	return reply.Request
}

// nodeFee reads the fee lnd-a itself recorded for a payment.
func nodeFee(t *testing.T, n *integration.Network, hash string) int64 {
	t.Helper()
	out, err := n.LNCLI(t.Context(), self, "listpayments")
	if err != nil {
		t.Fatalf("listpayments: %v: %s", err, out)
	}
	var list struct {
		Payments []struct {
			Hash string `json:"payment_hash"`
			Fee  string `json:"fee_msat"`
		} `json:"payments"`
	}
	if err := json.Unmarshal(out, &list); err != nil {
		t.Fatal(err)
	}
	for _, payment := range list.Payments {
		if payment.Hash == hash {
			fee, err := strconv.ParseInt(payment.Fee, 10, 64)
			if err != nil {
				t.Fatal(err)
			}
			return fee
		}
	}
	t.Fatalf("lnd-a has no payment %s", hash)
	return 0
}

// Catches: the fee the ledger charges differing from what the node really paid, on a
// sub-wallet that was funded from outside. It is the only test of a non-zero fee
// through the real LND client. Sub-wallets have no command, so the ledger and service
// run in this process, on the real node and relay.
func TestSubWalletPaysRealFee(t *testing.T) {
	n, err := integration.Load(integration.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	if err := n.WaitRouted(ctx); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	macaroon := filepath.Join(dir, "nwc4ln.macaroon")
	args := []string{"bakemacaroon", "--save_to", macaroon}
	for _, method := range lnd.Methods {
		args = append(args, "uri:"+method)
	}
	if out, err := n.LNCLI(ctx, self, args...); err != nil {
		t.Fatalf("bake macaroon: %v: %s", err, out)
	}
	lightning, err := lnd.New(lnd.Config{
		Host: n.LNDHost(self), TLSCertPath: filepath.Join(n.LNDDir(self), "tls.cert"), MacaroonPath: macaroon,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lightning.Close() }()
	db, err := store.Create(ctx, filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	money := ledger.New(db, lightning, log)
	service, err := nwc.New(ctx, db, lightning, money, n.RelayURL(), log)
	if err != nil {
		t.Fatal(err)
	}
	if err := money.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer money.Close()
	var running sync.WaitGroup
	running.Go(func() { service.Run(ctx) })
	defer running.Wait()
	defer cancel()

	connectionString, err := service.AddSubWallet(ctx, "holder")
	if err != nil {
		t.Fatal(err)
	}
	wallet, err := nwctest.Parse(connectionString)
	if err != nil {
		t.Fatal(err)
	}
	defer wallet.Close()
	waitForInfo(t, wallet)
	balance := func() int64 {
		t.Helper()
		response := call(t, wallet, nwctest.NIP44, "get_balance")
		if response.Error != nil {
			t.Fatalf("get_balance: %+v", response.Error)
		}
		var result struct {
			Balance int64 `json:"balance"`
		}
		if err := json.Unmarshal(response.Result, &result); err != nil {
			t.Fatal(err)
		}
		return result.Balance
	}
	if got := balance(); got != 0 {
		t.Fatalf("new sub-wallet told %d", got)
	}

	callCtx, stop := context.WithTimeout(ctx, callTimeout)
	defer stop()
	response, err := wallet.CallWith(callCtx, nwctest.NIP44, "make_invoice", map[string]int{"amount": 20_000_000})
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
	if out, err := n.LNCLI(ctx, other, "payinvoice", "--force", "--json", invoice.Invoice); err != nil {
		t.Fatalf("payinvoice: %v: %s", err, out)
	}
	lookupUntil(t, wallet, invoice.Hash, "settled")
	if got := balance(); got != 20_000_000 {
		t.Fatalf("balance %d after income", got)
	}

	payCtx, stopPay := context.WithTimeout(ctx, replyWithin)
	defer stopPay()
	response, err = wallet.CallWith(payCtx, nwctest.NIP44, "pay_invoice", map[string]string{
		"invoice": routedInvoice(t, n, 10_000),
	})
	if err != nil || response.Error != nil {
		t.Fatalf("pay_invoice: %v %+v", err, response)
	}
	var paid struct {
		Preimage string `json:"preimage"`
		Fee      int64  `json:"fees_paid"`
	}
	if err := json.Unmarshal(response.Result, &paid); err != nil {
		t.Fatal(err)
	}
	if paid.Fee == 0 {
		t.Fatal("no routing fee: this test proves nothing")
	}
	var rows struct {
		Transactions []struct {
			Type string `json:"type"`
			Hash string `json:"payment_hash"`
			Fees int64  `json:"fees_paid"`
		} `json:"transactions"`
	}
	listCtx, stopList := context.WithTimeout(ctx, callTimeout)
	defer stopList()
	response, err = wallet.CallWith(listCtx, nwctest.NIP44, "list_transactions", map[string]string{"type": "outgoing"})
	if err != nil || response.Error != nil {
		t.Fatalf("list_transactions: %v %+v", err, response)
	}
	if err := json.Unmarshal(response.Result, &rows); err != nil {
		t.Fatal(err)
	}
	if len(rows.Transactions) != 1 {
		t.Fatalf("transactions %+v", rows.Transactions)
	}
	if fee := nodeFee(t, n, rows.Transactions[0].Hash); fee != paid.Fee || fee != rows.Transactions[0].Fees {
		t.Fatalf("node paid %d, ledger charged %d and %d", fee, paid.Fee, rows.Transactions[0].Fees)
	}
	if got, want := balance(), 10_000_000-paid.Fee; got != want {
		t.Fatalf("balance %d, want %d: the unused reserve must return", got, want)
	}
}
