//go:build integration

package nwc_test

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/2rdzy/nwc4ln/internal/config"
	"github.com/2rdzy/nwc4ln/internal/node"
	"github.com/2rdzy/nwc4ln/internal/node/cln"
	"github.com/2rdzy/nwc4ln/internal/node/lnd"
	"github.com/2rdzy/nwc4ln/test/integration"
)

// The helpers here are the only part of the NWC tests that know the node type.

// setNode points a config at self, with a credential that allows only the methods
// the client uses: a baked macaroon on LND, a rune on Core Lightning.
func setNode(t *testing.T, n *integration.Network, cfg *config.Config, dir string) {
	t.Helper()
	if integration.IsCLN(self) {
		runePath := filepath.Join(dir, "nwc4ln.rune")
		if err := n.CLNRune(t.Context(), self, cln.Methods, runePath); err != nil {
			t.Fatalf("create rune: %v", err)
		}
		cfg.Node, cfg.CLNURL, cfg.CLNCACertPath, cfg.CLNRunePath = config.NodeCLN, n.CLNURL(self), n.CLNCACert(self), runePath
		return
	}
	macaroon := filepath.Join(dir, "nwc4ln.macaroon")
	args := []string{"bakemacaroon", "--save_to", macaroon}
	for _, method := range lnd.Methods {
		args = append(args, "uri:"+method)
	}
	if out, err := n.LNCLI(t.Context(), self, args...); err != nil {
		t.Fatalf("bake macaroon: %v: %s", err, out)
	}
	cfg.LNDHost = n.LNDHost(self)
	cfg.LNDTLSCertPath = filepath.Join(n.LNDDir(self), "tls.cert")
	cfg.LNDMacaroonPath = macaroon
}

// nodeClient makes the client for self, with the same credential as setNode.
func nodeClient(t *testing.T, n *integration.Network, dir string) node.Node {
	t.Helper()
	var cfg config.Config
	setNode(t, n, &cfg, dir)
	var (
		client interface {
			node.Node
			Close() error
		}
		err error
	)
	if integration.IsCLN(self) {
		client, err = cln.New(cln.Config{URL: cfg.CLNURL, CACertPath: cfg.CLNCACertPath, RunePath: cfg.CLNRunePath})
	} else {
		client, err = lnd.New(lnd.Config{Host: cfg.LNDHost, TLSCertPath: cfg.LNDTLSCertPath, MacaroonPath: cfg.LNDMacaroonPath})
	}
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

// waitReady returns once self can pay lnd-b and be paid by it.
func waitReady(ctx context.Context, n *integration.Network) error {
	if integration.IsCLN(self) {
		return n.WaitCLN(ctx)
	}
	return n.WaitChannels(ctx)
}

func clnJSON(t *testing.T, n *integration.Network, out any, args ...string) {
	t.Helper()
	output, err := n.CLNCLI(t.Context(), self, args...)
	if err != nil {
		t.Fatalf("%s: %v: %s", args[0], err, output)
	}
	if err := json.Unmarshal(output, out); err != nil {
		t.Fatal(err)
	}
}

// clnFee reads the fee Core Lightning recorded for a payment, from its parts.
func clnFee(t *testing.T, n *integration.Network, hash string) int64 {
	t.Helper()
	var list struct {
		Payments []struct {
			Status string `json:"status"`
			Amount int64  `json:"amount_msat"`
			Sent   int64  `json:"amount_sent_msat"`
		} `json:"payments"`
	}
	clnJSON(t, n, &list, "-k", "listsendpays", "payment_hash="+hash)
	var fee int64
	found := false
	for _, part := range list.Payments {
		if part.Status == "complete" {
			fee += part.Sent - part.Amount
			found = true
		}
	}
	if !found {
		t.Fatalf("%s has no completed payment %s", self, hash)
	}
	return fee
}

// clnHeldHTLCs counts Core Lightning's outgoing HTLCs for a hash.
func clnHeldHTLCs(t *testing.T, n *integration.Network, hash string) int {
	t.Helper()
	var list struct {
		Channels []struct {
			HTLCs []struct {
				Direction string `json:"direction"`
				Hash      string `json:"payment_hash"`
			} `json:"htlcs"`
		} `json:"channels"`
	}
	clnJSON(t, n, &list, "listpeerchannels")
	count := 0
	for _, channel := range list.Channels {
		for _, htlc := range channel.HTLCs {
			if htlc.Direction == "out" && htlc.Hash == hash {
				count++
			}
		}
	}
	return count
}

// invoiceState gives an invoice's state on self: LND's name for it, Core
// Lightning's status, or "deleted" when Core Lightning no longer has it.
func invoiceState(t *testing.T, n *integration.Network, hash string) string {
	t.Helper()
	if integration.IsCLN(self) {
		var list struct {
			Invoices []struct {
				Status string `json:"status"`
			} `json:"invoices"`
		}
		clnJSON(t, n, &list, "-k", "listinvoices", "payment_hash="+hash)
		if len(list.Invoices) == 0 {
			return "deleted"
		}
		return list.Invoices[0].Status
	}
	out, err := n.LNCLI(t.Context(), self, "lookupinvoice", hash)
	if err != nil {
		t.Fatalf("lookupinvoice: %v: %s", err, out)
	}
	var invoice struct {
		State string `json:"state"`
	}
	if err := json.Unmarshal(out, &invoice); err != nil {
		t.Fatal(err)
	}
	return invoice.State
}
