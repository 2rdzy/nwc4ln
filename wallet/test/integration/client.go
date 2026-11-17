package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// LNDDir is the node's data folder. It holds tls.cert and the macaroons.
func (n *Network) LNDDir(node string) string { return filepath.Join(n.Dir, node) }

// LNDHost is the node's gRPC address.
func (n *Network) LNDHost(node string) string {
	return fmt.Sprintf("127.0.0.1:%d", n.lndPort(node)+1)
}

// CLNDir is a Core Lightning node's data folder.
func (n *Network) CLNDir(node string) string { return filepath.Join(n.Dir, node) }

// CLNURL is a Core Lightning node's clnrest address.
func (n *Network) CLNURL(string) string {
	return fmt.Sprintf("https://127.0.0.1:%d", n.PortBase+portCLNA+1)
}

// CLNCACert is the certificate that signed clnrest's server certificate.
func (n *Network) CLNCACert(node string) string {
	return filepath.Join(n.CLNDir(node), "regtest", "ca.pem")
}

// CLNCLI runs lightning-cli against a Core Lightning node and returns its output.
func (n *Network) CLNCLI(ctx context.Context, node string, args ...string) ([]byte, error) {
	all := append([]string{"--network=regtest", "--lightning-dir=" + n.CLNDir(node)}, args...)
	return runTool(ctx, node, filepath.Join(n.BinDir, "cln", "usr", "bin", "lightning-cli"), all...)
}

func (n *Network) clnJSON(ctx context.Context, node string, out any, args ...string) error {
	output, err := n.CLNCLI(ctx, node, args...)
	if err != nil {
		return err
	}
	return json.Unmarshal(output, out)
}

// SelfNode is the node nwc4ln runs on in the NWC tests: cln-a when
// NWC4LN_IT_NODE is "cln", and lnd-a otherwise.
func SelfNode() string {
	if os.Getenv("NWC4LN_IT_NODE") == "cln" {
		return NodeCLNA
	}
	return NodeLNDA
}

// CLNRune creates a rune on a Core Lightning node that allows only the given
// methods, and saves it in a file with mode 0600, as an operator would.
func (n *Network) CLNRune(ctx context.Context, node string, methods []string, path string) error {
	alternatives := make([]string, len(methods))
	for i, method := range methods {
		alternatives[i] = "method=" + method
	}
	restrictions, err := json.Marshal([][]string{alternatives})
	if err != nil {
		return err
	}
	var created struct {
		Rune string `json:"rune"`
	}
	if err := n.clnJSON(ctx, node, &created, "-k", "createrune", "restrictions="+string(restrictions)); err != nil {
		return err
	}
	if created.Rune == "" {
		return errors.New("createrune gave no rune")
	}
	return os.WriteFile(path, []byte(created.Rune), 0o600)
}

// IsCLN says whether a node is Core Lightning.
func IsCLN(node string) bool { return node == NodeCLNA }

// RelayURL is the local relay's address.
func (n *Network) RelayURL() string {
	return fmt.Sprintf("ws://127.0.0.1:%d", n.PortBase+portRelay)
}

func (n *Network) lndPort(node string) int {
	switch node {
	case NodeLNDB:
		return n.PortBase + portLNDB
	case NodeLNDC:
		return n.PortBase + portLNDC
	}
	return n.PortBase + portLNDA
}

func runTool(ctx context.Context, name, binary string, args ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	var stdout, stderr bytes.Buffer
	cmd := exec.CommandContext(ctx, binary, args...)
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		return stdout.Bytes(), fmt.Errorf("%s: %s: %w: %s", name, filepath.Base(binary),
			err, strings.TrimSpace(stderr.String()))
	}
	return stdout.Bytes(), nil
}

// BitcoinCLI runs bitcoin-cli against the Knots node and returns its output.
func (n *Network) BitcoinCLI(ctx context.Context, args ...string) (string, error) {
	all := append([]string{
		"-regtest", "-datadir=" + n.knotsDir(), "-rpcconnect=127.0.0.1",
		"-rpcport=" + strconv.Itoa(n.PortBase+portKnotsRPC),
		"-rpcuser=" + rpcUser, "-rpcpassword=" + rpcPassword,
	}, args...)
	output, err := runTool(ctx, NodeKnots, filepath.Join(n.BinDir, "bitcoin-cli"), all...)
	return strings.TrimSpace(string(output)), err
}

// LNCLI runs lncli against an LND node and returns its output.
func (n *Network) LNCLI(ctx context.Context, node string, args ...string) ([]byte, error) {
	all := append([]string{
		"--lnddir=" + n.LNDDir(node), "--network=regtest", "--rpcserver=" + n.LNDHost(node),
	}, args...)
	return runTool(ctx, node, filepath.Join(n.BinDir, "lncli"), all...)
}

func (n *Network) lncliJSON(ctx context.Context, node string, out any, args ...string) error {
	output, err := n.LNCLI(ctx, node, args...)
	if err != nil {
		return err
	}
	return json.Unmarshal(output, out)
}

type nodeInfo struct {
	PubKey string `json:"identity_pubkey"`
	Synced bool   `json:"synced_to_chain"`
}

func (n *Network) getInfo(ctx context.Context, node string) (nodeInfo, error) {
	var info nodeInfo
	return info, n.lncliJSON(ctx, node, &info, "getinfo")
}

// Mine mines blocks to the miner wallet.
func (n *Network) Mine(ctx context.Context, blocks int) error {
	address, err := n.BitcoinCLI(ctx, "-rpcwallet=miner", "getnewaddress")
	if err != nil {
		return err
	}
	_, err = n.BitcoinCLI(ctx, "-rpcwallet=miner", "generatetoaddress", strconv.Itoa(blocks), address)
	return err
}

// Pay pays a new invoice from one node to another, of either kind. It passes only
// when the payer reports success and the receiver's invoice is settled.
func (n *Network) Pay(ctx context.Context, from, to string, sats int) error {
	request, hash, err := n.Invoice(ctx, to, int64(sats)*1000)
	if err != nil {
		return err
	}
	if err := n.PayInvoice(ctx, from, request); err != nil {
		return err
	}
	settled, err := n.InvoiceSettled(ctx, to, hash)
	if err != nil {
		return err
	}
	if !settled {
		return fmt.Errorf("%s invoice %s is not settled", to, hash)
	}
	return nil
}

// Invoice makes an invoice on a node, and returns it with its hash.
func (n *Network) Invoice(ctx context.Context, node string, amountMsat int64) (request, hash string, err error) {
	if IsCLN(node) {
		var invoice struct {
			Request string `json:"bolt11"`
			Hash    string `json:"payment_hash"`
		}
		label := "it-" + strconv.FormatInt(time.Now().UnixNano(), 10)
		err := n.clnJSON(ctx, node, &invoice, "-k", "invoice", "amount_msat="+strconv.FormatInt(amountMsat, 10),
			"label="+label, "description=it")
		return invoice.Request, invoice.Hash, err
	}
	var invoice struct {
		Hash    string `json:"r_hash"`
		Request string `json:"payment_request"`
	}
	err = n.lncliJSON(ctx, node, &invoice, "addinvoice", "--amt_msat", strconv.FormatInt(amountMsat, 10))
	return invoice.Request, invoice.Hash, err
}

// PayInvoice pays an invoice from a node, and passes only on success.
func (n *Network) PayInvoice(ctx context.Context, node, request string) error {
	if IsCLN(node) {
		var paid struct {
			Preimage string `json:"payment_preimage"`
		}
		output, err := n.CLNCLI(ctx, node, "-k", "xpay", "invstring="+request, "retry_for=30")
		if err != nil {
			return fmt.Errorf("%s payment: %w", node, err)
		}
		// lightning-cli prints xpay's progress as lines starting with "#".
		var body strings.Builder
		for line := range strings.Lines(string(output)) {
			if !strings.HasPrefix(line, "#") {
				body.WriteString(line)
			}
		}
		if err := json.Unmarshal([]byte(body.String()), &paid); err != nil {
			return fmt.Errorf("%s payment: %w: %s", node, err, output)
		}
		if paid.Preimage == "" {
			return fmt.Errorf("%s payment: no preimage", node)
		}
		return nil
	}
	// A failed payment also exits non-zero, so read the status first.
	output, runErr := n.LNCLI(ctx, node, "payinvoice", "--force", "--json", "--timeout", "30s", request)
	status, reason, err := lastPaymentStatus(output)
	if err != nil {
		return errors.Join(fmt.Errorf("%s payment: %w", node, err), runErr)
	}
	if status != "SUCCEEDED" {
		return fmt.Errorf("%s payment: status %s, reason %s", node, status, reason)
	}
	return nil
}

// InvoiceSettled says whether an invoice on a node is paid.
func (n *Network) InvoiceSettled(ctx context.Context, node, hash string) (bool, error) {
	if IsCLN(node) {
		var list struct {
			Invoices []struct {
				Status string `json:"status"`
			} `json:"invoices"`
		}
		if err := n.clnJSON(ctx, node, &list, "-k", "listinvoices", "payment_hash="+hash); err != nil {
			return false, err
		}
		return len(list.Invoices) == 1 && list.Invoices[0].Status == "paid", nil
	}
	var invoice struct {
		State string `json:"state"`
	}
	if err := n.lncliJSON(ctx, node, &invoice, "lookupinvoice", hash); err != nil {
		return false, err
	}
	return invoice.State == "SETTLED", nil
}

// lastPaymentStatus reads the last JSON object from `lncli payinvoice --json`,
// which prints one per update. Earlier objects can show a payment in flight.
func lastPaymentStatus(output []byte) (status, reason string, err error) {
	decoder := json.NewDecoder(bytes.NewReader(output))
	for {
		var update struct {
			Status string `json:"status"`
			Reason string `json:"failure_reason"`
		}
		err := decoder.Decode(&update)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return "", "", fmt.Errorf("bad JSON from lncli: %w", err)
		}
		status, reason = update.Status, update.Reason
	}
	if status == "" {
		return "", "", errors.New("lncli printed no payment status")
	}
	return status, reason, nil
}

// Smoke proves the network works: payments in both directions, BLAKE2b headers,
// and a relay that stores and returns an event.
func (n *Network) Smoke(ctx context.Context) error {
	if err := n.Pay(ctx, NodeLNDA, NodeLNDB, 1000); err != nil {
		return fmt.Errorf("smoke: payment a to b: %w", err)
	}
	if err := n.Pay(ctx, NodeLNDB, NodeLNDA, 1000); err != nil {
		return fmt.Errorf("smoke: payment b to a: %w", err)
	}
	if err := n.Pay(ctx, NodeCLNA, NodeLNDB, 1000); err != nil {
		return fmt.Errorf("smoke: payment cln-a to b: %w", err)
	}
	if err := n.Pay(ctx, NodeLNDB, NodeCLNA, 1000); err != nil {
		return fmt.Errorf("smoke: payment b to cln-a: %w", err)
	}
	if err := n.checkHeaders(ctx); err != nil {
		return fmt.Errorf("smoke: %w", err)
	}
	if err := n.checkRelay(ctx); err != nil {
		return fmt.Errorf("smoke: %w", err)
	}
	return nil
}

// A BLAKE2b header is 164 bytes, with header version 2. The genesis block keeps
// the old 80-byte format, so only heights from 1 count.
const blake2bHeaderBytes = 164

func (n *Network) checkHeaders(ctx context.Context) error {
	tip, err := n.BitcoinCLI(ctx, "getblockcount")
	if err != nil {
		return err
	}
	for _, height := range []string{"1", tip} {
		hash, err := n.BitcoinCLI(ctx, "getblockhash", height)
		if err != nil {
			return err
		}
		raw, err := n.BitcoinCLI(ctx, "getblockheader", hash, "false")
		if err != nil {
			return err
		}
		if len(raw) != 2*blake2bHeaderBytes {
			return fmt.Errorf("header at height %s has %d bytes, want %d", height, len(raw)/2, blake2bHeaderBytes)
		}
		verbose, err := n.BitcoinCLI(ctx, "getblockheader", hash)
		if err != nil {
			return err
		}
		var header struct {
			Version int `json:"header_version"`
		}
		if err := json.Unmarshal([]byte(verbose), &header); err != nil {
			return err
		}
		if header.Version != 2 {
			return fmt.Errorf("header at height %s has header_version %d, want 2", height, header.Version)
		}
	}
	return nil
}

func (n *Network) checkRelay(ctx context.Context) error {
	nak := filepath.Join(n.BinDir, "nak")
	content := "nwc4ln-it-" + strconv.FormatInt(time.Now().UnixNano(), 10)
	// A fixed test key: this event only ever reaches the local relay.
	key := strings.Repeat("0", 63) + "1"
	if _, err := runTool(ctx, NodeRelay, nak, "event", "--sec", key, "-k", "1", "-c", content, n.RelayURL()); err != nil {
		return err
	}
	output, err := runTool(ctx, NodeRelay, nak, "req", "-k", "1", "--limit", "20", n.RelayURL())
	if err != nil {
		return err
	}
	if !strings.Contains(string(output), content) {
		return errors.New("relay did not return the event it was sent")
	}
	return nil
}
