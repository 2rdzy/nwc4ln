package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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

// RelayURL is the local relay's address.
func (n *Network) RelayURL() string {
	return fmt.Sprintf("ws://127.0.0.1:%d", n.PortBase+portRelay)
}

func (n *Network) lndPort(node string) int {
	if node == NodeLNDB {
		return n.PortBase + portLNDB
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

// Pay pays a new invoice from one LND node to the other. It passes only when
// the payer reports SUCCEEDED and the receiver's invoice is SETTLED.
func (n *Network) Pay(ctx context.Context, from, to string, sats int) error {
	var invoice struct {
		Hash    string `json:"r_hash"`
		Request string `json:"payment_request"`
	}
	if err := n.lncliJSON(ctx, to, &invoice, "addinvoice", "--amt", strconv.Itoa(sats)); err != nil {
		return err
	}
	// A failed payment also exits non-zero, so read the status first.
	output, runErr := n.LNCLI(ctx, from, "payinvoice", "--force", "--json", "--timeout", "30s", invoice.Request)
	status, reason, err := lastPaymentStatus(output)
	if err != nil {
		return errors.Join(fmt.Errorf("%s payment: %w", from, err), runErr)
	}
	if status != "SUCCEEDED" {
		return fmt.Errorf("%s payment: status %s, reason %s", from, status, reason)
	}
	var settled struct {
		State string `json:"state"`
	}
	if err := n.lncliJSON(ctx, to, &settled, "lookupinvoice", invoice.Hash); err != nil {
		return err
	}
	if settled.State != "SETTLED" {
		return fmt.Errorf("%s invoice: state %s, want SETTLED", to, settled.State)
	}
	return nil
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
