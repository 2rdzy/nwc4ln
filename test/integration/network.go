package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// DefaultPortBase keeps clear of other local regtest networks, on 191xx, 193xx
// and 201xx.
const DefaultPortBase = 19500

// Port offsets from the base. Knots binds an onion listener on P2P+1, so that
// port stays reserved.
const (
	portKnotsP2P   = 0
	portKnotsOnion = 1
	portKnotsRPC   = 2
	portZMQBlock   = 4
	portZMQTx      = 5
	portLNDA       = 10 // P2P, then gRPC at +1 and REST at +2.
	portLNDB       = 15
	portLNDC       = 25
	portRelay      = 20
	portCLNA       = 30 // P2P, then clnrest at +1.
)

// Node names.
const (
	NodeKnots = "knots"
	NodeLNDA  = "lnd-a"
	NodeLNDB  = "lnd-b"
	NodeLNDC  = "lnd-c"
	NodeRelay = "relay"
	// NodeCLNA is Core Lightning, with channels to and from lnd-b.
	NodeCLNA = "cln-a"
)

// Throwaway credentials for a regtest node bound to loopback.
const rpcUser, rpcPassword = "nwc4ln-it", "regtest-only"

const (
	markerFile = ".nwc4ln-it"
	stateFile  = "state.json"
)

// Config says where a network lives.
type Config struct {
	Dir      string
	PortBase int
}

// DefaultConfig uses NWC4LN_IT_DIR, or a folder under the system temp folder.
func DefaultConfig() Config {
	dir := os.Getenv("NWC4LN_IT_DIR")
	if dir == "" {
		dir = filepath.Join(os.TempDir(), "nwc4ln", "it")
	}
	return Config{Dir: dir, PortBase: DefaultPortBase}
}

// Network is a network started by Up. It is saved to the state file, so that
// tests in another process can use it.
type Network struct {
	Dir      string
	BinDir   string
	PortBase int
	// Ready is set only when Up has finished, including its test payments.
	Ready bool
}

// Up starts a network, and returns once payments work both ways. If it fails
// it stops what it started and keeps the folder for its logs. Down removes it.
func Up(ctx context.Context, cfg Config) (*Network, error) {
	if _, err := os.Stat(cfg.Dir); err == nil {
		return nil, fmt.Errorf("%s exists: run down first", cfg.Dir)
	}
	if err := checkPortsFree(cfg.PortBase); err != nil {
		return nil, err
	}
	p, err := loadPins()
	if err != nil {
		return nil, err
	}
	binDir, err := fetchBinaries(ctx, p)
	if err != nil {
		return nil, err
	}
	if binDir, err = filepath.EvalSymlinks(binDir); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(cfg.Dir, 0o700); err != nil {
		return nil, err
	}
	if err := os.WriteFile(filepath.Join(cfg.Dir, markerFile), nil, 0o600); err != nil {
		return nil, err
	}
	n := &Network{Dir: cfg.Dir, BinDir: binDir, PortBase: cfg.PortBase}
	if err := n.save(); err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	if err := n.start(ctx); err != nil {
		return nil, errors.Join(fmt.Errorf("up failed, logs are in %s: %w", n.Dir, err), n.stopAll())
	}
	n.Ready = true
	return n, n.save()
}

func checkPortsFree(base int) error {
	offsets := []int{
		portKnotsP2P, portKnotsOnion, portKnotsRPC, portZMQBlock, portZMQTx,
		portLNDA, portLNDA + 1, portLNDA + 2, portLNDB, portLNDB + 1, portLNDB + 2,
		portLNDC, portLNDC + 1, portLNDC + 2, portRelay, portCLNA, portCLNA + 1,
	}
	for _, offset := range offsets {
		listener, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", base+offset))
		if err != nil {
			return fmt.Errorf("port %d is taken: %w", base+offset, err)
		}
		listener.Close()
	}
	return nil
}

func (n *Network) start(ctx context.Context) error {
	if err := n.startKnots(ctx); err != nil {
		return err
	}
	if _, err := n.BitcoinCLI(ctx, "-named", "createwallet", "wallet_name=miner"); err != nil {
		return err
	}
	// Coinbase outputs need 100 confirmations before they can be spent.
	if err := n.Mine(ctx, 110); err != nil {
		return err
	}
	for _, node := range []string{NodeLNDA, NodeLNDB, NodeLNDC, NodeCLNA} {
		if err := n.StartNode(ctx, node); err != nil {
			return err
		}
	}
	if err := n.StartNode(ctx, NodeRelay); err != nil {
		return err
	}
	return n.openChannels(ctx)
}

func (n *Network) knotsDir() string { return filepath.Join(n.Dir, NodeKnots) }

func (n *Network) startKnots(ctx context.Context) error {
	if err := os.MkdirAll(n.knotsDir(), 0o700); err != nil {
		return err
	}
	conf := fmt.Sprintf(`regtest=1
server=1
fallbackfee=0.0002
listenonion=0
dnsseed=0
discover=0
rpcuser=%s
rpcpassword=%s
[regtest]
port=%d
bind=127.0.0.1
rpcport=%d
rpcbind=127.0.0.1
rpcallowip=127.0.0.1
zmqpubrawblock=tcp://127.0.0.1:%d
zmqpubrawtx=tcp://127.0.0.1:%d
testactivationheight=blake2b@1
`, rpcUser, rpcPassword, n.PortBase+portKnotsP2P, n.PortBase+portKnotsRPC,
		n.PortBase+portZMQBlock, n.PortBase+portZMQTx)
	if err := os.WriteFile(filepath.Join(n.knotsDir(), "bitcoin.conf"), []byte(conf), 0o600); err != nil {
		return err
	}
	if err := n.spawn(NodeKnots, "bitcoind", "-datadir="+n.knotsDir()); err != nil {
		return err
	}
	return n.waitFor(ctx, NodeKnots, "Knots RPC", func() error {
		_, err := n.BitcoinCLI(ctx, "getblockcount")
		return err
	})
}

// StartNode starts an LND node or the relay, and waits until it answers.
func (n *Network) StartNode(ctx context.Context, node string) error {
	switch node {
	case NodeLNDA, NodeLNDB, NodeLNDC:
		if err := n.startLND(node); err != nil {
			return err
		}
		return n.waitFor(ctx, node, node+" RPC", func() error {
			_, err := n.getInfo(ctx, node)
			return err
		})
	case NodeCLNA:
		if err := n.startCLN(node); err != nil {
			return err
		}
		return n.waitFor(ctx, node, node+" REST", func() error {
			if _, err := n.CLNCLI(ctx, node, "getinfo"); err != nil {
				return err
			}
			conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", n.PortBase+portCLNA+1), time.Second)
			if err != nil {
				return err
			}
			return conn.Close()
		})
	case NodeRelay:
		err := n.spawn(NodeRelay, "nak", "serve", "--hostname", "127.0.0.1",
			"--port", strconv.Itoa(n.PortBase+portRelay))
		if err != nil {
			return err
		}
		return n.waitFor(ctx, node, "relay", func() error {
			conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", n.PortBase+portRelay), time.Second)
			if err != nil {
				return err
			}
			return conn.Close()
		})
	}
	return fmt.Errorf("unknown node %q", node)
}

func (n *Network) startLND(node string) error {
	dir := n.LNDDir(node)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	base := n.lndPort(node)
	local := func(port int) string { return fmt.Sprintf("127.0.0.1:%d", port) }
	return n.spawn(node, "lnd",
		"--lnddir="+dir,
		"--bitcoin.active", "--bitcoin.regtest", "--bitcoin.node=bitcoind",
		"--bitcoin.blake2b-activation-height=1",
		"--bitcoind.rpchost="+local(n.PortBase+portKnotsRPC),
		"--bitcoind.rpcuser="+rpcUser, "--bitcoind.rpcpass="+rpcPassword,
		"--bitcoind.zmqpubrawblock=tcp://"+local(n.PortBase+portZMQBlock),
		"--bitcoind.zmqpubrawtx=tcp://"+local(n.PortBase+portZMQTx),
		"--listen="+local(base), "--rpclisten="+local(base+1), "--restlisten="+local(base+2),
		"--alias="+node, "--noseedbackup", "--nobootstrap", "--trickledelay=1000", "--maxbackoff=2s",
	)
}

// clnFeerate fixes Core Lightning's fee rates, in sat per 1,000 weight units. Left
// to its estimates, it updated a channel's fee during a payment, which moves the
// figures tests compare exactly. With no offset on top, an HTLC output's fee
// (172 weight units) is a whole number of sats, as on the LND channels.
const clnFeerate = "2500"

// startCLN starts Core Lightning with clnrest over HTTPS, as a real node would
// have it. It polls Knots every second rather than every 30, and its gossip is
// fast, so that a new channel is usable in seconds.
func (n *Network) startCLN(node string) error {
	dir := n.CLNDir(node)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	return n.spawn(node, clnBinary,
		"--network=regtest", "--lightning-dir="+dir, "--alias="+node,
		fmt.Sprintf("--addr=127.0.0.1:%d", n.PortBase+portCLNA), "--disable-dns", "--disable-plugin=cln-grpc",
		"--bitcoin-cli="+filepath.Join(n.BinDir, "bitcoin-cli"), "--bitcoin-datadir="+n.knotsDir(),
		"--bitcoin-rpcconnect=127.0.0.1", fmt.Sprintf("--bitcoin-rpcport=%d", n.PortBase+portKnotsRPC),
		"--bitcoin-rpcuser="+rpcUser, "--bitcoin-rpcpassword="+rpcPassword,
		"--clnrest-host=127.0.0.1", fmt.Sprintf("--clnrest-port=%d", n.PortBase+portCLNA+1), "--clnrest-protocol=https",
		"--developer", "--dev-bitcoind-poll=1", "--dev-fast-gossip", "--force-feerates="+clnFeerate, "--commit-feerate-offset=0",
	)
}

func (n *Network) openChannels(ctx context.Context) error {
	var clnAddress struct {
		Address string `json:"bech32"`
	}
	if err := n.clnJSON(ctx, NodeCLNA, &clnAddress, "-k", "newaddr", "addresstype=bech32"); err != nil {
		return err
	}
	if _, err := n.BitcoinCLI(ctx, "-rpcwallet=miner", "sendtoaddress", clnAddress.Address, "1"); err != nil {
		return err
	}
	for _, node := range []string{NodeLNDA, NodeLNDB} {
		var address struct {
			Address string `json:"address"`
		}
		if err := n.lncliJSON(ctx, node, &address, "newaddress", "p2wkh"); err != nil {
			return err
		}
		if _, err := n.BitcoinCLI(ctx, "-rpcwallet=miner", "sendtoaddress", address.Address, "1"); err != nil {
			return err
		}
	}
	if err := n.Mine(ctx, 6); err != nil {
		return err
	}
	if err := n.waitSynced(ctx); err != nil {
		return err
	}
	infoB, err := n.getInfo(ctx, NodeLNDB)
	if err != nil {
		return err
	}
	infoA, err := n.getInfo(ctx, NodeLNDA)
	if err != nil {
		return err
	}
	address := fmt.Sprintf("%s@127.0.0.1:%d", infoB.PubKey, n.lndPort(NodeLNDB))
	if _, err := n.LNCLI(ctx, NodeLNDA, "connect", address); err != nil {
		return err
	}
	// The second open spends change, which needs a confirmation.
	if _, err := n.LNCLI(ctx, NodeLNDA, "openchannel", "--node_key", infoB.PubKey, "--local_amt", "1000000"); err != nil {
		return err
	}
	if err := n.Mine(ctx, 1); err != nil {
		return err
	}
	if err := n.waitSynced(ctx); err != nil {
		return err
	}
	if _, err := n.LNCLI(ctx, NodeLNDB, "openchannel", "--node_key", infoA.PubKey, "--local_amt", "1000000"); err != nil {
		return err
	}
	if err := n.Mine(ctx, 1); err != nil {
		return err
	}
	if err := n.openChannelToC(ctx); err != nil {
		return err
	}
	if err := n.Mine(ctx, 1); err != nil {
		return err
	}
	if err := n.openChannelsCLN(ctx); err != nil {
		return err
	}
	if err := n.Mine(ctx, 6); err != nil {
		return err
	}
	if err := n.WaitChannels(ctx); err != nil {
		return err
	}
	// Spendable counts a channel below its reserve as 0, so income on one would not
	// show. Move lnd-a's channel from lnd-b well above its 10,000 sat reserve.
	if err := n.Pay(ctx, NodeLNDB, NodeLNDA, 50_000); err != nil {
		return err
	}
	if err := n.WaitRouted(ctx); err != nil {
		return err
	}
	// The same for cln-a's channel from lnd-b, once cln-a's channels work.
	if err := n.WaitCLN(ctx); err != nil {
		return err
	}
	if err := n.Pay(ctx, NodeLNDB, NodeCLNA, 50_000); err != nil {
		return err
	}
	return n.WaitRoutedFrom(ctx, NodeCLNA)
}

// openChannelsCLN opens a channel each way between cln-a and lnd-b, so that
// cln-a can pay lnd-b and lnd-c, and be paid. cln-a opens one, so its figures
// include the commitment fee that the opener pays.
func (n *Network) openChannelsCLN(ctx context.Context) error {
	if err := n.waitSynced(ctx); err != nil {
		return err
	}
	infoB, err := n.getInfo(ctx, NodeLNDB)
	if err != nil {
		return err
	}
	var infoCLN struct {
		ID string `json:"id"`
	}
	if err := n.clnJSON(ctx, NodeCLNA, &infoCLN, "getinfo"); err != nil {
		return err
	}
	address := fmt.Sprintf("%s@127.0.0.1:%d", infoB.PubKey, n.lndPort(NodeLNDB))
	if _, err := n.CLNCLI(ctx, NodeCLNA, "connect", address); err != nil {
		return err
	}
	// The push gives lnd-b an output on this channel from the start. Without one, its
	// anchor is missing until the first HTLC, so a payment over this channel would
	// take 330 sat more than its HTLC from cln-a's figure.
	if _, err := n.CLNCLI(ctx, NodeCLNA, "-k", "fundchannel", "id="+infoB.PubKey, "amount=1000000",
		"push_msat=100000000"); err != nil {
		return err
	}
	_, err = n.LNCLI(ctx, NodeLNDB, "openchannel", "--node_key", infoCLN.ID, "--local_amt", "1000000")
	return err
}

// openChannelToC puts lnd-c behind lnd-b, so that a payment from lnd-a to it pays a
// real routing fee.
func (n *Network) openChannelToC(ctx context.Context) error {
	if err := n.waitSynced(ctx); err != nil {
		return err
	}
	infoC, err := n.getInfo(ctx, NodeLNDC)
	if err != nil {
		return err
	}
	address := fmt.Sprintf("%s@127.0.0.1:%d", infoC.PubKey, n.lndPort(NodeLNDC))
	if _, err := n.LNCLI(ctx, NodeLNDB, "connect", address); err != nil {
		return err
	}
	_, err = n.LNCLI(ctx, NodeLNDB, "openchannel", "--node_key", infoC.PubKey, "--local_amt", "1000000")
	return err
}

func (n *Network) waitSynced(ctx context.Context) error {
	return n.waitFor(ctx, "", "nodes to sync", func() error {
		tip, err := n.BitcoinCLI(ctx, "getblockcount")
		if err != nil {
			return err
		}
		var cln struct {
			Height int `json:"blockheight"`
		}
		if err := n.clnJSON(ctx, NodeCLNA, &cln, "getinfo"); err != nil {
			return err
		}
		if strconv.Itoa(cln.Height) != tip {
			return fmt.Errorf("%s is at %d, Knots at %s", NodeCLNA, cln.Height, tip)
		}
		for _, node := range []string{NodeLNDA, NodeLNDB, NodeLNDC} {
			info, err := n.getInfo(ctx, node)
			if err != nil {
				return err
			}
			if !info.Synced {
				return fmt.Errorf("%s is not synced", node)
			}
		}
		return nil
	})
}

// WaitChannels returns once a payment works in each direction. "active" on a
// channel is not enough: a payment sent right after it can still fail with
// FAILURE_REASON_INSUFFICIENT_BALANCE.
func (n *Network) WaitChannels(ctx context.Context) error {
	for _, nodes := range [][2]string{{NodeLNDA, NodeLNDB}, {NodeLNDB, NodeLNDA}} {
		err := n.waitFor(ctx, "", fmt.Sprintf("payment from %s to %s", nodes[0], nodes[1]), func() error {
			return n.Pay(ctx, nodes[0], nodes[1], 1)
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// WaitRouted returns once lnd-a can pay lnd-c through lnd-b. lnd-a must first hear of
// that channel by gossip. Call it after restarting either node in a test that needs
// the route.
func (n *Network) WaitRouted(ctx context.Context) error {
	return n.WaitRoutedFrom(ctx, NodeLNDA)
}

// WaitRoutedFrom returns once a node can pay lnd-c through lnd-b.
func (n *Network) WaitRoutedFrom(ctx context.Context, node string) error {
	return n.waitFor(ctx, "", "payment from "+node+" to lnd-c", func() error {
		return n.Pay(ctx, node, NodeLNDC, 1)
	})
}

// WaitCLN returns once cln-a and lnd-b can pay each other. Call it after
// restarting either in a test that uses cln-a.
func (n *Network) WaitCLN(ctx context.Context) error {
	for _, nodes := range [][2]string{{NodeCLNA, NodeLNDB}, {NodeLNDB, NodeCLNA}} {
		err := n.waitFor(ctx, "", fmt.Sprintf("payment from %s to %s", nodes[0], nodes[1]), func() error {
			return n.Pay(ctx, nodes[0], nodes[1], 1)
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// waitFor polls check until it passes. If node is set and has died, it stops
// early rather than waiting for the timeout.
func (n *Network) waitFor(ctx context.Context, node, what string, check func() error) error {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	for {
		err := check()
		if err == nil {
			return nil
		}
		if node != "" && !n.running(node) {
			return fmt.Errorf("%s exited, see %s", node, n.logPath(node))
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("timeout waiting for %s: %w", what, err)
		case <-time.After(500 * time.Millisecond):
		}
	}
}

func (n *Network) spawn(node, binary string, args ...string) error {
	log, err := os.OpenFile(n.logPath(node), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	defer log.Close()
	cmd := exec.Command(filepath.Join(n.BinDir, binary), args...)
	cmd.Stdout, cmd.Stderr = log, log
	// Its own session, so that it outlives the command that ran up.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := cmd.Start(); err != nil {
		return err
	}
	go func() { _ = cmd.Wait() }() // Reap it, if it stops while this process lives.
	return os.WriteFile(n.pidPath(node), []byte(strconv.Itoa(cmd.Process.Pid)), 0o600)
}

func (n *Network) logPath(node string) string { return filepath.Join(n.Dir, node+".log") }
func (n *Network) pidPath(node string) string { return filepath.Join(n.Dir, node+".pid") }

// clnBinary is lightningd's path in the binary folder.
var clnBinary = filepath.Join("cln", "usr", "bin", "lightningd")

func binaryOf(node string) string {
	switch node {
	case NodeKnots:
		return "bitcoind"
	case NodeRelay:
		return "nak"
	case NodeCLNA:
		return clnBinary
	}
	return "lnd"
}

// pidOf returns the node's process, or 0 if it is not running. It compares the
// executable, so that a reused PID is never taken for the node.
func (n *Network) pidOf(node string) int {
	data, err := os.ReadFile(n.pidPath(node))
	if err != nil {
		return 0
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil {
		return 0
	}
	exe, err := os.Readlink(fmt.Sprintf("/proc/%d/exe", pid))
	if err != nil || exe != filepath.Join(n.BinDir, binaryOf(node)) {
		return 0
	}
	return pid
}

func (n *Network) running(node string) bool { return n.pidOf(node) != 0 }

// Running says whether a node's process is up.
func (n *Network) Running(node string) bool { return n.running(node) }

// StopNode stops one node by its recorded PID. SIGTERM is a clean shutdown for
// all of them. SIGKILL follows only if the node does not exit in time.
func (n *Network) StopNode(node string) error {
	pid := n.pidOf(node)
	if pid == 0 {
		return nil
	}
	if err := syscall.Kill(pid, syscall.SIGTERM); err != nil {
		return fmt.Errorf("stop %s: %w", node, err)
	}
	if n.waitExit(node, 30*time.Second) {
		return nil
	}
	if err := syscall.Kill(pid, syscall.SIGKILL); err != nil {
		return fmt.Errorf("kill %s: %w", node, err)
	}
	if n.waitExit(node, 5*time.Second) {
		return nil
	}
	return fmt.Errorf("%s (PID %d) did not exit", node, pid)
}

func (n *Network) waitExit(node string, limit time.Duration) bool {
	for deadline := time.Now().Add(limit); time.Now().Before(deadline); {
		if !n.running(node) {
			return true
		}
		time.Sleep(100 * time.Millisecond)
	}
	return !n.running(node)
}

func (n *Network) stopAll() error {
	var errs []error
	for _, node := range []string{NodeRelay, NodeCLNA, NodeLNDC, NodeLNDB, NodeLNDA, NodeKnots} {
		errs = append(errs, n.StopNode(node))
	}
	return errors.Join(errs...)
}

// Down stops the network and removes its folder. A folder that is not a harness
// folder is left alone, because NWC4LN_IT_DIR could point anywhere.
func Down(cfg Config) error {
	if _, err := os.Stat(cfg.Dir); errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if _, err := os.Stat(filepath.Join(cfg.Dir, markerFile)); err != nil {
		return fmt.Errorf("refusing to remove %s: it has no %s file", cfg.Dir, markerFile)
	}
	if n, err := read(cfg); err == nil {
		if err := n.stopAll(); err != nil {
			return err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return os.RemoveAll(cfg.Dir)
}

func (n *Network) save() error {
	data, err := json.MarshalIndent(n, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(n.Dir, stateFile), data, 0o600)
}

func read(cfg Config) (*Network, error) {
	data, err := os.ReadFile(filepath.Join(cfg.Dir, stateFile))
	if err != nil {
		return nil, err
	}
	n := &Network{}
	return n, json.Unmarshal(data, n)
}

// Load returns the network started by Up.
func Load(cfg Config) (*Network, error) {
	n, err := read(cfg)
	if err != nil {
		return nil, fmt.Errorf("no network at %s, start one with `go run ./test/integration/cmd/regtest up`: %w", cfg.Dir, err)
	}
	if !n.Ready {
		return nil, fmt.Errorf("the network at %s did not finish starting", cfg.Dir)
	}
	return n, nil
}
