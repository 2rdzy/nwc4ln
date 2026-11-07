//go:build integration

// Package nwc_test runs the real nwc4ln binary against the regtest relay and LND.
package nwc_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/2rdzy/nwc4ln/internal/config"
	"github.com/2rdzy/nwc4ln/internal/node/lnd"
	"github.com/2rdzy/nwc4ln/internal/nwc/nwctest"
	"github.com/2rdzy/nwc4ln/internal/store"
	"github.com/2rdzy/nwc4ln/test/integration"
)

const (
	self        = integration.NodeLNDA
	callTimeout = 10 * time.Second
	// silence is how long to wait for a reply that must not come.
	silence = 3 * time.Second
)

// service is a running nwc4ln.
type service struct {
	dir              string
	connectionString string
}

// startService builds nwc4ln, adds one app and starts it. It stops by PID in a
// cleanup, so a failed test leaves no process behind.
func startService(t *testing.T, n *integration.Network) *service {
	t.Helper()
	ctx := t.Context()
	dir := filepath.Join(t.TempDir(), "data")
	binary := filepath.Join(filepath.Dir(dir), "nwc4ln")
	if out, err := exec.CommandContext(ctx, "go", "build", "-o", binary, "../../../cmd/nwc4ln").CombinedOutput(); err != nil {
		t.Fatalf("build: %v: %s", err, out)
	}
	path := filepath.Join(dir, "nwc4ln.toml")
	if out, err := exec.CommandContext(ctx, binary, "init", "--config", path).CombinedOutput(); err != nil {
		t.Fatalf("init: %v: %s", err, out)
	}
	macaroon := filepath.Join(dir, "nwc4ln.macaroon")
	args := []string{"bakemacaroon", "--save_to", macaroon}
	for _, method := range lnd.Methods {
		args = append(args, "uri:"+method)
	}
	if out, err := n.LNCLI(ctx, self, args...); err != nil {
		t.Fatalf("bake macaroon: %v: %s", err, out)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	cfg := config.Default(dir)
	cfg.Relay = n.RelayURL()
	cfg.LNDHost = n.LNDHost(self)
	cfg.LNDTLSCertPath = filepath.Join(n.LNDDir(self), "tls.cert")
	cfg.LNDMacaroonPath = macaroon
	if err := config.Write(path, cfg); err != nil {
		t.Fatal(err)
	}
	add := exec.CommandContext(ctx, binary, "app", "add", "--name", "test", "--config", path)
	stdout, err := add.Output()
	if err != nil {
		t.Fatalf("app add: %v", err)
	}
	logFile, err := os.Create(filepath.Join(filepath.Dir(dir), "nwc4ln.log"))
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(binary, "start", "--config", path)
	cmd.Stdout, cmd.Stderr = logFile, logFile
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Signal(os.Interrupt)
		_ = cmd.Wait()
		_ = logFile.Close()
		if t.Failed() {
			if log, err := os.ReadFile(logFile.Name()); err == nil {
				t.Logf("nwc4ln log:\n%s", log)
			}
		}
	})
	return &service{dir: dir, connectionString: strings.TrimSpace(string(stdout))}
}

// waitForInfo connects a client and waits until the service has published.
func waitForInfo(t *testing.T, c *nwctest.Client) *nwctest.Info {
	t.Helper()
	var last error
	for deadline := time.Now().Add(30 * time.Second); time.Now().Before(deadline); time.Sleep(200 * time.Millisecond) {
		ctx, cancel := context.WithTimeout(t.Context(), callTimeout)
		if last = c.Connect(ctx); last == nil {
			var info *nwctest.Info
			if info, last = c.ReadInfo(ctx); last == nil {
				cancel()
				return info
			}
		}
		cancel()
	}
	t.Fatalf("no info event: %v", last)
	return nil
}

func call(t *testing.T, c *nwctest.Client, scheme, method string) *nwctest.Response {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), callTimeout)
	defer cancel()
	response, err := c.Call(ctx, scheme, method)
	if err != nil {
		t.Fatalf("%s %s: %v", scheme, method, err)
	}
	return response
}

// Catches: wiring to the relay and LND, a wrong key being served, a revoked key
// being served, and the service going deaf when the relay restarts.
func TestNWC(t *testing.T) {
	n, err := integration.Load(integration.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	s := startService(t, n)
	client, err := nwctest.Parse(s.connectionString)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	t.Run("InfoEvent", func(t *testing.T) {
		info := waitForInfo(t, client)
		if !slices.Equal(info.Methods, []string{"get_info"}) || info.Encryption != "nip44_v2 nip04" {
			t.Fatalf("got %+v", info)
		}
	})

	t.Run("GetInfo", func(t *testing.T) {
		for _, scheme := range []string{nwctest.NIP44, nwctest.NIP04, nwctest.NoTag} {
			response := call(t, client, scheme, "get_info")
			if response.Error != nil {
				t.Fatalf("%q: %+v", scheme, response.Error)
			}
			var result struct {
				PubKey      string `json:"pubkey"`
				Network     string `json:"network"`
				BlockHeight uint32 `json:"block_height"`
				BlockHash   string `json:"block_hash"`
			}
			if err := json.Unmarshal(response.Result, &result); err != nil {
				t.Fatal(err)
			}
			if len(result.PubKey) != 66 || result.Network != "regtest" || result.BlockHeight == 0 || len(result.BlockHash) != 64 {
				t.Fatalf("%q: got %+v", scheme, result)
			}
		}
	})

	t.Run("WrongKey", func(t *testing.T) {
		stranger, err := client.Stranger()
		if err != nil {
			t.Fatal(err)
		}
		defer stranger.Close()
		ctx, cancel := context.WithTimeout(t.Context(), silence)
		defer cancel()
		if err := stranger.Connect(ctx); err != nil {
			t.Fatal(err)
		}
		if response, err := stranger.Call(ctx, nwctest.NIP44, "get_info"); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("got %+v, %v", response, err)
		}
	})

	t.Run("RelayRestarts", func(t *testing.T) {
		if err := n.StopNode(integration.NodeRelay); err != nil {
			t.Fatal(err)
		}
		// The relay keeps nothing, so a served request below proves a new
		// subscription, and a readable info event proves it was published again.
		if err := n.StartNode(t.Context(), integration.NodeRelay); err != nil {
			t.Fatal(err)
		}
		waitForInfo(t, client)
		if response := call(t, client, nwctest.NIP44, "get_info"); response.Error != nil {
			t.Fatalf("after restart: %+v", response.Error)
		}
	})

	t.Run("Revoked", func(t *testing.T) {
		db, err := store.Open(t.Context(), filepath.Join(s.dir, "nwc4ln.db"))
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = db.Close() }()
		if _, err := db.DB.ExecContext(t.Context(), `UPDATE connections SET revoked_at = 1`); err != nil {
			t.Fatal(err)
		}
		response := call(t, client, nwctest.NIP44, "get_info")
		if response.Error == nil || response.Error.Code != "UNAUTHORIZED" {
			t.Fatalf("got %+v", response)
		}
	})
}
