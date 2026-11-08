package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"io"
	"math/big"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/2rdzy/nwc4ln/internal/config"
	"github.com/2rdzy/nwc4ln/internal/nwc"
	"github.com/2rdzy/nwc4ln/internal/nwc/nwctest"
	"github.com/2rdzy/nwc4ln/internal/store"
)

// A failure here means a repeated init wipes the ledger or the settings.
func TestInitRefusesExistingData(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "new", "nwc4ln.toml")
	if err := run(ctx, []string{"init", "--config", path}, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	database := filepath.Join(filepath.Dir(path), databaseName)
	before, err := os.ReadFile(database)
	if err != nil {
		t.Fatal(err)
	}
	if err := run(ctx, []string{"init", "--config", path}, &bytes.Buffer{}); err == nil {
		t.Fatal("no error from a second init")
	}

	// Config gone, database still there: still refused.
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := run(ctx, []string{"init", "--config", path}, &bytes.Buffer{}); err == nil {
		t.Fatal("init ran over an existing database")
	}
	after, err := os.ReadFile(database)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("database changed")
	}
}

// A failure here means keys or settings are readable by other users.
func TestInitFileModes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "new", "nwc4ln.toml")
	if err := run(context.Background(), []string{"init", "--config", path}, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Dir(path)
	wants := map[string]os.FileMode{dir: dataDirMode, path: 0o600, filepath.Join(dir, databaseName): 0o600}
	for name, want := range wants {
		info, err := os.Stat(name)
		if err != nil {
			t.Fatal(err)
		}
		if got := info.Mode().Perm(); got != want {
			t.Errorf("%s has mode %o, want %o", name, got, want)
		}
	}
}

// A failure here means the operator gets a confusing failure instead of the cause.
func TestStartRefusesWithoutInit(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nwc4ln.toml")
	err := run(context.Background(), []string{"start", "--config", path}, &bytes.Buffer{})
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("got %v", err)
	}
	if err := config.Write(path, config.Default(filepath.Dir(path))); err != nil {
		t.Fatal(err)
	}
	err = run(context.Background(), []string{"start", "--config", path}, &bytes.Buffer{})
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("got %v", err)
	}
}

// Catches: a second service on the same data folder.
func TestStartRefusesWhileRunning(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "nwc4ln.toml")
	if err := run(t.Context(), []string{"init", "--config", path}, io.Discard); err != nil {
		t.Fatal(err)
	}
	release, err := store.Lock(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = release() }()
	// Without the lock, start would run until this ends.
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	err = run(ctx, []string{"start", "--config", path}, io.Discard)
	if !errors.Is(err, store.ErrLocked) {
		t.Fatalf("start: %v", err)
	}
}

func TestUnknownCommandShowsUsage(t *testing.T) {
	// App changes go through the running service's UI, so "app add" is gone.
	for _, args := range [][]string{nil, {"nonsense"}, {"init", "extra"}, {"app", "add", "--name", "shop"}} {
		err := run(context.Background(), args, &bytes.Buffer{})
		if err == nil || !strings.Contains(err.Error(), "usage") {
			t.Errorf("args %v: got %v", args, err)
		}
	}
}

// Runs the real binary. A failure here means a stop signal leaves the database
// open: a clean close removes the WAL file.
func TestStartExitsCleanlyOnSignal(t *testing.T) {
	dir := t.TempDir()
	binary := filepath.Join(dir, "nwc4ln")
	if out, err := exec.Command("go", "build", "-o", binary, ".").CombinedOutput(); err != nil {
		t.Fatalf("build: %v: %s", err, out)
	}
	path := filepath.Join(dir, "data", "nwc4ln.toml")
	if out, err := exec.Command(binary, "init", "--config", path).CombinedOutput(); err != nil {
		t.Fatalf("init: %v: %s", err, out)
	}
	writeServiceConfig(t, path, "ws://127.0.0.1:1")

	for _, sig := range []syscall.Signal{syscall.SIGTERM, syscall.SIGINT} {
		t.Run(sig.String(), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, binary, "start", "--config", path)
			stdout, err := cmd.StdoutPipe()
			if err != nil {
				t.Fatal(err)
			}
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			lines := bufio.NewScanner(stdout)
			if !lines.Scan() || lines.Text() != "started" {
				t.Fatalf("first line %q", lines.Text())
			}
			if _, err := os.Stat(filepath.Join(dir, "data", databaseName+"-wal")); err != nil {
				t.Fatalf("expected an open WAL file: %v", err)
			}
			if err := cmd.Process.Signal(sig); err != nil {
				t.Fatal(err)
			}
			if !lines.Scan() || lines.Text() != "stopped" {
				t.Fatalf("last line %q", lines.Text())
			}
			if err := cmd.Wait(); err != nil {
				t.Fatalf("exit: %v", err)
			}
			if _, err := os.Stat(filepath.Join(dir, "data", databaseName+"-wal")); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("WAL file left behind: %v", err)
			}
		})
	}
}

// writeServiceConfig replaces an init config with one start can use. Nothing
// listens on the relay or LND addresses: start must run anyway.
func writeServiceConfig(t *testing.T, path, relay string) {
	t.Helper()
	dir := filepath.Dir(path)
	cfg := config.Default(dir)
	cfg.Listen = "127.0.0.1:0" // Any free port, so tests cannot clash.
	cfg.Relay = relay
	cfg.LNDHost = "127.0.0.1:1"
	cfg.LNDTLSCertPath = filepath.Join(dir, "tls.cert")
	cfg.LNDMacaroonPath = filepath.Join(dir, "test.macaroon")
	if err := os.WriteFile(cfg.LNDTLSCertPath, testCertificate(t), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(cfg.LNDMacaroonPath, []byte("macaroon"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := config.Write(path, cfg); err != nil {
		t.Fatal(err)
	}
}

func testCertificate(t *testing.T) []byte {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1), NotAfter: time.Now().Add(time.Hour)}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

// Catches: the service running with no way to hear a client.
func TestStartRefusesBadRelay(t *testing.T) {
	for _, relay := range []string{"", "https://relay.example", "127.0.0.1:7777"} {
		path := filepath.Join(t.TempDir(), "nwc4ln.toml")
		if err := run(t.Context(), []string{"init", "--config", path}, io.Discard); err != nil {
			t.Fatal(err)
		}
		writeServiceConfig(t, path, relay)
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
		err := run(ctx, []string{"start", "--config", path}, io.Discard)
		cancel()
		if err == nil || !strings.Contains(err.Error(), "relay") {
			t.Errorf("relay %q: got %v", relay, err)
		}
	}
}

// Catches: start using the LND client, or no client, when the config names Core
// Lightning. The rune file may not be readable by others, and start must say so.
func TestStartUsesTheConfiguredNode(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nwc4ln.toml")
	if err := run(t.Context(), []string{"init", "--config", path}, io.Discard); err != nil {
		t.Fatal(err)
	}
	writeServiceConfig(t, path, "ws://127.0.0.1:1")
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	runePath := filepath.Join(filepath.Dir(path), "rune")
	if err := os.WriteFile(runePath, []byte("r"), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg.Node, cfg.CLNURL, cfg.CLNCACertPath, cfg.CLNRunePath = config.NodeCLN, "https://127.0.0.1:1", path, runePath
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := config.Write(path, cfg); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	err = run(ctx, []string{"start", "--config", path}, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "cln: rune file") {
		t.Fatalf("got %v, want the Core Lightning client's rune check", err)
	}
}

// Catches: a backup that refuses, or waits, while the service holds the data folder.
func TestBackupWhileRunning(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "nwc4ln.toml")
	if err := run(t.Context(), []string{"init", "--config", path}, io.Discard); err != nil {
		t.Fatal(err)
	}
	release, err := store.Lock(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = release() }()
	dest := filepath.Join(t.TempDir(), "backup.db")
	if err := run(t.Context(), []string{"backup", "--config", path, dest}, io.Discard); err != nil {
		t.Fatalf("backup while locked: %v", err)
	}
	if _, err := os.Stat(dest); err != nil {
		t.Fatal(err)
	}
	if err := run(t.Context(), []string{"backup", "--config", path}, io.Discard); err == nil ||
		!strings.Contains(err.Error(), "usage") {
		t.Fatalf("no file: %v", err)
	}
}

func adminKeyIn(t *testing.T, dir string) string {
	t.Helper()
	db, err := store.Open(t.Context(), filepath.Join(dir, databaseName))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	key, err := db.AdminKey(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	return key
}

// Catches: an admin key from init or set-admin that is not stored, or stored wrongly;
// a set-admin that runs beside the service; and a bad npub accepted as a key.
func TestAdminKey(t *testing.T) {
	_, first := nwctest.NewKey()
	_, second := nwctest.NewKey()
	firstNpub, err := nwc.EncodeNpub(first)
	if err != nil {
		t.Fatal(err)
	}
	secondNpub, err := nwc.EncodeNpub(second)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "nwc4ln.toml")
	if err := run(t.Context(), []string{"init", "--admin", "npub1nonsense", "--config", path}, io.Discard); err == nil {
		t.Fatal("init took a bad npub")
	}
	if err := run(t.Context(), []string{"init", "--admin", firstNpub, "--config", path}, io.Discard); err != nil {
		t.Fatal(err)
	}
	if got := adminKeyIn(t, dir); got != first {
		t.Fatalf("admin key %q after init, want %q", got, first)
	}

	release, err := store.Lock(dir) // The service is running.
	if err != nil {
		t.Fatal(err)
	}
	if err := run(t.Context(), []string{"set-admin", "--config", path, secondNpub}, io.Discard); !errors.Is(err, store.ErrLocked) {
		t.Fatalf("set-admin while running: %v", err)
	}
	if err := release(); err != nil {
		t.Fatal(err)
	}
	nsec := "nsec1vl029mgpspedva04g90vltkh6fvh240zqtv9k0t9af8935ke9laqsnlfe5"
	for _, bad := range []string{nsec, "npub1nonsense", second} {
		if err := run(t.Context(), []string{"set-admin", "--config", path, bad}, io.Discard); err == nil {
			t.Errorf("set-admin took %q", bad)
		}
	}
	if got := adminKeyIn(t, dir); got != first {
		t.Fatalf("admin key changed to %q by a refused set-admin", got)
	}
	if err := run(t.Context(), []string{"set-admin", "--config", path, secondNpub}, io.Discard); err != nil {
		t.Fatal(err)
	}
	if got := adminKeyIn(t, dir); got != second {
		t.Fatalf("admin key %q after set-admin, want %q", got, second)
	}
}
