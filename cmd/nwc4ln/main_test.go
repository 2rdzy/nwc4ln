package main

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/2rdzy/nwc4ln/internal/config"
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

func TestUnknownCommandShowsUsage(t *testing.T) {
	for _, args := range [][]string{nil, {"nonsense"}, {"init", "extra"}} {
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
