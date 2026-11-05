// Command nwc4ln offers Nostr Wallet Connect on top of a Lightning node.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/2rdzy/nwc4ln/internal/config"
	"github.com/2rdzy/nwc4ln/internal/store"
)

const (
	databaseName = "nwc4ln.db"
	dataDirMode  = 0o700
	usage        = "usage: nwc4ln <init|start> [--config path]"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	err := run(ctx, os.Args[1:], os.Stdout)
	stop()
	if err != nil {
		fmt.Fprintln(os.Stderr, "nwc4ln:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, out io.Writer) error {
	if len(args) == 0 {
		return errors.New(usage)
	}
	flags := flag.NewFlagSet(args[0], flag.ContinueOnError)
	flags.SetOutput(out)
	configPath := flags.String("config", "", "config file (default ~/.nwc4ln/nwc4ln.toml)")
	if err := flags.Parse(args[1:]); err != nil {
		return err
	}
	if flags.NArg() > 0 {
		return errors.New(usage)
	}
	if *configPath == "" {
		path, err := config.DefaultPath()
		if err != nil {
			return err
		}
		*configPath = path
	}
	path, err := filepath.Abs(*configPath)
	if err != nil {
		return err
	}
	switch args[0] {
	case "init":
		return runInit(ctx, path, out)
	case "start":
		return runStart(ctx, path, out)
	default:
		return errors.New(usage)
	}
}

// runInit creates the data folder, database and config. It refuses to run over
// existing data, because that would lose the ledger. The config is written
// last, so a failed init can be run again once the cause is fixed.
func runInit(ctx context.Context, configPath string, out io.Writer) error {
	dataDir := filepath.Dir(configPath)
	databasePath := filepath.Join(dataDir, databaseName)
	for _, path := range []string{configPath, databasePath} {
		if _, err := os.Lstat(path); err == nil {
			return fmt.Errorf("%s already exists, refusing to overwrite it", path)
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	if err := os.MkdirAll(dataDir, dataDirMode); err != nil {
		return fmt.Errorf("create data folder: %w", err)
	}
	db, err := store.Create(ctx, databasePath)
	if err != nil {
		return err
	}
	if err := db.Close(); err != nil {
		return fmt.Errorf("close database: %w", err)
	}
	if err := config.Write(configPath, config.Default(dataDir)); err != nil {
		return err
	}
	fmt.Fprintln(out, "created", configPath)
	return nil
}

// runStart opens the database and waits for a stop signal.
func runStart(ctx context.Context, configPath string, out io.Writer) (err error) {
	cfg, err := config.Load(configPath)
	if err != nil {
		return err
	}
	release, err := store.Lock(cfg.DataDir)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, release()) }()
	db, err := store.Open(ctx, filepath.Join(cfg.DataDir, databaseName))
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, db.Close()) }()
	fmt.Fprintln(out, "started")
	<-ctx.Done()
	fmt.Fprintln(out, "stopped")
	return nil
}
