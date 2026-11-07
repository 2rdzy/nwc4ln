// Command nwc4ln offers Nostr Wallet Connect on top of a Lightning node.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/2rdzy/nwc4ln/internal/config"
	"github.com/2rdzy/nwc4ln/internal/node/lnd"
	"github.com/2rdzy/nwc4ln/internal/nwc"
	"github.com/2rdzy/nwc4ln/internal/store"
)

const (
	databaseName = "nwc4ln.db"
	dataDirMode  = 0o700
	usage        = "usage: nwc4ln <init|start|app add --name name [--invoice]> [--config path]"
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
	command, rest := args[0], args[1:]
	if command == "app" {
		if len(rest) == 0 || rest[0] != "add" {
			return errors.New(usage)
		}
		command, rest = "app add", rest[1:]
	}
	flags := flag.NewFlagSet(command, flag.ContinueOnError)
	flags.SetOutput(out)
	configPath := flags.String("config", "", "config file (default ~/.nwc4ln/nwc4ln.toml)")
	name := flags.String("name", "", "app name (app add)")
	canInvoice := flags.Bool("invoice", false, "let the app make invoices (app add)")
	if err := flags.Parse(rest); err != nil {
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
	switch command {
	case "init":
		return runInit(ctx, path, out)
	case "start":
		return runStart(ctx, path, out)
	case "app add":
		return runAppAdd(ctx, path, *name, *canInvoice, out)
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

// runStart serves NWC requests until a stop signal. A relay or node that is down
// does not stop it: the service keeps trying, and answers what it can.
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
	if err := nwc.CheckRelay(cfg.Relay); err != nil {
		return err
	}
	client, err := lnd.New(lnd.Config{Host: cfg.LNDHost, TLSCertPath: cfg.LNDTLSCertPath, MacaroonPath: cfg.LNDMacaroonPath})
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, client.Close()) }()
	service, err := nwc.New(ctx, db, client, cfg.Relay, slog.Default())
	if err != nil {
		return err
	}
	fmt.Fprintln(out, "started")
	service.Run(ctx)
	fmt.Fprintln(out, "stopped")
	return nil
}

// runAppAdd creates an app and prints its connection string. It takes the data
// folder lock, so it refuses while the service runs: nothing changes the data
// from outside the running service.
func runAppAdd(ctx context.Context, configPath, name string, canInvoice bool, out io.Writer) (err error) {
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
	connectionString, err := nwc.AddApp(ctx, db, name, cfg.Relay, canInvoice)
	if err != nil {
		return err
	}
	fmt.Fprintln(out, connectionString)
	return nil
}
