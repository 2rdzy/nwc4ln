// Command nwc4ln offers Nostr Wallet Connect on top of a Lightning node.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/2rdzy/nwc4ln/internal/config"
	"github.com/2rdzy/nwc4ln/internal/ledger"
	"github.com/2rdzy/nwc4ln/internal/node"
	"github.com/2rdzy/nwc4ln/internal/node/cln"
	"github.com/2rdzy/nwc4ln/internal/node/lnd"
	"github.com/2rdzy/nwc4ln/internal/nwc"
	"github.com/2rdzy/nwc4ln/internal/store"
	"github.com/2rdzy/nwc4ln/internal/web"
)

const (
	databaseName = "nwc4ln.db"
	dataDirMode  = 0o700
	usage        = "usage: nwc4ln init [--admin npub] [--config path], nwc4ln start [--config path], " +
		"nwc4ln backup [--config path] <file>, or nwc4ln set-admin [--config path] <npub>"
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
	flags := flag.NewFlagSet(command, flag.ContinueOnError)
	flags.SetOutput(out)
	configPath := flags.String("config", "", "config file (default ~/.nwc4ln/nwc4ln.toml)")
	admin := flags.String("admin", "", "the admin's npub (init)")
	if err := flags.Parse(rest); err != nil {
		return err
	}
	wantArgs := 0
	if command == "backup" || command == "set-admin" {
		wantArgs = 1 // The file to write, or the npub.
	}
	if flags.NArg() != wantArgs {
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
		return runInit(ctx, path, *admin, out)
	case "start":
		return runStart(ctx, path, out)
	case "backup":
		return runBackup(ctx, path, flags.Arg(0), out)
	case "set-admin":
		return runSetAdmin(ctx, path, flags.Arg(0), out)
	default:
		return errors.New(usage)
	}
}

// runInit creates the data folder, database and config. It refuses to run over
// existing data, because that would lose the ledger. The config is written
// last, so a failed init can be run again once the cause is fixed. The admin key is
// optional here: set-admin sets it later.
func runInit(ctx context.Context, configPath, adminNpub string, out io.Writer) error {
	adminKey := ""
	if adminNpub != "" {
		key, err := nwc.DecodeNpub(adminNpub)
		if err != nil {
			return err
		}
		adminKey = key
	}
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
	if adminKey != "" {
		if err := db.SetAdminKey(ctx, adminKey); err != nil {
			return errors.Join(err, db.Close())
		}
	}
	if err := db.Close(); err != nil {
		return fmt.Errorf("close database: %w", err)
	}
	if err := config.Write(configPath, config.Default(dataDir)); err != nil {
		return err
	}
	fmt.Fprintln(out, "created", configPath)
	if adminKey == "" {
		fmt.Fprintln(out, "no admin key is set: run nwc4ln set-admin <npub> before you log in")
	}
	return nil
}

// runStart serves NWC requests until a stop signal. A relay or node that is down
// does not stop it: the service keeps trying, and answers what it can. It joins
// the relay only after the ledger has recovered, so that no request is answered
// before that. It refuses to run on a node that is not the ledger's. The admin UI
// starts first, so the admin can see the figures are unknown while the node is down.
// nodeClient is a node with a connection to close.
type nodeClient interface {
	node.Node
	Close() error
}

// newNode makes the client for the node type in the config.
func newNode(cfg config.Config) (nodeClient, error) {
	if cfg.NodeType() == config.NodeCLN {
		return cln.New(cln.Config{URL: cfg.CLNURL, CACertPath: cfg.CLNCACertPath, RunePath: cfg.CLNRunePath})
	}
	return lnd.New(lnd.Config{Host: cfg.LNDHost, TLSCertPath: cfg.LNDTLSCertPath, MacaroonPath: cfg.LNDMacaroonPath})
}

// checkFiles refuses to run on files other users can read or change: the database
// holds the wallet service keys, and the config names the node and its macaroon.
// The node clients check the macaroon and the rune themselves.
func checkFiles(configPath string, cfg config.Config) error {
	database := filepath.Join(cfg.DataDir, databaseName)
	for _, check := range []struct {
		run  func(string) error
		path string
	}{
		{config.CheckNotShared, configPath},
		{config.CheckNotShared, cfg.DataDir},
		{config.CheckPrivate, database},
		{config.CheckPrivate, database + "-wal"},
		{config.CheckPrivate, database + "-shm"},
	} {
		if err := check.run(check.path); err != nil {
			return err
		}
	}
	return nil
}

// loadChecked loads the config, then checks the files it names.
func loadChecked(configPath string) (config.Config, error) {
	cfg, err := config.Load(configPath)
	if err != nil {
		return config.Config{}, err
	}
	return cfg, checkFiles(configPath, cfg)
}

func runStart(ctx context.Context, configPath string, out io.Writer) (err error) {
	cfg, err := loadChecked(configPath)
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
	adminKey, err := db.AdminKey(ctx)
	if err != nil {
		return err
	}
	client, err := newNode(cfg)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, client.Close()) }()
	money := ledger.New(db, client, slog.Default())
	service, err := nwc.New(ctx, db, client, money, cfg.Relay, slog.Default())
	if err != nil {
		return err
	}
	money.OnSettled(service.Notify)
	ui, err := web.New(web.Config{
		AdminKey: adminKey, AllowedHosts: cfg.AllowedHosts, NodeType: cfg.NodeType(), Ledger: money, Apps: service, SubWallets: service,
		Log: slog.Default(),
	})
	if err != nil {
		return err
	}
	listener, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		return fmt.Errorf("admin UI: %w", err)
	}
	// The UI stops before the database closes, because the defers run in reverse.
	uiCtx, stopUI := context.WithCancel(ctx)
	uiDone := make(chan error, 1)
	go func() { uiDone <- ui.Serve(uiCtx, listener) }()
	defer func() {
		stopUI()
		err = errors.Join(err, <-uiDone)
	}()
	slog.Info("admin UI listening", "url", "http://"+listener.Addr().String())
	if adminKey == "" {
		slog.Warn("no admin key is set, so nobody can log in: stop the service and run nwc4ln set-admin <npub>")
	}
	fmt.Fprintln(out, "started")
	defer money.Close()
	if err := money.StartRetrying(ctx); err != nil {
		if ctx.Err() == nil {
			return err
		}
	} else {
		service.Run(ctx)
	}
	fmt.Fprintln(out, "stopped")
	return nil
}

// runBackup writes a copy of the database to a new file. It takes no lock, so it works
// while the service runs.
func runBackup(ctx context.Context, configPath, dest string, out io.Writer) error {
	cfg, err := loadChecked(configPath)
	if err != nil {
		return err
	}
	if err := store.Backup(ctx, filepath.Join(cfg.DataDir, databaseName), dest); err != nil {
		return err
	}
	fmt.Fprintln(out, "backup written to", dest)
	return nil
}

// runSetAdmin replaces the admin key. It takes the data folder lock, so it refuses
// while the service runs. Sessions live in memory, so the old admin's end with the
// stop that this needs.
func runSetAdmin(ctx context.Context, configPath, npub string, out io.Writer) (err error) {
	key, err := nwc.DecodeNpub(npub)
	if err != nil {
		return err
	}
	cfg, err := loadChecked(configPath)
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
	if err := db.SetAdminKey(ctx, key); err != nil {
		return err
	}
	fmt.Fprintln(out, "admin key set to", npub)
	return nil
}
