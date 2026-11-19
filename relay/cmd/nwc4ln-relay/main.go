// Command nwc4ln-relay is a Nostr relay locked to one wallet's Nostr Wallet
// Connect connections (spec section 16).
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

	"github.com/2rdzy/nwc4ln/relay/internal/allowlist"
	"github.com/2rdzy/nwc4ln/relay/internal/config"
	"github.com/2rdzy/nwc4ln/relay/internal/server"
)

const (
	dataDirMode = 0o700
	usage       = "usage: nwc4ln-relay init --control <npub or hex> [--config path], " +
		"or nwc4ln-relay start [--debug] [--config path]"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	err := run(ctx, os.Args[1:], os.Stdout, os.Stderr)
	stop()
	if err != nil {
		fmt.Fprintln(os.Stderr, "nwc4ln-relay:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, out, logOut io.Writer) error {
	if len(args) == 0 {
		return errors.New(usage)
	}
	command, rest := args[0], args[1:]
	flags := flag.NewFlagSet(command, flag.ContinueOnError)
	flags.SetOutput(out)
	configPath := flags.String("config", "", "config file (default ~/.nwc4ln-relay/nwc4ln-relay.toml)")
	control := flags.String("control", "", "the wallet's control public key, npub or hex (init)")
	debug := flags.Bool("debug", false, "log refused events and closed connections (start)")
	if err := flags.Parse(rest); err != nil {
		return err
	}
	if flags.NArg() != 0 {
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
		return runInit(path, *control, out)
	case "start":
		level := slog.LevelInfo
		if *debug {
			level = slog.LevelDebug
		}
		logger := slog.New(slog.NewTextHandler(logOut, &slog.HandlerOptions{Level: level}))
		return runStart(ctx, path, logger)
	default:
		return errors.New(usage)
	}
}

// runInit creates the data folder and the config. It refuses to overwrite a
// config.
func runInit(configPath, control string, out io.Writer) error {
	if control == "" {
		return errors.New("init needs --control: the wallet's control public key")
	}
	key, err := config.ParseKey(control)
	if err != nil {
		return err
	}
	dataDir := filepath.Dir(configPath)
	if err := os.MkdirAll(dataDir, dataDirMode); err != nil {
		return fmt.Errorf("create data folder: %w", err)
	}
	if err := config.Write(configPath, config.Default(dataDir, key)); err != nil {
		return err
	}
	fmt.Fprintf(out, "wrote %s\n", configPath)
	return nil
}

func runStart(ctx context.Context, configPath string, logger *slog.Logger) error {
	settings, err := config.Load(configPath)
	if err != nil {
		return err
	}
	for _, path := range []string{configPath, settings.DataDir} {
		if err := config.CheckNotShared(path); err != nil {
			return err
		}
	}
	lists, err := allowlist.Open(settings.DataDir, settings.ControlKey)
	if err != nil {
		return err
	}
	ln, err := net.Listen("tcp", settings.Listen)
	if err != nil {
		return err
	}
	return server.New(settings, lists, logger).Serve(ctx, ln)
}
