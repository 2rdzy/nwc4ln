// Command regtest starts and stops the integration network.
//
//	go run ./test/integration/cmd/regtest up      start it, downloading and building what is missing
//	go run ./test/integration/cmd/regtest smoke   check that it works
//	go run ./test/integration/cmd/regtest down    stop it and remove all of its data
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/2rdzy/nwc4ln/test/integration"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: regtest up|smoke|down")
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1]); err != nil {
		fmt.Fprintln(os.Stderr, "regtest:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, command string) error {
	cfg := integration.DefaultConfig()
	switch command {
	case "up":
		network, err := integration.Up(ctx, cfg)
		if err != nil {
			return err
		}
		fmt.Printf("network up in %s, relay %s\n", network.Dir, network.RelayURL())
	case "smoke":
		network, err := integration.Load(cfg)
		if err != nil {
			return err
		}
		if err := network.Smoke(ctx); err != nil {
			return err
		}
		fmt.Println("smoke passed")
	case "down":
		if err := integration.Down(cfg); err != nil {
			return err
		}
		fmt.Println("network down, data removed")
	default:
		return fmt.Errorf("unknown command %q", command)
	}
	return nil
}
