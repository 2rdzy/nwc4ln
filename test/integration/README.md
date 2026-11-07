# Integration network

A throwaway BLAKE2b regtest network: one Knots node with BLAKE2b from height 1, two lightning-fork LND nodes
(`lnd-a`, `lnd-b`) with a channel opened from each side, and a local Nostr relay (`nak serve`).

```sh
go run ./test/integration/cmd/regtest up      # start; the first run fetches and builds, a few minutes
go test -tags integration -p 1 -count=1 ./test/integration/...
go run ./test/integration/cmd/regtest smoke   # check a running network
go run ./test/integration/cmd/regtest down    # stop it and remove all of its data
```

Needs Linux `amd64`, `git` and Go. Versions are pinned in [versions.env](versions.env).

- **One network per run.** `up` saves a state file and tests use `Load`. Keep `-p 1`, because test packages would
  otherwise run at once on the same network. A test that stops a node must start it again.
- **New tests** carry `//go:build integration`. With no network they fail, and never skip.
- **Ready means paid.** `up` returns after a payment works each way. After restarting a node, call `WaitChannels`:
  a channel shown as active can still refuse a payment for a moment.
- **Folders.** Data is in `NWC4LN_IT_DIR` (default `$TMPDIR/nwc4ln/it`), binaries in `NWC4LN_IT_CACHE` (default
  `~/.cache/nwc4ln-it`). `down` removes only a folder that `up` created.
- **Acceptance.** `nwc/acceptance_test.go` runs `@getalby/sdk`, pinned in `nwc/acceptance/package-lock.json`, against a
  real nwc4ln. It needs Node.js and is used only here. `regtest lncli <node> <args>` lets the script act as `lnd-b`.
- **Ports** start at 19500. `up` refuses to start if one is taken.
