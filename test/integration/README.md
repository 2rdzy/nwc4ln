# Integration network

A throwaway BLAKE2b regtest network: one Knots node with BLAKE2b from height 1, three lightning-fork LND nodes, one Core
Lightning node from the privkeyio fork, and a local Nostr relay (`nak serve`). `lnd-a` and `lnd-b` have a channel opened
from each side, and so do `cln-a` and `lnd-b`. `lnd-c` sits behind `lnd-b`, so a payment from `lnd-a` or `cln-a` to
`lnd-c` pays a real routing fee.

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
- **Core Lightning.** `NWC4LN_IT_NODE=cln` runs the NWC tests with nwc4ln on `cln-a` instead of `lnd-a`. Its release
  build is pinned per Ubuntu release (24.04 for CI, 26.04 locally), and it reads clnrest over HTTPS with a rune limited
  to the client's methods. It polls Knots every second and has fixed fee rates, so figures do not move during a test.
- **Ports** start at 19500. `up` refuses to start if one is taken.

## Long tests

`nwc/long_test.go` has a random soak (`TestSoak`), a crash loop on the real binary (`TestCrashLoop`) and node and
relay outages (`TestOutages`). By default they run a short version, as PR CI does. The environment sets their length,
listed at the top of that file. `scripts/longtests.sh` starts a network, runs the three, and stops it:

```sh
NWC4LN_SOAK_FOR=6h scripts/longtests.sh
NWC4LN_IT_NODE=cln NWC4LN_SOAK_FOR=24h NWC4LN_CRASH_FOR=1h scripts/longtests.sh
```

For runs of hours, set `NWC4LN_IT_DIR` to a folder on disk. A failed soak prints its seed: `NWC4LN_LONG_SEED` replays
it. The weekly `longtests` workflow runs an hour on each node, and can be started by hand with another length.
