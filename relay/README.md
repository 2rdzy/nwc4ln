# nwc4ln-relay

A Nostr relay locked to one wallet's Nostr Wallet Connect connections. The relay program of [nwc4ln](../README.md).

> **Status: in development.** The relay runs on its own. The wallet does not send it an allow-list yet (planned next).
> Not ready for real funds.

## What it does

A public relay carries anyone's events. This one carries only the NWC traffic of the connections one wallet serves,
and refuses everything else.

- **The wallet decides who may talk.** The wallet signs an allow-list with its control key: one pair per connection,
  a client key and its service key. The relay's config holds only the control key's public key, and no private key.
- **Writes.** A listed client may send requests (kind 23194) to its own service key only. A listed service key may
  send info events (13194), responses (23195) and notifications (23196, 23197). Every other event is refused.
- **Reads.** A filter must ask only for NWC kinds and name listed service keys as authors. The wallet's own filter asks
  for requests by `#p`, every key a listed service key. A stranger cannot list info events or learn a service key,
  and the allow-list is never served to anyone.
- **Before the first allow-list,** the relay accepts only an allow-list.
- **No database.** Info events are kept in memory, one per service key. Requests are kept for 10 minutes by default,
  so a wallet that reconnects after a short outage catches up. The allow-list is saved to a file, so a restart keeps
  it.
- **Limits.** Messages up to 128 KiB, which fits the largest NWC reply. Rate limits per connection, and caps on open
  connections and subscriptions. A connection that sends nothing accepted is closed after 30 seconds, and a flood of
  them cannot lock out the wallet.

## Run it

The relay listens on a loopback address in clear text. Put a TLS proxy, such as Caddy or nginx, in front of it.

```sh
nwc4ln-relay init --control <the wallet's control npub>
nwc4ln-relay start
```

`init` writes `~/.nwc4ln-relay/nwc4ln-relay.toml`. `start --debug` also logs refused events and closed connections.

## The allow-list

An ephemeral event of kind 29194, signed by the control key, with empty content and one tag per connection:

```json
["pair", "<client key hex>", "<service key hex>"]
```

Keys are 64 lowercase hex digits. No key appears twice, and any other tag is refused. The list is the whole list of
connections the wallet serves, never a change. The relay refuses a list that is not newer than the one it has, or
that is more than one minute ahead of or ten minutes behind its clock. Its refusal names the saved list's
`created_at`, so the wallet can stamp its next list later. [`internal/allowlist/testdata/allowlist.json`](internal/allowlist/testdata/allowlist.json)
is a sample.

## Development

```sh
go test -race ./...                      # unit tests
go test -race -tags relay ./test/...     # tests against the real binary
```

Only `internal/server` imports `khatru`; the rules in `internal/policy`, `internal/allowlist` and `internal/memstore`
are tested without it.

## License

[MIT](../LICENSE).
