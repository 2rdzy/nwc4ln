# nwc4ln

**NWC for Lightning:** connect apps with spending limits, and create sub-wallets, on LND or Core Lightning over Nostr
Wallet Connect.

> **Status: early development.** Not ready for use.

## What it will do

`nwc4ln` is a small, self-hosted service that sits on top of a Lightning node you already run. It does not manage
the node. Channels, peers and on-chain funds stay with LND or Core Lightning and the tools you use for them.

- **Connect apps over NWC** ([NIP-47](https://github.com/nostr-protocol/nips/blob/master/47.md)). An app spends from
  your main wallet, like a virtual credit card. Each app has its own connection and permissions, with an optional
  spending limit, renewal period and expiry date. If an app is hacked, the limit caps the damage.
- **Create sub-wallets** for other people. A sub-wallet has its own balance, starts at 0 and has no limit. It is funded
  by invoice, keeps what it receives, and its funds are set aside from your main wallet inside `nwc4ln`.
- **Admin web UI,** with login by Nostr key. Only the admin logs in. Everyone else connects over NWC.
- **Ship as one binary,** written in Go, with the web UI built in. Linux `amd64` first.

Testing covers the BLAKE2b chain of Bitcoin Knots only, using
[lightning-fork](https://github.com/paulscode/lightning-fork) for LND and
[privkeyio/lightning](https://github.com/privkeyio/lightning) for Core Lightning. NWC messages and `lnbc` invoices are the same on
any chain, and the code has no chain-specific logic, but other chains are untested.

A separate small service is planned for Lightning addresses (LNURL-pay). It will use NWC to create invoices, so that
only it faces the internet.

## What it is not

- **Not a node manager.** Use your node's own tools.
- **Not a pure proxy.** LND and Core Lightning hold one balance and know nothing about sub-wallets, so `nwc4ln` keeps
  a ledger of its own.
- **Not self-custody for sub-wallet users.** All funds stay in the operator's node. A sub-wallet balance is an entry
  in the operator's database. The operator can take it, and spending from the node with other tools can leave it
  unpayable. Its holder must trust the operator.

## Planned order of work

1. NWC on LND with one connection.
2. Apps with limits, then sub-wallets.
3. Admin web UI with Nostr login.
4. Core Lightning.
5. Lightning address service.

## License

[MIT](LICENSE). The software is offered as is, with no warranty. You use it at your own risk.
