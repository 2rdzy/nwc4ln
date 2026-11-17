# nwc4ln

**NWC for Lightning:** connect apps with spending limits, and create sub-wallets, on LND or Core Lightning over Nostr
Wallet Connect.

> **Status: in development.** Not ready for real funds.

## Programs

This repository holds three programs. Each folder is its own Go module, and no folder imports code from another.

| Folder | Binary | What |
|---|---|---|
| [`wallet/`](wallet/README.md) | `nwc4ln-wallet` | The wallet: NWC, apps with limits, sub-wallets and the admin web UI, on top of your node |
| `relay/` | `nwc4ln-relay` | Planned: a Nostr relay locked to the wallet's connections. Optional |
| `address/` | `nwc4ln-address` | Planned: a Lightning address service (LNURL-pay) that creates invoices over NWC |

A `system/` folder will hold tests that run more than one program.

## Planned order of work

1. NWC on LND with one connection.
2. Apps with limits, then sub-wallets.
3. Admin web UI with Nostr login.
4. Core Lightning.
5. Repository structure: the wallet moves to `wallet/`.
6. Our own relay.
7. Lightning address service.
8. Live updates and a logo.
9. Recovery after a node is restored, and small fixes.
10. Docs and the first release.

## License

[MIT](LICENSE). The software is offered as is, with no warranty. You use it at your own risk.
