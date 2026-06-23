# tribuchet-action

Feasibility probe: can two GitHub Actions runners (both behind NAT)
reach each other over [libp2p]?

The `p2p-probe` binary is a minimal libp2p peer with TCP + QUIC
transports, the circuit relay v2 client, `identify`, `dcutr` (hole
punching) and `ping`. The [workflow](.github/workflows/p2p-probe.yml)
starts two parallel jobs:

1. **listener** reserves a slot on a public IPFS bootstrap relay,
   uploads its relayed multiaddr as a run artifact, and waits for a
   ping.
2. **dialer** polls for the artifact, dials the listener through the
   relay, lets DCUtR try to upgrade to a direct connection, and
   reports whether the resulting connection was relayed or direct.

The dialer step prints a line like

```
DIALER_RESULT relayed=true direct=true hole_punch=true
```

`relayed=true` means libp2p connectivity works at all (good enough for
a control channel). `direct=true` / `hole_punch=true` means the two
runners managed a direct NAT-traversed connection, which is what
tribuchet would want for bulk NAR transfer.

## Running locally

```sh
cargo build --release
./target/release/p2p-probe listen --out /tmp/rv.json &
# wait for /tmp/rv.json to appear, then on another host:
./target/release/p2p-probe dial --rendezvous /tmp/rv.json
```

Pass `--relay <multiaddr>` (repeatable) to use a specific relay
instead of the public IPFS bootstrap nodes.

[libp2p]: https://github.com/libp2p/rust-libp2p
