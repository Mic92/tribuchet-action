# tribuchet-action

> **Archived.** Experiment complete; see [conclusion](#conclusion).

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

## Conclusion

Tested on GitHub-hosted `ubuntu-latest` runners, 2026-06-23
([run](https://github.com/Mic92/tribuchet-action/actions/runs/28024695079)).

```
DIALER_RESULT relayed=true direct=false hole_punch=false
```

| | result |
|---|---|
| DHT relay discovery | works; reservation accepted on a random kubo peer in ~2 s |
| Relayed connection (runner ↔ runner) | works, ~95 ms RTT |
| DCUtR hole punch | **fails** (`InboundError(UnexpectedEof)`); Azure NAT on hosted runners is symmetric/endpoint-dependent |
| Bandwidth | circuit-relay-v2 *limited* relays cap each connection to **128 KiB / 2 min** by spec — fine for signalling, useless for NAR transfer |

**Takeaway for [tribuchet]:** libp2p gives a zero-infra control
channel between two NAT'd CI runners, but no usable data plane: the
hole punch doesn't go through, and stranger-operated relays are
throttled. Since the tribuchet hub already has a public address, the
existing gRPC/mTLS dial-out from workers is strictly simpler and
unmetered. Worker↔worker direct transfer (the one place libp2p could
have helped) is exactly what fails here. Not adopting libp2p.

[tribuchet]: https://github.com/Mic92/tribuchet
