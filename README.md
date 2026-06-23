# tribuchet-action

Feasibility probes for connecting two GitHub-hosted Actions runners
behind NAT, in service of [tribuchet] remote builds.

## tailscale probe (current)

[Workflow](.github/workflows/tailscale-probe.yml) brings two parallel
jobs onto the same tailnet via `tailscale/github-action`, then:

1. **server** publishes its tailnet IP as a run artifact and runs
   `iperf3 -s`.
2. **client** polls for the artifact, runs `tailscale ping` (reports
   DERP relay vs direct path) and `iperf3` in both directions, and
   writes the throughput to the step summary.

Needs repo secret `TS_AUTHKEY` (ephemeral, reusable, pre-approved).

## libp2p probe (concluded, code removed)

See git history at `e4c3b61`. Result on `ubuntu-latest`, 2026-06-23:

```
DIALER_RESULT relayed=true direct=false hole_punch=false
```

| | result |
|---|---|
| DHT relay discovery | works; reservation on a random kubo peer in ~2 s |
| Relayed connection (runner ↔ runner) | works, ~95 ms RTT |
| DCUtR hole punch | **fails** (Azure symmetric NAT) |
| Bandwidth | circuit-relay-v2 *limited* relays cap at **128 KiB / 2 min** |

libp2p gives a zero-infra control channel but no usable data plane on
hosted runners; not adopted.

[tribuchet]: https://github.com/Mic92/tribuchet
