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

Needs repo secrets `TS_OAUTH_CLIENT_ID` / `TS_OAUTH_SECRET` from a
Tailscale OAuth client with the **Auth Keys: write** scope, bound to
`tag:ci` (which must exist under `tagOwners` in the tailnet ACL).

### Result (`ubuntu-latest`, 2026-06-23, [run](https://github.com/Mic92/tribuchet-action/actions/runs/28025937315))

```
pong from ts-probe-server via DERP(ord) in 172ms
pong from ts-probe-server via DERP(ord) in 62ms
pong from ts-probe-server via DERP(ord) in 62ms
pong from ts-probe-server via 20.118.213.18:39952 in 37ms
```

| | result |
|---|---|
| Path | DERP relay first, **upgraded to direct WireGuard** after ~3 pings |
| RTT | 62 ms (DERP) → 37 ms (direct) |
| iperf3 forward | **643 Mbit/s** |
| iperf3 reverse | **617 Mbit/s** |

Tailscale's NAT traversal succeeds between two Azure-NAT'd hosted
runners where libp2p DCUtR did not, and the resulting direct path
moves ~600 Mbit/s — ample for NAR transfer. **Viable transport for
tribuchet workers on GitHub-hosted CI**, with zero self-hosted infra
(or Headscale for an unlimited self-hosted control plane).

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
