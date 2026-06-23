//! p2p-probe: minimal libp2p peer used to test whether two GitHub Actions
//! runners (both behind NAT) can reach each other, using circuit relay v2
//! for rendezvous and DCUtR for hole punching.
//!
//! listen mode: joins the public IPFS (Amino) DHT, discovers peers that
//!   advertise the relay hop protocol, reserves a slot on one of them,
//!   writes the relayed multiaddr to `--out`, then waits until the dialer
//!   pings it (or times out).
//!
//! dial mode: reads the rendezvous file, dials the listener via the relay,
//!   lets DCUtR attempt a direct connection, and reports whether the
//!   resulting ping ran over a relayed or a direct connection.

use std::{
    collections::{HashSet, VecDeque},
    path::PathBuf,
    str::FromStr,
    time::{Duration, Instant},
};

use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand};
use futures::StreamExt;
use libp2p::{
    core::{multiaddr::Protocol, transport::ListenerId, ConnectedPoint},
    dcutr, identify, identity, kad, noise, ping, relay,
    swarm::{NetworkBehaviour, SwarmEvent},
    tcp, yamux, Multiaddr, PeerId,
};
use serde::{Deserialize, Serialize};
use tracing_subscriber::EnvFilter;

/// Public IPFS bootstrap nodes. They are only used to join the Amino DHT;
/// they refuse relay reservations themselves, so we discover other DHT
/// peers that advertise the relay hop protocol and reserve there instead.
const DHT_BOOTSTRAP: &[&str] = &[
    "/dnsaddr/bootstrap.libp2p.io/p2p/QmNnooDu7bfjPFoTZYxMNLWUQJyrVwtbZg5gBMjTezGAJN",
    "/dnsaddr/bootstrap.libp2p.io/p2p/QmQCU2EcMqAqQPR2i9bChDtGNJchTbq5TbXJJ16u19uLTa",
    "/dnsaddr/bootstrap.libp2p.io/p2p/QmbLHAnMoJPWSCR5Zhtx6BHJX9KiKNN6tpvbUcqanj75Nb",
    "/dnsaddr/bootstrap.libp2p.io/p2p/QmcZf59bWwK5XFi76CZX8cbJ4BhTzzA3gU1ZjYZcYW3dwt",
];

#[derive(Parser, Debug)]
#[command(about = "libp2p NAT traversal probe for GitHub Actions")]
struct Opts {
    /// Explicit relay multiaddr(s) to reserve on. When given, DHT discovery
    /// is skipped.
    #[arg(long = "relay")]
    relays: Vec<Multiaddr>,

    /// Overall timeout for the run.
    #[arg(long, default_value = "300")]
    timeout_secs: u64,

    /// How long to spend discovering a relay via the DHT before giving up.
    #[arg(long, default_value = "120")]
    discovery_secs: u64,

    #[command(subcommand)]
    mode: Mode,
}

#[derive(Subcommand, Debug)]
enum Mode {
    /// Reserve a slot on a relay, publish the relayed address, wait for a ping.
    Listen {
        /// File to write the rendezvous info (JSON) to once a relay
        /// reservation is accepted.
        #[arg(long)]
        out: PathBuf,
    },
    /// Dial the listener via the relay and try to upgrade to a direct
    /// connection via DCUtR.
    Dial {
        /// Rendezvous file produced by `listen`.
        #[arg(long)]
        rendezvous: PathBuf,
    },
}

#[derive(Serialize, Deserialize, Debug)]
struct Rendezvous {
    peer_id: String,
    /// `<relay>/p2p-circuit/p2p/<peer_id>` addresses the dialer should try.
    circuit_addrs: Vec<String>,
}

#[derive(NetworkBehaviour)]
struct Behaviour {
    relay_client: relay::client::Behaviour,
    identify: identify::Behaviour,
    dcutr: dcutr::Behaviour,
    ping: ping::Behaviour,
    kad: kad::Behaviour<kad::store::MemoryStore>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let opts = Opts::parse();

    let key = identity::Keypair::generate_ed25519();
    let local_peer_id = key.public().to_peer_id();
    tracing::info!(%local_peer_id, "local peer id");

    let mut swarm = libp2p::SwarmBuilder::with_existing_identity(key)
        .with_tokio()
        .with_tcp(
            tcp::Config::default().nodelay(true),
            noise::Config::new,
            yamux::Config::default,
        )?
        .with_quic()
        .with_dns()?
        .with_relay_client(noise::Config::new, yamux::Config::default)?
        .with_behaviour(|keypair, relay_behaviour| {
            let id = keypair.public().to_peer_id();
            let mut kad = kad::Behaviour::with_config(
                id,
                kad::store::MemoryStore::new(id),
                kad::Config::new(kad::PROTOCOL_NAME),
            );
            // Stay a DHT client: we are behind NAT and only need lookups.
            kad.set_mode(Some(kad::Mode::Client));
            Behaviour {
                relay_client: relay_behaviour,
                identify: identify::Behaviour::new(identify::Config::new(
                    "/tribuchet-probe/0.1".into(),
                    keypair.public(),
                )),
                dcutr: dcutr::Behaviour::new(id),
                ping: ping::Behaviour::new(ping::Config::new()),
                kad,
            }
        })?
        .with_swarm_config(|c| c.with_idle_connection_timeout(Duration::from_secs(60)))
        .build();

    // Listen on both stacks; v6 is best-effort because not every runner
    // has it, but when it does QUIC/v6 tends to hole-punch better than v4.
    for addr in [
        "/ip4/0.0.0.0/tcp/0",
        "/ip4/0.0.0.0/udp/0/quic-v1",
        "/ip6/::/tcp/0",
        "/ip6/::/udp/0/quic-v1",
    ] {
        match swarm.listen_on(addr.parse()?) {
            Ok(_) => {}
            Err(e) => tracing::warn!(addr, %e, "listen_on failed (continuing)"),
        }
    }

    // Give the listeners a moment to bind so identify can report real
    // addresses to the relay.
    let warmup = tokio::time::sleep(Duration::from_secs(1));
    tokio::pin!(warmup);
    loop {
        tokio::select! {
            () = &mut warmup => break,
            ev = swarm.select_next_some() => if let SwarmEvent::NewListenAddr { address, .. } = ev {
                tracing::info!(%address, "listening");
            }
        }
    }

    // Seed the DHT routing table with the public bootstrap nodes so the
    // listener can discover relay-capable peers without any private infra.
    let mut bootstrap_peers: HashSet<PeerId> = HashSet::new();
    for s in DHT_BOOTSTRAP {
        let ma: Multiaddr = Multiaddr::from_str(s).expect("static addr");
        if let Some(id) = peer_id_of(&ma) {
            bootstrap_peers.insert(id);
            swarm.behaviour_mut().kad.add_address(&id, ma);
        }
    }
    let _ = swarm.behaviour_mut().kad.bootstrap();

    let deadline = Instant::now() + Duration::from_secs(opts.timeout_secs);

    match opts.mode {
        Mode::Listen { out } => {
            run_listen(
                &mut swarm,
                &opts.relays,
                &bootstrap_peers,
                local_peer_id,
                &out,
                Duration::from_secs(opts.discovery_secs),
                deadline,
            )
            .await
        }
        Mode::Dial { rendezvous } => run_dial(&mut swarm, &rendezvous, deadline).await,
    }
}

fn peer_id_of(addr: &Multiaddr) -> Option<PeerId> {
    addr.iter().find_map(|p| match p {
        Protocol::P2p(id) => Some(id),
        _ => None,
    })
}

/// Heuristic for "probably reachable from the public internet": skip
/// loopback, RFC1918, link-local and CGNAT ranges so we don't waste
/// reservation attempts on a relay's LAN address.
fn is_public(addr: &Multiaddr) -> bool {
    for p in addr.iter() {
        match p {
            Protocol::P2pCircuit => return false,
            Protocol::Ip4(ip) => {
                if ip.is_loopback() || ip.is_private() || ip.is_link_local() {
                    return false;
                }
                let o = ip.octets();
                // 100.64.0.0/10 (CGNAT)
                if o[0] == 100 && (64..128).contains(&o[1]) {
                    return false;
                }
            }
            Protocol::Ip6(ip) => {
                if ip.is_loopback() || ip.is_unspecified() {
                    return false;
                }
                let seg = ip.segments();
                // fc00::/7 (ULA), fe80::/10 (link-local)
                if (seg[0] & 0xfe00) == 0xfc00 || (seg[0] & 0xffc0) == 0xfe80 {
                    return false;
                }
            }
            _ => {}
        }
    }
    true
}

#[allow(clippy::too_many_arguments)]
async fn run_listen(
    swarm: &mut libp2p::Swarm<Behaviour>,
    explicit_relays: &[Multiaddr],
    bootstrap_peers: &HashSet<PeerId>,
    local_peer_id: PeerId,
    out: &PathBuf,
    discovery: Duration,
    deadline: Instant,
) -> Result<()> {
    // Relay candidates discovered via identify, keyed by the address we will
    // listen on (already including /p2p/<relay>).
    let mut candidates: VecDeque<(PeerId, Multiaddr)> = VecDeque::new();
    let mut tried: HashSet<PeerId> = HashSet::new();
    // Reservation attempt currently in flight.
    let mut active: Option<(PeerId, ListenerId)> = None;
    let mut accepted: Vec<Multiaddr> = Vec::new();

    // If the user passed explicit relays, skip DHT discovery entirely.
    for r in explicit_relays {
        if let Some(id) = peer_id_of(r) {
            candidates.push_back((id, r.clone()));
        } else {
            tracing::warn!(addr=%r, "relay addr has no /p2p peer id, ignoring");
        }
    }
    let use_dht = explicit_relays.is_empty();

    let discovery_deadline = Instant::now() + discovery;
    let mut next_walk = Instant::now();

    loop {
        // Kick a random-walk query every few seconds to keep meeting new
        // peers; identify on those connections is what surfaces relay
        // candidates.
        if use_dht && Instant::now() >= next_walk {
            swarm
                .behaviour_mut()
                .kad
                .get_closest_peers(PeerId::random());
            next_walk = Instant::now() + Duration::from_secs(5);
        }

        // Start the next reservation attempt if idle.
        if active.is_none() {
            while let Some((peer, addr)) = candidates.pop_front() {
                if !tried.insert(peer) {
                    continue;
                }
                tracing::info!(%peer, %addr, "attempting reservation");
                match swarm.listen_on(addr.clone().with(Protocol::P2pCircuit)) {
                    Ok(id) => {
                        active = Some((peer, id));
                        break;
                    }
                    Err(e) => tracing::warn!(%peer, %addr, %e, "listen_on failed"),
                }
            }
        }

        if !accepted.is_empty() {
            break;
        }
        if Instant::now() >= discovery_deadline && active.is_none() && candidates.is_empty() {
            bail!(
                "no relay accepted a reservation within {}s ({} peers tried)",
                discovery.as_secs(),
                tried.len()
            );
        }

        let until_walk = next_walk.saturating_duration_since(Instant::now());
        let ev = tokio::select! {
            ev = swarm.select_next_some() => ev,
            () = tokio::time::sleep(until_walk), if use_dht => continue,
        };

        match ev {
            SwarmEvent::Behaviour(BehaviourEvent::Identify(identify::Event::Received {
                peer_id,
                info,
                ..
            })) => {
                // Learn our public address from anyone who tells us; DCUtR
                // needs an external address on this side too.
                swarm.add_external_address(info.observed_addr.clone());
                if use_dht
                    && !bootstrap_peers.contains(&peer_id)
                    && !tried.contains(&peer_id)
                    && info.protocols.contains(&relay::HOP_PROTOCOL_NAME)
                {
                    if let Some(a) = info
                        .listen_addrs
                        .iter()
                        .find(|a| is_public(a))
                        .or(info.listen_addrs.first())
                    {
                        let addr = a.clone().with(Protocol::P2p(peer_id));
                        tracing::info!(%peer_id, %addr, "found relay candidate");
                        candidates.push_back((peer_id, addr));
                    }
                }
            }
            SwarmEvent::NewListenAddr {
                listener_id,
                address,
            } if active.map(|(_, l)| l) == Some(listener_id) => {
                tracing::info!(%address, "relay reservation accepted");
                accepted.push(address);
            }
            SwarmEvent::ListenerClosed {
                listener_id,
                reason,
                ..
            } if active.map(|(_, l)| l) == Some(listener_id) => {
                tracing::warn!(?reason, "relay listener closed");
                active = None;
            }
            SwarmEvent::OutgoingConnectionError { peer_id, error, .. }
                if active.map(|(p, _)| Some(p)) == Some(peer_id) =>
            {
                tracing::warn!(%error, "relay dial failed");
                if let Some((_, l)) = active.take() {
                    let _ = swarm.remove_listener(l);
                }
            }
            SwarmEvent::Behaviour(BehaviourEvent::Kad(ev)) => tracing::debug!(?ev),
            other => tracing::debug!(?other),
        }
    }

    let rendezvous = Rendezvous {
        peer_id: local_peer_id.to_string(),
        // The relay client already appends /p2p/<self> to the listen addr.
        circuit_addrs: accepted.iter().map(ToString::to_string).collect(),
    };
    let tmp = out.with_extension("tmp");
    tokio::fs::write(&tmp, serde_json::to_vec_pretty(&rendezvous)?).await?;
    tokio::fs::rename(&tmp, out).await?;
    tracing::info!(file=%out.display(), "wrote rendezvous");

    // Wait for the dialer to reach us. DHT discovery means we are connected
    // to dozens of unrelated peers that also ping, so only count pings from
    // peers that actually arrived via the relayed circuit (or were later
    // upgraded by DCUtR from such a connection).
    let mut dialer_peers: HashSet<PeerId> = HashSet::new();
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            bail!("timed out waiting for dialer");
        }
        let ev = tokio::select! {
            ev = swarm.select_next_some() => ev,
            () = tokio::time::sleep(remaining) => bail!("timed out waiting for dialer"),
        };
        match ev {
            SwarmEvent::ConnectionEstablished {
                peer_id, endpoint, ..
            } if is_relayed(&endpoint) && !bootstrap_peers.contains(&peer_id) => {
                tracing::info!(peer=%peer_id, ?endpoint, "dialer connected via relay");
                dialer_peers.insert(peer_id);
            }
            SwarmEvent::ConnectionEstablished {
                peer_id, endpoint, ..
            } if dialer_peers.contains(&peer_id) => {
                tracing::info!(peer=%peer_id, ?endpoint, "dialer direct connection");
            }
            SwarmEvent::Behaviour(BehaviourEvent::Dcutr(ev)) => {
                tracing::info!(?ev, "dcutr");
            }
            SwarmEvent::Behaviour(BehaviourEvent::Ping(ping::Event {
                peer,
                result: Ok(rtt),
                ..
            })) if dialer_peers.contains(&peer) => {
                tracing::info!(peer=%peer, ?rtt, "ping from dialer — success");
                println!("LISTENER_OK peer={peer} rtt_ms={}", rtt.as_millis());
                return Ok(());
            }
            other => tracing::debug!(?other),
        }
    }
}

async fn run_dial(
    swarm: &mut libp2p::Swarm<Behaviour>,
    rendezvous: &PathBuf,
    deadline: Instant,
) -> Result<()> {
    let data = tokio::fs::read(rendezvous)
        .await
        .with_context(|| format!("reading {}", rendezvous.display()))?;
    let rv: Rendezvous = serde_json::from_slice(&data)?;
    let remote: PeerId = rv.peer_id.parse().context("peer_id")?;
    tracing::info!(remote=%remote, addrs=?rv.circuit_addrs, "dialing listener");

    // Dial the relay first so identify can learn our observed address;
    // DCUtR needs that to coordinate the hole punch.
    for a in &rv.circuit_addrs {
        let ma: Multiaddr = a.parse()?;
        let relay_only: Multiaddr = ma
            .iter()
            .take_while(|p| !matches!(p, Protocol::P2pCircuit))
            .collect();
        if let Err(e) = swarm.dial(relay_only.clone()) {
            tracing::warn!(addr=%relay_only, %e, "relay pre-dial failed");
        }
    }
    learn_observed_addr(swarm, Duration::from_secs(15)).await;

    for a in &rv.circuit_addrs {
        let ma: Multiaddr = a.parse()?;
        swarm.dial(ma)?;
    }

    let mut got_direct = false;
    let mut got_relayed = false;
    let mut hole_punch: Option<bool> = None;
    // Keep going a bit after first ping so DCUtR has time to upgrade.
    let mut grace: Option<Instant> = None;

    loop {
        let hard = deadline.saturating_duration_since(Instant::now());
        let soft = grace.map(|g| g.saturating_duration_since(Instant::now()));
        let remaining = soft.map_or(hard, |s| s.min(hard));
        if remaining.is_zero() {
            break;
        }
        let ev = tokio::select! {
            ev = swarm.select_next_some() => ev,
            () = tokio::time::sleep(remaining) => break,
        };
        match ev {
            SwarmEvent::ConnectionEstablished {
                peer_id, endpoint, ..
            } if peer_id == remote => {
                if is_relayed(&endpoint) {
                    got_relayed = true;
                    tracing::info!(?endpoint, "relayed connection established");
                } else {
                    got_direct = true;
                    tracing::info!(?endpoint, "direct connection established");
                }
            }
            SwarmEvent::Behaviour(BehaviourEvent::Dcutr(dcutr::Event {
                remote_peer_id,
                result,
            })) if remote_peer_id == remote => {
                let ok = result.is_ok();
                hole_punch = Some(ok);
                tracing::info!(ok, ?result, "dcutr result");
            }
            SwarmEvent::Behaviour(BehaviourEvent::Ping(ping::Event {
                peer,
                result: Ok(rtt),
                ..
            })) if peer == remote => {
                tracing::info!(?rtt, "ping ok");
                if grace.is_none() {
                    grace = Some(Instant::now() + Duration::from_secs(20));
                }
            }
            SwarmEvent::OutgoingConnectionError { peer_id, error, .. }
                if peer_id == Some(remote) =>
            {
                tracing::warn!(%error, "outgoing connection error");
            }
            SwarmEvent::Behaviour(BehaviourEvent::Identify(identify::Event::Received {
                info,
                ..
            })) => {
                swarm.add_external_address(info.observed_addr);
            }
            other => tracing::debug!(?other),
        }
    }

    println!(
        "DIALER_RESULT relayed={got_relayed} direct={got_direct} hole_punch={}",
        hole_punch.map_or("none".into(), |b| b.to_string())
    );

    if got_relayed || got_direct {
        Ok(())
    } else {
        bail!("never connected to listener")
    }
}

fn is_relayed(ep: &ConnectedPoint) -> bool {
    let addr = match ep {
        ConnectedPoint::Dialer { address, .. } => address,
        ConnectedPoint::Listener { send_back_addr, .. } => send_back_addr,
    };
    addr.iter().any(|p| matches!(p, Protocol::P2pCircuit))
}

async fn learn_observed_addr(swarm: &mut libp2p::Swarm<Behaviour>, timeout: Duration) {
    let until = Instant::now() + timeout;
    loop {
        let remaining = until.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return;
        }
        let ev = tokio::select! {
            ev = swarm.select_next_some() => ev,
            () = tokio::time::sleep(remaining) => return,
        };
        if let SwarmEvent::Behaviour(BehaviourEvent::Identify(identify::Event::Received {
            info: identify::Info { observed_addr, .. },
            ..
        })) = ev
        {
            tracing::info!(%observed_addr, "learned observed address");
            swarm.add_external_address(observed_addr);
            return;
        }
    }
}
