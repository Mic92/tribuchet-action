//! p2p-probe: minimal libp2p peer used to test whether two GitHub Actions
//! runners (both behind NAT) can reach each other, using a public circuit
//! relay v2 for rendezvous and DCUtR for hole punching.
//!
//! listen mode: connects to one of the given relays, makes a reservation,
//!   writes the relayed multiaddr to `--out`, then waits until the dialer
//!   pings it (or times out).
//!
//! dial mode: reads the rendezvous file, dials the listener via the relay,
//!   lets DCUtR attempt a direct connection, and reports whether the
//!   resulting ping ran over a relayed or a direct connection.

use std::{
    collections::HashSet,
    path::PathBuf,
    str::FromStr,
    time::{Duration, Instant},
};

use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand};
use futures::StreamExt;
use libp2p::{
    core::{multiaddr::Protocol, transport::ListenerId, ConnectedPoint},
    dcutr, identify, identity, noise, ping, relay,
    swarm::{NetworkBehaviour, SwarmEvent},
    tcp, yamux, Multiaddr, PeerId,
};
use serde::{Deserialize, Serialize};
use tracing_subscriber::EnvFilter;

/// Public IPFS bootstrap nodes that speak circuit relay v2. Used when no
/// `--relay` is given so the experiment needs no self-hosted infrastructure.
const DEFAULT_RELAYS: &[&str] = &[
    "/dnsaddr/bootstrap.libp2p.io/p2p/QmNnooDu7bfjPFoTZYxMNLWUQJyrVwtbZg5gBMjTezGAJN",
    "/dnsaddr/bootstrap.libp2p.io/p2p/QmQCU2EcMqAqQPR2i9bChDtGNJchTbq5TbXJJ16u19uLTa",
    "/dnsaddr/bootstrap.libp2p.io/p2p/QmbLHAnMoJPWSCR5Zhtx6BHJX9KiKNN6tpvbUcqanj75Nb",
    "/dnsaddr/bootstrap.libp2p.io/p2p/QmcZf59bWwK5XFi76CZX8cbJ4BhTzzA3gU1ZjYZcYW3dwt",
];

#[derive(Parser, Debug)]
#[command(about = "libp2p NAT traversal probe for GitHub Actions")]
struct Opts {
    /// Relay multiaddr(s) to use (repeatable). Defaults to public IPFS
    /// bootstrap nodes.
    #[arg(long = "relay")]
    relays: Vec<Multiaddr>,

    /// Overall timeout for the run.
    #[arg(long, default_value = "300")]
    timeout_secs: u64,

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
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let opts = Opts::parse();

    let relays: Vec<Multiaddr> = if opts.relays.is_empty() {
        DEFAULT_RELAYS
            .iter()
            .map(|s| Multiaddr::from_str(s).expect("static addr"))
            .collect()
    } else {
        opts.relays.clone()
    };

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
        .with_behaviour(|keypair, relay_behaviour| Behaviour {
            relay_client: relay_behaviour,
            identify: identify::Behaviour::new(identify::Config::new(
                "/tribuchet-probe/0.1".into(),
                keypair.public(),
            )),
            dcutr: dcutr::Behaviour::new(keypair.public().to_peer_id()),
            ping: ping::Behaviour::new(ping::Config::new()),
        })?
        .with_swarm_config(|c| c.with_idle_connection_timeout(Duration::from_secs(60)))
        .build();

    swarm.listen_on("/ip4/0.0.0.0/tcp/0".parse()?)?;
    swarm.listen_on("/ip4/0.0.0.0/udp/0/quic-v1".parse()?)?;

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

    let deadline = Instant::now() + Duration::from_secs(opts.timeout_secs);
    let relay_peers: HashSet<PeerId> = relays.iter().filter_map(peer_id_of).collect();

    match opts.mode {
        Mode::Listen { out } => {
            run_listen(
                &mut swarm,
                &relays,
                &relay_peers,
                local_peer_id,
                &out,
                deadline,
            )
            .await
        }
        Mode::Dial { rendezvous } => {
            run_dial(&mut swarm, &rendezvous, &relay_peers, deadline).await
        }
    }
}

fn peer_id_of(addr: &Multiaddr) -> Option<PeerId> {
    addr.iter().find_map(|p| match p {
        Protocol::P2p(id) => Some(id),
        _ => None,
    })
}

async fn run_listen(
    swarm: &mut libp2p::Swarm<Behaviour>,
    relays: &[Multiaddr],
    relay_peers: &HashSet<PeerId>,
    local_peer_id: PeerId,
    out: &PathBuf,
    deadline: Instant,
) -> Result<()> {
    // Try each relay until one accepts a reservation.
    let mut accepted: Vec<Multiaddr> = Vec::new();
    'relays: for relay in relays {
        tracing::info!(%relay, "attempting reservation");
        let listener: ListenerId = match swarm.listen_on(relay.clone().with(Protocol::P2pCircuit)) {
            Ok(id) => id,
            Err(e) => {
                tracing::warn!(%relay, error=%e, "listen_on failed");
                continue;
            }
        };
        let attempt_deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let remaining = attempt_deadline.saturating_duration_since(Instant::now());
            let ev = tokio::select! {
                ev = swarm.select_next_some() => ev,
                () = tokio::time::sleep(remaining) => {
                    tracing::warn!(%relay, "reservation timed out");
                    let _ = swarm.remove_listener(listener);
                    continue 'relays;
                }
            };
            match ev {
                SwarmEvent::Behaviour(BehaviourEvent::RelayClient(
                    relay::client::Event::ReservationReqAccepted { .. },
                )) => {
                    tracing::info!(%relay, "reservation accepted");
                }
                SwarmEvent::NewListenAddr {
                    listener_id,
                    address,
                } if listener_id == listener => {
                    tracing::info!(%address, "relayed listen addr");
                    accepted.push(address);
                    // One relayed addr is enough to publish; stop probing relays.
                    break 'relays;
                }
                SwarmEvent::ListenerClosed {
                    listener_id,
                    reason,
                    ..
                } if listener_id == listener => {
                    tracing::warn!(%relay, ?reason, "relay listener closed");
                    continue 'relays;
                }
                SwarmEvent::OutgoingConnectionError { error, .. } => {
                    tracing::warn!(%relay, %error, "relay dial failed");
                    let _ = swarm.remove_listener(listener);
                    continue 'relays;
                }
                other => tracing::debug!(?other),
            }
        }
    }

    if accepted.is_empty() {
        bail!("no relay accepted a reservation");
    }

    let rendezvous = Rendezvous {
        peer_id: local_peer_id.to_string(),
        circuit_addrs: accepted
            .iter()
            .map(|a| a.clone().with(Protocol::P2p(local_peer_id)).to_string())
            .collect(),
    };
    let tmp = out.with_extension("tmp");
    tokio::fs::write(&tmp, serde_json::to_vec_pretty(&rendezvous)?).await?;
    tokio::fs::rename(&tmp, out).await?;
    tracing::info!(file=%out.display(), "wrote rendezvous");

    // Wait for the dialer to reach us.
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
            } if !relay_peers.contains(&peer_id) => {
                let direct = !is_relayed(&endpoint);
                tracing::info!(peer=%peer_id, direct, ?endpoint, "dialer connected");
            }
            SwarmEvent::Behaviour(BehaviourEvent::Dcutr(ev)) => {
                tracing::info!(?ev, "dcutr");
            }
            SwarmEvent::Behaviour(BehaviourEvent::Ping(ping::Event {
                peer,
                result: Ok(rtt),
                ..
            })) if !relay_peers.contains(&peer) => {
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
    relay_peers: &HashSet<PeerId>,
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
    let mut extra_relay_peers = relay_peers.clone();
    for a in &rv.circuit_addrs {
        let ma: Multiaddr = a.parse()?;
        let relay_only: Multiaddr = ma
            .iter()
            .take_while(|p| !matches!(p, Protocol::P2pCircuit))
            .collect();
        if let Some(id) = peer_id_of(&relay_only) {
            extra_relay_peers.insert(id);
        }
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
            SwarmEvent::Behaviour(BehaviourEvent::Identify(ev)) => tracing::debug!(?ev),
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
