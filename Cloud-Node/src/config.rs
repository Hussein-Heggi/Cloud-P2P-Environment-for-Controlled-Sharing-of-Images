use clap::Parser;
use std::net::SocketAddr;

#[derive(Debug, Clone, Parser)]
#[command(name = "server", about = "Cloud P2P server")]
pub struct Config {
    /// This node's ID (1..=N)
    #[arg(long, default_value_t = 1)]
    pub node_id: u32,

    /// Bind address for UDP service (optional; derived from peers() if omitted)
    #[arg(long)]
    pub udp_bind: Option<String>,

    /// Simulated failure every N seconds
    #[arg(long, default_value_t = 60)]
    pub fail_every_secs: u64,

    /// Simulated failure duration
    #[arg(long, default_value_t = 20)]
    pub fail_duration_secs: u64,
}

impl Config {
    /// SAME on every server. Order => node_id mapping.
    pub fn peers() -> &'static [&'static str] {
        &[
            "10.40.61.79:8080",   // node_id 1
            "10.40.58.169:8081",  // node_id 2
            "10.40.50.93:8083",   // node_id 3
        ]
    }

    pub fn udp_bind_addr(&self) -> SocketAddr {
        if let Some(b) = &self.udp_bind {
            return b.parse().expect("valid bind address");
        }
        let idx = (self.node_id as usize) - 1;
        let me = Self::peers().get(idx).expect("node_id out of range");
        me.parse().expect("valid constant peer address")
    }

    pub fn peer_addrs() -> Vec<SocketAddr> {
        Self::peers().iter().map(|s| s.parse().unwrap()).collect()
    }

    /// Election runs on service_port + 100
    pub fn election_bind_addr(&self) -> SocketAddr {
        let svc = self.udp_bind_addr();
        let port = svc.port().saturating_add(100);
        SocketAddr::new(svc.ip(), port)
    }

    pub fn election_peer_addrs() -> Vec<SocketAddr> {
        Self::peer_addrs()
            .into_iter()
            .map(|mut a| { let p = a.port().saturating_add(100); a.set_port(p); a })
            .collect()
    }

    pub fn validate(&self) {
        let n = Self::peers().len() as u32;
        assert!((1..=n).contains(&self.node_id), "node_id out of range");
    }
}
