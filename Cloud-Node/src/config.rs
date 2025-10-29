use clap::Parser;
use std::net::SocketAddr;

#[derive(Debug, Clone, Parser)]
#[command(name = "server", about = "Cloud P2P server")]
pub struct Config {
    /// 1-based node ID
    #[arg(long, default_value_t = 1)]
    pub node_id: u32,

    /// Bind for service UDP (client requests, stego)
    #[arg(long)]
    pub udp_bind: Option<String>,

    /// Bind for election/heartbeat UDP (cluster-internal)
    #[arg(long)]
    pub election_bind: Option<String>,

    /// Failure simulation cadence
    #[arg(long, default_value_t = 60)]
    pub fail_every_secs: u64,

    /// Failure simulation outage length
    #[arg(long, default_value_t = 20)]
    pub fail_duration_secs: u64,
}

impl Config {
    /// Service peers (client-facing UDP)
    pub fn service_peers() -> &'static [&'static str] {
        &[
            "10.40.61.79:8180",  // node 1
            "10.40.58.169:8181", // node 2
            "10.40.50.93:8183",  // node 3
        ]
    }

    /// Election/heartbeat peers (server-to-server UDP) — CHANGED
    pub fn election_peers() -> &'static [&'static str] {
        &[
            "10.40.61.79:8080",  // node 1
            "10.40.58.169:8081", // node 2
            "10.40.50.93:8083",  // node 3
        ]
    }


    fn parse_addr(s: &str) -> SocketAddr {
        s.parse::<SocketAddr>().expect("valid IP:PORT")
    }

    /// This node’s service bind (or override via --udp-bind)
    pub fn udp_bind_addr(&self) -> SocketAddr {
        if let Some(b) = &self.udp_bind {
            return Self::parse_addr(b);
        }
        let idx = (self.node_id as usize).checked_sub(1).expect("node_id >= 1");
        Self::parse_addr(
            *Self::service_peers()
                .get(idx)
                .expect("node_id out of range for service peers"),
        )
    }

    /// This node’s election bind (or override via --election-bind)
    pub fn election_bind_addr(&self) -> SocketAddr {
        if let Some(b) = &self.election_bind {
            return Self::parse_addr(b);
        }
        let idx = (self.node_id as usize).checked_sub(1).expect("node_id >= 1");
        Self::parse_addr(
            *Self::election_peers()
                .get(idx)
                .expect("node_id out of range for election peers"),
        )
    }

    /// All **service** peers parsed (use this to broadcast client traffic, if needed)
    pub fn service_peer_addrs() -> Vec<SocketAddr> {
        Self::service_peers()
            .iter()
            .map(|s| Self::parse_addr(s))
            .collect()
    }

    /// All **election** peers parsed (used by election.rs)
    pub fn election_peer_addrs() -> Vec<SocketAddr> {
        Self::election_peers()
            .iter()
            .map(|s| Self::parse_addr(s))
            .collect()
    }

    /// Optional sanity checks (called from main.rs)
    pub fn validate(&self) {
        // Ensure node_id is in range
        let n = Self::service_peers().len();
        assert!(
            (1..=n as u32).contains(&self.node_id),
            "node_id {} must be in 1..={}",
            self.node_id,
            n
        );
        // Ensure service/election lists are same length and disjoint ports per node
        assert_eq!(
            Self::service_peers().len(),
            Self::election_peers().len(),
            "service/election peers must have same length"
        );
        for i in 0..n {
            let svc: SocketAddr = Self::parse_addr(Self::service_peers()[i]);
            let ele: SocketAddr = Self::parse_addr(Self::election_peers()[i]);
            assert_eq!(
                svc.ip(),
                ele.ip(),
                "service and election IPs must match for node {}",
                i + 1
            );
            assert_ne!(
                svc.port(),
                ele.port(),
                "service and election ports must differ for node {}",
                i + 1
            );
        }
    }
}