use clap::Parser;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

#[derive(Debug, Clone, Parser)]
#[command(name = "server", about = "Cloud P2P server")]
pub struct Config {
    /// 1-based node ID
    #[arg(long, default_value_t = 1)]
    pub node_id: u32,

    /// Bind for service UDP (client requests)
    #[arg(long)]
    pub udp_bind: Option<String>,

    /// Bind for election/heartbeat UDP (cluster-internal)
    #[arg(long)]
    pub elect_bind: Option<String>,

    /// Bind for assignment UDP (leader -> cluster executor announcements)
    #[arg(long)]
    pub assign_bind: Option<String>,

    /// Pacing delay in microseconds per response packet (0 = no pacing)
    #[arg(long, default_value_t = 150)]
    pub pacing_us: u64,

    // -------- Assignment timing --------
    /// How often the leader broadcasts ASSIGN (ms)
    #[arg(long, default_value_t = 1500)]
    pub assign_broadcast_every_ms: u64,

    /// Lease duration for ASSIGN (ms)
    #[arg(long, default_value_t = 1000000)]
    pub assign_lease_ms: u32,

    // -------- Failure simulation (re-enabled) --------
    /// Period between failures (seconds). 0 disables injection until set.
    #[arg(long, default_value_t = 0)]
    pub fail_every_secs: u64,

    /// Duration of each failure window (seconds). 0 disables injection until set.
    #[arg(long, default_value_t = 0)]
    pub fail_duration_secs: u64,
}

impl Config {
    /// Service peers (client-facing UDP)
    pub fn service_peers() -> &'static [&'static str] {
        &[
            "10.40.63.10:8180",  // node 1
            "10.40.58.169:8181", // node 2
            "10.40.50.93:8183",  // node 3
        ]
    }

    /// Election/heartbeat peers (server-to-server UDP)
    pub fn election_peers() -> &'static [&'static str] {
        &[
            "10.40.63.10:8080",
            "10.40.58.169:8081",
            "10.40.50.93:8083",
        ]
    }

    /// Assignment peers (server-to-server UDP)
    pub fn assignment_peers() -> &'static [&'static str] {
        &[
            "10.40.63.10:8280",
            "10.40.58.169:8281",
            "10.40.50.93:8283",
        ]
    }

    pub fn service_bind_addr(&self) -> Option<SocketAddr> {
        if let Some(b) = &self.udp_bind {
            Some(Self::parse_addr(b))
        } else {
            let idx = (self.node_id - 1) as usize;
            Some(Self::parse_addr(Self::service_peers()[idx]))
        }
    }

    pub fn election_bind_addr(&self) -> Option<SocketAddr> {
        if let Some(b) = &self.elect_bind {
            Some(Self::parse_addr(b))
        } else {
            let idx = (self.node_id - 1) as usize;
            Some(Self::parse_addr(Self::election_peers()[idx]))
        }
    }

    pub fn assignment_bind_addr(&self) -> Option<SocketAddr> {
        if let Some(b) = &self.assign_bind {
            Some(Self::parse_addr(b))
        } else {
            let idx = (self.node_id - 1) as usize;
            Some(Self::parse_addr(Self::assignment_peers()[idx]))
        }
    }

    pub fn service_peer_addrs() -> Vec<SocketAddr> {
        Self::service_peers()
            .iter()
            .map(|s| Self::parse_addr(s))
            .collect()
    }

    pub fn election_peer_addrs() -> Vec<SocketAddr> {
        Self::election_peers()
            .iter()
            .map(|s| Self::parse_addr(s))
            .collect()
    }

    pub fn assignment_peer_addrs(&self) -> Vec<SocketAddr> {
        Self::assignment_peers()
            .iter()
            .map(|s| Self::parse_addr(s))
            .collect()
    }

    pub fn parse_addr(s: &str) -> SocketAddr {
        s.parse()
            .unwrap_or_else(|_| panic!("Invalid socket address: {}", s))
    }

    pub fn validate(&self) {
        let n = Self::service_peers().len();
        assert!(
            (1..=n as u32).contains(&self.node_id),
            "node_id {} must be in 1..={}",
            self.node_id,
            n
        );
        assert_eq!(
            Self::service_peers().len(),
            Self::election_peers().len(),
            "service and election peer lists must have same length"
        );
        assert_eq!(
            Self::service_peers().len(),
            Self::assignment_peers().len(),
            "service and assignment peer lists must have same length"
        );
        for i in 0..n {
            let svc = Self::parse_addr(Self::service_peers()[i]);
            let ele = Self::parse_addr(Self::election_peers()[i]);
            let asg = Self::parse_addr(Self::assignment_peers()[i]);
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
            assert_eq!(
                svc.ip(),
                asg.ip(),
                "service and assignment IPs must match for node {}",
                i + 1
            );
            assert_ne!(
                svc.port(),
                asg.port(),
                "service and assignment ports must differ for node {}",
                i + 1
            );
        }
    }

    /// Static executor IP for this phase
    pub fn static_executor_ip(&self) -> IpAddr {
        IpAddr::V4(Ipv4Addr::new(10, 40, 63, 10))
    }
}