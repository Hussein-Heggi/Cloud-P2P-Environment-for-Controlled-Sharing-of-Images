use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ClientConfig {
    pub servers: Vec<ServerEntry>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ServerEntry {
    pub address: String,
}

impl ClientConfig {
    pub fn load() -> Result<Self> {
        let config_str = std::fs::read_to_string("config.toml")?;
        let config: ClientConfig = toml::from_str(&config_str)?;
        Ok(config)
    }

    pub fn server_addresses(&self) -> Vec<SocketAddr> {
        self.servers
            .iter()
            .filter_map(|s| s.address.parse().ok())
            .collect()
    }
}
