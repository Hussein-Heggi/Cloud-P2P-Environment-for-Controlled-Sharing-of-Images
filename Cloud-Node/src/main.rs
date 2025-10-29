use clap::Parser;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::net::UdpSocket;
use tokio::sync::{RwLock, Mutex};
use tokio::time::{sleep, interval};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

// === Configuration ===
const LEADER_TIMEOUT_SECS: u64 = 5;
const PEER_EXPIRY_SECS: u64 = 6;
const CUSTOM_BUFFER_SIZE_GB: usize = 8;  // 8GB custom buffer
const MAX_PACKETS_IN_BUFFER: usize = 8 * 1024 * 1024;  // ~8M packets

// === Packet Buffer ===
#[derive(Clone)]
struct PacketData {
  data: Vec<u8>,
  source: SocketAddr,
  received_at: SystemTime,
}

// === Request tracking structures ===
#[derive(Debug, Clone)]
struct RequestHeader {
  version: u32,
  request_type: String,
  req_id: u64,
  mime: String,
  image_len: usize,
  chunk_size: usize,
  total_chunks: u32,
}

#[derive(Debug)]
struct ChunkData {
  seq_no: u32,
  data: Vec<u8>,
}

#[derive(Debug)]
struct RequestState {
  header: RequestHeader,
  chunks: HashMap<u32, ChunkData>,
  first_seen: SystemTime,
  last_updated: SystemTime,
  completed_at: Option<SystemTime>,
}

impl RequestState {
  fn new(header: RequestHeader) -> Self {
      let now = SystemTime::now();
      Self {
          header,
          chunks: HashMap::new(),
          first_seen: now,
          last_updated: now,
          completed_at: None,
      }
  }

  fn is_complete(&self) -> bool {
      self.chunks.len() == self.header.total_chunks as usize
  }

  fn completeness_percent(&self) -> f64 {
      (self.chunks.len() as f64 / self.header.total_chunks as f64) * 100.0
  }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
enum Message {
  Discovery { sender_id: u32, timestamp: u64 },
  LeaderAnnounce { leader_id: u32, timestamp: u64 },
  Election { sender_id: u32, timestamp: u64 },
  ElectionOk { sender_id: u32, timestamp: u64 },
  Coordinator { leader_id: u32, timestamp: u64 },
  Heartbeat { leader_id: u32, successor_id: Option<u32>, timestamp: u64 },
  HeartbeatAck { sender_id: u32, timestamp: u64 },
}

#[derive(Debug, Clone, PartialEq)]
enum NodeState {
  Follower,
  Candidate,
  Leader,
}

#[derive(Debug, Clone, Deserialize)]
struct NodeConfig {
  id: u32,
  address: String,
}

#[derive(Debug, Clone, Deserialize)]
struct Config {
  nodes: Vec<NodeConfig>,
}

struct Node {
  id: u32,
  address: SocketAddr,
  all_nodes: HashMap<u32, SocketAddr>,
  state: Arc<RwLock<NodeState>>,
  current_leader: Arc<RwLock<Option<u32>>>,
  successor_hint: Arc<RwLock<Option<u32>>>,
  active_nodes: Arc<RwLock<HashMap<u32, SystemTime>>>,
  last_heartbeat: Arc<RwLock<SystemTime>>,
  election_in_progress: Arc<RwLock<bool>>,
  socket: Arc<UdpSocket>,
   // Custom packet buffer (8GB in RAM)
  packet_buffer: Arc<Mutex<VecDeque<PacketData>>>,
  buffer_size: Arc<AtomicUsize>,
  buffer_dropped: Arc<AtomicU64>,
   // Request tracking
  requests: Arc<RwLock<HashMap<u64, RequestState>>>,
  total_headers: Arc<AtomicU64>,
  total_chunks_counter: Arc<AtomicU64>,
  total_datagrams: Arc<AtomicU64>,
  completed_requests: Arc<AtomicU64>,
}

impl Node {
  async fn new(id: u32, config: &Config) -> Result<Self, Box<dyn std::error::Error>> {
      let node_config = config
          .nodes
          .iter()
          .find(|n| n.id == id)
          .ok_or("Node ID not found in config")?;

      let address: SocketAddr = node_config.address.parse()?;
      let socket = UdpSocket::bind(address).await?;

      let mut all_nodes = HashMap::new();
      for node in &config.nodes {
          all_nodes.insert(node.id, node.address.parse()?);
      }

      let buffer_capacity = MAX_PACKETS_IN_BUFFER;
      let buffer_gb = (buffer_capacity * 1500) / (1024 * 1024 * 1024);  // Estimate with 1500 byte packets
    
      println!("Node {} starting at {}", id, address);
      println!("✓ Custom packet buffer: {}GB ({} packets max)", buffer_gb, buffer_capacity);

      Ok(Self {
          id,
          address,
          all_nodes,
          state: Arc::new(RwLock::new(NodeState::Follower)),
          current_leader: Arc::new(RwLock::new(None)),
          successor_hint: Arc::new(RwLock::new(None)),
          active_nodes: Arc::new(RwLock::new(HashMap::new())),
          last_heartbeat: Arc::new(RwLock::new(SystemTime::now())),
          election_in_progress: Arc::new(RwLock::new(false)),
          socket: Arc::new(socket),
        
          packet_buffer: Arc::new(Mutex::new(VecDeque::with_capacity(buffer_capacity))),
          buffer_size: Arc::new(AtomicUsize::new(0)),
          buffer_dropped: Arc::new(AtomicU64::new(0)),
        
          requests: Arc::new(RwLock::new(HashMap::new())),
          total_headers: Arc::new(AtomicU64::new(0)),
          total_chunks_counter: Arc::new(AtomicU64::new(0)),
          total_datagrams: Arc::new(AtomicU64::new(0)),
          completed_requests: Arc::new(AtomicU64::new(0)),
      })
  }

  async fn start(self: Arc<Self>) {
      // Start UDP receiver (fastest possible - just buffer packets)
      let node_clone = Arc::clone(&self);
      tokio::spawn(async move {
          node_clone.udp_receiver().await;
      });

      // Start packet processors (multiple workers)
      for worker_id in 0..8 {
          let node_clone = Arc::clone(&self);
          tokio::spawn(async move {
              node_clone.packet_processor(worker_id).await;
          });
      }

      sleep(Duration::from_millis(500)).await;

      self.discover_cluster().await;

      let node_clone = Arc::clone(&self);
      tokio::spawn(async move { node_clone.monitor_leader().await; });

      let node_clone = Arc::clone(&self);
      tokio::spawn(async move { node_clone.send_heartbeats().await; });

      let node_clone = Arc::clone(&self);
      tokio::spawn(async move { node_clone.report_status().await; });

      let node_clone = Arc::clone(&self);
      tokio::spawn(async move { node_clone.report_request_stats().await; });

      let node_clone = Arc::clone(&self);
      tokio::spawn(async move { node_clone.batch_cleanup_completed_requests().await; });

      let node_clone = Arc::clone(&self);
      tokio::spawn(async move { node_clone.report_buffer_stats().await; });

      println!("Node {} started with custom 8GB RAM buffer + 8 processor threads", self.id);
  }

  async fn udp_receiver(&self) {
      let mut buf = vec![0u8; 65536];
    
      loop {
          match self.socket.recv_from(&mut buf).await {
              Ok((len, addr)) => {
                  self.total_datagrams.fetch_add(1, Ordering::Relaxed);
                
                  let packet = PacketData {
                      data: buf[..len].to_vec(),
                      source: addr,
                      received_at: SystemTime::now(),
                  };
                
                  // Try to add to buffer
                  let mut buffer = self.packet_buffer.lock().await;
                  if buffer.len() < MAX_PACKETS_IN_BUFFER {
                      buffer.push_back(packet);
                      self.buffer_size.store(buffer.len(), Ordering::Relaxed);
                  } else {
                      // Buffer full - drop packet
                      self.buffer_dropped.fetch_add(1, Ordering::Relaxed);
                  }
              }
              Err(e) => {
                  eprintln!("Node {}: Error receiving: {}", self.id, e);
              }
          }
      }
  }

  async fn packet_processor(&self, worker_id: usize) {
      loop {
          // Pop packet from buffer
          let packet = {
              let mut buffer = self.packet_buffer.lock().await;
              let size = buffer.len();
              self.buffer_size.store(size, Ordering::Relaxed);
              buffer.pop_front()
          };
        
          if let Some(pkt) = packet {
              // Try election message first
              if let Ok(message) = serde_json::from_slice::<Message>(&pkt.data) {
                  self.handle_message(message, pkt.source).await;
                  continue;
              }
            
              // Process as client request
              self.handle_client_data(&pkt.data, pkt.source).await;
          } else {
              // Buffer empty, sleep briefly
              tokio::time::sleep(Duration::from_micros(100)).await;
          }
      }
  }

  async fn report_buffer_stats(&self) {
      let mut interval = interval(Duration::from_secs(2));
      loop {
          interval.tick().await;
        
          let size = self.buffer_size.load(Ordering::Relaxed);
          let dropped = self.buffer_dropped.load(Ordering::Relaxed);
          let usage_pct = (size as f64 / MAX_PACKETS_IN_BUFFER as f64) * 100.0;
        
          if size > 0 || dropped > 0 {
              println!("\n📦 Buffer: {} packets ({:.1}% full) | Dropped: {}",
                  size, usage_pct, dropped);
          }
      }
  }

  fn calculate_successor(&self, active_nodes: &HashMap<u32, SystemTime>, exclude_id: u32) -> Option<u32> {
      let mut ids: Vec<u32> = active_nodes.keys().copied().collect();
      ids.push(self.id);
      ids.sort_unstable_by(|a, b| b.cmp(a));
      ids.dedup();
      ids.into_iter().find(|&id| id != exclude_id)
  }

  async fn discover_cluster(&self) {
      println!("Node {}: Starting cluster discovery...", self.id);

      let discovery_msg = Message::Discovery {
          sender_id: self.id,
          timestamp: current_timestamp(),
      };

      for (node_id, addr) in &self.all_nodes {
          if *node_id != self.id {
              self.send_message(addr, &discovery_msg).await;
          }
      }

      sleep(Duration::from_secs(2)).await;

      let leader = *self.current_leader.read().await;
      if leader.is_none() {
          println!("Node {}: No leader found, starting election...", self.id);
          self.start_election().await;
      } else {
          println!("Node {}: Discovered leader is Node {}", self.id, leader.unwrap());
      }
  }

  async fn start_election(&self) {
      let mut election_in_progress = self.election_in_progress.write().await;
      if *election_in_progress {
          return;
      }
      *election_in_progress = true;
      drop(election_in_progress);

      println!("Node {}: Starting election...", self.id);

      let successor_hint = *self.successor_hint.read().await;

      if let Some(successor_id) = successor_hint {
          if successor_id == self.id {
              println!("Node {}: I am the successor! Becoming leader directly.", self.id);
              self.become_leader().await;
              *self.election_in_progress.write().await = false;
              return;
          } else if successor_id > self.id {
              println!("Node {}: Deferring to known successor Node {}", self.id, successor_id);

              let election_msg = Message::Election {
                  sender_id: self.id,
                  timestamp: current_timestamp(),
              };

              if let Some(successor_addr) = self.all_nodes.get(&successor_id) {
                  self.send_message(successor_addr, &election_msg).await;
              }

              sleep(Duration::from_millis(800)).await;

              let state = self.state.read().await;
              if *state == NodeState::Leader {
                  drop(state);
                  *self.election_in_progress.write().await = false;
                  return;
              }
              drop(state);

              println!("Node {}: Successor didn't respond, proceeding with full election", self.id);
          }
      }

      *self.state.write().await = NodeState::Candidate;

      let election_msg = Message::Election {
          sender_id: self.id,
          timestamp: current_timestamp(),
      };

      let mut higher_nodes_exist = false;

      for (node_id, addr) in &self.all_nodes {
          if *node_id > self.id {
              higher_nodes_exist = true;
              self.send_message(addr, &election_msg).await;
          }
      }

      sleep(Duration::from_secs(2)).await;

      let state = self.state.read().await;
      if *state == NodeState::Candidate {
          drop(state);
          if !higher_nodes_exist {
              self.become_leader().await;
          } else {
              let state = self.state.read().await;
              if *state == NodeState::Candidate {
                  drop(state);
                  self.become_leader().await;
              }
          }
      }

      *self.election_in_progress.write().await = false;
  }

  async fn become_leader(&self) {
      println!("Node {}: Becoming leader!", self.id);
      *self.state.write().await = NodeState::Leader;
      *self.current_leader.write().await = Some(self.id);

      let coordinator_msg = Message::Coordinator {
          leader_id: self.id,
          timestamp: current_timestamp(),
      };

      for (node_id, addr) in &self.all_nodes {
          if *node_id != self.id {
              self.send_message(addr, &coordinator_msg).await;
          }
      }
  }

  async fn send_heartbeats(&self) {
      let mut interval = interval(Duration::from_secs(2));

      loop {
          interval.tick().await;

          let state = self.state.read().await;
          if *state == NodeState::Leader {
              drop(state);

              let mut active_nodes = self.active_nodes.write().await;
              let now = SystemTime::now();
              active_nodes.retain(|_, last_seen| {
                  now.duration_since(*last_seen)
                      .unwrap_or(Duration::from_secs(999))
                      .as_secs()
                      < PEER_EXPIRY_SECS
              });

              let successor_id = self.calculate_successor(&active_nodes, self.id);
              drop(active_nodes);

              let heartbeat_msg = Message::Heartbeat {
                  leader_id: self.id,
                  successor_id,
                  timestamp: current_timestamp(),
              };

              for (node_id, addr) in &self.all_nodes {
                  if *node_id != self.id {
                      self.send_message(addr, &heartbeat_msg).await;
                  }
              }
          }
      }
  }

  async fn monitor_leader(&self) {
      let mut interval = interval(Duration::from_millis(1000));

      loop {
          interval.tick().await;

          let state = self.state.read().await;
          if *state != NodeState::Leader {
              drop(state);

              let leader = *self.current_leader.read().await;
              if leader.is_some() {
                  let last_hb = self.last_heartbeat.read().await;
                  let elapsed = SystemTime::now()
                      .duration_since(*last_hb)
                      .unwrap_or(Duration::from_secs(0));
                  drop(last_hb);

                  if elapsed.as_secs() > LEADER_TIMEOUT_SECS {
                      println!(
                          "Node {}: Leader timeout detected! (> {}s)",
                          self.id, LEADER_TIMEOUT_SECS
                      );
                      *self.current_leader.write().await = None;
                      self.start_election().await;
                  }
              }
          }
      }
  }

  async fn handle_client_data(&self, data: &[u8], src: SocketAddr) {
      if let Some(header) = parse_json_header(data) {
          self.total_headers.fetch_add(1, Ordering::Relaxed);
        
          let mut reqs = self.requests.write().await;
          reqs.insert(header.req_id, RequestState::new(header));
          return;
      }

      if let Some((req_id, seq_no, _frame_total_chunks, payload)) = parse_chunk_frame(data) {
          self.total_chunks_counter.fetch_add(1, Ordering::Relaxed);

          let mut reqs = self.requests.write().await;
          if let Some(state) = reqs.get_mut(&req_id) {
              state.last_updated = SystemTime::now();
              state.chunks.insert(seq_no, ChunkData {
                  seq_no,
                  data: payload,
              });

              if state.is_complete() && state.completed_at.is_none() {
                  state.completed_at = Some(SystemTime::now());
                
                  let elapsed = state.last_updated.duration_since(state.first_seen)
                      .unwrap_or(Duration::from_secs(0));
                
                  println!("\n✅ Node {}: REQUEST FULLY ASSEMBLED", self.id);
                  println!("  ├─ Source: {}", src);
                  println!("  ├─ Request ID: {}", req_id);
                  println!("  ├─ Type: {}", state.header.request_type);
                  println!("  ├─ MIME: {}", state.header.mime);
                  println!("  ├─ Total Size: {} bytes", state.header.image_len);
                  println!("  ├─ Chunks Received: {}/{}", state.chunks.len(), state.header.total_chunks);
                  println!("  ├─ Chunk Size: {} bytes", state.header.chunk_size);
                  println!("  └─ Assembly Time: {:.3}s (will be processed in 2s)", elapsed.as_secs_f64());
              }
          }
      }
  }

  async fn handle_message(&self, message: Message, _addr: SocketAddr) {
      match message {
          Message::Discovery { sender_id, .. } => {
              let mut active_nodes = self.active_nodes.write().await;
              active_nodes.insert(sender_id, SystemTime::now());
              drop(active_nodes);

              let state = self.state.read().await;
              if *state == NodeState::Leader {
                  drop(state);

                  let response = Message::LeaderAnnounce {
                      leader_id: self.id,
                      timestamp: current_timestamp(),
                  };

                  if let Some(sender_addr) = self.all_nodes.get(&sender_id) {
                      self.send_message(sender_addr, &response).await;
                  }
              }
          }

          Message::LeaderAnnounce { leader_id, .. } => {
              let current = *self.current_leader.read().await;
              if current.is_none() || leader_id > current.unwrap() {
                  println!("Node {}: Accepting Node {} as leader", self.id, leader_id);
                  *self.current_leader.write().await = Some(leader_id);
                  *self.state.write().await = NodeState::Follower;
                  *self.last_heartbeat.write().await = SystemTime::now();
              }
          }

          Message::Election { sender_id, .. } => {
              let mut active_nodes = self.active_nodes.write().await;
              active_nodes.insert(sender_id, SystemTime::now());
              drop(active_nodes);

              if sender_id < self.id {
                  let ok_msg = Message::ElectionOk {
                      sender_id: self.id,
                      timestamp: current_timestamp(),
                  };

                  if let Some(sender_addr) = self.all_nodes.get(&sender_id) {
                      self.send_message(sender_addr, &ok_msg).await;
                  }

                  let election_in_progress = *self.election_in_progress.read().await;
                  if !election_in_progress {
                      self.start_election().await;
                  }
              }
          }

          Message::ElectionOk { sender_id, .. } => {
              println!("Node {}: Higher node {} responded to election", self.id, sender_id);
              *self.state.write().await = NodeState::Follower;
          }

          Message::Coordinator { leader_id, .. } => {
              println!("Node {}: New coordinator is Node {}", self.id, leader_id);
              *self.current_leader.write().await = Some(leader_id);
              *self.state.write().await = NodeState::Follower;
              *self.last_heartbeat.write().await = SystemTime::now();
          }

          Message::Heartbeat { leader_id, successor_id, .. } => {
              let current = *self.current_leader.read().await;
              if current == Some(leader_id) {
                  *self.last_heartbeat.write().await = SystemTime::now();
                  *self.successor_hint.write().await = successor_id;




                  let ack_msg = Message::HeartbeatAck {
                      sender_id: self.id,
                      timestamp: current_timestamp(),
                  };




                  if let Some(leader_addr) = self.all_nodes.get(&leader_id) {
                      self.send_message(leader_addr, &ack_msg).await;
                  }
              }
          }

          Message::HeartbeatAck { sender_id, .. } => {
              let state = self.state.read().await;
              if *state == NodeState::Leader {
                  drop(state);
                  let mut active_nodes = self.active_nodes.write().await;
                  active_nodes.insert(sender_id, SystemTime::now());
              }
          }
      }
  }

  async fn send_message(&self, addr: &SocketAddr, message: &Message) {
      if let Ok(data) = serde_json::to_vec(message) {
          let _ = self.socket.send_to(&data, addr).await;
      }
  }

  async fn batch_cleanup_completed_requests(&self) {
      let mut interval = interval(Duration::from_secs(5));
      loop {
          interval.tick().await;
        
          let mut reqs = self.requests.write().await;
          let mut to_remove = Vec::new();
          let mut batch_count = 0;
          let now = SystemTime::now();
        
          for (req_id, state) in reqs.iter() {
              if let Some(completed_at) = state.completed_at {
                  let time_since_completion = now.duration_since(completed_at)
                      .unwrap_or(Duration::from_secs(0));
                
                  if time_since_completion.as_secs() >= 2 {
                      to_remove.push(*req_id);
                      batch_count += 1;
                      if batch_count >= 100 {
                          break;
                      }
                  }
              }
          }
        
          for req_id in &to_remove {
              reqs.remove(req_id);
          }
        
          let removed = to_remove.len();
          if removed > 0 {
              self.completed_requests.fetch_add(removed as u64, Ordering::Relaxed);
              println!("\n🗑️  Node {}: Batch cleanup removed {} processed requests", self.id, removed);
          }
      }
  }

  async fn report_status(&self) {
      let mut interval = interval(Duration::from_secs(5));
      loop {
          interval.tick().await;

          let state = self.state.read().await.clone();
          let leader = *self.current_leader.read().await;
          let last_hb = self.last_heartbeat.read().await;
          let elapsed = SystemTime::now()
              .duration_since(*last_hb)
              .unwrap_or(Duration::from_secs(0))
              .as_secs_f64();
          drop(last_hb);

          if state == NodeState::Leader {
              let active_nodes = self.active_nodes.read().await;
              let computed_succ = self.calculate_successor(&active_nodes, self.id);
              let active_count = active_nodes.len() + 1;
              let live_peers: Vec<u32> = active_nodes.keys().copied().collect();
              drop(active_nodes);




              println!(
                  "Node {} Status: State={:?}, Leader={:?}, Successor(computed)={:?}, Active nodes={}, Live={:?}, Time since heartbeat={:.1}s",
                  self.id, state, leader, computed_succ, active_count, live_peers, elapsed
              );
          } else {
              let successor_hint = *self.successor_hint.read().await;
              println!(
                  "Node {} Status: State={:?}, Leader={:?}, Successor(hint)={:?}, Time since heartbeat={:.1}s",
                  self.id, state, leader, successor_hint, elapsed
              );
          }
      }
  }

  async fn report_request_stats(&self) {
      let mut interval = interval(Duration::from_secs(10));
      let start = SystemTime::now();
    
      loop {
          interval.tick().await;
        
          let reqs = self.requests.read().await;
          let headers = self.total_headers.load(Ordering::Relaxed);
          let chunks = self.total_chunks_counter.load(Ordering::Relaxed);
          let datagrams = self.total_datagrams.load(Ordering::Relaxed);
          let completed_cleared = self.completed_requests.load(Ordering::Relaxed);
        
          if headers > 0 || chunks > 0 {
              let elapsed = SystemTime::now()
                  .duration_since(start)
                  .unwrap_or(Duration::from_secs(1));
            
              let now = SystemTime::now();
              let in_memory = reqs.len();
              let in_memory_completed = reqs.values().filter(|r| r.is_complete()).count();
              let in_progress = in_memory - in_memory_completed;
            
              let waiting_for_processing = reqs.values().filter(|r| {
                  if let Some(completed_at) = r.completed_at {
                      let elapsed = now.duration_since(completed_at).unwrap_or(Duration::from_secs(0));
                      elapsed.as_secs() < 2
                  } else {
                      false
                  }
              }).count();
            
              let avg_chunks = if in_memory_completed > 0 {
                  chunks / in_memory_completed as u64
              } else {
                  0
              };
            
              println!("\n╔═══ Node {} Request Stats ═══╗", self.id);
              println!("║ Total datagrams received: {}", datagrams);
              println!("║ Processed & cleared: {}", completed_cleared);
              println!("║ In memory: {} (assembling: {}, waiting 2s: {}, ready: {})",
                  in_memory, in_progress, waiting_for_processing,
                  in_memory_completed - waiting_for_processing);
              println!("║ Headers: {} | Chunks: {}", headers, chunks);
              println!("║ Avg chunks per request: {}", avg_chunks);
              println!("║ Rate: {:.1} headers/s, {:.1} chunks/s",
                  headers as f64 / elapsed.as_secs_f64(),
                  chunks as f64 / elapsed.as_secs_f64());
            
              if in_progress > 0 && in_progress <= 3 {
                  println!("║ Assembling:");
                  for (req_id, state) in reqs.iter() {
                      if !state.is_complete() {
                          println!("║   req_id: {} - {:.1}% ({}/{} chunks)",
                              req_id,
                              state.completeness_percent(),
                              state.chunks.len(),
                              state.header.total_chunks);
                      }
                  }
              }
              println!("╚══════════════════════════════╝");
          }
      }
  }
}

fn parse_json_header(data: &[u8]) -> Option<RequestHeader> {
  let text = std::str::from_utf8(data).ok()?;
   if !text.contains("\"kind\":\"client_header\"") {
      return None;
  }

  let version = text.split("\"version\":").nth(1)?.split(',').next()?.trim().parse::<u32>().ok()?;
  let request_type = text.split("\"request_type\":\"").nth(1)?.split('\"').next()?.to_string();
  let req_id = text.split("\"req_id\":").nth(1)?.split(',').next()?.trim().parse::<u64>().ok()?;
  let mime = text.split("\"mime\":\"").nth(1)?.split('\"').next()?.to_string();
  let image_len = text.split("\"image_len\":").nth(1)?.split(',').next()?.trim().parse::<usize>().ok()?;
  let chunk_size = text.split("\"chunk_size\":").nth(1)?.split(',').next()?.trim().parse::<usize>().ok()?;
  let total_chunks = text.split("\"total_chunks\":").nth(1)?.split('}').next()?.trim().parse::<u32>().ok()?;

  Some(RequestHeader {
      version,
      request_type,
      req_id,
      mime,
      image_len,
      chunk_size,
      total_chunks,
  })
}

fn parse_chunk_frame(data: &[u8]) -> Option<(u64, u32, u32, Vec<u8>)> {
  if data.len() < 18 {
      return None;
  }

  let version = u16::from_be_bytes([data[0], data[1]]);
  if version != 1 {
      return None;
  }

  let req_id = u64::from_be_bytes([
      data[2], data[3], data[4], data[5], data[6], data[7], data[8], data[9],
  ]);

  let seq_no = u32::from_be_bytes([data[10], data[11], data[12], data[13]]);
  let total_chunks = u32::from_be_bytes([data[14], data[15], data[16], data[17]]);
  let payload = data[18..].to_vec();
  Some((req_id, seq_no, total_chunks, payload))
}

fn current_timestamp() -> u64 {
  SystemTime::now()
      .duration_since(SystemTime::UNIX_EPOCH)
      .unwrap()
      .as_secs()
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
  #[arg(short, long)]
  id: u32,




  #[arg(short, long)]
  config: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
  let args = Args::parse();




  let config_json = r#"{
      "nodes": [
          {"id": 0, "address": "10.40.61.79:8080"},
          {"id": 1, "address": "10.40.58.169:8081"},
          {"id": 2, "address": "10.40.50.93:8083"}
      ]
  }"#;

  let config: Config = if let Some(config_path) = args.config {
      let config_str = tokio::fs::read_to_string(config_path).await?;
      serde_json::from_str(&config_str)?
  } else {
      serde_json::from_str(config_json)?
  };

  let node = Arc::new(Node::new(args.id, &config).await?);
  node.clone().start().await;

  tokio::signal::ctrl_c().await?;
  println!("\nShutting down node {}...", args.id);

  Ok(())
}









