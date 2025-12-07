//! Simple client for testing the new protocol
//! Usage:
//!   cargo run -- join <username> <server_ip:port> [image1] [image2] ...
//!   cargo run -- listen <username> <server_ip:port>

use anyhow::Result;
use std::env;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::RwLock;

mod protocol;
mod simple_client;
mod dos;
mod viewer;
mod extraction;
mod owner;
mod api_server;

use simple_client::{ClientState, SharedClientState, CLIENT_PORT};

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();

    if args.len() < 2 {
        print_usage();
        return Ok(());
    }

    match args[1].as_str() {
        "join" => {
            if args.len() < 4 {
                println!("Usage: {} join <username> <server_ip:port> [image1] [image2] ...", args[0]);
                return Ok(());
            }

            let username = args[2].clone();
            let server_addr: SocketAddr = args[3].parse()?;
            let images: Vec<String> = args.iter().skip(4).cloned().collect();

            run_join_mode(username, server_addr, images).await?;
        }

        "listen" => {
            if args.len() < 4 {
                println!("Usage: {} listen <username> <server_ip:port>", args[0]);
                return Ok(());
            }

            let username = args[2].clone();
            let server_addr: SocketAddr = args[3].parse()?;

            run_listen_mode(username, server_addr).await?;
        }

        "interactive" => {
            if args.len() < 4 {
                println!("Usage: {} interactive <username> <server_ip:port> [image1] [image2] ...", args[0]);
                return Ok(());
            }

            let username = args[2].clone();
            let server_addr: SocketAddr = args[3].parse()?;
            let images: Vec<String> = args.iter().skip(4).cloned().collect();

            run_interactive_mode(username, server_addr, images).await?;
        }

        "help" | "-h" | "--help" => {
            print_usage();
        }

        "upload" => {
            if args.len() < 8 {
                println!("Usage: {} upload <username> <server_ip:port> <image_name> <true_path> <cover_path> <meta_json_path>", args[0]);
                return Ok(());
            }
            let username = args[2].clone();
            let server_addr: SocketAddr = args[3].parse()?;
            let image_name = args[4].clone();
            let true_path = args[5].clone();
            let cover_path = args[6].clone();
            let meta_path = args[7].clone();
            simple_client::upload_owner_image(&username, server_addr, &image_name, &true_path, &cover_path, &meta_path, true).await?;
        }

        "approve" => {
            if args.len() < 6 {
                println!("Usage: {} approve <username> <server_ip:port> <image_name> <req_id>", args[0]);
                return Ok(());
            }
            let username = args[2].clone();
            let server_addr: SocketAddr = args[3].parse()?;
            let image_name = args[4].clone();
            let req_id: u32 = args[5].parse().unwrap_or(1);
            simple_client::approve_and_receive(&username, server_addr, &image_name, req_id).await?;
        }

        "upload-and-fetch" => {
            if args.len() < 8 {
                println!("Usage: {} upload-and-fetch <username> <server_ip:port> <image_name> <true_path> <cover_path> <meta_json_path> [req_id]", args[0]);
                return Ok(());
            }
            let username = args[2].clone();
            let server_addr: SocketAddr = args[3].parse()?;
            let image_name = args[4].clone();
            let true_path = args[5].clone();
            let cover_path = args[6].clone();
            let meta_path = args[7].clone();
            let req_id: u32 = if args.len() > 8 { args[8].parse().unwrap_or(1) } else { 1 };

            // Upload (briefly, no stay-open)
            simple_client::upload_owner_image(&username, server_addr, &image_name, &true_path, &cover_path, &meta_path, false).await?;
            // Then approve and fetch
            simple_client::approve_and_receive(&username, server_addr, &image_name, req_id).await?;
        }

        _ => {
            println!("Unknown command: {}", args[1]);
            print_usage();
        }
    }

    Ok(())
}

/// Join server and run ping loop
async fn run_join_mode(
    username: String,
    server_addr: SocketAddr,
    images: Vec<String>,
) -> Result<()> {
    println!("=== CLIENT JOIN MODE (TCP) ===");
    println!("Username: {}", username);
    println!("Server: {}", server_addr);
    println!("Client port: {}", CLIENT_PORT);
    println!("Images: {:?}", images);
    println!();

    // Initialize state
    let mut state = ClientState::new(username.clone(), server_addr);
    state.images = images;
    state.client_port = CLIENT_PORT;
    let state: SharedClientState = Arc::new(RwLock::new(state));

    // Connect to server via TCP
    let (mut reader, writer) = simple_client::connect_to_server(server_addr).await?;
    println!();

    // Send JOIN (no listener yet, to avoid racing on the socket)
    if let Err(e) = simple_client::join_server(state.clone(), writer.clone(), &mut reader).await {
        println!("[CLIENT] ⚠️  JOIN failed: {}", e);
        return Ok(());
    }

    println!("[CLIENT] ✅ Successfully joined server!");
    println!("[CLIENT] Starting ping loop (every 10 seconds)...");
    println!();

    // Start listener task AFTER JOIN is complete (sole reader for the socket)
    let listener_task = {
        let state_clone = state.clone();
        let reader_clone = reader;
        let writer_clone = writer.clone();
        tokio::spawn(async move {
            if let Err(e) = simple_client::run_listener(state_clone, reader_clone, writer_clone).await {
                println!("[CLIENT-LISTENER] Error: {}", e);
            }
        })
    };

    // Start ping loop
    let ping_task = {
        let state_clone = state.clone();
        let writer_clone = writer.clone();
        tokio::spawn(async move {
            if let Err(e) = simple_client::ping_loop(state_clone, writer_clone).await {
                println!("[CLIENT-PING] Error: {}", e);
            }
        })
    };

    // Wait for Ctrl+C
    println!("[CLIENT] Client is running. Press Ctrl+C to exit.");
    tokio::signal::ctrl_c().await?;

    println!("\n[CLIENT] Shutting down...");

    // Abort tasks
    listener_task.abort();
    ping_task.abort();

    Ok(())
}

/// Listen mode - only listen for incoming messages
async fn run_listen_mode(
    username: String,
    server_addr: SocketAddr,
) -> Result<()> {
    println!("=== CLIENT LISTEN MODE (TCP) ===");
    println!("Username: {}", username);
    println!("Server: {}", server_addr);
    println!();

    // Initialize state
    let state = ClientState::new(username.clone(), server_addr);
    let state: SharedClientState = Arc::new(RwLock::new(state));

    // Connect to server via TCP
    let (reader, writer) = simple_client::connect_to_server(server_addr).await?;

    println!("[CLIENT] Listening for messages from server...");
    println!();

    // Run listener
    simple_client::run_listener(state, reader, writer).await?;

    Ok(())
}

/// Interactive mode - JOIN + PING + HTTP API Server for Web UI
async fn run_interactive_mode(
    username: String,
    server_addr: SocketAddr,
    images: Vec<String>,
) -> Result<()> {
    println!("=== CLIENT INTERACTIVE MODE (TCP + HTTP API) ===");
    println!("Username: {}", username);
    println!("Server: {}", server_addr);
    println!("Client port: {}", CLIENT_PORT);
    println!("Images: {:?}", images);
    println!();

    // Initialize state
    let mut state = ClientState::new(username.clone(), server_addr);
    state.images = images;
    state.client_port = CLIENT_PORT;
    let state: SharedClientState = Arc::new(RwLock::new(state));

    // Connect to server via TCP
    let (mut reader, writer) = simple_client::connect_to_server(server_addr).await?;
    println!();

    // Send JOIN
    if let Err(e) = simple_client::join_server(state.clone(), writer.clone(), &mut reader).await {
        println!("[CLIENT] ⚠️  JOIN failed: {}", e);
        return Ok(());
    }

    println!("[CLIENT] ✅ Successfully joined server!");
    println!();

    // Start listener task
    let listener_task = {
        let state_clone = state.clone();
        let reader_clone = reader;
        let writer_clone = writer.clone();
        tokio::spawn(async move {
            if let Err(e) = simple_client::run_listener(state_clone, reader_clone, writer_clone).await {
                println!("[CLIENT-LISTENER] Error: {}", e);
            }
        })
    };

    // Start ping loop
    let ping_task = {
        let state_clone = state.clone();
        let writer_clone = writer.clone();
        tokio::spawn(async move {
            if let Err(e) = simple_client::ping_loop(state_clone, writer_clone).await {
                println!("[CLIENT-PING] Error: {}", e);
            }
        })
    };

    // Start HTTP API server
    let api_task = {
        let state_clone = state.clone();
        let writer_clone = writer.clone();
        tokio::spawn(async move {
            if let Err(e) = api_server::run_api_server(state_clone, writer_clone, 3001).await {
                eprintln!("[API-SERVER] Error: {}", e);
            }
        })
    };

    println!("✅ Client is running!");
    println!("📱 HTTP API: http://localhost:3001");
    println!("🌐 Web UI: http://localhost:3000 (run 'npm run dev' in Client-Node/web-ui/)");
    println!();
    println!("Press Ctrl+C to exit.");

    // Wait for Ctrl+C
    tokio::signal::ctrl_c().await?;

    println!("\n[CLIENT] Shutting down...");

    // Abort tasks
    listener_task.abort();
    ping_task.abort();
    api_task.abort();

    Ok(())
}

fn print_usage() {
    println!("Simple Client - Test the new protocol (TCP)");
    println!();
    println!("USAGE:");
    println!("  cargo run -- join <username> <server_ip:port> [image1] [image2] ...");
    println!("  cargo run -- listen <username> <server_ip:port>");
    println!("  cargo run -- interactive <username> <server_ip:port> [image1] [image2] ...");
    println!("  cargo run -- upload <username> <server_ip:port> <image_name> <true_path> <cover_path> <meta_json_path>");
    println!("  cargo run -- approve <username> <server_ip:port> <image_name> <req_id>");
    println!("  cargo run -- upload-and-fetch <username> <server_ip:port> <image_name> <true_path> <cover_path> <meta_json_path> [req_id]");
    println!();
    println!("EXAMPLES:");
    println!("  # Interactive mode with Web UI (RECOMMENDED)");
    println!("  cargo run -- interactive alice 10.40.61.79:9080 sunset.jpg mountain.png");
    println!("  # Then open http://localhost:3000 in browser (requires 'npm run dev' in web-ui/)");
    println!();
    println!("  # Join server with 2 images (use TCP port 9000)");
    println!("  cargo run -- join alice 10.40.61.79:9000 sunset.jpg mountain.png");
    println!();
    println!("  # Join server with no images");
    println!("  cargo run -- join bob 10.40.61.79:9000");
    println!();
    println!("  # Listen mode (receive messages only)");
    println!("  cargo run -- listen charlie 10.40.61.79:9000");
    println!();
    println!("  # Upload owner image + cover + metadata");
    println!("  cargo run -- upload alice 10.40.61.79:9000 secret secret.png cover.png meta.json");
    println!("  # Upload and immediately fetch embedded image");
    println!("  cargo run -- upload-and-fetch alice 10.40.61.79:9000 secret secret.png cover.png meta.json 1");
    println!();
}
