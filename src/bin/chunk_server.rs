//! This module implements the `ChunkServer` for a simplified version of the
//! Google File System (GFS). It manages the storage and retrieval of chunks
//! (data blocks) in the system.

use std::{
	env,
	net::{IpAddr, Ipv6Addr},
};
use futures::StreamExt;
use tarpc::context::Context;
use tarpc::serde_transport::tcp::{connect, listen};
use tarpc::server::{BaseChannel, Channel};
use tarpc::tokio_serde::formats::Json;
use gfs_lite::{Chunk, ChunkMasterClient};
use gfs_lite::chunk::ChunkServer;

/// The `main` function sets up the `ChunkServer`, connects it to the
/// `ChunkMaster`, and starts listening for chunk operations.
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
	// parse the command-line arguments for the master servers IP-address 
	let master_addr =
		env::args().nth(1)
			.and_then(|ip| ip.parse::<IpAddr>().ok())
			.unwrap_or(IpAddr::V6(Ipv6Addr::LOCALHOST));
	
	// connect to the master server
	let tcp_connect = connect((master_addr, 50000), Json::default).await?;

	// open a channel for client commands
	let master = ChunkMasterClient::new(Default::default(), tcp_connect).spawn();

	// initialize the chunk server
	let listener = listen((Ipv6Addr::LOCALHOST, 0), Json::default).await?;
	let addr = listener.local_addr(); // Get the dynamically assigned address
	println!("Chunk Server listening on {addr}");

	let id = master.register(Context::current(), addr).await?;
	println!("Chunk Server registered with Master Server (ID: {id})");

	let chunk_server = ChunkServer::new(master, id);

	// listen for RPC traffic from clients
	listener
		.filter_map(|r| async  { r.ok() })
		.map(|transport| BaseChannel::with_defaults(transport)
			.execute(chunk_server.clone().serve())
			.for_each(|future| async move { tokio::spawn(future); }))
		.buffer_unordered(10)
		.for_each(|_| async {})
		.await;
	Ok(())
}
