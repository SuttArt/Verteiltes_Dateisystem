//! This binary crate implements a simplified version of the master server for
//! the Google File System (GFS).

use std::future::ready;
use std::net::Ipv6Addr;
use futures::StreamExt;
use tarpc::serde_transport::tcp::{listen};
use tarpc::server::{BaseChannel, Channel};
use tarpc::tokio_serde::formats::Json;
use gfs_lite::{ChunkMaster, Master};
use gfs_lite::master::GfsMaster;

/// The `main` function sets up and runs the TCP servers for both the `Master`
/// and `ChunkMaster` services. It listens on different ports for each service
/// and spawns tasks to handle incoming connections.
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
	// create the GFS master server
	let server = GfsMaster::default();

	// Clone it for each service, to fix problem with "move"
	let master_server = server.clone();
	let chunk_master_server = server.clone();

	// simultaneously listen to the chunk servers and the client applications

	// Listener for Master service (Client-side communication)
	let client_listener = listen((Ipv6Addr::LOCALHOST, 50000), Json::default).await?;
	println!("Master service listening on {}", client_listener.local_addr());

	// Listener for ChunkMaster service (Chunk-server-side communication)
	let chunk_listener = listen((Ipv6Addr::LOCALHOST, 50001), Json::default).await?;
	println!("ChunkMaster service listening on {}", chunk_listener.local_addr());

	// spawn the listener for client-side communication with the GFS master
	tokio::spawn(
		client_listener
			.filter_map(|r| ready(r.ok()))
			.map(move |transport| BaseChannel::with_defaults(transport)
				.execute(Master::serve(master_server.clone()))
				.for_each(|future| async move { tokio::spawn(future); }))
			.buffer_unordered(10)
			.for_each(|_| ready(()))
	);

	// spawn the listener for chunk-server-side communication with the GFS master.
	tokio::spawn(
		chunk_listener
			.filter_map(|r| ready(r.ok()))
			.map(move |transport| BaseChannel::with_defaults(transport)
				.execute(ChunkMaster::serve(chunk_master_server.clone()))
				.for_each(|future| async move { tokio::spawn(future); }))
			.buffer_unordered(10)
			.for_each(|_| ready(()))
	);

	Ok(())
}
