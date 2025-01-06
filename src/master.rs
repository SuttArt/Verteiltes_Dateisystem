//! This module implements a simplified version of the Google File System (GFS)
//! master server.
//! 
//! The master server primarily focuses on managing chunk servers and handling
//! their metadata.

use std::{
	net::SocketAddr,
	sync::{Arc, RwLock},
	collections::HashMap
};

// used for randomise chunk-server
use rand::seq::SliceRandom;

use tarpc::context::Context;

use crate::{ChunkMaster, Master};

/// `GfsMaster` is a wrapper around `Inner` that provides thread-safe access.
#[derive(Clone, Default)]
pub struct GfsMaster(Arc<RwLock<Inner>>);

/// `Inner` holds the state of the `GfsMaster`.
/// It contains a list of chunk servers and a registry mapping URLs to their
/// respective chunk server addresses.
#[derive(Default)]
struct Inner {
	// list of chunk servers
	chunk_servers: Vec<SocketAddr>,
	// registry mapping URLs to their respective chunk server addresses
	registry: HashMap<String, SocketAddr>,
}

// Implementation of the `Master` trait for `GfsMaster`.
impl Master for GfsMaster {
	async fn lookup(self, _: Context, url: String) -> SocketAddr {
		// take read lock
		let inner = self.0.read().unwrap();
		// find url
		if let Some(&address) = inner.registry.get(&url) {
			address
		} else {
			// Choose random chunk server if nothing found
			if let Some(&random_server) = inner.chunk_servers.choose(&mut rand::thread_rng()) {
				random_server
			} else {
				panic!("No chunk servers available for lookup!");
			}
		}
	}
}

// Implementation of the `ChunkMaster` trait for `GfsMaster`.
impl ChunkMaster for GfsMaster {
	async fn register(self, _: Context, socket_addr: SocketAddr) -> u64 {
		// take write lock
		let mut inner = self.0.write().unwrap();
		inner.chunk_servers.push(socket_addr);
		// Index should be the last one
		inner.chunk_servers.len() as u64 - 1
	}
	
	async fn insert(self, _: Context, sender: u64, url: String) {
		// take write lock
		let mut inner = self.0.write().unwrap();
		// find address in List
		if let Some(&address) = inner.chunk_servers.get(sender as usize) {
			inner.registry.insert(url, address);
		}
	}
	
	async fn remove(self, _: Context, url: String) {
		// take write lock
		let mut inner = self.0.write().unwrap();
		inner.registry.remove(&url);
	}
}
