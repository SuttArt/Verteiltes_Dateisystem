//! This module implements the `ChunkServer` for a simplified version of the
//! Google File System (GFS). It manages the storage and retrieval of chunks
//! (data blocks) in the system.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use tarpc::context::Context;

use crate::{Chunk, ChunkMasterClient};

/// `ChunkServer` is responsible for handling chunk operations and interacting
/// with the `ChunkMaster`.
#[derive(Clone)]
pub struct ChunkServer(Arc<Inner>);

/// `Inner` holds the state of the `ChunkServer`, including a reference to the
/// `ChunkMasterClient`, a hashmap for chunk storage, and the server's own ID.
struct Inner {
	// reference to the `ChunkMasterClient`
	master_ref: ChunkMasterClient,
	// hashmap for chunk storage
	storage: RwLock<HashMap<String, String>>,
	// server's own ID
	id: u64,
}

impl ChunkServer {
	/// Creates a new ChunkServer, given the remote object for the master server
	/// and its ID.
	pub fn new(master: ChunkMasterClient, my_id: u64) -> Self {
		Self(Arc::new(Inner {
			master_ref: master,
			storage: RwLock::new(HashMap::new()),
			id: my_id,
		}))
	}
}

impl Chunk for ChunkServer {
	async fn get(self, _: Context, url: String) -> Option<String> {
		// take read lock
		let inner = self.0.storage.read().unwrap();
		// find data. None if not exist
		if let Some(data) = inner.get(&url).cloned() {
			Some(data)
		} else {
			None
		}
	}
	async fn set(self, ctx: Context, url: String, chunk: Option<String>) -> Option<String> {
		// take write lock
		let mut inner = self.0.storage.write().unwrap();
		match chunk {
			// Write data
			Some(chunk) => {
				let old_data  = inner.insert(url, chunk);

				// Notify the Master Server of the new/rewritten chunk
				self.0.master_ref.insert(ctx, self.0.id, url).await.unwrap();

				old_data
			},
			// Delete data
			None => {
				let old_data = inner.remove(&url);

				// Notify the Master Server of the removal
				self.0.master_ref.remove(ctx, url).await.unwrap();

				old_data
			}
		}
	}
}
