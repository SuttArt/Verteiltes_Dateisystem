use std::{
	env,
	net::{IpAddr, Ipv6Addr}
};
use tarpc::{
    client, context,
    tokio_serde::formats::Json,
    serde_transport::tcp::connect,
};
use tokio::io::{AsyncBufReadExt, BufReader};
use gfs_lite::{MasterClient, ChunkClient};

/// A simple test-client that should be extended to a command-line client
/// that can interface with the file system.
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
	// parse the command-line arguments for the master servers IP-address 
	let master_addr =
		env::args().nth(1)
			.and_then(|ip| ip.parse::<IpAddr>().ok())
			.unwrap_or(IpAddr::V6(Ipv6Addr::LOCALHOST));
	
	// establish a connection with the master server
    let transport = connect((master_addr, 50000), Json::default).await?;
    let master = MasterClient::new(client::Config::default(), transport).spawn();

    // Start an interactive command-line interface
    println!("Connected to Master Server at {}:50000", master_addr);
    println!("Commands: ");
    println!("    lookup <file>");
    println!("    set <file> <data>");
    println!("    get <file>");
    println!("    exit");

    // Copie from https://users.rust-lang.org/t/how-to-read-stdin-in-tokio/33295
    // Get tokio's version of stdin, which implements AsyncRead
    let stdin = tokio::io::stdin();
    // Create a buffered wrapper, which implements BufRead
    let mut reader = BufReader::new(stdin).lines();

    while let Some(line) = reader.next_line().await? {
        let mut parts = line.trim().split_whitespace();
        let command = parts.next(); // Retrieves the first word of the input as the command.

        match command {
            Some("lookup") => {
                if let Some(file) = parts.next() {
                    let chunk_addr = master.lookup(context::current(), file.to_string()).await?;
                    println!("Chunk Server Address: {chunk_addr}");
                } else {
                    // Red text: "\x1b[31m{}\x1b[0m"
                    println!("\x1b[31m{}\x1b[0m", "Usage: lookup <file>");
                }
            }
            Some("set") => {
                if let (Some(file), Some(data)) = (parts.next(), parts.next()) {
                    let chunk_addr = master.lookup(context::current(), file.to_string()).await?;
                    println!("Chunk Server Address: {chunk_addr}");

                    // Connect to the chunk server
                    let transport = connect(chunk_addr, Json::default).await?;
                    let chunk = ChunkClient::new(client::Config::default(), transport).spawn();
                    let result = chunk
                        .set(context::current(), file.to_string(), Some(data.to_string()))
                        .await?;
                    println!("Previous version of the value: {result:?}");
                } else {
                    // Red text: "\x1b[31m{}\x1b[0m"
                    println!("\x1b[31m{}\x1b[0m", "Usage: set <file> <data>");
                }
            }
            Some("get") => {
                if let Some(file) = parts.next() {
                    let chunk_addr = master.lookup(context::current(), file.to_string()).await?;
                    println!("Chunk Server Address: {chunk_addr}");

                    // Connect to the chunk server
                    let transport = connect(chunk_addr, Json::default).await?;
                    let chunk = ChunkClient::new(client::Config::default(), transport).spawn();
                    let result = chunk.get(context::current(), file.to_string()).await?;
                    println!("Get Result: {result:?}");
                } else {
                    // Red text: "\x1b[31m{}\x1b[0m"
                    println!("\x1b[31m{}\x1b[0m", "Usage: get <file>");
                }
            }
            Some("exit") => {
                println!("Exiting...");
                break;
            }
            Some(cmd) => {
                // Red text: "\x1b[31m{}\x1b[0m"
                println!("\x1b[31m{}{cmd}\x1b[0m", "Unknown command: ");
            }
            None => {
                // Red text: "\x1b[31m{}\x1b[0m"
                println!("\x1b[31m{}\x1b[0m", "Enter a command or type 'exit' to quit.");
            }
        }
    }

    Ok(())
}

// let transport = connect((master_addr, 50000), Json::default).await?;
// let master = MasterClient::new(client::Config::default(), transport).spawn();
//
// // attempt to look up the chunk server responsible for some file
// let chunk_addr = master.lookup(context::current(), "Foo".to_string()).await?;
// println!("Chunk-Server Addr: {chunk_addr:?}");
//
// // connect to the corresponding chunk server
// let transport = connect(chunk_addr, Json::default).await?;
// let chunk = ChunkClient::new(client::Config::default(), transport).spawn();
//
// // attempt to write data to that file
// let store_result = chunk.set(context::current(), "Foo".to_string(), Some("Bar".to_string())).await?;
// println!("Store Result: {store_result:?}");
//
// // read back the data
// let retrieve_result = chunk.get(context::current(), "Foo".to_string()).await?;
// println!("Retrieve Result: {retrieve_result:?}");