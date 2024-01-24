use log::{error, info};
use serde::{Deserialize, Serialize};
use std::env;
use std::time::Instant;
use tokio::io::{self, AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use uuid::Uuid;

//  the possible states a node can take on
enum State {
    Follower,
    Candidate,
    Leader,
}

struct LogEntry {
    key: String,
    value: i32,
}

struct Node {
    id: Uuid,
    state: State,
    current_term: u32,
    voted_for: Option<u32>,
    log: Vec<LogEntry>,
    last_heartbeat: Instant,
}

#[derive(Serialize, Deserialize)]
struct RequestVoteRequest {
    term: u32,
    candidate_id: u32,
    last_log_index: u32,
    last_log_term: u32,
}

#[derive(Serialize, Deserialize)]
struct RequestVoteResponse {
    term: u32,
    vote_granted: bool,
}

impl Node {
    /**
     * Constructor for a raft node. All nodes start as followers with no logs
     */
    fn new() -> Self {
        Self {
            id: Uuid::new_v4(),
            state: State::Follower,
            current_term: 0,
            voted_for: None,
            log: Vec::new(),
            last_heartbeat: Instant::now(),
        }
    }
    /**
     * This method is called when this node requests a vote from other nodes
     */
    async fn request_vote(&self) -> RequestVoteRequest {
        return RequestVoteRequest {
            term: self.current_term,
            candidate_id: 0,
            last_log_index: 0,
            last_log_term: 0,
        };
    }
    /**
     * This method is called when a vote is requested of this node
     * It should return the current term and whether or not the vote was granted
     */
    async fn handle_request_vote(request: RequestVoteRequest) -> RequestVoteResponse {
        return RequestVoteResponse {
            term: 0,
            vote_granted: false,
        };
    }

    /**
     * This method is called when this node is requested to append entries to its own log from another node.
     */
    async fn append_entries(
        term: u32,
        leader_id: u32,
        prev_log_index: u32,
        prev_log_term: u32,
        entries: Vec<LogEntry>,
        leader_commit: u32,
    ) -> (u32, bool) {
        return (0, false);
    }

    fn heartbeat_received(&mut self) {
        self.last_heartbeat = Instant::now();
    }
}

async fn listen_for_messages(local_port: &str) -> io::Result<()> {
    let listener = TcpListener::bind(local_port).await.unwrap();
    info!("Server running on port {}", local_port);
    loop {
        let (mut socket, _) = listener.accept().await?;

        tokio::spawn(async move {
            let mut buf = String::new();
            match socket.read_to_string(&mut buf).await {
                Ok(_) => match serde_json::from_str::<RequestVoteRequest>(&buf) {
                    Ok(request_vote) => {
                        //  received a vote request from some other node
                        let RequestVoteResponse { term, vote_granted } =
                            Node::handle_request_vote(request_vote).await;

                        let serialized_response =
                            serde_json::to_string(&RequestVoteResponse { term, vote_granted })
                                .unwrap();

                        match socket.write_all(serialized_response.as_bytes()).await {
                            Ok(_) => {}
                            Err(_) => {}
                        }
                    }
                    Err(_) => {}
                },
                Err(_) => {}
            }
        });
    }
}

#[tokio::main]
async fn main() {
    //  initialize the logger
    env::set_var("RUST_LOG", "info");
    env_logger::init();

    //  read the argument
    let args: Vec<String> = env::args().collect();
    if args.len() != 2 {
        eprintln!("Usage: {} <port>", args[0]);
        return;
    }
    let port = &args[1];
    let address = format!("127.0.0.1:{}", port);

    //  Create a TCP listener
    let listener = TcpListener::bind(address).await.unwrap();
    info!("Server running on port {}", port);

    //  listen for messages
    loop {
        match listener.accept().await {
            Ok((mut socket, addr)) => {
                info!("New connection from: {}", addr);

                tokio::spawn(async move {
                    let mut buf = [0; 1024];

                    loop {
                        //  read the data
                        let n = match socket.read(&mut buf).await {
                            Ok(n) if n == 0 => return,
                            Ok(n) => n,
                            Err(err) => {
                                error!("Failed to read from socket; err={:?}", err);
                                return;
                            }
                        };
                        //  write the data
                        if let Err(e) = socket.write_all(&buf[0..n]).await {
                            error!("Failed to write back to socket; err={:?}", e);
                            return;
                        }
                    }
                });
            }
            Err(err) => error!("Failed to accept connection: {}", err),
        }
    }
}
