use log::{error, info};
use rand::Rng;
use serde::{Deserialize, Serialize};
use serde_json::error::Error as SerdeError;
use std::env;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{self, AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::tcp::{ReadHalf, WriteHalf};
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use uuid::Uuid;

//  the possible states a node can take on
#[derive(PartialEq)]
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
    voted_for: Option<Uuid>,
    log: Vec<LogEntry>,
    last_heartbeat: Instant,
    address: String,
    peers: Vec<String>,
    election_timeout: Duration,
}

#[derive(Serialize, Deserialize)]
struct RequestVoteRequest {
    term: u32,
    candidate_id: Uuid,
    last_log_index: u32,
    last_log_term: u32,
}

#[derive(Serialize, Deserialize, Debug)]
struct RequestVoteResponse {
    term: u32,
    vote_granted: bool,
}

impl Node {
    /**
     * Constructor for a raft node. All nodes start as followers with no logs
     */
    fn new(address: String, peers: Vec<String>) -> Self {
        let peers = peers.into_iter().filter(|peer| peer != &address).collect();
        //  add jitter to the election timeout
        let election_timeout = Duration::from_millis(1000 + rand::thread_rng().gen_range(0..1000));
        Self {
            id: Uuid::new_v4(),
            state: State::Follower,
            current_term: 0,
            voted_for: None,
            log: Vec::new(),
            last_heartbeat: Instant::now(),
            address,
            peers,
            election_timeout,
        }
    }
    /**
     * This method is called when this node requests a vote from other nodes
     */
    fn request_vote(&self) -> RequestVoteRequest {
        return RequestVoteRequest {
            term: self.current_term,
            candidate_id: self.id,
            last_log_index: 0,
            last_log_term: 0,
        };
    }
    /**
     * This method is called when a vote is requested of this node
     * It should return the current term and whether or not the vote was granted
     */
    async fn handle_request_vote(&mut self, request: RequestVoteRequest) -> RequestVoteResponse {
        //  when sending a vote to some other node, set the voted for field
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

    fn election_timeout_elapsed(&self, election_timeout: Duration) -> bool {
        self.last_heartbeat.elapsed() > election_timeout
    }

    async fn start_election(&mut self) {
        //  Increment the current term, become candidate and vote for self
        self.state = State::Candidate;
        self.current_term += 1;
        self.voted_for = Some(self.id);

        //  Create the request struct
        let vote_request = self.request_vote();
        let serialized_request = serde_json::to_string(&vote_request).unwrap() + "\n";

        //  Send a request to obtain votes from other candidates
        for peer in &self.peers {
            //  make the request
            match TcpStream::connect(peer).await {
                Ok(mut stream) => {
                    stream
                        .write_all(serialized_request.as_bytes())
                        .await
                        .unwrap();

                    let mut response = String::new();
                    stream.read_to_string(&mut response).await.unwrap();
                    let vote_response: RequestVoteResponse =
                        serde_json::from_str(&response).unwrap();
                    info!("The response is: {:?}", vote_response);
                }
                Err(e) => {
                    self.state = State::Follower;
                    self.voted_for = None;
                    return;
                }
            }
        }
        info!("Resetting the variables");
        self.last_heartbeat = Instant::now();
        self.voted_for = None;
        self.current_term -= 1;
        self.state = State::Follower;
    }
}

async fn process_connection(
    mut reader: BufReader<TcpStream>,
    node: Arc<Mutex<Node>>,
) -> io::Result<()> {
    let mut line = String::new();
    while reader.read_line(&mut line).await? != 0 {
        //  keep reading until EOF
        let request_vote = match serde_json::from_str(&line.trim_end()) {
            Ok(request) => request,
            Err(e) => {
                error!("Failed to process the request {}", e);
                continue;
            }
        };
        let mut node_guard = node.lock().await;
        let response = node_guard.handle_request_vote(request_vote).await;
        let serialized_response = serde_json::to_string(&response).unwrap();
        if let Err(e) = reader
            .get_mut()
            .write_all(serialized_response.as_bytes())
            .await
        {
            error!("Failed to write response back: {}", e);
        }
        line.clear();
    }
    Ok(())
}

async fn listen_for_messages(local_port: &str, node: Arc<Mutex<Node>>) -> io::Result<()> {
    let address = format!("127.0.0.1:{}", local_port);
    info!("Server running on port {}", local_port);
    let listener = TcpListener::bind(address).await?;

    //  keep listening for connections
    loop {
        let (socket, _) = listener.accept().await?;
        let node_clone = Arc::clone(&node);
        let reader = BufReader::new(socket);

        //  spawn a separate thread to process the request
        tokio::spawn(async move {
            if let Err(e) = process_connection(reader, node_clone).await {
                error!("Error processing connection {}", e);
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

    //  initialize variables
    let local_address = format!("127.0.0.1:{}", &args[1]);
    let all_addresses = vec!["127.0.0.1:8080".to_string(), "127.0.0.1:8081".to_string()];
    let node = Arc::new(Mutex::new(Node::new(local_address, all_addresses)));
    let node_for_listener = Arc::clone(&node);

    //  listen for messages on a separate thread
    tokio::spawn(async move {
        if let Err(e) = listen_for_messages(&args[1], node_for_listener).await {
            error!("Failed to listen to messages; err={:?}", e);
        }
    });

    //  main loop
    loop {
        let mut node_guard = node.lock().await;
        //  start an election only if the following conditions are met:
        // 1. The node is a follower
        // 2. The election timeout has elapsed
        // 3. The node has not yet voted for anyone in this term
        if node_guard.state == State::Follower
            && node_guard.election_timeout_elapsed(node_guard.election_timeout)
            && node_guard.voted_for == None
        {
            //  start an election
            node_guard.start_election().await;
        }
        drop(node_guard);
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
