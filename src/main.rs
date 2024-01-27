use log::{error, info};
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::env;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{self, AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::stream;
use tokio::sync::Mutex;
use uuid::Uuid;

trait Time {
    fn now(&self) -> Instant;
}

//  the possible states a node can take on
#[derive(PartialEq)]
enum State {
    Follower,
    Candidate,
    Leader,
}

#[derive(Serialize, Deserialize, Debug)]
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
    address: String,
    peers: Vec<String>,
    election_timeout: Duration,
    heartbeat_interval: Duration,
    last_heartbeat: Instant,
    port: u32,
}

#[derive(Serialize, Deserialize, Debug)]
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

#[derive(Serialize, Deserialize, Debug)]
struct AppendEntryRequest {
    term: u32,
    leader_id: Uuid,
    prev_log_term: u32,
    entries: Vec<LogEntry>,
    leader_commit: u32,
}

#[derive(Serialize, Deserialize, Debug)]
struct AppendEntryResponse {
    term: u32,     //  the current term of the follower who responded
    success: bool, //  whether the request was successful on the follower
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type")]
enum Message {
    RequestVote(RequestVoteRequest),
    AppendEntry(AppendEntryRequest),
}

impl Node {
    /**
     * Constructor for a raft node. All nodes start as followers with no logs
     */
    fn new(port: u32, address: String, peers: Vec<String>) -> Self {
        let peers = peers.into_iter().filter(|peer| peer != &address).collect();
        //  add jitter to the election timeout
        let election_timeout = Duration::from_millis(1000 + rand::thread_rng().gen_range(0..1000));
        let heartbeat_interval = Duration::from_millis(100);
        Self {
            id: Uuid::new_v4(),
            state: State::Follower,
            current_term: 0,
            voted_for: None,
            log: Vec::new(),
            address,
            peers,
            election_timeout,
            heartbeat_interval,
            last_heartbeat: Instant::now(),
            port,
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
        if request.term < self.current_term || self.voted_for != None {
            //  cannot grant node a vote
            return RequestVoteResponse {
                term: self.current_term,
                vote_granted: false,
            };
        }

        //  casting a vote
        self.voted_for = Some(request.candidate_id);
        return RequestVoteResponse {
            term: self.current_term,
            vote_granted: true,
        };
    }

    /**
     * This method is called when this node tries to append entries to its followers
     * It also doubles as a heartbeat
     */
    fn append_entries(&self) -> AppendEntryRequest {
        return AppendEntryRequest {
            term: self.current_term,
            leader_id: self.id,
            prev_log_term: self.current_term,
            entries: vec![],
            leader_commit: 0,
        };
    }

    /**
     * Thi method is called when this node receives a request from a leader (presumably) to append an entry to itself
     * It also doubles as a heartbeat
     */
    async fn handle_append_entries(
        &mut self,
        term: u32,
        leader_id: Uuid,
        prev_log_index: u32,
        prev_log_term: u32,
        entries: Vec<LogEntry>,
        leader_commit: u32,
    ) -> (u32, bool) {
        if term < self.current_term {
            return (term, false);
        }

        self.heartbeat_received();
        return (self.current_term, true);
    }

    fn heartbeat_received(&mut self) {
        self.last_heartbeat = Instant::now();
    }

    fn election_timeout_elapsed(&self, election_timeout: Duration) -> bool {
        self.last_heartbeat.elapsed() > election_timeout
    }
}

async fn communicate_with_peer(peer: &str, serialized_request: &str) -> Result<String, io::Error> {
    let mut stream = TcpStream::connect(peer).await?;
    stream.write_all(serialized_request.as_bytes()).await?;
    let mut reader = BufReader::new(stream);
    let mut response = String::new();
    reader.read_line(&mut response).await?;
    Ok(response)
}

async fn run_heartbeat_mechanism(node: Arc<Mutex<Node>>) {
    let node_guard = node.lock().await;
    let mut interval = tokio::time::interval(node_guard.heartbeat_interval);
    drop(node_guard);

    loop {
        interval.tick().await;
        send_heartbeat(node.clone()).await;
    }
}

async fn send_heartbeat(node: Arc<Mutex<Node>>) {
    let mut node_guard = node.lock().await;
    if node_guard.state != State::Leader {
        info!("Cannot send heartbeat, returning");
        return;
    }

    info!("Sending heartbeat");
    let append_entry_req = node_guard.append_entries();
    let message = Message::AppendEntry(append_entry_req);
    let serialized_request = serde_json::to_string(&message).unwrap() + "\n";
    for peer in &node_guard.peers {
        let response = match communicate_with_peer(peer, &serialized_request).await {
            Ok(response) => response,
            Err(e) => {
                error!("Failed to communicate with peer: {}", e);
                continue;
            }
        };

        let append_entries_response: AppendEntryResponse = match serde_json::from_str(&response) {
            Ok(response) => response,
            Err(e) => {
                error!("Failed to deserialize the response: {}", e);
                continue;
            }
        };

        if !append_entries_response.success
            && append_entries_response.term > node_guard.current_term
        {
            info!("There is a new leader, stepping down");
            node_guard.current_term = append_entries_response.term;
            node_guard.state = State::Follower;
            return;
        }
    }
}

async fn start_election(node: Arc<Mutex<Node>>) {
    let mut node_guard = node.lock().await;
    info!("Rechecking the conditions before starting the election");
    if node_guard.state != State::Follower || node_guard.voted_for != None {
        info!("Cannot run election for this node, returning");
        return;
    }
    info!("Starting election");
    node_guard.state = State::Candidate;
    node_guard.current_term += 1;
    node_guard.voted_for = Some(node_guard.id);

    let vote_request = node_guard.request_vote();
    let message = Message::RequestVote(vote_request);
    let serialized_request = serde_json::to_string(&message).unwrap() + "\n";

    let mut votes = 1;

    for peer in &node_guard.peers {
        let response = match communicate_with_peer(peer, &serialized_request).await {
            Ok(response) => response,
            Err(e) => {
                error!("Failed to communicate with peer: {}", e);
                continue;
            }
        };

        let vote_response: RequestVoteResponse = match serde_json::from_str(&response) {
            Ok(response) => response,
            Err(e) => {
                error!("Failed to deserialize the response: {}", e);
                continue;
            }
        };

        info!("The response is: {:?}", vote_response);
        if vote_response.term > node_guard.current_term {
            info!("This node is not in the current term, update it and give up on leader election");
            node_guard.current_term = vote_response.term;
            break;
        }

        if vote_response.vote_granted {
            votes += 1;
        }
    }

    if votes <= node_guard.peers.len() / 2 {
        node_guard.heartbeat_received();
        node_guard.voted_for = None;
        node_guard.current_term -= 1;
        node_guard.state = State::Follower;
        return;
    }

    node_guard.state = State::Leader;
}

async fn process_message(
    mut reader: BufReader<TcpStream>,
    node: Arc<Mutex<Node>>,
) -> Result<(), io::Error> {
    let mut line = String::new();
    let mut node_guard = node.lock().await;
    while reader.read_line(&mut line).await? != 0 {
        match serde_json::from_str(&line.trim_end()) {
            Ok(Message::RequestVote(request)) => {
                info!("Received vote request");
                let response = node_guard.handle_request_vote(request).await;
                let serialized_response = serde_json::to_string(&response).unwrap() + "\n";
                if let Err(e) = reader
                    .get_mut()
                    .write_all(serialized_response.as_bytes())
                    .await
                {
                    error!("Failed to write the response back, {}", e);
                }
            }
            Ok(Message::AppendEntry(request)) => {
                info!("Received append entry");
                let response = node_guard
                    .handle_append_entries(
                        request.term,
                        request.leader_id,
                        0,
                        0,
                        request.entries,
                        request.leader_commit,
                    )
                    .await;
                let serialized_response = serde_json::to_string(&response).unwrap() + "\n";
                if let Err(e) = reader
                    .get_mut()
                    .write_all(serialized_response.as_bytes())
                    .await
                {
                    error!("Failed to write the response back, {}", e);
                }
            }

            Err(e) => {
                error!("Failed to process the request {}", e);
                continue;
            }
        }
    }
    Ok(())
}

async fn listen_for_messages(node: Arc<Mutex<Node>>) -> io::Result<()> {
    let port = node.lock().await.port;
    let address = format!("127.0.0.1:{}", port);
    info!("Server running on port {}", port);
    let listener = TcpListener::bind(address).await?;

    //  keep listening for connections
    loop {
        let (socket, _) = listener.accept().await?;
        let node_clone = Arc::clone(&node);
        let reader = BufReader::new(socket);

        //  spawn a separate thread to process the request
        tokio::spawn(async move {
            if let Err(e) = process_message(reader, node_clone).await {
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
    let port: u32 = args[1].parse().expect("Port should be a number");

    //  initialize variables
    let local_address = format!("127.0.0.1:{}", &args[1]);
    let all_addresses = vec![
        "127.0.0.1:8080".to_string(),
        "127.0.0.1:8081".to_string(),
        "127.0.0.1:8082".to_string(),
    ];
    let node = Arc::new(Mutex::new(Node::new(port, local_address, all_addresses)));
    let node_for_listener = Arc::clone(&node);

    //  listen for messages on a separate thread
    tokio::spawn(async move {
        if let Err(e) = listen_for_messages(node_for_listener).await {
            error!("Failed to listen to messages; err={:?}", e);
        }
    });

    //  main loop
    loop {
        let node_clone = node.clone();
        let node_clone_for_heartbeat = node.clone();
        let node_guard = node.lock().await;
        //  start an election only if the following conditions are met:
        // 1. The node is a follower
        // 2. The election timeout has elapsed
        // 3. The node has not yet voted for anyone in this term
        if node_guard.state == State::Follower
            && node_guard.election_timeout_elapsed(node_guard.election_timeout)
            && node_guard.voted_for == None
        {
            drop(node_guard);
            start_election(node_clone).await;
            let node_guard = node.lock().await;
            if node_guard.state == State::Leader {
                drop(node_guard);
                tokio::spawn(async move {
                    run_heartbeat_mechanism(node_clone_for_heartbeat).await;
                });
            }
        } else {
            drop(node_guard);
        }

        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn initialize_cluster(ports: Vec<u32>) -> Vec<Arc<Mutex<Node>>> {
    let mut nodes = Vec::new();

    let all_addresses = ports
        .iter()
        .map(|port| format!("127.0.0.1:{}", port))
        .collect::<Vec<_>>();
    for port in &ports {
        let local_address = format!("127.0.0.1:{}", port);
        let node = Arc::new(Mutex::new(Node::new(
            *port,
            local_address,
            all_addresses.clone(),
        )));
        nodes.push(node);
    }
    nodes
}

#[tokio::test]
async fn test_leader_election() {
    let _ = env_logger::builder().is_test(true).try_init();
    let ports = vec![8080, 8081, 8082];
    let nodes = initialize_cluster(ports);

    //  start the listener process on each node
    for node in &nodes {
        let node_for_listener = Arc::clone(&node);
        tokio::spawn(async move {
            listen_for_messages(node_for_listener).await.unwrap();
        });
    }

    tokio::time::sleep(Duration::from_secs(1)).await;

    //  trigger an election on one of the nodes
    start_election(Arc::clone(&nodes[0])).await;

    let node_guard = nodes[0].lock().await;
    assert!(node_guard.state == State::Leader);
}
