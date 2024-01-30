use serde::de::DeserializeOwned;
use serde_json;
use std::io::{self, BufRead};
use std::marker::PhantomData;
use std::net::{TcpListener, TcpStream};
use std::sync::Mutex;
use std::time::Duration;
use std::{collections::HashMap, io::BufReader};

use log::{error, info};
use rand::Rng;
use serde::{Deserialize, Serialize};

type Term = u64;

type ServerId = u64;

/**
 * RPC Stuff
 */

struct RPCManager<T: DeserializeOwned> {
    server_id: ServerId,
    server_address: String,
    port: u64,
    _marker: PhantomData<T>,
}

impl<T: DeserializeOwned> RPCManager<T> {
    fn process_message(mut reader: BufReader<TcpStream>) -> Result<(), io::Error> {
        for line in reader.lines() {
            match line {
                Ok(json) => {
                    let rpc: RPC<T> = serde_json::from_str(&json)
                        .expect("There was an error deserializing the response");
                }
                Err(e) => {
                    error!("There was an error while reading the message {}", e);
                }
            }
        }
        Ok(())
    }

    fn start(&self) {
        let listener = match TcpListener::bind(&self.server_address) {
            Ok(tcp_listener) => tcp_listener,
            Err(e) => {
                error!(
                    "There was an error while binding to the address {} for server id {} {}",
                    self.server_address, self.server_id, e
                );
                panic!("There is a problem");
            }
        };

        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    let reader = BufReader::new(stream);
                }
                Err(e) => {
                    error!("There was an error while accepting a connection {}", e);
                }
            }
        }
    }
}

#[derive(Serialize, Deserialize)]
enum RPC<T> {
    VoteRequest(VoteRequest),
    VoteResponse(VoteResponse),
    AppendEntriesRequest(AppendEntriesRequest<T>),
    AppendEntriesResponse(AppendEntriesResponse),
}
#[derive(Serialize, Deserialize)]
struct VoteRequest {
    term: Term,
    candidate_id: ServerId,
    last_log_index: u64,
    last_log_term: Term,
}

#[derive(Serialize, Deserialize)]
struct VoteResponse {
    term: Term,
    vote_granted: bool,
}

#[derive(Serialize, Deserialize)]
struct AppendEntriesRequest<T> {
    term: Term,
    leader_id: ServerId,
    prev_log_index: u64,
    prev_log_term: Term,
    entries: Vec<LogEntry<T>>,
    leader_commit_index: u64,
}

#[derive(Serialize, Deserialize)]
struct AppendEntriesResponse {
    term: Term,
    success: bool,
}

/**
 * End RPC stuff
 */

struct ServerConfig {
    election_timeout: Duration, //  used to calculate the actual election timeout
    heartbeat_interval: Duration,
}

trait StateMachine<T> {
    fn apply_set(&mut self, key: String, value: T);
    fn apply_get(&mut self, key: String) -> Option<&T>;
    fn apply(&mut self, entries: Vec<LogEntry<T>>);
}

#[derive(Serialize, Deserialize)]
enum LogEntryCommand {
    Set = 0,
    Delete = 1,
}

#[derive(Serialize, Deserialize)]
struct LogEntry<T> {
    term: Term,
    index: u64,
    command: LogEntryCommand,
    key: String,
    value: T,
}

enum RaftNodeStatus {
    Leader,
    Candidate,
    Follower,
}

struct RaftNodeState<T> {
    current_term: Term,
    voted_for: Option<ServerId>,
    log: Vec<LogEntry<T>>,
    status: RaftNodeStatus,
    election_timeout: Duration,
}

struct RaftNode<T, S: StateMachine<T>> {
    id: ServerId,
    state: Mutex<RaftNodeState<T>>,
    state_machine: S,
    config: ServerConfig,
    peers: Vec<ServerId>,
}

impl<T, S: StateMachine<T>> RaftNode<T, S> {
    fn new(id: ServerId, state_machine: S, config: ServerConfig, peers: Vec<ServerId>) -> Self {
        let election_jitter = rand::thread_rng().gen_range(0..100);
        let election_timeout = config.election_timeout + Duration::from_millis(election_jitter);
        let server_state = RaftNodeState {
            current_term: 0,
            voted_for: None,
            log: Vec::new(),
            status: RaftNodeStatus::Follower,
            election_timeout,
        };
        let server = RaftNode {
            id,
            state: Mutex::new(server_state),
            state_machine,
            config,
            peers,
        };
        info!("Initialising a new raft server with id {}", id);
        server
    }
}

fn main() {}

//  An example state machine - a key value store
struct KeyValueStore<T> {
    store: HashMap<String, T>,
}

impl<T> StateMachine<T> for KeyValueStore<T> {
    fn apply_set(&mut self, key: String, value: T) {
        self.store.insert(key, value);
    }

    fn apply_get(&mut self, key: String) -> Option<&T> {
        return self.store.get(&key);
    }

    fn apply(&mut self, entries: Vec<LogEntry<T>>) {
        for entry in entries {
            self.store.insert(entry.key, entry.value);
        }
    }
}
