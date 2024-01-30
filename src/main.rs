use serde::de::DeserializeOwned;
use serde_json;
use std::io::{self, BufRead, Write};
use std::marker::PhantomData;
use std::net::{TcpListener, TcpStream};
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::Duration;
use std::{collections::HashMap, io::BufReader};

use log::{error, info};
use rand::Rng;
use serde::{Deserialize, Serialize};

mod types;

use crate::types::{ServerId, Term};

// pub trait RaftDataType: DeserializeOwned + Send + 'static {}
// impl<T: DeserializeOwned + Send + 'static> RaftDataType for T {}

/**
 * RPC Stuff
 */

struct RPCManager<T: DeserializeOwned> {
    server_id: ServerId,
    server_address: String,
    port: u64,
    to_node_sender: mpsc::Sender<RPCMessage<T>>,
    _marker: PhantomData<T>,
}

impl<T: Serialize + DeserializeOwned> RPCManager<T> {
    fn new(
        server_id: ServerId,
        server_address: String,
        port: u64,
        to_node_sender: mpsc::Sender<RPCMessage<T>>,
    ) -> Self {
        RPCManager {
            server_id,
            server_address,
            port,
            to_node_sender,
            _marker: PhantomData,
        }
    }

    fn process_message(&self, reader: BufReader<TcpStream>) -> Result<(), io::Error> {
        for line in reader.lines() {
            let json = line?;
            let rpc: RPCMessage<T> = serde_json::from_str(&json)?;
            match self.to_node_sender.send(rpc) {
                Ok(_) => (),
                Err(e) => {
                    error!(
                        "There was an error while sending the rpc message to the node {}",
                        e
                    );
                    panic!(
                        "There was an error while sending the rpc message to the node {}",
                        e
                    );
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
                panic!(
                    "There was an error while binding to the address {} for server id {} {}",
                    self.server_address, self.server_id, e
                );
            }
        };

        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    let reader = BufReader::new(stream);
                    match self.process_message(reader) {
                        Ok(_) => (),
                        Err(e) => {
                            error!("There was an error while processing the message {}", e);
                        }
                    }
                }
                Err(e) => {
                    error!("There was an error while accepting a connection {}", e);
                }
            }
        }
    }

    fn send_message(
        from_server_id: ServerId,
        to_server_id: ServerId,
        to_address: String,
        message: RPCMessage<T>,
    ) {
        let mut stream = match TcpStream::connect(to_address) {
            Ok(stream) => stream,
            Err(e) => {
                error!(
                    "There was an error while connecting to the server {} {}",
                    to_server_id, e
                );
                panic!(
                    "There was an error while connecting to the server {} {}",
                    to_server_id, e
                );
            }
        };

        let serialized_request = match serde_json::to_string(&message) {
            Ok(serialized_message) => serialized_message,
            Err(e) => {
                error!("There was an error while serializing the message {}", e);
                panic!("There was an error while serializing the message {}", e);
            }
        };

        if let Err(e) = stream.write_all(serialized_request.as_bytes()) {
            error!("There was an error while sending the message {}", e);
            panic!("There was an error while sending the message {}", e);
        }
    }
}

#[derive(Serialize, Deserialize)]
enum RPCMessage<T> {
    VoteRequest(VoteRequest),
    VoteResponse(VoteResponse),
    AppendEntriesRequest(AppendEntriesRequest<T>),
    AppendEntriesResponse(AppendEntriesResponse),
}
#[derive(Serialize, Deserialize, Debug)]
struct VoteRequest {
    term: Term,
    candidate_id: ServerId,
    last_log_index: u64,
    last_log_term: Term,
}

#[derive(Serialize, Deserialize, Debug)]
struct VoteResponse {
    term: Term,
    vote_granted: bool,
    candidate_id: ServerId, //  the id of the server granting the vote, used for de-duplication
}

#[derive(Serialize, Deserialize, Debug)]
struct AppendEntriesRequest<T> {
    term: Term,
    leader_id: ServerId,
    prev_log_index: u64,
    prev_log_term: Term,
    entries: Vec<LogEntry<T>>,
    leader_commit_index: u64,
}

#[derive(Serialize, Deserialize, Debug)]
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
    address: String,
    port: u64,
}

trait StateMachine<T> {
    fn apply_set(&mut self, key: String, value: T);
    fn apply_get(&mut self, key: String) -> Option<&T>;
    fn apply(&mut self, entries: Vec<LogEntry<T>>);
}

#[derive(Serialize, Deserialize, Debug)]
enum LogEntryCommand {
    Set = 0,
    Delete = 1,
}

#[derive(Serialize, Deserialize, Debug)]
struct LogEntry<T> {
    term: Term,
    index: u64,
    command: LogEntryCommand,
    key: String,
    value: T,
}

#[derive(PartialEq)]
enum RaftNodeStatus {
    Leader,
    Candidate,
    Follower,
}

struct RaftNodeState<T> {
    //  persistent state on all servers
    current_term: Term,
    voted_for: Option<ServerId>,
    log: Vec<LogEntry<T>>,

    //  volatile state on all servers
    commit_index: u64,
    last_applied: u64,
    status: RaftNodeStatus,

    //  volatile state for leaders
    next_index: Vec<u64>,
    match_index: Vec<u64>,

    election_timeout: Duration,

    //  election variables
    votes_received: HashMap<ServerId, bool>,
}

struct RaftNode<T: DeserializeOwned + Send + 'static + std::fmt::Debug, S: StateMachine<T>> {
    id: ServerId,
    state: Mutex<RaftNodeState<T>>,
    state_machine: S,
    config: ServerConfig,
    peers: Vec<ServerId>,
    to_node_sender: mpsc::Sender<RPCMessage<T>>,
    from_rpc_receiver: Arc<Mutex<mpsc::Receiver<RPCMessage<T>>>>,
}

impl<T: Serialize + DeserializeOwned + Send + 'static + std::fmt::Debug, S: StateMachine<T>>
    RaftNode<T, S>
{
    /**
         * \* Message handlers
    \* i = recipient, j = sender, m = message

    \* Server i receives a RequestVote request from server j with
    \* m.mterm <= currentTerm[i].
    HandleRequestVoteRequest(i, j, m) ==
        LET logOk == \/ m.mlastLogTerm > LastTerm(log[i])
                     \/ /\ m.mlastLogTerm = LastTerm(log[i])
                        /\ m.mlastLogIndex >= Len(log[i])
            grant == /\ m.mterm = currentTerm[i]
                     /\ logOk
                     /\ votedFor[i] \in {Nil, j}
        IN /\ m.mterm <= currentTerm[i]
           /\ \/ grant  /\ votedFor' = [votedFor EXCEPT ![i] = j]
              \/ ~grant /\ UNCHANGED votedFor
           /\ Reply([mtype        |-> RequestVoteResponse,
                     mterm        |-> currentTerm[i],
                     mvoteGranted |-> grant,
                     \* mlog is used just for the `elections' history variable for
                     \* the proof. It would not exist in a real implementation.
                     mlog         |-> log[i],
                     msource      |-> i,
                     mdest        |-> j],
                     m)
           /\ UNCHANGED <<state, currentTerm, candidateVars, leaderVars, logVars>>
         */
    fn handle_vote_request(&self, request: VoteRequest) -> VoteResponse {
        let mut state_guard = self.state.lock().unwrap();

        if state_guard.status == RaftNodeStatus::Leader {
            return VoteResponse {
                term: state_guard.current_term,
                vote_granted: false,
                candidate_id: self.id,
            };
        }

        //  basic term check first
        if request.term < state_guard.current_term {
            return VoteResponse {
                term: state_guard.current_term,
                vote_granted: false,
                candidate_id: self.id,
            };
        }

        //  this node has already voted for someone else in this term and it is not the requesting node
        if state_guard.voted_for != None && state_guard.voted_for != Some(request.candidate_id) {
            return VoteResponse {
                term: state_guard.current_term,
                vote_granted: false,
                candidate_id: self.id,
            };
        }

        let last_log_term = state_guard.log.last().map_or(0, |entry| entry.term);
        let last_log_index = state_guard.log.last().map_or(0, |entry| entry.index);

        let log_check = request.last_log_term > last_log_term
            || (request.last_log_term == last_log_term && request.last_log_index >= last_log_index);

        if !log_check {
            return VoteResponse {
                term: state_guard.current_term,
                vote_granted: false,
                candidate_id: self.id,
            };
        }

        //  all checks have passed - grant the vote but only after recording it
        state_guard.voted_for = Some(request.candidate_id);
        VoteResponse {
            term: state_guard.current_term,
            vote_granted: true,
            candidate_id: self.id,
        }
    }

    /**
         * \* Server i receives a RequestVote response from server j with
    \* m.mterm = currentTerm[i].
    HandleRequestVoteResponse(i, j, m) ==
        \* This tallies votes even when the current state is not Candidate, but
        \* they won't be looked at, so it doesn't matter.
        /\ m.mterm = currentTerm[i]
        /\ votesResponded' = [votesResponded EXCEPT ![i] =
                                  votesResponded[i] \cup {j}]
        /\ \/ /\ m.mvoteGranted
              /\ votesGranted' = [votesGranted EXCEPT ![i] =
                                      votesGranted[i] \cup {j}]
              /\ voterLog' = [voterLog EXCEPT ![i] =
                                  voterLog[i] @@ (j :> m.mlog)]
           \/ /\ ~m.mvoteGranted
              /\ UNCHANGED <<votesGranted, voterLog>>
        /\ Discard(m)
        /\ UNCHANGED <<serverVars, votedFor, leaderVars, logVars>>
         */
    fn handle_vote_response(&self, vote_response: VoteResponse) {
        let mut state_guard = self.state.lock().unwrap();

        if !vote_response.vote_granted && vote_response.term > state_guard.current_term {
            //  the term is different, update term and downgrade to follower
            state_guard.current_term = vote_response.term;
            state_guard.status = RaftNodeStatus::Follower;
            return;
        }

        //  the vote wasn't granted, the reason is unknown
        if !vote_response.vote_granted {
            return;
        }

        //  the vote was granted - check if i achieved quorum
        let quorum = self.peers.len() / 2;
        state_guard
            .votes_received
            .insert(vote_response.candidate_id, vote_response.vote_granted);
        let sum_of_votes_granted = state_guard
            .votes_received
            .iter()
            .filter(|(_, vote_granted)| **vote_granted)
            .count();

        if sum_of_votes_granted == quorum {
            //  the candidate should become a leader
        }
    }

    //  From this point onward, are all the starter methods to bring the node up and handle communication
    //  between the node and the RPC manager
    fn new(id: ServerId, state_machine: S, config: ServerConfig, peers: Vec<ServerId>) -> Self {
        let election_jitter = rand::thread_rng().gen_range(0..100);
        let election_timeout = config.election_timeout + Duration::from_millis(election_jitter);
        let server_state = RaftNodeState {
            current_term: 0,
            voted_for: None,
            log: Vec::new(),
            status: RaftNodeStatus::Follower,
            election_timeout,
            votes_received: HashMap::new(),
            commit_index: 0,
            last_applied: 0,
            next_index: Vec::new(),
            match_index: Vec::new(),
        };
        let (to_node_sender, from_rpc_receiver) = mpsc::channel();
        let server = RaftNode {
            id,
            state: Mutex::new(server_state),
            state_machine,
            config,
            peers,
            to_node_sender,
            from_rpc_receiver: Arc::new(Mutex::new(from_rpc_receiver)),
        };
        info!("Initialising a new raft server with id {}", id);
        server
    }

    fn start(&self) {
        //  start the rpc manager here
        let rpc_manager = RPCManager::new(
            self.id,
            self.config.address.clone(),
            self.config.port,
            self.to_node_sender.clone(),
        );
        thread::spawn(move || {
            rpc_manager.start();
        });
        self.listen_for_messages();
    }

    fn listen_for_messages(&self) {
        let receiver = Arc::clone(&self.from_rpc_receiver);
        thread::spawn(move || {
            let receiver_guard = receiver.lock().unwrap();
            for message in receiver_guard.iter() {
                match message {
                    RPCMessage::VoteRequest(vote_request) => {
                        info!("Received a vote request {:?}", vote_request);
                    }
                    RPCMessage::VoteResponse(vote_response) => {
                        info!("Received a vote response {:?}", vote_response);
                    }
                    RPCMessage::AppendEntriesRequest(append_entries_request) => {
                        info!(
                            "Received an append entries request {:?}",
                            append_entries_request
                        );
                    }
                    RPCMessage::AppendEntriesResponse(append_entries_response) => {
                        info!(
                            "Received an append entries response {:?}",
                            append_entries_response
                        );
                    }
                }
            }
        });
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
