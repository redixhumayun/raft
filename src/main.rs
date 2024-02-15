#![allow(dead_code)] //  TODO: Remove this later. It's getting too noisy now.

use clock::{Clock, MockClock, RealClock};
use core::fmt;
use serde::de::DeserializeOwned;
use serde_json;
use std::cell::{Ref, RefCell};
use std::io::{self, BufRead, Write};
use std::net::{TcpListener, TcpStream};
use std::str::FromStr;
use std::sync::atomic::AtomicBool;
use std::sync::mpsc::{Receiver, Sender};
use std::sync::{mpsc, Arc, Mutex, MutexGuard};
use std::thread;
use std::time::{Duration, Instant};
use std::{collections::HashMap, io::BufReader};
use types::RaftTypeTrait;

use log::{debug, error, info};
use rand::Rng;
use serde::{Deserialize, Serialize};

mod clock;
mod storage;
mod types;

use crate::types::{ServerId, Term};
use storage::RaftFileOps;

/**
 * RPC Stuff
 */

trait Communication<T: RaftTypeTrait> {
    fn start(&self);

    fn stop(&self);

    fn send_message(&self, to_address: String, message: MessageWrapper<T>);
}

#[derive(Debug, Clone)]
struct MessageWrapper<T: RaftTypeTrait> {
    from_node_id: ServerId,
    to_node_id: ServerId,
    message: RPCMessage<T>,
}

struct MockRPCManager<T: RaftTypeTrait> {
    server_id: ServerId,
    to_node_sender: mpsc::Sender<MessageWrapper<T>>,
    sent_messages: RefCell<Vec<MessageWrapper<T>>>,
}

impl<T: RaftTypeTrait> MockRPCManager<T> {
    fn new(server_id: ServerId, to_node_sender: mpsc::Sender<MessageWrapper<T>>) -> Self {
        MockRPCManager {
            server_id,
            to_node_sender,
            sent_messages: RefCell::new(vec![]),
        }
    }
}

impl<T: RaftTypeTrait> MockRPCManager<T> {
    fn start(&self) {}

    fn stop(&self) {}

    fn send_message(&self, _to_address: String, message: MessageWrapper<T>) {
        self.sent_messages.borrow_mut().push(message);
    }

    fn get_messages_in_queue(&mut self) -> Vec<MessageWrapper<T>> {
        let mut mock_messages_vector: Vec<MessageWrapper<T>> = Vec::new();
        for message in self.sent_messages.borrow_mut().drain(..) {
            mock_messages_vector.push(message.clone());
        }
        mock_messages_vector
    }

    fn replay_messages_in_queue(&self) -> Ref<Vec<MessageWrapper<T>>> {
        self.sent_messages.borrow()
    }
}

struct RPCManager<T: RaftTypeTrait> {
    server_id: ServerId,
    server_address: String,
    port: u64,
    to_node_sender: mpsc::Sender<RPCMessage<T>>,
    is_running: Arc<AtomicBool>,
}

impl<T: RaftTypeTrait> RPCManager<T> {
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
            is_running: Arc::new(AtomicBool::new(false)),
        }
    }

    fn start(&self) {
        let server_address = self.server_address.clone();
        let server_id = self.server_id.clone();
        let to_node_sender = self.to_node_sender.clone();
        let is_running = self.is_running.clone();
        is_running.store(true, std::sync::atomic::Ordering::SeqCst);
        thread::spawn(move || {
            let listener = match TcpListener::bind(&server_address) {
                Ok(tcp_listener) => tcp_listener,
                Err(e) => {
                    panic!(
                        "There was an error while binding to the address {} for server id {} {}",
                        server_address, server_id, e
                    );
                }
            };

            for stream in listener.incoming() {
                if !is_running.load(std::sync::atomic::Ordering::SeqCst) {
                    break;
                }
                match stream {
                    Ok(stream) => {
                        let reader = BufReader::new(stream);
                        for line in reader.lines() {
                            let json = match line {
                                Ok(l) => l,
                                Err(e) => {
                                    panic!("There was an error while reading the line {}", e);
                                }
                            };
                            let rpc: RPCMessage<T> = match serde_json::from_str(&json) {
                                Ok(message) => message,
                                Err(e) => {
                                    panic!(
                                        "There was an error while deserializing the message {}",
                                        e
                                    );
                                }
                            };
                            match to_node_sender.send(rpc) {
                                Ok(_) => (),
                                Err(e) => {
                                    panic!(
                                        "There was an error while sending the rpc message to the node {}",
                                        e
                                    );
                                }
                            }
                        }
                    }
                    Err(e) => {
                        if !is_running.load(std::sync::atomic::Ordering::SeqCst) {
                            break;
                        }
                        error!("There was an error while accepting a connection {}", e);
                    }
                }
            }
        });
    }

    fn stop(&self) {
        self.is_running
            .store(false, std::sync::atomic::Ordering::SeqCst);
    }

    /**
     * This method is called from the raft node to allow it to communicate with other nodes
     * via the RPC manager.
     */
    fn send_message(&self, to_address: String, message: MessageWrapper<T>) {
        info!(
            "Sending a message from server {} to server {} and the message is {:?}",
            message.from_node_id, message.to_node_id, message.message
        );
        let mut stream = match TcpStream::connect(to_address) {
            Ok(stream) => stream,
            Err(e) => {
                panic!(
                    "There was an error while connecting to the server {} {}",
                    message.to_node_id, e
                );
            }
        };

        let serialized_request = match serde_json::to_string(&message.message) {
            Ok(serialized_message) => serialized_message,
            Err(e) => {
                panic!("There was an error while serializing the message {}", e);
            }
        };

        if let Err(e) = stream.write_all(serialized_request.as_bytes()) {
            panic!("There was an error while sending the message {}", e);
        }
    }
}

enum CommunicationLayer<T: RaftTypeTrait> {
    MockRPCManager(MockRPCManager<T>),
    RPCManager(RPCManager<T>),
}

impl<T: RaftTypeTrait> Communication<T> for CommunicationLayer<T> {
    fn start(&self) {
        match self {
            CommunicationLayer::MockRPCManager(manager) => manager.start(),
            CommunicationLayer::RPCManager(manager) => manager.start(),
        }
    }

    fn stop(&self) {
        match self {
            CommunicationLayer::MockRPCManager(manager) => manager.stop(),
            CommunicationLayer::RPCManager(manager) => manager.stop(),
        }
    }

    fn send_message(&self, to_address: String, message: MessageWrapper<T>) {
        match self {
            CommunicationLayer::MockRPCManager(manager) => {
                manager.send_message(to_address, message)
            }
            CommunicationLayer::RPCManager(manager) => manager.send_message(to_address, message),
        }
    }
}

impl<T: RaftTypeTrait> CommunicationLayer<T> {
    fn get_messages(&mut self) -> Vec<MessageWrapper<T>> {
        match self {
            CommunicationLayer::MockRPCManager(manager) => {
                return manager.get_messages_in_queue();
            }
            CommunicationLayer::RPCManager(_) => {
                panic!("This method is not supported for the RPCManager");
            }
        }
    }

    fn replay_messages(&self) -> Ref<Vec<MessageWrapper<T>>> {
        match self {
            CommunicationLayer::MockRPCManager(manager) => {
                return manager.replay_messages_in_queue();
            }
            CommunicationLayer::RPCManager(_) => {
                panic!("This method is not supported for the RPCManager");
            }
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
enum RPCMessage<T: Clone> {
    VoteRequest(VoteRequest),
    VoteResponse(VoteResponse),
    AppendEntriesRequest(AppendEntriesRequest<T>),
    AppendEntriesResponse(AppendEntriesResponse),
}

impl PartialEq for RPCMessage<i32> {
    fn eq(&self, other: &Self) -> bool {
        match self {
            RPCMessage::VoteRequest(request) => {
                if let RPCMessage::VoteRequest(other_request) = other {
                    return request == other_request;
                }
                return false;
            }
            RPCMessage::VoteResponse(response) => {
                if let RPCMessage::VoteResponse(other_response) = other {
                    return response == other_response;
                }
                return false;
            }
            RPCMessage::AppendEntriesRequest(request) => {
                if let RPCMessage::AppendEntriesRequest(other_request) = other {
                    return request == other_request;
                }
                return false;
            }
            RPCMessage::AppendEntriesResponse(response) => {
                if let RPCMessage::AppendEntriesResponse(other_response) = other {
                    return response == other_response;
                }
                return false;
            }
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
struct VoteRequest {
    term: Term,
    candidate_id: ServerId,
    last_log_index: u64,
    last_log_term: Term,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
struct VoteResponse {
    term: Term,
    vote_granted: bool,
    candidate_id: ServerId, //  the id of the server granting the vote, used for de-duplication
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
struct AppendEntriesRequest<T: Clone> {
    request_id: u16,
    term: Term,
    leader_id: ServerId,
    prev_log_index: u64,
    prev_log_term: Term,
    entries: Vec<LogEntry<T>>,
    leader_commit_index: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
struct AppendEntriesResponse {
    request_id: u16,
    server_id: ServerId,
    term: Term,
    success: bool,
    match_index: u64,
}
/**
 * End RPC stuff
 */

#[derive(Debug)]
struct ServerConfig {
    election_timeout: Duration, //  used to calculate the actual election timeout
    heartbeat_interval: Duration,
    address: String,
    port: u64,
    cluster_nodes: Vec<u64>,
    id_to_address_mapping: HashMap<ServerId, String>,
}

trait StateMachine<T: Serialize + DeserializeOwned + Clone> {
    fn apply_set(&self, key: String, value: T);
    fn apply_get(&self, key: &str) -> Option<T>;
    fn apply(&self, entries: Vec<LogEntry<T>>);
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
enum LogEntryCommand {
    Set = 0,
    Delete = 1,
}

impl FromStr for LogEntryCommand {
    type Err = io::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let v: u64 = s
            .trim()
            .parse()
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        match v {
            0 => Ok(LogEntryCommand::Set),
            1 => Ok(LogEntryCommand::Delete),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Invalid log entry command",
            )),
        }
    }
}

impl fmt::Display for LogEntryCommand {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LogEntryCommand::Set => write!(f, "0"),
            LogEntryCommand::Delete => write!(f, "1"),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
struct LogEntry<T: Clone> {
    term: Term,
    index: u64,
    command: LogEntryCommand,
    key: String,
    value: T,
}

#[derive(PartialEq, Debug)]
enum RaftNodeStatus {
    Leader,
    Candidate,
    Follower,
}

enum RaftNodeClock {
    RealClock(RealClock),
    MockClock(MockClock),
}

impl RaftNodeClock {
    fn advance(&mut self, duration: Duration) {
        match self {
            RaftNodeClock::RealClock(_) => (), //  this method should do nothing for a real clock
            RaftNodeClock::MockClock(clock) => clock.advance(duration),
        }
    }
}

impl Clock for RaftNodeClock {
    fn now(&self) -> Instant {
        match self {
            RaftNodeClock::RealClock(clock) => clock.now(),
            RaftNodeClock::MockClock(clock) => clock.now(),
        }
    }
}

struct RaftNodeState<T: Serialize + DeserializeOwned + Clone> {
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

    //  election variables
    votes_received: HashMap<ServerId, bool>,
    election_timeout: Duration,
    last_heartbeat: Instant,
}

struct RaftNode<T: RaftTypeTrait, S: StateMachine<T>, F: RaftFileOps<T>> {
    id: ServerId,
    state: Mutex<RaftNodeState<T>>,
    state_machine: S,
    config: ServerConfig,
    peers: Vec<ServerId>,
    to_node_sender: mpsc::Sender<MessageWrapper<T>>,
    from_rpc_receiver: mpsc::Receiver<MessageWrapper<T>>,
    rpc_manager: CommunicationLayer<T>,
    persistence_manager: F,
    clock: RaftNodeClock,
}

impl<T: RaftTypeTrait, S: StateMachine<T> + Send, F: RaftFileOps<T> + Send> RaftNode<T, S, F> {
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
    fn handle_vote_request(&mut self, request: &VoteRequest) -> VoteResponse {
        let mut state_guard = self.state.lock().unwrap();
        debug!("ENTER: handle_vote_request on node {}", self.id);

        //  basic term check first - if the request term is lower ignore the request
        if request.term < state_guard.current_term {
            debug!("EXIT: handle_vote_request on node {}. Not granting the vote because request term is lower", self.id);
            return VoteResponse {
                term: state_guard.current_term,
                vote_granted: false,
                candidate_id: self.id,
            };
        }

        //  this node has already voted for someone else in this term and it is not the requesting node
        if state_guard.voted_for != None && state_guard.voted_for != Some(request.candidate_id) {
            debug!("EXIT: handle_vote_request on node {}. Not granting the vote because the node has already voted for someone else", self.id);
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

        //  all checks have passed - grant the vote but only after recording it. Also write it to disk
        state_guard.voted_for = Some(request.candidate_id);
        match self
            .persistence_manager
            .write_term_and_voted_for(state_guard.current_term, state_guard.voted_for)
        {
            Ok(()) => (),
            Err(e) => {
                error!("There was a problem writing the term and voted_for variables to stable storage for node {}: {}", self.id, e);
            }
        }
        debug!(
            "EXIT: handle_vote_request on node {}. Granting the vote for {}",
            self.id, request.candidate_id
        );
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
    fn handle_vote_response(&mut self, vote_response: VoteResponse) {
        let mut state_guard = self.state.lock().unwrap();

        if state_guard.status != RaftNodeStatus::Candidate {
            return;
        }

        if !vote_response.vote_granted && vote_response.term > state_guard.current_term {
            //  the term is different, update term, downgrade to follower and persist to local storage
            state_guard.current_term = vote_response.term;
            state_guard.status = RaftNodeStatus::Follower;
            match self
                .persistence_manager
                .write_term_and_voted_for(state_guard.current_term, Option::None)
            {
                Ok(()) => (),
                Err(e) => {
                    error!("There was a problem writing the term and voted_for variables to stable storage for node {}: {}", self.id, e);
                }
            }
            return;
        }

        //  the vote wasn't granted, the reason is unknown
        if !vote_response.vote_granted {
            return;
        }

        //  the vote was granted - check if node achieved quorum
        state_guard
            .votes_received
            .insert(vote_response.candidate_id, vote_response.vote_granted);

        if self.can_become_leader(&state_guard) {
            self.become_leader(&mut state_guard);
        }
    }

    fn handle_append_entries_request(
        &mut self,
        request: &AppendEntriesRequest<T>,
    ) -> AppendEntriesResponse {
        let mut state_guard = self.state.lock().unwrap();
        debug!("ENTER: handle_append_entries_request for node {}", self.id);

        //  the term check
        if request.term < state_guard.current_term {
            debug!("The term in the request is less than the current term, ignoring the request");
            debug!("EXIT: handle_append_entries_request for node {}", self.id);
            return AppendEntriesResponse {
                request_id: request.request_id,
                server_id: self.id,
                term: state_guard.current_term,
                success: false,
                match_index: state_guard.log.last().map_or(0, |entry| entry.index),
            };
        }

        self.reset_election_timeout(&mut state_guard);

        // log consistency check - check that the term and the index match at the log entry the leader expects
        let log_index_to_check = request.prev_log_index.saturating_sub(1) as usize;
        let prev_log_term = state_guard
            .log
            .get(log_index_to_check)
            .map_or(0, |entry| entry.term);
        let prev_log_index = state_guard
            .log
            .get(log_index_to_check)
            .map_or(0, |entry| entry.index);
        let log_ok = request.prev_log_index == 0
            || (request.prev_log_index > 0
                && request.prev_log_index <= state_guard.log.len() as u64
                && request.prev_log_term == prev_log_term);

        debug!("The message has prev_log_index: {} and prev_log_term: {} and the node's log has prev_log_index: {} and prev_log_term: {}", request.prev_log_index, request.prev_log_term, prev_log_index, prev_log_term);
        //  the log check is not OK, give false response
        if !log_ok {
            debug!("The node's log is {:?}", state_guard.log);
            debug!("The log check failed because request has prev_log_index as {} and prev_log_term as {} and the node has prev_log_index as {} and prev_log_term as {}", request.prev_log_index, request.prev_log_term, state_guard.log.len(), prev_log_term);
            debug!("EXIT: handle_append_entries_request for node {}", self.id);
            return AppendEntriesResponse {
                request_id: request.request_id,
                server_id: self.id,
                term: state_guard.current_term,
                success: false,
                match_index: state_guard.log.last().map_or(0, |entry| entry.index),
            };
        }

        if request.entries.len() == 0 {
            //  this is a  heartbeat message
            debug!("This is a heartbeat message, returning success");
            state_guard.commit_index = request.leader_commit_index;
            self.apply_entries(&mut state_guard);
            debug!("EXIT: handle_append_entries_request for node {}", self.id);
            return AppendEntriesResponse {
                request_id: request.request_id,
                server_id: self.id,
                term: state_guard.current_term,
                success: true,
                match_index: state_guard.log.last().map_or(0, |entry| entry.index),
            };
        }

        //  if there are any subsequent logs on the follower, truncate them and append the new logs
        if self.len_as_u64(&state_guard.log) > request.prev_log_index {
            state_guard.log.truncate(request.prev_log_index as usize);
            state_guard.log.extend_from_slice(&request.entries);
            if let Err(e) = self
                .persistence_manager
                .append_logs_at(&request.entries, request.prev_log_index.saturating_sub(1))
            {
                error!("There was a problem appending logs to stable storage for node {} at position {}: {}", self.id, request.prev_log_index - 1, e);
            }
            return AppendEntriesResponse {
                request_id: request.request_id,
                server_id: self.id,
                term: state_guard.current_term,
                success: true,
                match_index: state_guard.log.last().map_or(0, |entry| entry.index),
            };
        }

        //  There are no subsequent logs on the follower, so no possibility of conflicts. Which means the logs can just be appended
        //  and the response can be returned
        state_guard.log.append(&mut request.entries.clone());
        if let Err(e) = self.persistence_manager.append_logs(&request.entries) {
            error!(
                "There was a problem appending logs to stable storage for node {}: {}",
                self.id, e
            );
        }

        debug!(
            "The new match index is {}",
            state_guard.log.last().map_or(0, |entry| entry.index)
        );
        debug!("EXIT: handle_append_entries_request for node {}", self.id);
        AppendEntriesResponse {
            request_id: request.request_id,
            server_id: self.id,
            term: state_guard.current_term,
            success: true,
            match_index: state_guard.log.last().map_or(0, |entry| entry.index),
        }
    }

    fn handle_append_entries_response(&mut self, response: AppendEntriesResponse) {
        let mut state_guard = self.state.lock().unwrap();
        if state_guard.status != RaftNodeStatus::Leader {
            return;
        }

        let server_index = response.server_id as usize;

        if !response.success {
            //  reduce the next index for that server and try again
            debug!("The append entry request was not successful, decrementing next_index and retrying request");
            state_guard.next_index[server_index] = state_guard.next_index[server_index]
                .saturating_sub(1)
                .max(1);

            self.retry_append_request(&mut state_guard, server_index, response.server_id);
            return;
        }

        //  the response is successful
        debug!(
            "The response was successful, updating next index and match index for server {} and the response is {:?}",
            response.server_id, response
        );
        debug!(
            "The old set of next indices {:?} and match indices{:?}",
            state_guard.next_index, state_guard.match_index
        );
        state_guard.next_index[(response.server_id) as usize] = response.match_index + 1;
        state_guard.match_index[(response.server_id) as usize] = response.match_index;
        debug!(
            "The new set of next indices {:?} and match indices {:?}",
            state_guard.next_index, state_guard.match_index
        );
        self.advance_commit_index(&mut state_guard);
        self.apply_entries(&mut state_guard);
    }

    //  Utility functions for main RPC's
    /**
    * BecomeLeader(i) ==
        /\ state[i] = Candidate
        /\ votesGranted[i] \in Quorum
        /\ state'      = [state EXCEPT ![i] = Leader]
        /\ nextIndex'  = [nextIndex EXCEPT ![i] =
                            [j \in Server |-> Len(log[i]) + 1]]
        /\ matchIndex' = [matchIndex EXCEPT ![i] =
                            [j \in Server |-> 0]]
        /\ elections'  = elections \cup
                            {[eterm     |-> currentTerm[i],
                            eleader   |-> i,
                            elog      |-> log[i],
                            evotes    |-> votesGranted[i],
                            evoterLog |-> voterLog[i]]}
        /\ UNCHANGED <<messages, currentTerm, votedFor, candidateVars, logVars>>
    */
    fn become_leader(&self, state_guard: &mut MutexGuard<'_, RaftNodeState<T>>) {
        info!("Node {} has become the leader", self.id);
        state_guard.status = RaftNodeStatus::Leader;
        let last_log_index = state_guard.log.last().map_or(0, |entry| entry.index);
        state_guard.next_index = self
            .config
            .cluster_nodes
            .iter()
            .map(|_| last_log_index + 1)
            .collect();
        state_guard.match_index = vec![0; self.config.cluster_nodes.len()];
    }

    fn retry_append_request(
        &self,
        state_guard: &mut MutexGuard<'_, RaftNodeState<T>>,
        server_index: usize,
        to_server_id: u64,
    ) {
        let next_index_of_follower = state_guard.next_index[server_index] as usize;
        let new_next_index_of_follower = next_index_of_follower.saturating_sub(1).max(1);
        state_guard.next_index[server_index] = new_next_index_of_follower as u64;

        debug!(
            "The next index of follower {} was {} and now it is {}",
            to_server_id, next_index_of_follower, new_next_index_of_follower
        );
        let entries_to_send = state_guard
            .log
            .get(new_next_index_of_follower - 1..)
            .unwrap()
            .to_vec();
        let prev_log_index = if new_next_index_of_follower > 1 {
            state_guard
                .log
                .get(new_next_index_of_follower - 1)
                .unwrap()
                .index
        } else {
            0
        };
        let prev_log_term = if new_next_index_of_follower > 1 {
            state_guard
                .log
                .get(new_next_index_of_follower - 1)
                .unwrap()
                .term
        } else {
            0
        };

        let request = AppendEntriesRequest {
            request_id: rand::thread_rng().gen(),
            term: state_guard.current_term,
            leader_id: self.id,
            prev_log_index,
            prev_log_term,
            entries: entries_to_send,
            leader_commit_index: state_guard.commit_index,
        };
        debug!("Retrying append entries with request {:?}", request);
        let message = RPCMessage::<T>::AppendEntriesRequest(request);
        let to_address = self
            .config
            .id_to_address_mapping
            .get(&to_server_id)
            .unwrap();
        let message_wrapper = MessageWrapper {
            from_node_id: self.id,
            to_node_id: to_server_id,
            message,
        };
        self.rpc_manager
            .send_message(to_address.clone(), message_wrapper);
    }

    /**
    * \* Leader i advances its commitIndex.
       \* This is done as a separate step from handling AppendEntries responses,
       \* in part to minimize atomic regions, and in part so that leaders of
       \* single-server clusters are able to mark entries committed.
       AdvanceCommitIndex(i) ==
           /\ state[i] = Leader
           /\ LET \* The set of servers that agree up through index.
               Agree(index) == {i} \cup {k \in Server :
                                               matchIndex[i][k] >= index}
               \* The maximum indexes for which a quorum agrees
               agreeIndexes == {index \in 1..Len(log[i]) :
                                       Agree(index) \in Quorum}
               \* New value for commitIndex'[i]
               newCommitIndex ==
                   IF /\ agreeIndexes /= {}
                       /\ log[i][Max(agreeIndexes)].term = currentTerm[i]
                   THEN
                       Max(agreeIndexes)
                   ELSE
                       commitIndex[i]
           IN commitIndex' = [commitIndex EXCEPT ![i] = newCommitIndex]
           /\ UNCHANGED <<messages, serverVars, candidateVars, leaderVars, log>>
        */
    fn advance_commit_index(&self, state_guard: &mut MutexGuard<'_, RaftNodeState<T>>) {
        debug!("ENTER: advance_commit_index for node {}", self.id);
        assert!(state_guard.status == RaftNodeStatus::Leader);

        //  find all match indexes that have quorum
        let mut match_index_count: HashMap<u64, u64> = HashMap::new();
        for &server_match_index in &state_guard.match_index {
            *match_index_count.entry(server_match_index).or_insert(0) += 1;
        }
        debug!("The match index count: {:?}", match_index_count);

        let new_commit_index = match_index_count
            .iter()
            .filter(|(&match_index, &count)| {
                count >= self.quorum_size().try_into().unwrap()
                    && state_guard
                        .log
                        .get((match_index as usize).saturating_sub(1))
                        .map_or(false, |entry| entry.term == state_guard.current_term)
            })
            .map(|(&match_index, _)| match_index)
            .max();

        debug!(
            "The old commit index was: {} and the new commit index is: {}",
            state_guard.commit_index,
            new_commit_index.unwrap_or(state_guard.commit_index)
        );
        if let Some(max_index) = new_commit_index {
            state_guard.commit_index = max_index;
        }
        debug!("EXIT: advance_commit_index for node {}", self.id);
    }

    /// This function will check whether there are any more entries that can be applied to the state machine for the node
    fn apply_entries(&self, state_guard: &mut MutexGuard<'_, RaftNodeState<T>>) {
        debug!("ENTER: apply_entries for node {}", self.id);
        let mut entries_to_apply: Vec<LogEntry<T>> = Vec::new();
        debug!(
            "last_applied: {}, commit_index: {}",
            state_guard.last_applied, state_guard.commit_index
        );
        while state_guard.last_applied < state_guard.commit_index {
            debug!("Updating the state machine for node {}", self.id);
            debug!("The current log is {:?}", state_guard.log);
            state_guard.last_applied += 1;
            let entry_at_index = state_guard
                .log
                .get((state_guard.last_applied - 1) as usize)
                .unwrap();
            entries_to_apply.push(entry_at_index.clone());
        }

        if entries_to_apply.len() > 0 {
            self.state_machine.apply(entries_to_apply);
        }
        debug!("EXIT: apply_entries for node {}", self.id);
    }

    /**
         * \* Server i restarts from stable storage.
    \* It loses everything but its currentTerm, votedFor, and log.
    Restart(i) ==
        /\ state'          = [state EXCEPT ![i] = Follower]
        /\ votesResponded' = [votesResponded EXCEPT ![i] = {}]
        /\ votesGranted'   = [votesGranted EXCEPT ![i] = {}]
        /\ voterLog'       = [voterLog EXCEPT ![i] = [j \in {} |-> <<>>]]
        /\ nextIndex'      = [nextIndex EXCEPT ![i] = [j \in Server |-> 1]]
        /\ matchIndex'     = [matchIndex EXCEPT ![i] = [j \in Server |-> 0]]
        /\ commitIndex'    = [commitIndex EXCEPT ![i] = 0]
        /\ UNCHANGED <<messages, currentTerm, votedFor, log, elections>>
         */
    fn restart(&mut self) {
        let mut state_guard = self.state.lock().unwrap();
        state_guard.status = RaftNodeStatus::Follower;
        state_guard.next_index = vec![1; self.config.cluster_nodes.len()];
        state_guard.match_index = vec![0; self.config.cluster_nodes.len()];
        state_guard.commit_index = 0;
        state_guard.votes_received = HashMap::new();

        let (term, voted_for) = match self.persistence_manager.read_term_and_voted_for() {
            Ok((t, v)) => (t, v),
            Err(e) => {
                panic!(
                    "There was an error while reading the term and voted for {}",
                    e
                );
            }
        };
        state_guard.current_term = term;
        state_guard.voted_for = Some(voted_for);

        let log_entries = match self.persistence_manager.read_logs(1) {
            Ok(entries) => entries,
            Err(e) => {
                panic!("There was an error while reading the logs {}", e);
            }
        };
        state_guard.log = log_entries;
    }

    /// Any RPC with a  newer term should force the node to advance its own term, reset to a Follower and set its voted_for to None
    /// before continuing to process the message
    fn update_term(&self, state_guard: &mut MutexGuard<'_, RaftNodeState<T>>, new_term: Term) {
        debug!("ENTER: update_term for node {}", self.id);
        state_guard.current_term = new_term;
        state_guard.voted_for = None;
        state_guard.status = RaftNodeStatus::Follower;

        if let Err(e) = self
            .persistence_manager
            .write_term_and_voted_for(new_term, None)
        {
            //  valid to panic here because this is a write to disk
            panic!(
                "There was an error while writing the term and voted for to stable storage {}",
                e
            );
        }
        debug!("EXIT: update_term for node {}", self.id);
    }

    fn update_node_term_if_required(&mut self, message_wrapper: MessageWrapper<T>) {
        let mut state_guard = self.state.lock().unwrap();
        match message_wrapper.message {
            RPCMessage::VoteRequest(message) => {
                if message.term > state_guard.current_term {
                    self.update_term(&mut state_guard, message.term);
                }
            }
            RPCMessage::VoteResponse(message) => {
                if message.term > state_guard.current_term {
                    self.update_term(&mut state_guard, message.term);
                }
            }
            RPCMessage::AppendEntriesRequest(message) => {
                if message.term > state_guard.current_term {
                    self.update_term(&mut state_guard, message.term);
                }
            }
            RPCMessage::AppendEntriesResponse(message) => {
                if message.term > state_guard.current_term {
                    self.update_term(&mut state_guard, message.term);
                }
            }
        }
    }

    fn is_message_stale(&self, message_wrapper: MessageWrapper<T>) -> bool {
        let state_guard = self.state.lock().unwrap();
        match message_wrapper.message {
            RPCMessage::VoteRequest(message) => message.term < state_guard.current_term,
            RPCMessage::VoteResponse(message) => message.term < state_guard.current_term,
            RPCMessage::AppendEntriesRequest(message) => message.term < state_guard.current_term,
            RPCMessage::AppendEntriesResponse(message) => message.term < state_guard.current_term,
        }
    }
    /**
     * Receive(m) ==
    LET i == m.mdest
        j == m.msource
    IN \* Any RPC with a newer term causes the recipient to advance
       \* its term first. Responses with stale terms are ignored.
       \/ UpdateTerm(i, j, m)
       \/ /\ m.mtype = RequestVoteRequest
          /\ HandleRequestVoteRequest(i, j, m)
       \/ /\ m.mtype = RequestVoteResponse
          /\ \/ DropStaleResponse(i, j, m)
             \/ HandleRequestVoteResponse(i, j, m)
       \/ /\ m.mtype = AppendEntriesRequest
          /\ HandleAppendEntriesRequest(i, j, m)
       \/ /\ m.mtype = AppendEntriesResponse
          /\ \/ DropStaleResponse(i, j, m)
             \/ HandleAppendEntriesResponse(i, j, m)
     */
    fn receive(&mut self, message_wrapper: MessageWrapper<T>) -> Option<(RPCMessage<T>, ServerId)> {
        self.update_node_term_if_required(message_wrapper.clone());
        let message_wrapper_clone = message_wrapper.clone();
        match message_wrapper.message {
            RPCMessage::VoteRequest(request) => {
                let response = self.handle_vote_request(&request);
                Some((RPCMessage::VoteResponse(response), request.candidate_id))
            }
            RPCMessage::VoteResponse(response) => {
                if self.is_message_stale(message_wrapper_clone.clone()) {
                    return None;
                }
                self.handle_vote_response(response);
                None
            }
            RPCMessage::AppendEntriesRequest(request) => {
                let response = self.handle_append_entries_request(&request);
                Some((
                    RPCMessage::AppendEntriesResponse(response),
                    request.leader_id,
                ))
            }
            RPCMessage::AppendEntriesResponse(response) => {
                if self.is_message_stale(message_wrapper_clone.clone()) {
                    return None;
                }
                self.handle_append_entries_response(response);
                None
            }
        }
    }

    fn can_become_leader(&self, state_guard: &MutexGuard<'_, RaftNodeState<T>>) -> bool {
        let sum_of_votes_received = state_guard
            .votes_received
            .iter()
            .filter(|(_, vote_granted)| **vote_granted)
            .count();
        let quorum = self.quorum_size();
        sum_of_votes_received >= quorum
    }

    /// This method will cause a node to elevate itself to candidate, cast a vote for itself, write that
    /// vote to storage and then send out messages to all other peers requesting votes
    fn start_election(&self, state_guard: &mut MutexGuard<'_, RaftNodeState<T>>) {
        debug!("ENTER: start_election for node {}", self.id);
        state_guard.status = RaftNodeStatus::Candidate;
        state_guard.current_term += 1;
        state_guard.voted_for = Some(self.id);
        state_guard.votes_received.insert(self.id, true);
        if let Err(e) = self
            .persistence_manager
            .write_term_and_voted_for(state_guard.current_term, state_guard.voted_for)
        {
            panic!(
                "There was an error while writing the term and voted for to stable storage {}",
                e
            );
        }

        //  Check to ensure that a single node system will be immediately elected a leader. Feels a bit hacky
        if self.can_become_leader(state_guard) {
            self.become_leader(state_guard);
            debug!(
                "EXIT: start_election and node {} has become the leader",
                self.id
            );
            return;
        }

        for peer in &self.peers {
            if peer == &self.id {
                continue;
            }
            //  send out a vote request to all the remaining nodes
            let vote_request = VoteRequest {
                term: state_guard.current_term,
                candidate_id: self.id,
                last_log_index: state_guard.log.last().map_or(0, |entry| entry.index),
                last_log_term: state_guard.log.last().map_or(0, |entry| entry.term),
            };
            let message = RPCMessage::<T>::VoteRequest(vote_request);
            let to_address = self.config.id_to_address_mapping.get(&peer).expect(
                format!("Cannot find the id to address mapping for peer id {}", peer).as_str(),
            );
            let message_wrapper = MessageWrapper {
                from_node_id: self.id,
                to_node_id: *peer,
                message,
            };
            self.rpc_manager
                .send_message(to_address.clone(), message_wrapper);
        }
        debug!("EXIT: start_election for node {}", self.id);
    }

    fn send_heartbeat(&self, state_guard: &MutexGuard<'_, RaftNodeState<T>>) {
        if state_guard.status != RaftNodeStatus::Leader {
            return;
        }

        for peer in &self.peers {
            if *peer == self.id {
                //  ignore self referencing node
                continue;
            }

            let heartbeat_request = AppendEntriesRequest {
                request_id: rand::thread_rng().gen(),
                term: state_guard.current_term,
                leader_id: self.id,
                prev_log_index: state_guard.log.last().map_or(0, |entry| entry.index),
                prev_log_term: state_guard.log.last().map_or(0, |entry| entry.term),
                entries: Vec::<LogEntry<T>>::new(),
                leader_commit_index: state_guard.commit_index,
            };
            let message = RPCMessage::<T>::AppendEntriesRequest(heartbeat_request);
            let to_address = self.config.id_to_address_mapping.get(peer).expect(
                format!("Cannot find the id to address mapping for peer id {}", peer).as_str(),
            );
            let message_wrapper = MessageWrapper {
                from_node_id: self.id,
                to_node_id: *peer,
                message,
            };
            self.rpc_manager
                .send_message(to_address.clone(), message_wrapper);
        }
    }

    fn reset_election_timeout(&self, state_guard: &mut MutexGuard<'_, RaftNodeState<T>>) {
        state_guard.last_heartbeat = self.clock.now();
    }

    fn check_election_timeout(&self, state_guard: &mut MutexGuard<'_, RaftNodeState<T>>) {
        if (self.clock.now() - state_guard.last_heartbeat) >= state_guard.election_timeout {
            match state_guard.status {
                RaftNodeStatus::Leader => {
                    //  do nothing
                }
                RaftNodeStatus::Candidate => {
                    state_guard.status = RaftNodeStatus::Follower;
                    self.reset_election_timeout(state_guard);
                }
                RaftNodeStatus::Follower => {
                    self.reset_election_timeout(state_guard);
                    self.start_election(state_guard);
                }
            }
        }
    }

    /// Returns the log entry at a given index
    fn get_log_entry_at(
        &self,
        state_guard: &MutexGuard<'_, RaftNodeState<T>>,
        log_entry_index: u32,
    ) -> LogEntry<T> {
        let log_entry = state_guard.log.get(log_entry_index as usize).unwrap();
        return log_entry.clone();
    }

    /// Returns the last log entry
    fn get_last_log_entry(&self) -> LogEntry<T> {
        let state_guard = self.state.lock().unwrap();
        return self.get_log_entry_at(&state_guard, state_guard.log.len() as u32 - 1);
    }

    /// Method exposed by the node to allow a client to set a key value pair on the state machine
    fn set_key_value_pair(&mut self, key: String, value: T) -> Result<usize, String> {
        if !self.is_leader() {
            return Err("This node is not the leader".to_string());
        }
        let mut state_guard = self.state.lock().unwrap();
        info!(
            "Setting the key: {}, value: {} pair on node {}",
            key, value, self.id
        );
        let log_entry = LogEntry {
            term: state_guard.current_term,
            index: state_guard.log.last().map_or(1, |entry| entry.index + 1),
            command: LogEntryCommand::Set,
            key,
            value,
        };
        let prev_log_index = state_guard.log.last().map_or(0, |entry| entry.index);
        let prev_log_term = state_guard.log.last().map_or(0, |entry| entry.term);
        state_guard.log.push(log_entry.clone());
        let index = state_guard.log.len() - 1;
        if let Err(e) = self
            .persistence_manager
            .append_logs(&vec![log_entry.clone()])
        {
            return Err(format!(
                "There was an error while appending logs to stable storage {}",
                e
            ));
        }
        //  update the match index of the leader
        //  This isn't strictly required but it makes subsequent calculations to advance the
        //  commit_index easier
        // state_guard.match_index[self.id as usize] += 1;
        state_guard.match_index[self.id as usize] = state_guard.last_applied + 1;
        for peer in &self.peers {
            if *peer == self.id {
                continue;
            }
            let append_entry_request = AppendEntriesRequest {
                request_id: rand::thread_rng().gen(),
                term: state_guard.current_term,
                leader_id: self.id,
                prev_log_index,
                prev_log_term,
                entries: vec![log_entry.clone()],
                leader_commit_index: state_guard.commit_index,
            };
            let message = RPCMessage::<T>::AppendEntriesRequest(append_entry_request);
            let to_address = self.config.id_to_address_mapping.get(peer).expect(
                format!("Cannot find the id to address mapping for peer id {}", peer).as_str(),
            );
            let message_wrapper = MessageWrapper {
                from_node_id: self.id,
                to_node_id: *peer,
                message,
            };
            self.rpc_manager
                .send_message(to_address.clone(), message_wrapper);
        }
        Ok(index)
    }
    //  End utility functions for main RPC's

    //  Helper functions
    ///  The quorum size is (N/2) + 1, where N = number of servers in the cluster and N is odd
    fn quorum_size(&self) -> usize {
        (self.config.cluster_nodes.len() / 2) + 1
    }

    fn len_as_u64(&self, v: &Vec<LogEntry<T>>) -> u64 {
        v.len() as u64
    }

    fn is_leader(&self) -> bool {
        let state_guard = self.state.lock().unwrap();
        state_guard.status == RaftNodeStatus::Leader
    }

    fn advance_time_by(&mut self, duration: Duration) {
        self.clock.advance(duration);
    }

    fn query_state_machine<'a>(&'a self, keys: &Vec<&'a str>) -> Vec<(&str, Option<T>)> {
        let mut results: Vec<(&str, Option<T>)> = vec![];
        for key in keys {
            let result = self.state_machine.apply_get(key);
            results.push((key, result));
        }
        results
    }
    //  End helper functions

    //  From this point onward are all the starter methods to bring the node up and handle communication
    //  between the node and the RPC manager

    /// Method to construct a new raft node
    fn new(
        id: ServerId,
        state_machine: S,
        config: ServerConfig,
        peers: Vec<ServerId>,
        persistence_manager: F,
        rpc_manager: CommunicationLayer<T>,
        to_node_sender: Sender<MessageWrapper<T>>,
        from_rpc_receiver: Receiver<MessageWrapper<T>>,
        clock: RaftNodeClock,
    ) -> Self {
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
            last_heartbeat: Instant::now(),
        };
        let server = RaftNode {
            id,
            state: Mutex::new(server_state),
            state_machine,
            config,
            peers,
            to_node_sender,
            from_rpc_receiver,
            rpc_manager,
            persistence_manager,
            clock,
        };
        server
    }

    fn listen_for_messages(&mut self) {
        if let Ok(message_wrapper) = self.from_rpc_receiver.try_recv() {
            // info!("Received a message on node {}: {:?}", self.id, message);
            info!(
                "Received message from node {} on node {} {:?}",
                message_wrapper.from_node_id, self.id, message_wrapper.message
            );
            if let Some((message_response, from_id)) = self.receive(message_wrapper.clone()) {
                let to_address = self.config.id_to_address_mapping.get(&from_id).expect(
                    format!(
                        "Cannot find the id to address mapping for peer id {}",
                        from_id
                    )
                    .as_str(),
                );
                let message_wrapper = MessageWrapper {
                    from_node_id: self.id,
                    to_node_id: from_id,
                    message: message_response,
                };
                self.rpc_manager
                    .send_message(to_address.clone(), message_wrapper);
            }
        }
    }

    fn start(&mut self) {
        let _state_guard = self.state.lock().unwrap();
        self.rpc_manager.start();
    }

    fn stop(&self) {
        let mut state_guard = self.state.lock().unwrap();
        self.rpc_manager.stop();
        let address = &self.config.address;
        let _ = TcpStream::connect(address); //  doing this so that the listener actually closes (H/T to Phil Eaton https://github.com/eatonphil/raft-rs/blob/main/src/lib.rs#L1921)

        state_guard.current_term = 0;
        state_guard.commit_index = 0;
        state_guard.last_applied = 0;
        state_guard.log.clear();
        state_guard.voted_for = None;
        state_guard.votes_received.clear();
        state_guard.next_index.clear();
        state_guard.match_index.clear();
    }

    fn tick(&mut self) {
        let mut state_guard = self.state.lock().unwrap();
        match state_guard.status {
            RaftNodeStatus::Leader => {
                //  the leader should send heartbeats to all followers
                self.send_heartbeat(&state_guard);
            }
            RaftNodeStatus::Candidate | RaftNodeStatus::Follower => {
                //  the candidate or follower should check if the election timeout has elapsed
                self.check_election_timeout(&mut state_guard);
            }
        }
        drop(state_guard);

        //  listen to incoming messages from the RPC manager
        self.listen_for_messages();
        self.advance_time_by(Duration::from_millis(1));
    }
}

fn main() {}

//  An example state machine - a key value store
struct KeyValueStore<T> {
    store: RefCell<HashMap<String, T>>,
}

impl<T> KeyValueStore<T> {
    fn new() -> Self {
        KeyValueStore {
            store: RefCell::new(HashMap::new()),
        }
    }
}

impl<T: RaftTypeTrait> StateMachine<T> for KeyValueStore<T> {
    fn apply_set(&self, key: String, value: T) {
        self.store.borrow_mut().insert(key, value);
    }

    fn apply_get(&self, key: &str) -> Option<T> {
        self.store.borrow().get(key).cloned()
    }

    fn apply(&self, entries: Vec<LogEntry<T>>) {
        debug!("Applying the following entries: {:?}", entries);
        for entry in entries {
            self.store.borrow_mut().insert(entry.key, entry.value);
        }
    }
}

#[cfg(test)]

mod tests {
    use super::*;
    mod common {

        use std::{
            collections::{BTreeMap, HashSet},
            iter::zip,
        };

        use super::*;
        use storage::DirectFileOpsWriter;
        #[derive(Clone)]
        pub struct ClusterConfig {
            pub election_timeout: Duration,
            pub heartbeat_interval: Duration,
            pub ports: Vec<u64>,
        }

        pub struct TestCluster {
            pub nodes: Vec<RaftNode<i32, KeyValueStore<i32>, DirectFileOpsWriter>>,
            pub nodes_map:
                BTreeMap<ServerId, RaftNode<i32, KeyValueStore<i32>, DirectFileOpsWriter>>,
            pub message_queue: Vec<MessageWrapper<i32>>,
            pub connectivity: HashMap<ServerId, HashSet<ServerId>>,
            pub config: ClusterConfig,
        }

        impl TestCluster {
            pub fn tick(&mut self) {
                //  Collect all messages from nodes and store them in the central queue
                self.nodes.iter_mut().for_each(|node| {
                    let mut messages_from_node = node.rpc_manager.get_messages();
                    self.message_queue.append(&mut messages_from_node);
                });

                //  allow each node to tick
                self.nodes.iter_mut().for_each(|node| {
                    node.tick();
                });

                //  deliver all messages from the central queue
                self.message_queue
                    .drain(..)
                    .into_iter()
                    .for_each(|message| {
                        let node = self
                            .nodes
                            .iter()
                            .find(|node| node.id == message.to_node_id)
                            .unwrap();
                        //  check if these pair of nodes are partitioned
                        if self
                            .connectivity
                            .get_mut(&message.from_node_id)
                            .unwrap()
                            .contains(&message.to_node_id)
                        {
                            match node.to_node_sender.send(message) {
                                Ok(_) => (),
                                Err(e) => {
                                    panic!(
                                        "There was an error while sending the message to the node: {}",
                                        e
                                    )
                                }
                            };
                        }
                    });
                // since i am not mocking the network layer in tests, i need this for now to
                // simulate actual passage of time so that the TCP layer can actually deliver the
                // message
                // thread::sleep(Duration::from_millis(10));
            }

            pub fn tick_by(&mut self, tick_interval: u64) {
                for _ in 0..tick_interval {
                    self.tick();
                }
            }

            pub fn advance_time_by_for_node(&mut self, node_id: ServerId, duration: Duration) {
                let node = self
                    .nodes
                    .iter_mut()
                    .find(|node| node.id == node_id)
                    .expect(&format!("Could not find node with id: {}", node_id));
                node.advance_time_by(duration);
            }

            pub fn advance_time_by_variably(&mut self, duration: Duration) {
                //  for each node in the cluster, advance it's mock clock by the duration + some random variation
                for node in &mut self.nodes {
                    let jitter = rand::thread_rng().gen_range(0..50);
                    let new_duration = duration + Duration::from_millis(jitter);
                    node.advance_time_by(new_duration);
                }
            }

            pub fn advance_time_by(&mut self, duration: Duration) {
                //  for each node in the cluster, advance it's mock clock by the duration
                for node in &mut self.nodes {
                    node.advance_time_by(duration);
                }
            }

            pub fn transmit_message_to_all_nodes(&mut self, message: MessageWrapper<i32>) {
                for node in &mut self.nodes {
                    match node.to_node_sender.send(message.clone()) {
                        Ok(_) => (),
                        Err(e) => {
                            panic!(
                                "There was an error while sending the message to the node: {}",
                                e
                            );
                        }
                    }
                }
            }

            pub fn send_message_to_all_nodes(
                &mut self,
                message: MessageWrapper<i32>,
            ) -> Vec<Option<(RPCMessage<i32>, ServerId)>> {
                //  send this message to every node
                let mut responses: Vec<Option<(RPCMessage<i32>, ServerId)>> = Vec::new();
                for node in &mut self.nodes {
                    let response = node.receive(message.clone());
                    responses.push(response);
                }
                responses
            }

            pub fn get_by_id(
                &self,
                id: ServerId,
            ) -> &RaftNode<i32, KeyValueStore<i32>, DirectFileOpsWriter> {
                self.nodes.iter().find(|node| node.id == id).unwrap()
            }

            pub fn get_by_id_mut(
                &mut self,
                id: ServerId,
            ) -> &mut RaftNode<i32, KeyValueStore<i32>, DirectFileOpsWriter> {
                self.nodes.iter_mut().find(|node| node.id == id).unwrap()
            }

            pub fn has_leader(&self) -> bool {
                self.nodes.iter().filter(|node| node.is_leader()).count() == 1
            }

            pub fn has_leader_in_partition(&self, group: &[ServerId]) -> bool {
                let nodes: Vec<&RaftNode<i32, KeyValueStore<i32>, DirectFileOpsWriter>> = group
                    .iter()
                    .map(|node_id| self.nodes.iter().find(|node| node.id == *node_id).unwrap())
                    .collect();
                nodes.iter().filter(|node| node.is_leader()).count() == 1
            }

            pub fn get_leader(
                &self,
            ) -> Option<&RaftNode<i32, KeyValueStore<i32>, DirectFileOpsWriter>> {
                self.nodes.iter().filter(|node| node.is_leader()).last()
            }

            pub fn get_leader_mut(
                &mut self,
            ) -> Option<&mut RaftNode<i32, KeyValueStore<i32>, DirectFileOpsWriter>> {
                self.nodes.iter_mut().filter(|node| node.is_leader()).last()
            }

            pub fn get_leader_in_cluster(
                &self,
                group: &[ServerId],
            ) -> Option<&RaftNode<i32, KeyValueStore<i32>, DirectFileOpsWriter>> {
                self.nodes
                    .iter()
                    .filter(|node| group.contains(&node.id) && node.is_leader())
                    .last()
            }

            pub fn get_leader_mut_in_cluster(
                &mut self,
                group: &[ServerId],
            ) -> Option<&mut RaftNode<i32, KeyValueStore<i32>, DirectFileOpsWriter>> {
                self.nodes
                    .iter_mut()
                    .filter(|node| group.contains(&node.id) && node.is_leader())
                    .last()
            }

            pub fn get_follower_mut(
                &mut self,
            ) -> Option<&mut RaftNode<i32, KeyValueStore<i32>, DirectFileOpsWriter>> {
                self.nodes
                    .iter_mut()
                    .filter(|node| !node.is_leader())
                    .last()
            }

            pub fn get_all_followers(
                &self,
            ) -> Vec<&RaftNode<i32, KeyValueStore<i32>, DirectFileOpsWriter>> {
                self.nodes.iter().filter(|node| !node.is_leader()).collect()
            }

            pub fn drop_node(&mut self, id: ServerId) -> bool {
                let old_length = self.nodes.len();
                self.nodes.retain(|node| node.id != id);
                self.nodes.len() != old_length
            }

            pub fn send_client_request_partition(
                &mut self,
                key: String,
                value: i32,
                command: LogEntryCommand,
                group: &[ServerId],
            ) -> usize {
                if command == LogEntryCommand::Set {
                    let leader = self
                        .get_leader_mut_in_cluster(group)
                        .expect("Could not find leader in partitioned cluster");
                    leader.set_key_value_pair(key, value).unwrap()
                } else {
                    0
                }
            }

            pub fn send_client_request(
                &mut self,
                key: String,
                value: i32,
                command: LogEntryCommand,
            ) -> usize {
                if command == LogEntryCommand::Set {
                    let leader = self.get_leader_mut().expect("Could not find leader");
                    leader.set_key_value_pair(key, value).unwrap()
                } else {
                    0
                }
            }

            pub fn wait_for_stable_leader_partition(
                &mut self,
                max_ticks: u64,
                group: &[ServerId],
            ) -> bool {
                let mut ticks = 0;
                loop {
                    if ticks >= max_ticks {
                        break;
                    }
                    if self.has_leader_in_partition(group) {
                        return true;
                    }
                    self.tick();
                    ticks += 1;
                }
                false
            }

            pub fn wait_for_stable_leader(&mut self, max_ticks: u64) -> bool {
                let mut ticks = 0;
                loop {
                    if ticks >= max_ticks {
                        break;
                    }
                    if self.has_leader() {
                        return true;
                    }
                    self.tick();
                    ticks += 1;
                }
                false
            }

            pub fn wait_until_entry_applied_partition(
                &mut self,
                log_entry: &LogEntry<i32>,
                expected_index: usize,
                max_ticks: u64,
                group: &[ServerId],
            ) -> bool {
                let mut ticks = 0;
                while ticks < max_ticks {
                    let is_in_log = self
                        .nodes
                        .iter()
                        .filter(|node| group.contains(&node.id))
                        .all(|node| {
                            let state_guard = node.state.lock().unwrap();
                            state_guard.log.get(expected_index) == Some(&log_entry)
                        });
                    let key = &log_entry.key;
                    let is_applied = self
                        .nodes
                        .iter()
                        .filter(|node| group.contains(&node.id))
                        .all(|node| {
                            let r = node.state_machine.apply_get(key).is_some();
                            r
                        });
                    debug!("is_in_log: {}, is_applied: {}", is_in_log, is_applied);
                    if is_in_log && is_applied {
                        info!(
                            "Took {} ticks to apply the entry across the partitioned cluster",
                            ticks
                        );
                        return true;
                    }
                    self.tick();
                    ticks += 1;
                }
                false
            }

            pub fn wait_until_entry_applied(
                &mut self,
                log_entry: &LogEntry<i32>,
                expected_index: usize,
                max_ticks: u64,
            ) -> bool {
                let mut ticks = 0;
                while ticks < max_ticks {
                    let is_in_log = self.nodes.iter().all(|node| {
                        let state_guard = node.state.lock().unwrap();
                        state_guard.log.get(expected_index) == Some(&log_entry)
                    });
                    let key = &log_entry.key;
                    let is_applied = self.nodes.iter().all(|node| {
                        let r = node.state_machine.apply_get(key).is_some();
                        r
                    });
                    debug!("is_in_log: {}, is_applied: {}", is_in_log, is_applied);
                    if is_in_log && is_applied {
                        info!("Took {} ticks to apply the entry across the cluster", ticks);
                        return true;
                    }
                    self.tick();
                    ticks += 1;
                }
                false
            }

            pub fn apply_entries_across_cluster_partition(
                &mut self,
                log_entries: Vec<&LogEntry<i32>>,
                group: &[ServerId],
                max_ticks: u64,
            ) {
                for log_entry in log_entries {
                    let log_index = self.send_client_request_partition(
                        log_entry.key.clone(),
                        log_entry.value,
                        LogEntryCommand::Set,
                        group,
                    );

                    let applied = self
                        .wait_until_entry_applied_partition(log_entry, log_index, max_ticks, group);
                    if !applied {
                        panic!("Could not apply the log entry across the partitioned cluster");
                    }
                }
            }

            pub fn apply_entries_across_cluster(
                &mut self,
                log_entries: Vec<&LogEntry<i32>>,
                max_ticks: u64,
            ) {
                for log_entry in log_entries {
                    let log_index = self.send_client_request(
                        log_entry.key.clone(),
                        log_entry.value,
                        LogEntryCommand::Set,
                    );

                    let applied = self.wait_until_entry_applied(log_entry, log_index, max_ticks);
                    if !applied {
                        panic!("Could not apply the log entry across the cluster");
                    }
                }
            }

            pub fn verify_logs_across_cluster_partition_for(
                &mut self,
                expected_log_entries: Vec<&LogEntry<i32>>,
                max_ticks: u64,
                group: &[ServerId],
            ) -> bool {
                let mut ticks = 0;
                while ticks < max_ticks {
                    let is_in_log = self
                        .nodes
                        .iter()
                        .filter(|node| group.contains(&node.id))
                        .all(|node| {
                            let state_guard = node.state.lock().unwrap();
                            // state_guard.log == expected_log_entries
                            zip(&state_guard.log, &expected_log_entries)
                                .into_iter()
                                .all(|(log_1, expected_log_1)| log_1 == *expected_log_1)
                        });
                    if is_in_log {
                        info!(
                            "Took {} ticks to verify the logs across the partitioned cluster",
                            ticks
                        );
                        return true;
                    }
                    self.tick();
                    ticks += 1;
                }
                false
            }

            pub fn verify_logs_across_cluster_for(
                &mut self,
                expected_log_entries: Vec<&LogEntry<i32>>,
                max_ticks: u64,
            ) -> bool {
                let mut ticks = 0;
                while ticks < max_ticks {
                    let is_in_log = self.nodes.iter().all(|node| {
                        let state_guard = node.state.lock().unwrap();
                        // state_guard.log == expected_log_entries
                        zip(&state_guard.log, &expected_log_entries)
                            .into_iter()
                            .all(|(log_1, expected_log_1)| log_1 == *expected_log_1)
                    });
                    if is_in_log {
                        info!("Took {} ticks to verify the logs across the cluster", ticks);
                        return true;
                    }
                    self.tick();
                    ticks += 1;
                }
                false
            }

            /// Separates the cluster into two smaller clusters where only the nodes within
            /// each cluster can communicate between themselves
            pub fn partition(&mut self, group1: &[ServerId], group2: &[ServerId]) {
                for &node_id in group1 {
                    self.connectivity
                        .get_mut(&node_id)
                        .unwrap()
                        .retain(|&id| group1.contains(&id));
                }
                for &node_id in group2 {
                    self.connectivity
                        .get_mut(&node_id)
                        .unwrap()
                        .retain(|&id| group2.contains(&id));
                }
                debug!(
                    "The connectivity after partitioning: {:?}",
                    self.connectivity
                );
            }

            /// Removes any partition in the cluster and restores full connectivity among all nodes
            pub fn heal_partition(&mut self) {
                let all_node_ids = self
                    .nodes
                    .iter()
                    .map(|node| node.id)
                    .collect::<HashSet<ServerId>>();
                for node_id in all_node_ids.iter() {
                    self.connectivity.insert(*node_id, all_node_ids.clone());
                }
            }

            pub fn query_state_machine_across_nodes<'a>(
                &'a self,
                keys: Vec<&'a str>,
            ) -> HashMap<ServerId, Vec<(&str, Option<i32>)>> {
                let mut hm: HashMap<ServerId, Vec<(&str, Option<i32>)>> = HashMap::new();
                for node in &self.nodes {
                    let key_value_pairs = node.query_state_machine(&keys);
                    hm.insert(node.id, key_value_pairs);
                }
                hm
            }

            pub fn new(number_of_nodes: u64, config: ClusterConfig) -> Self {
                let mut nodes: Vec<RaftNode<i32, KeyValueStore<i32>, DirectFileOpsWriter>> =
                    Vec::new();
                let nodes_map: BTreeMap<
                    ServerId,
                    RaftNode<i32, KeyValueStore<i32>, DirectFileOpsWriter>,
                > = BTreeMap::new();

                let node_ids: Vec<ServerId> = (0..number_of_nodes).collect();
                let addresses: Vec<String> = config
                    .ports
                    .iter()
                    .map(|port| format!("127.0.0.1:{}", port))
                    .collect();
                let mut id_to_address_mapping: HashMap<ServerId, String> = HashMap::new();
                for (node_id, address) in node_ids.iter().zip(addresses.iter()) {
                    id_to_address_mapping.insert(*node_id, address.clone());
                }

                let mut counter = 0;
                for (node_id, address) in node_ids.iter().zip(addresses.iter()) {
                    let server_config = ServerConfig {
                        election_timeout: config.election_timeout,
                        heartbeat_interval: config.heartbeat_interval,
                        address: address.clone(),
                        port: config.ports[counter],
                        cluster_nodes: node_ids.clone(),
                        id_to_address_mapping: id_to_address_mapping.clone(),
                    };

                    let state_machine = KeyValueStore::<i32>::new();
                    let persistence_manager = DirectFileOpsWriter::new("data", *node_id).unwrap();
                    let (to_node_sender, from_rpc_receiver) =
                        mpsc::channel::<MessageWrapper<i32>>();
                    let rpc_manager = CommunicationLayer::MockRPCManager(MockRPCManager::new(
                        *node_id,
                        to_node_sender.clone(),
                    ));
                    let mock_clock = RaftNodeClock::MockClock(MockClock {
                        current_time: Instant::now(),
                    });

                    let node = RaftNode::new(
                        *node_id,
                        state_machine,
                        server_config,
                        node_ids.clone(),
                        persistence_manager,
                        rpc_manager,
                        to_node_sender,
                        from_rpc_receiver,
                        mock_clock,
                    );
                    nodes.push(node);
                    // nodes_map.insert(*node_id, node);    //  TODO: Cannot clone node here. Need to get rid of vector before BTreeMap can be used
                    counter += 1;
                }
                let message_queue = Vec::new();

                let mut connectivity_hm: HashMap<ServerId, HashSet<ServerId>> = HashMap::new();
                for node_id in &node_ids {
                    connectivity_hm.insert(*node_id, node_ids.clone().into_iter().collect());
                }

                TestCluster {
                    nodes,
                    nodes_map,
                    message_queue,
                    connectivity: connectivity_hm,
                    config,
                }
            }

            pub fn start(&mut self) {
                for node in &mut self.nodes {
                    node.start();
                }
            }

            pub fn stop(&self) {
                for node in &self.nodes {
                    node.stop();
                }
            }
        }
    }

    mod single_node_cluster {
        use super::common::*;
        use super::*;

        const ELECTION_TIMEOUT: Duration = Duration::from_millis(150);
        const HEARTBEAT_INTERVAL: Duration = Duration::from_millis(50);
        const MAX_TICKS: u64 = 100;

        /// This test checks whether a node in a single cluster will become leader as soon as the election timeout is reached
        #[test]
        fn leader_election() {
            let _ = env_logger::builder().is_test(true).try_init();
            //  create a cluster with a single node first
            let cluster_config = ClusterConfig {
                election_timeout: ELECTION_TIMEOUT,
                heartbeat_interval: HEARTBEAT_INTERVAL,
                ports: vec![8000],
            };
            let mut cluster = TestCluster::new(1, cluster_config);
            cluster.start();
            cluster.advance_time_by(ELECTION_TIMEOUT + Duration::from_millis(100 + 5)); //  picking 255 here because 150 + a max jitter of 100 guarantees that election has timed out
            cluster.wait_for_stable_leader(MAX_TICKS);
            cluster.stop();
            assert_eq!(cluster.has_leader(), true);
        }

        /// This test checks the functionality of a node receiving a vote request
        #[test]
        fn request_vote_success() {
            let _ = env_logger::builder().is_test(true).try_init();
            let cluster_config = ClusterConfig {
                election_timeout: ELECTION_TIMEOUT,
                heartbeat_interval: HEARTBEAT_INTERVAL,
                ports: vec![8000],
            };
            let mut cluster = TestCluster::new(1, cluster_config);

            let request = VoteRequest {
                term: 1,
                candidate_id: 1,
                last_log_index: 0,
                last_log_term: 0,
            };
            let message = RPCMessage::VoteRequest(request);
            let message_wrapper = MessageWrapper {
                from_node_id: 1,
                to_node_id: 0,
                message,
            };

            let response = cluster.send_message_to_all_nodes(message_wrapper);

            let vote_response = match response.get(0).unwrap() {
                Some((RPCMessage::VoteResponse(response), _)) => response,
                _ => panic!("The response is not a vote response"),
            };

            assert_eq!(
                *vote_response,
                VoteResponse {
                    term: 1,
                    vote_granted: true,
                    candidate_id: 0
                }
            );
        }

        /// This test asserts that a node refuses to grant a vote because its own log
        /// is ahead of the candidates log
        #[test]
        fn request_vote_fail_log_check() {
            let _ = env_logger::builder().is_test(true).try_init();
            let cluster_config = ClusterConfig {
                election_timeout: ELECTION_TIMEOUT,
                heartbeat_interval: HEARTBEAT_INTERVAL,
                ports: vec![8000],
            };
            let mut cluster = TestCluster::new(1, cluster_config);

            let node = cluster.get_by_id_mut(0);
            let mut node_state = node.state.lock().unwrap();
            node_state.current_term = 2;
            node_state.log.push(LogEntry {
                term: 1,
                index: 1,
                command: LogEntryCommand::Set,
                key: "a".to_string(),
                value: 1,
            });
            node_state.log.push(LogEntry {
                term: 2,
                index: 2,
                command: LogEntryCommand::Set,
                key: "b".to_string(),
                value: 2,
            });
            drop(node_state);

            let request = VoteRequest {
                term: 1,
                candidate_id: 1,
                last_log_index: 1,
                last_log_term: 1,
            };
            let message = RPCMessage::VoteRequest(request);
            let message_wrapper = MessageWrapper {
                from_node_id: 1,
                to_node_id: 0,
                message,
            };

            let response = cluster.send_message_to_all_nodes(message_wrapper);
            let vote_response = match response.get(0).unwrap() {
                Some((RPCMessage::VoteResponse(response), _)) => response,
                _ => panic!("The response is not a vote response"),
            };
            assert_eq!(
                *vote_response,
                VoteResponse {
                    term: 2,
                    vote_granted: false,
                    candidate_id: 0
                }
            );
        }

        #[test]
        fn append_entries_success() {
            let _ = env_logger::builder().is_test(true).try_init();
            let cluster_config = ClusterConfig {
                election_timeout: ELECTION_TIMEOUT,
                heartbeat_interval: HEARTBEAT_INTERVAL,
                ports: vec![8000],
            };
            let mut cluster = TestCluster::new(1, cluster_config);

            {
                let node = cluster.get_by_id_mut(0);
                let mut node_state = node.state.lock().unwrap();
                node_state.current_term = 1;
                node_state.voted_for = Some(1);
            }

            let request = AppendEntriesRequest {
                request_id: 1,
                term: 1,
                leader_id: 1,
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![LogEntry {
                    term: 1,
                    index: 1,
                    command: LogEntryCommand::Set,
                    key: "a".to_string(),
                    value: 1,
                }],
                leader_commit_index: 0,
            };
            let message = RPCMessage::AppendEntriesRequest(request);
            let message_wrapper = MessageWrapper {
                from_node_id: 1,
                to_node_id: 0,
                message,
            };

            let response = cluster.send_message_to_all_nodes(message_wrapper);
            let append_response = match response.get(0).unwrap() {
                Some((RPCMessage::AppendEntriesResponse(response), _)) => response,
                _ => panic!("The response is not an append entries response"),
            };
            assert_eq!(
                *append_response,
                AppendEntriesResponse {
                    request_id: 1,
                    term: 1,
                    success: true,
                    server_id: 0,
                    match_index: 1
                }
            );

            {
                let node = cluster.get_by_id_mut(0);
                let node_state = node.state.lock().unwrap();
                assert_eq!(node_state.log.len(), 1);
            }
        }

        #[test]
        fn restore_from_durable_storage() {
            let _ = env_logger::builder().is_test(true).try_init();
            let cluster_config = ClusterConfig {
                election_timeout: ELECTION_TIMEOUT,
                heartbeat_interval: HEARTBEAT_INTERVAL,
                ports: vec![8000],
            };
            let mut cluster = TestCluster::new(1, cluster_config);

            let vote_request = VoteRequest {
                term: 1,
                candidate_id: 1,
                last_log_index: 0,
                last_log_term: 0,
            };
            let vote_message: RPCMessage<i32> = RPCMessage::VoteRequest(vote_request);
            let vote_message_wrapper = MessageWrapper {
                from_node_id: 1,
                to_node_id: 0,
                message: vote_message,
            };

            cluster.send_message_to_all_nodes(vote_message_wrapper);

            let request = AppendEntriesRequest {
                request_id: 1,
                term: 1,
                leader_id: 1,
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![LogEntry {
                    term: 1,
                    index: 1,
                    command: LogEntryCommand::Set,
                    key: "a".to_string(),
                    value: 1,
                }],
                leader_commit_index: 0,
            };
            let message = RPCMessage::AppendEntriesRequest(request);
            let message_wrapper = MessageWrapper {
                from_node_id: 1,
                to_node_id: 0,
                message,
            };

            cluster.send_message_to_all_nodes(message_wrapper);

            let node = cluster.get_by_id_mut(0);
            node.stop();
            node.restart();
            let log_length = node.state.lock().unwrap().log.len();
            let term = node.state.lock().unwrap().current_term;
            let voted_for = node.state.lock().unwrap().voted_for.clone();
            cluster.stop();
            assert_eq!(log_length, 1);
            assert_eq!(term, 1);
            assert_eq!(voted_for, Some(1));
        }

        /// This test will determine whether a leader correctly advances its commit index
        /// based on the list of match indexes it maintains for each follower
        #[test]
        fn advance_commit_index_of_leader() {
            let _ = env_logger::builder().is_test(true).try_init();
            let cluster_config = ClusterConfig {
                election_timeout: ELECTION_TIMEOUT,
                heartbeat_interval: HEARTBEAT_INTERVAL,
                ports: vec![8000],
            };
            let mut cluster = TestCluster::new(1, cluster_config);
            cluster.start();
            cluster.advance_time_by(ELECTION_TIMEOUT + Duration::from_millis(50));
            cluster.wait_for_stable_leader(MAX_TICKS);
            assert_eq!(cluster.has_leader(), true);
            let leader_node = cluster.get_leader().unwrap();
            {
                let mut state_guard = leader_node.state.lock().unwrap();
                state_guard.log.push(LogEntry {
                    term: 1,
                    index: 1,
                    command: LogEntryCommand::Set,
                    key: "a".to_string(),
                    value: 1,
                });
                state_guard.log.push(LogEntry {
                    term: 1,
                    index: 2,
                    command: LogEntryCommand::Set,
                    key: "b".to_string(),
                    value: 2,
                });
                state_guard.commit_index = 0;
                state_guard.match_index = vec![2];
                leader_node.advance_commit_index(&mut state_guard);
                assert_eq!(state_guard.commit_index, 2);
            }
        }

        /// This test asserts that a node that receives a set of log entries that is in conflict with its own log entries, then it will find the conflict
        /// point and truncate all log entries from that point onwards before writing the new log entries
        #[test]
        fn log_conflict() {
            let _ = env_logger::builder().is_test(true).try_init();
            let cluster_config = ClusterConfig {
                election_timeout: ELECTION_TIMEOUT,
                heartbeat_interval: HEARTBEAT_INTERVAL,
                ports: vec![8000],
            };
            let entries_on_node = vec![
                LogEntry {
                    term: 1,
                    index: 1,
                    command: LogEntryCommand::Set,
                    key: "a".to_string(),
                    value: 1,
                },
                LogEntry {
                    term: 1,
                    index: 2,
                    command: LogEntryCommand::Set,
                    key: "b".to_string(),
                    value: 1,
                },
                LogEntry {
                    term: 1,
                    index: 3,
                    command: LogEntryCommand::Set,
                    key: "c".to_string(),
                    value: 2,
                },
            ];
            let mut cluster = TestCluster::new(1, cluster_config);
            {
                let node = cluster.get_by_id_mut(0);
                node.state.lock().unwrap().log = entries_on_node;
            }

            let new_entries_sent_to_node = vec![
                LogEntry {
                    term: 2,
                    index: 2,
                    command: LogEntryCommand::Set,
                    key: "d".to_string(),
                    value: 1,
                },
                LogEntry {
                    term: 2,
                    index: 3,
                    command: LogEntryCommand::Set,
                    key: "d".to_string(),
                    value: 2,
                },
            ];
            let request = AppendEntriesRequest {
                request_id: 1,
                term: 2,
                leader_id: 1,
                prev_log_index: 1,
                prev_log_term: 1,
                entries: new_entries_sent_to_node,
                leader_commit_index: 1,
            };
            let message = RPCMessage::AppendEntriesRequest(request);
            let message_wrapper = MessageWrapper {
                from_node_id: 1,
                to_node_id: 0,
                message,
            };
            cluster.send_message_to_all_nodes(message_wrapper);

            //  check that the log of the node is correct here now
            let resolved_log = vec![
                LogEntry {
                    term: 1,
                    index: 1,
                    command: LogEntryCommand::Set,
                    key: "a".to_string(),
                    value: 1,
                },
                LogEntry {
                    term: 2,
                    index: 2,
                    command: LogEntryCommand::Set,
                    key: "d".to_string(),
                    value: 1,
                },
                LogEntry {
                    term: 2,
                    index: 3,
                    command: LogEntryCommand::Set,
                    key: "d".to_string(),
                    value: 2,
                },
            ];
            {
                let node = cluster.get_by_id(0);
                let log = &node.state.lock().unwrap().log;
                info!("Log entries: {:?}", log);
                assert_eq!(log.len(), 3);
                assert_eq!(*log, resolved_log);
            }
        }
    }

    mod two_node_cluster {
        use super::common::*;
        use super::*;

        const ELECTION_TIMEOUT: Duration = Duration::from_millis(150);
        const HEARTBEAT_INTERVAL: Duration = Duration::from_millis(50);
        const MAX_TICKS: u64 = 100;

        /// Assert that in a 2-node cluster, the more advanced node becomes leader
        /// after a few ticks
        #[test]
        fn leader_election() {
            let _ = env_logger::builder().is_test(true).try_init();
            let cluster_config = ClusterConfig {
                election_timeout: ELECTION_TIMEOUT,
                heartbeat_interval: HEARTBEAT_INTERVAL,
                ports: vec![8000, 8001],
            };
            let mut cluster = TestCluster::new(2, cluster_config);
            cluster.start();
            let node = cluster.get_by_id_mut(0);
            node.advance_time_by(ELECTION_TIMEOUT + Duration::from_millis(50));
            cluster.wait_for_stable_leader(MAX_TICKS);
            cluster.stop();
            assert_eq!(cluster.has_leader(), true);
        }

        /// This test will determine whether an elected leader sends heartbeats regularly during its term
        /// It does so by checking that the leader and term haven't changed after the iterations are done
        /// This indicates that a leader regularly sent heartbeats which prevented an election timeout
        #[test]
        fn leader_send_heartbeats() {
            let _ = env_logger::builder().is_test(true).try_init();
            let cluster_config = ClusterConfig {
                election_timeout: ELECTION_TIMEOUT,
                heartbeat_interval: HEARTBEAT_INTERVAL,
                ports: vec![8000, 8001],
            };
            let mut cluster = TestCluster::new(2, cluster_config);
            cluster.start();
            let node = cluster.get_by_id_mut(0);
            node.advance_time_by(ELECTION_TIMEOUT + Duration::from_millis(100));
            cluster.wait_for_stable_leader(MAX_TICKS);
            let (leader_id, leader_term) = {
                let leader = cluster.get_leader().unwrap();
                let leader_term = leader.state.lock().unwrap().current_term;
                (leader.id, leader_term)
            };
            cluster.tick_by(MAX_TICKS);
            let new_leader = cluster.get_leader().unwrap();
            let new_leader_term = new_leader.state.lock().unwrap().current_term;
            cluster.stop();
            assert_eq!(new_leader.id, leader_id);
            assert_eq!(new_leader_term, leader_term);
        }

        /// This test will determine whether an elected leader will correctly send a key-value pair it receives from a client
        /// to all its followers. The leader should also append the entry to its own log before that.
        #[test]
        fn apply_entries() {
            let _ = env_logger::builder().is_test(true).try_init();
            let cluster_config = ClusterConfig {
                election_timeout: ELECTION_TIMEOUT,
                heartbeat_interval: HEARTBEAT_INTERVAL,
                ports: vec![8000, 8001],
            };
            let mut cluster = TestCluster::new(2, cluster_config);
            cluster.start();
            cluster.advance_time_by(ELECTION_TIMEOUT);
            cluster.wait_for_stable_leader(MAX_TICKS);
            let (log_entry, entry_index) = {
                let leader = cluster.get_leader_mut().unwrap();
                let entry_index = leader.set_key_value_pair("a".to_string(), 1).unwrap();
                let log_entry = LogEntry {
                    term: 1,
                    index: 1,
                    command: LogEntryCommand::Set,
                    key: "a".to_string(),
                    value: 1,
                };
                let leader_state = leader.state.lock().unwrap();
                assert_eq!(leader_state.log.len(), 1);
                assert_eq!(leader_state.log.get(0), Some(&log_entry));
                (log_entry, entry_index)
            };
            let result = cluster.wait_until_entry_applied(&log_entry, entry_index, MAX_TICKS);
            assert_eq!(result, true);
            cluster.stop();
        }
    }

    mod three_node_cluster {
        use super::common::*;
        use super::*;

        const ELECTION_TIMEOUT: Duration = Duration::from_millis(150);
        const HEARTBEAT_INTERVAL: Duration = Duration::from_millis(50);
        const MAX_TICKS: u64 = 100;

        /// Assert that in a 3-node cluster one of the nodes eventually becomes leader within a certain number of ticks
        #[test]
        fn leader_election() {
            let _ = env_logger::builder().is_test(true).try_init();
            let cluster_config = ClusterConfig {
                election_timeout: ELECTION_TIMEOUT,
                heartbeat_interval: HEARTBEAT_INTERVAL,
                ports: vec![8000, 8001, 8002],
            };
            let mut cluster = TestCluster::new(3, cluster_config);
            cluster.advance_time_by(ELECTION_TIMEOUT);
            cluster.start();
            cluster.wait_for_stable_leader(MAX_TICKS);
            cluster.stop();
            assert_eq!(cluster.has_leader(), true);
        }

        /// This test will determine whether an elected leader sends heartbeats regularly during its term
        /// It does so by checking that the leader and term haven't changed after the iterations are done
        /// This indicates that a leader regularly sent heartbeats which prevented an election timeout
        #[test]
        fn leader_send_heartbeats() {
            let _ = env_logger::builder().is_test(true).try_init();
            let cluster_config = ClusterConfig {
                election_timeout: ELECTION_TIMEOUT,
                heartbeat_interval: HEARTBEAT_INTERVAL,
                ports: vec![8000, 8001, 8002],
            };
            let mut cluster = TestCluster::new(3, cluster_config);
            cluster.start();
            cluster.advance_time_by(ELECTION_TIMEOUT);
            cluster.wait_for_stable_leader(MAX_TICKS);
            let (current_leader_id, current_leader_term) = {
                let leader = cluster.get_leader().unwrap();
                let term = leader.state.lock().unwrap().current_term;
                (leader.id, term)
            };
            cluster.tick_by(MAX_TICKS);
            let new_leader = cluster.get_leader().unwrap();
            let new_leader_term = new_leader.state.lock().unwrap().current_term;
            assert_eq!(current_leader_id, new_leader.id);
            assert_eq!(current_leader_term, new_leader_term);
        }

        /// This test will check whether a key value pair submitted by a client is appended to the logs of the nodes in the cluster
        /// and replicated across the state machines of the cluster
        #[test]
        fn apply_entries() {
            let _ = env_logger::builder().is_test(true).try_init();
            let cluster_config = ClusterConfig {
                election_timeout: ELECTION_TIMEOUT,
                heartbeat_interval: HEARTBEAT_INTERVAL,
                ports: vec![8000, 8001, 8002],
            };
            let mut cluster = TestCluster::new(3, cluster_config);
            cluster.start();
            cluster.advance_time_by_for_node(0, ELECTION_TIMEOUT);
            cluster.wait_for_stable_leader(MAX_TICKS);
            let current_leader_term = cluster
                .get_leader()
                .unwrap()
                .state
                .lock()
                .unwrap()
                .current_term;
            let last_log_index = cluster
                .get_leader()
                .unwrap()
                .state
                .lock()
                .unwrap()
                .log
                .last()
                .map_or(0, |entry| entry.index);
            let log_entry = LogEntry {
                term: current_leader_term,
                index: last_log_index + 1,
                command: LogEntryCommand::Set,
                key: "a".to_string(),
                value: 1,
            };
            cluster.apply_entries_across_cluster(vec![&log_entry], MAX_TICKS);
            cluster.verify_logs_across_cluster_for(vec![&log_entry], MAX_TICKS);
        }

        /// This test models the scenario where a follower has an additional entry that has not been committed across the cluster and is absent on the leader
        /// The situation in which this could potentially come up is when there are three nodes - A, B & C. C is the leader and receives a client request
        /// and appends it to its own log but then crashes before it can replicate it. A becomes the leader and C comes back online. However, C now has an
        /// additional log entry that is not present on the leader

        /// This test models the scenario where a follower has a log entry that is out of sync with the leader. When the leader sends a heartbeat, it will
        /// recognize this and attempt to fix the incorrect log entry by rewinding the log back to a point where there is no conflict
        #[test]
        fn retry_failed_append_entry_request() {
            let _ = env_logger::builder().is_test(true).try_init();
            let cluster_config = ClusterConfig {
                election_timeout: ELECTION_TIMEOUT,
                heartbeat_interval: HEARTBEAT_INTERVAL,
                ports: vec![8000, 8001, 8002],
            };
            let mut cluster = TestCluster::new(3, cluster_config);
            cluster.start();
            cluster.advance_time_by_for_node(0, ELECTION_TIMEOUT);
            cluster.wait_for_stable_leader(MAX_TICKS);
            assert_eq!(cluster.has_leader(), true);

            let current_leader_term = cluster
                .get_leader()
                .unwrap()
                .state
                .lock()
                .unwrap()
                .current_term;
            let mut last_log_index = cluster
                .get_leader()
                .unwrap()
                .state
                .lock()
                .unwrap()
                .log
                .last()
                .map_or(0, |entry| entry.index);
            last_log_index += 1;
            let log_entry_1 = LogEntry {
                term: current_leader_term,
                index: last_log_index,
                command: LogEntryCommand::Set,
                key: "a".to_string(),
                value: 1,
            };
            last_log_index += 1;
            let log_entry_2 = LogEntry {
                term: current_leader_term,
                index: last_log_index,
                command: LogEntryCommand::Set,
                key: "b".to_string(),
                value: 1,
            };

            cluster.apply_entries_across_cluster(vec![&log_entry_1, &log_entry_2], MAX_TICKS);
            cluster.verify_logs_across_cluster_for(vec![&log_entry_1, &log_entry_2], MAX_TICKS);

            debug!("Healing starts here!");
            //  now change the log on one of the followers so that the log check fails
            {
                let follower_node = cluster.get_follower_mut().unwrap();
                let mut follower_node_state_guard = follower_node.state.lock().unwrap();
                debug!("Changing log for node {}", follower_node.id);
                follower_node_state_guard.log.pop();
                follower_node_state_guard.log.push(LogEntry {
                    term: 2,
                    index: 3,
                    command: LogEntryCommand::Set,
                    key: "c".to_string(),
                    value: 2,
                });
            }

            //  wait for the cluster to "heal" and assert that the logs are up to date
            let expected_log_entries = vec![&log_entry_1, &log_entry_2];
            let result = cluster.verify_logs_across_cluster_for(expected_log_entries, MAX_TICKS);
            cluster.stop();
            assert_eq!(result, true);
        }

        /// This test models the scenario where the leader in a cluster is network partitioned from the
        /// rest of the cluster and a new leader is elected
        #[test]
        fn network_partition_new_leader() {
            let _ = env_logger::builder().is_test(true).try_init();
            let cluster_config = ClusterConfig {
                election_timeout: ELECTION_TIMEOUT,
                heartbeat_interval: HEARTBEAT_INTERVAL,
                ports: vec![8000, 8001, 8002],
            };
            let mut cluster = TestCluster::new(3, cluster_config);
            cluster.start();
            cluster.advance_time_by_for_node(0, ELECTION_TIMEOUT);
            cluster.wait_for_stable_leader(MAX_TICKS);

            //  partition the leader from the rest of the group
            debug!("PARTITION HERE");
            let group1 = &[cluster.get_leader().unwrap().id];
            let group2 = &cluster
                .get_all_followers()
                .iter()
                .map(|node| node.id)
                .collect::<Vec<ServerId>>();
            cluster.partition(group1, group2);
            cluster.advance_time_by_for_node(1, ELECTION_TIMEOUT + Duration::from_millis(100));
            cluster.wait_for_stable_leader_partition(MAX_TICKS, group2);
            cluster.stop();
            assert_eq!(cluster.has_leader_in_partition(group2), true);
        }

        /// The test models the following scenario
        /// 1. A leader is elected
        /// 2. Client requests are received and replicated
        /// 3. A partition occurs and a new leader is elected
        /// 4. Clients requests are processed by the new leader
        /// 5. The partition heals and the old leader rejoins the cluster
        /// 6. The old leader must recognize its a follower and get caught up with the new leader

        #[test]
        fn network_partition_log_healing() {
            let _ = env_logger::builder().is_test(true).try_init();
            let cluster_config = ClusterConfig {
                election_timeout: ELECTION_TIMEOUT,
                heartbeat_interval: HEARTBEAT_INTERVAL,
                ports: vec![8000, 8001, 8002],
            };
            let mut cluster = TestCluster::new(3, cluster_config);

            //  cluster starts and a leader is elected
            cluster.start();
            cluster.advance_time_by_for_node(0, ELECTION_TIMEOUT);
            cluster.wait_for_stable_leader(MAX_TICKS);
            assert_eq!(cluster.has_leader(), true);

            //  client requests are received and replicated across cluster
            debug!("Applying entries");
            let current_leader_term = cluster
                .get_leader()
                .unwrap()
                .state
                .lock()
                .unwrap()
                .current_term;
            let mut last_log_index = cluster
                .get_leader()
                .unwrap()
                .state
                .lock()
                .unwrap()
                .log
                .last()
                .map_or(0, |entry| entry.index);
            last_log_index += 1;
            let log_entry_1 = LogEntry {
                term: current_leader_term,
                index: last_log_index,
                command: LogEntryCommand::Set,
                key: "a".to_string(),
                value: 1,
            };
            last_log_index += 1;
            let log_entry_2 = LogEntry {
                term: current_leader_term,
                index: last_log_index,
                command: LogEntryCommand::Set,
                key: "b".to_string(),
                value: 2,
            };
            cluster.apply_entries_across_cluster(vec![&log_entry_1, &log_entry_2], MAX_TICKS);
            cluster.verify_logs_across_cluster_for(vec![&log_entry_1, &log_entry_2], MAX_TICKS);

            //  partition and new leader election here
            let group1 = &[cluster.get_leader().unwrap().id];
            let group2 = &cluster
                .get_all_followers()
                .iter()
                .map(|node| node.id)
                .collect::<Vec<ServerId>>();
            debug!("PARTITION HERE");
            cluster.partition(group1, group2);
            cluster.advance_time_by_for_node(1, ELECTION_TIMEOUT + Duration::from_millis(100));
            cluster.wait_for_stable_leader_partition(MAX_TICKS, group2);
            assert_eq!(cluster.has_leader_in_partition(group2), true);

            //  send requests to group2 leader
            let log_entry_3 = {
                debug!("GROUP2 REQUESTS");
                let current_leader_term = cluster
                    .get_leader_in_cluster(group2)
                    .unwrap()
                    .state
                    .lock()
                    .unwrap()
                    .current_term;
                let mut last_log_index = cluster
                    .get_leader()
                    .unwrap()
                    .state
                    .lock()
                    .unwrap()
                    .log
                    .last()
                    .map_or(0, |entry| entry.index);
                last_log_index += 1;
                let log_entry_3 = LogEntry {
                    term: current_leader_term,
                    index: last_log_index,
                    command: LogEntryCommand::Set,
                    key: "c".to_string(),
                    value: 3,
                };
                cluster.apply_entries_across_cluster_partition(
                    vec![&log_entry_3],
                    group2,
                    MAX_TICKS,
                );
                log_entry_3
            };

            //  partition heals and old leader rejoins the cluster
            debug!("PARTITION HEALS HERE");
            let leader_id = cluster.get_leader_in_cluster(group2).unwrap().id;
            cluster.heal_partition();
            cluster.tick_by(MAX_TICKS);
            cluster.wait_for_stable_leader(MAX_TICKS);
            assert_eq!(cluster.get_leader().unwrap().id, leader_id);
            cluster.verify_logs_across_cluster_for(
                vec![&log_entry_1, &log_entry_2, &log_entry_3],
                MAX_TICKS,
            );
        }
    }
}
