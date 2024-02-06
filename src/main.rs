use clock::{Clock, MockClock, RealClock};
use core::fmt;
use serde::de::DeserializeOwned;
use serde_json;
use std::cell::RefCell;
use std::cmp::min;
use std::io::{self, BufRead, Write};
use std::marker::PhantomData;
use std::net::{TcpListener, TcpStream};
use std::str::FromStr;
use std::sync::atomic::AtomicBool;
use std::sync::{mpsc, Arc, Mutex, MutexGuard};
use std::thread;
use std::time::{Duration, Instant};
use std::{collections::HashMap, io::BufReader};
use types::RaftTypeTrait;

use log::{error, info};
use rand::Rng;
use serde::{Deserialize, Serialize};

mod clock;
mod storage;
mod types;

use crate::types::{ServerId, Term};
use storage::{DirectFileOpsWriter, RaftFileOps};

/**
 * RPC Stuff
 */

struct RPCManager<T: RaftTypeTrait> {
    server_id: ServerId,
    server_address: String,
    port: u64,
    to_node_sender: mpsc::Sender<RPCMessage<T>>,
    is_running: Arc<AtomicBool>,
    _marker: PhantomData<T>,
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
            _marker: PhantomData,
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
    fn send_message(
        &self,
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

#[derive(Serialize, Deserialize, Clone)]
enum RPCMessage<T: Clone> {
    VoteRequest(VoteRequest),
    VoteResponse(VoteResponse),
    AppendEntriesRequest(AppendEntriesRequest<T>),
    AppendEntriesResponse(AppendEntriesResponse),
}
#[derive(Serialize, Deserialize, Debug, Clone)]
struct VoteRequest {
    term: Term,
    candidate_id: ServerId,
    last_log_index: u64,
    last_log_term: Term,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct VoteResponse {
    term: Term,
    vote_granted: bool,
    candidate_id: ServerId, //  the id of the server granting the vote, used for de-duplication
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct AppendEntriesRequest<T: Clone> {
    term: Term,
    leader_id: ServerId,
    prev_log_index: u64,
    prev_log_term: Term,
    entries: Vec<LogEntry<T>>,
    leader_commit_index: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct AppendEntriesResponse {
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
    fn apply_get(&self, key: String) -> Option<T>;
    fn apply(&self, entries: Vec<LogEntry<T>>);
}

#[derive(Serialize, Deserialize, Debug, Clone)]
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

#[derive(Serialize, Deserialize, Debug, Clone)]
struct LogEntry<T: Clone> {
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

enum RaftNodeClock {
    RealClock(RealClock),
    MockClock(MockClock),
}

impl RaftNodeClock {
    fn advance(&mut self, duration: Duration) {
        match self {
            RaftNodeClock::RealClock(_) => {
                panic!("This method should never be called for a real clock");
            }
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
    to_node_sender: mpsc::Sender<RPCMessage<T>>,
    from_rpc_receiver: mpsc::Receiver<RPCMessage<T>>,
    rpc_manager: RPCManager<T>,
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

        //  the term check
        if request.term < state_guard.current_term && state_guard.status == RaftNodeStatus::Follower
        {
            return AppendEntriesResponse {
                server_id: self.id,
                term: state_guard.current_term,
                success: false,
                match_index: request.prev_log_index,
            };
        }

        // the log check
        let prev_log_term_for_this_node = state_guard
            .log
            .get(request.prev_log_index as usize)
            .map_or(0, |entry| entry.term);
        let log_ok = request.prev_log_index == 0
            || (request.prev_log_index > 0
                && request.prev_log_index <= state_guard.log.len() as u64
                && request.prev_log_term == prev_log_term_for_this_node);

        //  the log check is not OK, give false response
        if !log_ok {
            return AppendEntriesResponse {
                server_id: self.id,
                term: state_guard.current_term,
                success: false,
                match_index: request.prev_log_index,
            };
        }

        //  the candidate check - revert to follower if true
        if request.term == state_guard.current_term
            && state_guard.status == RaftNodeStatus::Candidate
        {
            state_guard.status = RaftNodeStatus::Follower;
        }

        //  heartbeat check - register the heartbeat time
        if request.entries.len() == 0 {
            self.reset_election_timeout(&mut state_guard);
            return AppendEntriesResponse {
                server_id: self.id,
                term: state_guard.current_term,
                success: true,
                match_index: request.prev_log_index, // no new logs have been committed
            };
        }

        //  the request can be accepted, try and append the logs
        //  this is the point where the logs will be inserted
        let log_index = request.prev_log_index + 1; //  the first log index will be 1

        //  Now, we know the prefix is ok. First, check that there are no subsequent logs on the follower.
        //  If there are subequent logs, and there is a conflict, discard logs from the conflict point onwards
        if (state_guard.log.len() as u64) > log_index {
            let max_range = min(
                self.len_as_u64(&state_guard.log) - log_index,
                self.len_as_u64(&request.entries),
            );
            let mut conflict_point = -1;

            //  Check if a conflict point exists
            for index in 0..max_range {
                let follower_entry = state_guard
                    .log
                    .get((log_index - 1 + index) as usize)
                    .expect(&format!(
                        "There is no entry at index for the follower {}",
                        log_index - 1 + index
                    ));

                let message_entry = request
                    .entries
                    .get((log_index - 1 + index) as usize)
                    .expect(&format!(
                        "There is no entry at index {} in the message",
                        log_index - 1 + index
                    ));

                if follower_entry.term != message_entry.term {
                    conflict_point = (log_index - 1 + index) as i64;
                    break;
                }
            }

            if conflict_point != -1 {
                info!(
                    "Conflict point detected, truncating logs for node {}",
                    self.id
                );
                state_guard.log.truncate(conflict_point as usize);
            } else {
                conflict_point = self.len_as_u64(&state_guard.log) as i64;
            }

            //  Now, start appending logs from the conflict point onwards
            let entries_to_insert = &request.entries[(conflict_point as usize)..];
            state_guard.log.extend_from_slice(entries_to_insert);
            if let Err(e) = self
                .persistence_manager
                .append_logs_at(&entries_to_insert.to_vec(), conflict_point as u64)
            {
                error!(
                    "There was a problem appending logs to stable storage for node {}: {}",
                    self.id, e
                );
            }
            if request.leader_commit_index > state_guard.commit_index {
                state_guard.commit_index = min(
                    request.leader_commit_index,
                    self.len_as_u64(&state_guard.log),
                );
            }
            return AppendEntriesResponse {
                server_id: self.id,
                term: state_guard.current_term,
                success: true,
                match_index: request.prev_log_index + self.len_as_u64(&request.entries), //  the last known log to be committed
            };
        }

        //  There are no subsequent logs on the follower, so no possibility of conflicts. Which means the logs can just be appended
        //  and the response can be returned
        state_guard.log.append(&mut request.entries.clone());

        return AppendEntriesResponse {
            server_id: self.id,
            term: state_guard.current_term,
            success: true,
            match_index: request.prev_log_index + self.len_as_u64(&request.entries), //  TODO: Fix this
        };
    }

    fn handle_append_entries_response(&mut self, response: AppendEntriesResponse) {
        let mut state_guard = self.state.lock().unwrap();
        if state_guard.current_term < response.term && !response.success {
            return;
        }

        let server_index = response.server_id as usize;

        if !response.success {
            //  check if the response term is greater - if it is, step down as leader and record the new term
            if state_guard.current_term < response.term {
                // self.update_term(&mut state_guard, response.term);
                if let Err(e) = self
                    .persistence_manager
                    .write_term_and_voted_for(response.term, Option::None)
                {
                    error!(
                        "There was a problem writing the term and voted_for variables to stable storage for node {}: {}",
                        self.id, e
                    );
                }
                return;
            }

            //  there isn't a new term - reduce the next index for that server and try again
            state_guard.next_index[server_index] = state_guard.next_index[server_index]
                .saturating_sub(1)
                .max(1);

            self.retry_append_request(&state_guard, server_index, response.server_id);
            return;
        }

        //  the response is successful
        state_guard.next_index[response.server_id as usize] = response.match_index + 1;
        state_guard.match_index[response.server_id as usize] = response.match_index;
        self.advance_commit_index(&mut state_guard);
        self.apply_entries(&mut state_guard);
        return;
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

    fn update_term(&self, state_guard: &mut MutexGuard<'_, RaftNodeState<T>>, new_term: Term) {
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
        return;
    }

    fn retry_append_request(
        &self,
        state_guard: &MutexGuard<'_, RaftNodeState<T>>,
        server_index: usize,
        to_server_id: u64,
    ) {
        let next_log_index = state_guard.next_index[server_index] as usize;
        let entries_to_send: Vec<LogEntry<T>> = if next_log_index < state_guard.log.len() {
            state_guard.log[(next_log_index - 1) as usize..].to_vec()
        } else {
            vec![]
        };

        let prev_log_index = if next_log_index > 1 {
            (next_log_index - 1) as u64
        } else {
            0
        };
        let prev_log_term = if next_log_index > 1 {
            state_guard.log[next_log_index - 1].term
        } else {
            0
        };

        let request = AppendEntriesRequest {
            term: state_guard.current_term,
            leader_id: self.id,
            prev_log_index,
            prev_log_term,
            entries: entries_to_send,
            leader_commit_index: state_guard.commit_index,
        };
        let message = RPCMessage::<T>::AppendEntriesRequest(request);
        let to_address = self
            .config
            .id_to_address_mapping
            .get(&to_server_id)
            .unwrap();
        self.rpc_manager
            .send_message(self.id, to_server_id, to_address.clone(), message);
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
        assert!(state_guard.status == RaftNodeStatus::Leader);

        //  find all match indexes that have quorum
        let mut match_index_count: HashMap<u64, u64> = HashMap::new();
        for &server_match_index in &state_guard.match_index {
            *match_index_count.entry(server_match_index).or_insert(0) += 1;
        }

        let new_commit_index = match_index_count
            .iter()
            .filter(|(&index, &count)| {
                count >= self.quorum_size().try_into().unwrap()
                    && state_guard
                        .log
                        .get((index - 1) as usize)
                        .map_or(false, |entry| entry.term == state_guard.current_term)
            })
            .map(|(&match_index, _)| match_index)
            .max();

        if let Some(max_index) = new_commit_index {
            state_guard.commit_index = max_index;
        }
    }

    /**
     * This function will move the last_applied to make it catch up with commit_index and apply all the entries in that range
     * to the state machine
     */
    fn apply_entries(&self, state_guard: &mut MutexGuard<'_, RaftNodeState<T>>) {
        assert!(state_guard.status == RaftNodeStatus::Leader);
        let mut entries_to_apply: Vec<LogEntry<T>> = Vec::new();
        while state_guard.last_applied < state_guard.commit_index {
            state_guard.last_applied += 1;
            let entry_at_index = state_guard
                .log
                .get((state_guard.last_applied - 1) as usize)
                .unwrap();
            entries_to_apply.push(entry_at_index.clone());
        }

        self.state_machine.apply(entries_to_apply);
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

    fn message_update_term_if_required(&mut self, message: RPCMessage<T>) {
        let mut state_guard = self.state.lock().unwrap();
        match message {
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

    fn is_message_stale(&self, message: RPCMessage<T>) -> bool {
        let state_guard = self.state.lock().unwrap();
        match message {
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
    fn receive(&mut self, message: RPCMessage<T>) -> Option<(RPCMessage<T>, ServerId)> {
        self.message_update_term_if_required(message.clone());
        return match message {
            RPCMessage::VoteRequest(request) => {
                let response = self.handle_vote_request(&request);
                Some((RPCMessage::VoteResponse(response), request.candidate_id))
            }
            RPCMessage::VoteResponse(response) => {
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
                self.handle_append_entries_response(response);
                None
            }
        };
    }

    fn can_become_leader(&self, state_guard: &MutexGuard<'_, RaftNodeState<T>>) -> bool {
        info!("Checking if node {} can become leader", self.id);
        let sum_of_votes_received = state_guard
            .votes_received
            .iter()
            .filter(|(_, vote_granted)| **vote_granted)
            .count();
        let quorum = self.quorum_size();
        info!(
            "Node has received {} votes and quorum size is {}",
            sum_of_votes_received, quorum
        );
        return sum_of_votes_received >= quorum;
    }

    /*
    This method will cause a node to elevate itself to candidate, cast a vote for itself, write that
    vote to storage and then send out messages to all other peers requesting votes
    */
    fn start_election(&self, state_guard: &mut MutexGuard<'_, RaftNodeState<T>>) {
        info!("Node {} is starting an election", self.id);
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
            self.rpc_manager
                .send_message(self.id, *peer, to_address.clone(), message);
        }
    }

    fn send_heartbeat(&self) {
        let state_guard = self.state.lock().unwrap();
        if state_guard.status != RaftNodeStatus::Leader {
            return;
        }

        for peer in &self.peers {
            if *peer == self.id {
                //  ignore self referencing node
                continue;
            }
            let heartbeat_request = AppendEntriesRequest {
                term: state_guard.current_term,
                leader_id: self.id,
                prev_log_index: state_guard.log.last().unwrap().index,
                prev_log_term: state_guard.log.last().unwrap().term,
                entries: Vec::<LogEntry<T>>::new(),
                leader_commit_index: state_guard.commit_index,
            };
            let to_address = self.config.id_to_address_mapping.get(peer).expect(
                format!("Cannot find the id to address mapping for peer id {}", peer).as_str(),
            );
            let message = RPCMessage::<T>::AppendEntriesRequest(heartbeat_request);

            self.rpc_manager
                .send_message(self.id, *peer, to_address.clone(), message);
        }
    }

    fn reset_election_timeout(&self, state_guard: &mut MutexGuard<'_, RaftNodeState<T>>) {
        state_guard.last_heartbeat = Instant::now();
    }

    fn check_election_timeout(&self, state_guard: &mut MutexGuard<'_, RaftNodeState<T>>) {
        if (self.clock.now() - state_guard.last_heartbeat) >= state_guard.election_timeout {
            match state_guard.status {
                RaftNodeStatus::Leader => {
                    //  do nothing
                    //  TODO: Is this valid? Should a leader really do nothing when an election times out?
                }
                RaftNodeStatus::Candidate => {
                    state_guard.status = RaftNodeStatus::Follower;
                    self.reset_election_timeout(state_guard);
                }
                RaftNodeStatus::Follower => {
                    info!("Node {} timed out, starting an election", self.id);
                    self.reset_election_timeout(state_guard);
                    self.start_election(state_guard);
                }
            }
        }
    }
    //  End utility functions for main RPC's

    //  Helper functions
    //  The quorum size is (N/2) + 1, where N = number of servers in the cluster
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
    //  End helper functions

    //  From this point onward are all the starter methods to bring the node up and handle communication
    //  between the node and the RPC manager
    fn new(
        id: ServerId,
        state_machine: S,
        config: ServerConfig,
        peers: Vec<ServerId>,
        persistence_manager: F,
        clock: RaftNodeClock,
    ) -> Self {
        info!(
            "Creating a new node with the following id and config: {} {:?}",
            id, config
        );
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
        let (to_node_sender, from_rpc_receiver) = mpsc::channel();
        let rpc_manager = RPCManager::new(
            id,
            config.address.clone(),
            config.port,
            to_node_sender.clone(),
        );
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
        if let Ok(message) = self.from_rpc_receiver.try_recv() {
            if self.is_message_stale(message.clone()) {
                //  dropping a stale message
                return;
            }

            if let Some((message_response, from_id)) = self.receive(message.clone()) {
                let to_address = self.config.id_to_address_mapping.get(&from_id).expect(
                    format!(
                        "Cannot find the id to address mapping for peer id {}",
                        from_id
                    )
                    .as_str(),
                );
                self.rpc_manager.send_message(
                    self.id,
                    from_id,
                    to_address.to_owned(),
                    message_response,
                );
            }
        }
    }

    fn start(&mut self) {
        let _state_guard = self.state.lock().unwrap();
        self.rpc_manager.start();
    }

    fn stop(&self) {
        let _state_guard = self.state.lock().unwrap();
        self.rpc_manager.stop();
    }

    fn tick(&mut self) {
        info!("Ticking for node {}", self.id);
        let mut state_guard = self.state.lock().unwrap();
        match state_guard.status {
            RaftNodeStatus::Leader => {
                //  the leader should send heartbeats to all followers
                info!("The node is a leader, sending heartbeats to all followers");
                self.send_heartbeat();
            }
            RaftNodeStatus::Candidate | RaftNodeStatus::Follower => {
                //  the candidate or follower should check if the election timeout has elapsed
                info!("The node is a candidate or follower, checking election timeout");
                self.check_election_timeout(&mut state_guard);
            }
        }
        drop(state_guard);

        //  listen to incoming messages from the RPC manager
        self.listen_for_messages();
    }
}

fn main() {
    let persistence_manager = match DirectFileOpsWriter::new("temp", 0) {
        Ok(pm) => pm,
        Err(e) => {
            panic!(
                "There was an error while creating the persistence manager {}",
                e
            );
        }
    };
    let state_machine = KeyValueStore::<String>::new();
    let server_config = ServerConfig {
        election_timeout: Duration::from_millis(150),
        heartbeat_interval: Duration::from_millis(50),
        address: "127.0.0.1:8080".to_string(),
        port: 0,
        cluster_nodes: vec![1, 2, 3],
        id_to_address_mapping: HashMap::new(),
    };
    let clock = RaftNodeClock::RealClock(RealClock {});
    let mut server = RaftNode::new(
        0,
        state_machine,
        server_config,
        vec![1, 2, 3],
        persistence_manager,
        clock,
    );
    server.start();
}

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

    fn apply_get(&self, key: String) -> Option<T> {
        return self.store.borrow().get(&key).cloned();
    }

    fn apply(&self, entries: Vec<LogEntry<T>>) {
        for entry in entries {
            self.store.borrow_mut().insert(entry.key, entry.value);
        }
    }
}

#[cfg(test)]
/**
 * All these nodes assume a single node in the cluster
 */
mod single_node_tests {
    struct ClusterConfig {
        election_timeout: Duration,
        heartbeat_interval: Duration,
        ports: Vec<u64>,
    }

    struct TestCluster {
        nodes: Vec<RaftNode<i32, KeyValueStore<i32>, DirectFileOpsWriter>>,
        _config: ClusterConfig,
    }

    impl TestCluster {
        fn tick(&mut self) {
            info!("Ticking for the cluster");
            self.nodes.iter_mut().for_each(|node| {
                info!("Ticking for a node");
                node.tick();
            });
        }

        fn tick_by(&mut self, tick_interval: u64) {
            for _ in 0..tick_interval {
                //  call the tick method for the cluster here
                self.tick();
            }
        }

        fn advance_time_by(&mut self, duration: Duration) {
            //  for each node in the cluster, advance it's mock clock by the duration
            for node in &mut self.nodes {
                match &mut node.clock {
                    RaftNodeClock::RealClock(_) => {
                        error!("Tests running with an actual clock! This should not be happening!")
                    }
                    RaftNodeClock::MockClock(ref mut mc) => mc.advance(duration),
                }
            }
        }

        fn new(number_of_nodes: u64, config: ClusterConfig) -> Self {
            let mut nodes: Vec<RaftNode<i32, KeyValueStore<i32>, DirectFileOpsWriter>> = Vec::new();
            let node_ids: Vec<ServerId> = (1..=number_of_nodes).collect();
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
                let mock_clock = RaftNodeClock::MockClock(MockClock {
                    current_time: Instant::now(),
                });

                let node = RaftNode::new(
                    *node_id,
                    state_machine,
                    server_config,
                    node_ids.clone(),
                    persistence_manager,
                    mock_clock,
                );
                nodes.push(node);
                counter += 1;
            }
            TestCluster {
                nodes,
                _config: config,
            }
        }

        fn start(&mut self) {
            for node in &mut self.nodes {
                node.start();
            }
        }

        fn stop(&self) {
            for node in &self.nodes {
                node.stop();
            }
        }

        fn has_leader(&self) -> bool {
            self.nodes.iter().filter(|node| node.is_leader()).count() == 1
        }

        fn get_leader(&self) -> Option<&RaftNode<i32, KeyValueStore<i32>, DirectFileOpsWriter>> {
            self.nodes.iter().filter(|node| node.is_leader()).last()
        }
    }
    use super::*;
    use crate::ServerConfig;

    const ELECTION_TIMEOUT: Duration = Duration::from_millis(150);
    const HEARTBEAT_INTERVAL: Duration = Duration::from_millis(50);

    #[test]
    fn single_node_leader_election() {
        let _ = env_logger::builder().is_test(true).try_init();
        //  create a cluster with a single node first
        let cluster_config = ClusterConfig {
            election_timeout: ELECTION_TIMEOUT,
            heartbeat_interval: HEARTBEAT_INTERVAL,
            ports: vec![8000],
        };
        let mut cluster = TestCluster::new(1, cluster_config);
        cluster.start();
        cluster.advance_time_by(Duration::from_millis(255)); //  picking 255 here because 150 + a max jitter of 100 guarantees that election has timed out
        cluster.tick_by(1);
        cluster.stop();
        assert_eq!(cluster.has_leader(), true);
    }
}

#[cfg(test)]
mod cluster_tests {

    use super::*;

    /**
     * Use this method to create a test cluster of nodes
     */
    type TestCluster = Vec<RaftNode<i32, KeyValueStore<i32>, DirectFileOpsWriter>>;
    fn create_test_cluster(number_of_nodes: u64, starting_port: u64) -> TestCluster {
        let mut nodes: Vec<RaftNode<i32, KeyValueStore<i32>, DirectFileOpsWriter>> = Vec::new();
        let node_ids: Vec<ServerId> = (1..=number_of_nodes).collect();
        let ports: Vec<u64> = (starting_port..starting_port + number_of_nodes).collect();
        let addresses: Vec<String> = ports
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
                election_timeout: Duration::from_millis(150),
                heartbeat_interval: Duration::from_millis(50),
                address: address.clone(),
                port: ports[counter],
                cluster_nodes: node_ids.clone(),
                id_to_address_mapping: id_to_address_mapping.clone(),
            };

            let state_machine = KeyValueStore::<i32>::new();
            let persistence_manager = DirectFileOpsWriter::new("data", *node_id).unwrap();
            let mock_clock = RaftNodeClock::MockClock(MockClock {
                current_time: Instant::now(),
            });

            let node = RaftNode::new(
                *node_id,
                state_machine,
                server_config,
                node_ids.clone(),
                persistence_manager,
                mock_clock,
            );
            nodes.push(node);
            counter += 1;
        }
        nodes
    }

    fn advance_cluster_by(cluster: &mut TestCluster, advance_by: Duration) {
        for node in cluster {
            node.clock.advance(advance_by);
        }
    }

    fn cluster_tick(cluster: &mut TestCluster) {
        for node in cluster {
            node.tick();
        }
    }

    #[test]
    fn test_leader_election() {
        let mut cluster = create_test_cluster(3, 8000);
        advance_cluster_by(&mut cluster, Duration::from_millis(250));
        cluster_tick(&mut cluster);
    }
}
