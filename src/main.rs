use std::collections::HashMap;
use std::time::Duration;

type Term = u64;

type ServerId = u64;

struct ServerConfig {
    election_timeout: Duration,
    election_timeout_jitter: Duration,
    heartbeat_interval: Duration,
}

trait StateMachine<T> {
    fn apply_set(&mut self, key: String, value: T);
    fn apply_get(&mut self, key: String) -> Option<&T>;
}

enum LogEntryCommand {
    Set = 0,
    Delete = 1,
}

struct LogEntry<T> {
    term: Term,
    index: u64,
    command: LogEntryCommand,
    key: String,
    value: Option<T>,
}

struct RaftNode<T, S: StateMachine<T>> {
    id: ServerId,
    current_term: Term,
    voted_for: Option<ServerId>,
    state_machine: S,
    config: ServerConfig,
    log: Vec<LogEntry<T>>,
    peers: Vec<ServerId>,
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
}
