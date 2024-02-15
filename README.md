# Raft
This repo is a minimalistic reproduction of Raft for educational purposes.

The classic Raft paper - https://raft.github.io/raft.pdf. It's probably one of the more readable papers I've come across. Pair this with the [Raft visualisation](https://thesecretlivesofdata.com/raft/) which is a great tool to understand Raft.

##  The Code Structure

Most of the code lives in the `main.rs` file currently, which is by design since this is an educational project. The only things I placed outside of the `main.rs` file were the clock module and the storage module. There is also a `types.rs` file for the more complicated types.

I separated out the storage layers and the clock layers because I was unsure what I was doing with it initially so I wanted to hide it behind an API and worry about the implementation later.

The clock injection itself wasn't a big deal actually but I ended up doing a plain-text CSV file with no indexing for storage which will probably be a pretty big problem at scale. This is still something I need to fix.

##  The Main Components

There are 3 main components to a Raft cluster

1. Storage (represented in the storage module)
2. Clocks (represented in the `clock.rs` file)
3. Networking (represented as the RPCManager struct in `main.rs`)
4. Nodes or servers (represented as the RaftNode struct in `main.rs`)

The node struct contains most of the logic around the RPC methods (shown in the screenshot below)

![](/assets/raft-rpc.png)

##  Testing

I've ended up creating a little bit of monstrosity in terms of testing because I ended up going with creating explicit scenarios. But, I've tried as far as I can to create a deterministic state machine that can be tested by mocking the networking layer and the clocks. This allows me to realiably forward time and drop messages between nodes to re-create real-world situation.

The testing logic is housed within the `TestCluster` struct in `main.rs`. The tests using this struct attempt to re-create explicit scenarios like a concurrent leader election and network partition to ensure the logic works correctly.

## Where From Here

Currently the code is able to handle:

* Leader Election & Stability
* Log Replication
* Durable storage and recovery from storage
* Network partition recovery

What I have yet to do:

* Log snapshots
* Benchmarks

##  Credits

A huge part of accomplishing this was being able to reference existing repos that do a Raft implementation

* Phil Eaton's Raft implementation in Rust (https://github.com/eatonphil/raft-rs)
* Jacky Zhao's Raft implementation also in Rust (https://github.com/jackyzha0/miniraft)

A large part of the testing inspiration came from the second repo.