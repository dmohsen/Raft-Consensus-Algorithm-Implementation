# Raft Consensus Algorithm

A Go implementation of the Raft distributed consensus protocol, built as part of a distributed systems course at Boston University. Raft ensures that a cluster of servers maintains a consistent, replicated log even in the presence of node failures and network partitions.

## Overview

Raft organizes client requests into a replicated log shared across all server nodes. As long as a majority of nodes are reachable, the cluster elects a single leader, replicates log entries to followers, and commits entries in order — guaranteeing all nodes apply the same commands in the same sequence.

## What's Implemented

**Leader Election**
- Randomized election timeouts (300–450ms for followers, 150ms for leaders) to avoid split votes
- `RequestVote` RPC with log completeness check — candidates must be at least as up-to-date as the voter's log to receive a vote
- Automatic step-down when a higher term is discovered

**Log Replication**
- `AppendEntries` RPC used for both heartbeats and log replication
- Followers validate `PrevLogIndex` and `PrevLogTerm` before appending entries
- Conflict detection: mismatched entries are truncated before new ones are appended
- Leader tracks `nextIndex` and `matchIndex` per follower to manage replication progress

**Commit and Apply**
- Leader advances `commitIndex` when a log entry is replicated on a majority of nodes
- Followers update `commitIndex` based on `LeaderCommit` in `AppendEntries`
- Committed entries are sent to the service via `applyCh` in order

**Persistence**
- `currentTerm`, `votedFor`, and `log` are encoded and saved to stable storage on every change using `labgob`
- State is restored from the persister on startup, allowing nodes to rejoin after a crash

## Tech Stack

- Go
- `sync.Mutex` for concurrency control
- `sync/atomic` for kill flag
- `labrpc` for simulated RPC (supports lossy networks, delays, reordering)
- `labgob` for state serialization

## Key Design Decisions

- A single `ticker` goroutine drives both election timeouts and heartbeat sending
- Leader sends heartbeats at 150ms intervals; followers time out between 300–450ms
- On failed `AppendEntries`, `nextIndex` resets to 1 and retries from the beginning
- Persistent state is saved on every write to `currentTerm`, `votedFor`, or `log`

## Testing

Tested against 50+ cases covering:
- Initial election and re-election after leader failure
- Log replication under normal and partitioned conditions
- Concurrent `Start()` calls
- Leader rejoin after partition
- Crash and restart with state recovery
- Unreliable networks with delayed/dropped RPCs

```bash
go test -race -run 4A   # leader election tests
go test -race -run 4B   # log replication and persistence tests
```
