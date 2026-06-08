package raft

//
// This is an outline of the API that raft must expose to
// the service (or tester). See comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   Create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   Start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   Each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester) in the same server.
//

import (
	"bytes"
	"cs350/labgob"
	"cs350/labrpc"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// As each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). Set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // This peer's index into peers[]
	dead      int32               // Set by Kill()

	// Your data here (4A, 4B).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    int
	log         []LogEntry
	state       int

	commitIndex int
	lastApplied int

	nextIndex     []int
	matchIndex    []int
	lastHeartbeat time.Time
	voteCount     int
	applyCh       chan ApplyMsg
}

type LogEntry struct {
	Command interface{}
	Term    int
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// Return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isleader bool
	// Your code here (4A).
	term = rf.currentTerm
	isleader = (rf.state == 2)
	return term, isleader
}

// Save Raft's persistent state to stable storage, where it
// can later be retrieved after a crash and restart. See paper's
// Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (4B).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// Restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (4B).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var entries []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&entries) != nil {
		log.Fatalf("failed to decode Raft state")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = entries
	}
}

// Example RequestVote RPC arguments structure.
// Field names must start with capital letters!
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// RequestVoteReply structure
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// Example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()

	reply.VoteGranted = false
	if args.Term < rf.currentTerm {
		rf.mu.Unlock()
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist()
	}

	lastLogIndex := len(rf.log) - 1
	lastLogTerm := rf.log[lastLogIndex].Term

	upToDate := args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && upToDate {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
		rf.persist()
		rf.state = 0
		rf.lastHeartbeat = time.Now()
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()
}

// Example code to send a RequestVote RPC to a server.
// Server is the index of the target server in rf.peers[].
// Expects RPC arguments in args. Fills in *reply with RPC reply,
// so caller should pass &reply.
//
// The types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// Look at the comments in ../labrpc/labrpc.go for more details.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// The service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. If this
// server isn't the leader, returns false. Otherwise start the
// agreement and return immediately. There is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. Even if the Raft instance has been killed,
// this function should return gracefully.
//
// The first return value is the index that the command will appear at
// if it's ever committed. The second return value is the current
// term. The third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isLeader := (rf.state == 2)
	if !isLeader {
		return -1, rf.currentTerm, false
	}

	newEntry := LogEntry{
		Term:    rf.currentTerm,
		Command: command,
	}

	rf.log = append(rf.log, newEntry)
	logIndex := len(rf.log) - 1
	rf.matchIndex[rf.me] = logIndex
	rf.persist()
	go rf.sendHeartbeats()
	return logIndex, rf.currentTerm, true
}

// func (rf *Raft) replicateLogEntries() {
// 	rf.mu.Lock()
// 	defer rf.mu.Unlock()

// 	for i := range rf.peers {
// 		if i != rf.me && rf.state == 2 {
// 			rf.sendAppendEntries(i)
// 		}
// 	}
// }

// The tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. Your code can use killed() to
// check whether Kill() has been called. The use of atomic avoids the
// need for a lock.
//
// The issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. Any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	// fmt.Println("calling append entries")
	// fmt.Println("These are entries:", args.Entries)
	reply.Success = false
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		// fmt.Println("error 501")
		reply.Success = false
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return
	}

	rf.state = 0
	// if args.PrevLogIndex >= len(rf.log) {
	// 	reply.Success = false
	// 	reply.Term = rf.currentTerm
	// 	rf.mu.Unlock()
	// 	return
	// }

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.mu.Unlock()
		return
	}

	rf.lastHeartbeat = time.Now()
	rf.currentTerm = args.Term
	rf.persist()

	if args.PrevLogIndex >= len(rf.log) || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		// fmt.Println("error 502")
		reply.Success = false
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return
	}

	logEntries := []LogEntry{}
	for i := range args.Entries {
		logIndex := args.PrevLogIndex + i + 1
		if len(rf.log) <= logIndex {
			logEntries = args.Entries[i:]
			break
		}
		if rf.log[logIndex].Command != args.Entries[i].Command || rf.log[logIndex].Term != args.Entries[i].Term {
			rf.log = rf.log[0:logIndex]
			rf.persist()
			logEntries = args.Entries[i:]
			break
		}
	}
	rf.log = append(rf.log, logEntries...)
	rf.persist()

	LastEntryIndex := len(rf.log) - 1
	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit < LastEntryIndex {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = LastEntryIndex
		}
	}
	// fmt.Println(rf.log, "is the follower log for server", rf.me)
	reply.Success = true
	rf.votedFor = args.LeaderId
	reply.Term = rf.currentTerm
	rf.lastHeartbeat = time.Now()
	rf.persist()
	rf.mu.Unlock()
}

// Helper function to apply committed log entries to the state machine
func (rf *Raft) applyCommitted() {

	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		applyMsg := ApplyMsg{
			CommandValid: true,
			Command:      rf.log[i].Command,
			CommandIndex: i,
		}
		rf.applyCh <- applyMsg
		rf.lastApplied = i
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {
		electionTimeout := rf.randomizedElectionTimeout()
		time.Sleep(electionTimeout)
		rf.mu.Lock()
		rf.applyCommitted()
		if rf.state == 2 {
			rf.mu.Unlock()
			rf.sendHeartbeats()
		} else {

			if rf.state != 2 && time.Since(rf.lastHeartbeat) >= electionTimeout && rf.votedFor == -1 {
				rf.currentTerm += 1
				rf.mu.Unlock()
				// fmt.Printf("starting election")
				rf.startElection()
				rf.mu.Lock()
				if rf.voteCount > len(rf.peers)/2 {
					// fmt.Printf("election won")
					rf.state = 2
					rf.matchIndex[rf.me] = len(rf.log) - 1
					if rf.matchIndex[rf.me] == -1 {
						rf.matchIndex[rf.me] = 0
					}
					for i := range rf.peers {
						rf.nextIndex[i] = len(rf.log)
						if i != rf.me {
							rf.matchIndex[i] = 0
						}
					}
					rf.mu.Unlock()
					// fmt.Printf("sending heartbeats")
					rf.sendHeartbeats()
				} else {
					// fmt.Printf("election lost")
					rf.state = 0
					rf.mu.Unlock()
				}
			} else {
				rf.mu.Unlock()
			}
		}
		rf.mu.Lock()
		rf.voteCount = 0
		rf.votedFor = -1
		rf.persist()
		rf.mu.Unlock()
	}
}

// The service or tester wants to create a Raft server. The ports
// of all the Raft servers (including this one) are in peers[]. This
// server's port is peers[me]. All the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:         peers,
		persister:     persister,
		me:            me,
		dead:          0,
		currentTerm:   0,
		votedFor:      -1,
		log:           []LogEntry{{nil, 0}},
		state:         0,
		commitIndex:   0,
		lastApplied:   0,
		nextIndex:     make([]int, len(peers)),
		matchIndex:    make([]int, len(peers)),
		lastHeartbeat: time.Now(),
		voteCount:     0,
		applyCh:       applyCh,
	}

	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections.
	go rf.ticker()

	return rf
}

func (rf *Raft) randomizedElectionTimeout() time.Duration {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state == 2 {
		return time.Duration(150) * time.Millisecond

	}
	return time.Duration(300+rand.Intn(150)) * time.Millisecond
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.votedFor = rf.me
	rf.voteCount = 1
	rf.state = 1
	rf.persist()
	rf.lastHeartbeat = time.Now()

	lastLogIndex := len(rf.log) - 1
	lastLogTerm := rf.log[lastLogIndex].Term
	args := RequestVoteArgs{
		rf.currentTerm,
		rf.me,
		lastLogIndex,
		lastLogTerm,
	}
	rf.mu.Unlock()
	for i := range rf.peers {
		if i != rf.me {
			var reply RequestVoteReply
			ok := rf.peers[i].Call("Raft.RequestVote", &args, &reply)
			if ok {
				if reply.VoteGranted {
					// fmt.Printf("election vote granted")
					rf.mu.Lock()
					rf.voteCount += 1
					rf.mu.Unlock()

				}
				if reply.Term > rf.currentTerm {
					rf.mu.Lock()
					rf.currentTerm = reply.Term
					rf.votedFor = -1
					rf.lastHeartbeat = time.Now()
					rf.state = 0
					rf.persist()
					rf.mu.Unlock()
					return
				}
			}
		}
	}
}

func (rf *Raft) sendHeartbeats() {
	// fmt.Println(rf.me, "is calling send heartbeats")
	rf.updateCommitIndex()
	for server := range rf.peers {
		if server != rf.me {
			go func(i int) {
				// fmt.Println(rf.me, "is sending append entries")
				rf.sendAppendEntries(i)

			}(server)
		}
	}

}

func (rf *Raft) sendAppendEntries(server int) {
	rf.mu.Lock()
	prevLogIndex := rf.nextIndex[server] - 1
	prevLogTerm := rf.log[prevLogIndex].Term
	entries := []LogEntry{}
	if rf.nextIndex[server] <= rf.matchIndex[rf.me] {
		entries = rf.log[rf.nextIndex[server]:]
		// fmt.Println("this should be entries sent", entries)
		rf.lastHeartbeat = time.Now()
	}

	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}
	rf.mu.Unlock()
	var reply AppendEntriesReply
	// fmt.Println("sending this to be appended:", args.Entries)
	ok := rf.peers[server].Call("Raft.AppendEntries", &args, &reply)

	if !ok {
		return
	}
	rf.mu.Lock()
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.state = 0
		rf.lastHeartbeat = time.Now()
		rf.persist()
		rf.mu.Unlock()
		return
	}

	if reply.Success {
		rf.nextIndex[server] = prevLogIndex + len(entries) + 1
		rf.matchIndex[server] = rf.nextIndex[server] - 1
		rf.lastHeartbeat = time.Now()
		rf.persist()
	} else {
		rf.nextIndex[server] = 1
	}
	rf.mu.Unlock()
}
func (rf *Raft) updateCommitIndex() {
	rf.mu.Lock()
	for i := len(rf.log) - 1; i > rf.commitIndex; i-- {
		count := 1
		for j := range rf.peers {
			if j != rf.me && rf.matchIndex[j] >= i && rf.log[i].Term == rf.currentTerm {
				count++
			}
		}
		if count > len(rf.peers)/2 {
			rf.commitIndex = i
			break
		}
	}
	rf.mu.Unlock()
}
