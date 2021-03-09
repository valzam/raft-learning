package raft

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"../labrpc"
)

func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

// ApplyMsg : as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

// LogEntry to be commited to the log
type LogEntry struct {
	Term    int
	Command interface{}
}

// AppendEntryArgs is sent to peers by the leader to initate the commit of a new log entry
// or periodically as a heartbeat
type AppendEntryArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int         // index of log entry immediately preceding new ones
	PrevLogTerm  int         // term of prevLogIndex entry
	Entries      []*LogEntry // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int         // leader’s commitIndex
}

// AppendEntryReply is filled in by peers to inform the leader that they have received a new log entry
type AppendEntryReply struct {
	Term    int
	Success bool
}

// RequestVoteArgs are passed to the RequestVote RPC method
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateID  int
	LastLogIndex int // index of candidate’s last log entry
	LastLogTerm  int // term of candidate’s last log entry
}

// RequestVoteReply is filled in by the RequestVote RPC method
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// Raft is a Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.RWMutex        // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	term      int                 // latest term server has seen (initialized to 0 on first boot, increases monotonically)

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// LOG and metadata
	log         []*LogEntry // log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)
	commitIndex int         // index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int         // index of highest log entry applied to state machine (initialized to 0, increases monotonically)

	// ELECTION state
	electionTimeout     time.Duration // Timeout for leader heartbeat. Calls for election if timeout reached
	lastHeardFromLeader time.Time     // Time since the last AppendEntry message has arrived from the leader
	votedFor            int           // candidateId that received vote in current term (or null if none)

	// LEADER state
	isLeader   bool  // whether this peer thinks it's the leader of a cluster
	nextIndex  []int // for each server, index of the next log entry to send to that server (initialized to leaderlast log index + 1)
	matchIndex []int // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
}

// Write locked access methods
func (rf *Raft) resetElectionTimeout() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.lastHeardFromLeader = time.Now()
}

func (rf *Raft) maybeJoinFutureTerm(newTerm int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Instance is already on up2date term, nothing to do
	if newTerm <= rf.term {
		return
	}
	println(fmt.Sprintf("Instance %d in term %d joined future term %d as follower", rf.me, rf.term, newTerm))

	rf.votedFor = -1
	rf.term = newTerm
	rf.isLeader = false

}

func (rf *Raft) advanceTerm() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.votedFor = rf.me
	rf.term++

	return rf.term
}

func (rf *Raft) grantVote(votee int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.votedFor = votee
}

func (rf *Raft) promoteToLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.isLeader = true
}

// Read locked access functions
func (rf *Raft) shouldCallElection() (bool, time.Duration) {
	rf.mu.RLock()
	defer rf.mu.RUnlock()

	t := time.Now()
	elapsed := t.Sub(rf.lastHeardFromLeader)

	return elapsed < rf.electionTimeout || rf.isLeader, elapsed
}

// GetState returns public state information
func (rf *Raft) GetState() (int, bool) {
	rf.mu.RLock()
	defer rf.mu.RUnlock()

	return rf.term, rf.isLeader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
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
}

// RequestVote is called by the RPC framework to request this raft instance's vote in a leader election
func (rf *Raft) RequestVote(message *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	term, _ := rf.GetState()

	rf.resetElectionTimeout()
	rf.maybeJoinFutureTerm(message.Term)

	reply.Term = term
	shouldGrantVote := rf.shouldGrantVote(message)

	reply.VoteGranted = shouldGrantVote

	if !shouldGrantVote {
		rf.mu.RLock()
		println(fmt.Sprintf("Instance %d refused vote request from candidate %d: instance term %d, has voted for %d", rf.me, message.CandidateID, rf.term, rf.votedFor))
		rf.mu.RUnlock()
	} else {
		rf.grantVote(message.CandidateID)

		rf.mu.RLock()
		println(fmt.Sprintf("Instance %d received vote request from candidate %d for term %d and granted the vote", rf.me, message.CandidateID, message.Term))
		rf.mu.RUnlock()

	}

}

func (rf *Raft) shouldGrantVote(args *RequestVoteArgs) bool {
	rf.mu.RLock()
	defer rf.mu.RUnlock()

	termIsCurrent := !(args.Term < rf.term)
	noConflictingVotes := rf.votedFor == -1 || rf.votedFor == args.CandidateID
	hasCurrentLog := rf.lastApplied <= args.LastLogIndex

	shouldGrantVote := termIsCurrent && noConflictingVotes && hasCurrentLog

	return shouldGrantVote
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply, resultCh chan bool) {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	if ok {
		rf.maybeJoinFutureTerm(reply.Term)
		resultCh <- reply.VoteGranted
	}
}

// AppendEntry is called by the RPC framework to add new log entries
func (rf *Raft) AppendEntry(message *AppendEntryArgs, reply *AppendEntryReply) {

	// Your code here (2A, 2B).
	rf.resetElectionTimeout()
	rf.maybeJoinFutureTerm(message.Term)

	rf.mu.RLock()
	term := rf.term
	log := rf.log
	rf.mu.RUnlock()

	// if some older leader sends messages, disregard
	if message.Term < term {
		reply.Term = term
		reply.Success = false

		return
	}

	// If this instance's log is empty simply accept whatever the leader sends
	if len(log) == 0 {

		rf.mu.Lock()
		rf.log = append(log, message.Entries...)
		rf.lastApplied = len(rf.log)
		if message.LeaderCommit > rf.commitIndex {
			rf.commitIndex = min(rf.commitIndex, len(rf.log))
		}
		rf.mu.Unlock()

		reply.Term = term
		reply.Success = true

		return
	}

	//Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
	prevLeaderLogEntry := log[message.PrevLogIndex]
	if prevLeaderLogEntry.Term != log[rf.lastApplied].Term {
		reply.Term = term
		reply.Success = false

		return
	}

	// Append new log entries
	rf.mu.Lock()
	rf.log = append(log, message.Entries...)
	rf.mu.Unlock()

	reply.Term = term
	reply.Success = true
}

func (rf *Raft) sendAppendEntry(server int, args *AppendEntryArgs, reply *AppendEntryReply) {
	ok := rf.peers[server].Call("Raft.AppendEntry", args, reply)

	if ok {
		rf.maybeJoinFutureTerm(reply.Term)
	}
}

func (rf *Raft) maybeSendHeartbeat() {
	for {
		rf.mu.RLock()
		if rf.isLeader && !rf.killed() {
			rf.sendHeartbeat()
		}
		rf.mu.RUnlock()
		time.Sleep(50 * time.Millisecond)
	}
}

func (rf *Raft) sendHeartbeat() {
	args := &AppendEntryArgs{}
	args.Term = rf.term
	args.LeaderID = rf.me
	for i := range rf.peers {
		if i != rf.me {
			reply := &AppendEntryReply{}
			go rf.sendAppendEntry(i, args, reply)
		}
	}
}

func (rf *Raft) maybeRequestVote() {
	for {
		shouldCallVote, elapsed := rf.shouldCallElection()
		if shouldCallVote {
			time.Sleep(10 * time.Millisecond)
			continue
		}

		// Set up this instance for leader election
		rf.resetElectionTimeout()
		votesReceived := 1
		majority := (len(rf.peers) / 2) + 1
		term := rf.advanceTerm()
		println(fmt.Sprintf("Instance %d initiated leader vote for new term %d, haven't heard from current leader in %dms", rf.me, term, elapsed.Milliseconds()))

		args := &RequestVoteArgs{}
		args.Term = term
		args.CandidateID = rf.me

		tally := make(chan bool)

		// Request vote from all peers
		for i := range rf.peers {
			if i != rf.me {
				reply := &RequestVoteReply{}
				go rf.sendRequestVote(i, args, reply, tally)
			}
		}

		// Gather votes from peers
		// Timeout after 1 second if no response is returned, presume election failed if majority times out
		// If majority has been reached promote to leader and stop couting
		for j := 1; j < len(rf.peers); j++ {
			select {
			case vote := <-tally:
				if vote {
					votesReceived++
				}
			case <-time.After(1 * time.Second):
				continue
			}

			if votesReceived >= majority {
				rf.promoteToLeader()
				go rf.sendHeartbeat()
				println(fmt.Sprintf("Instance %d became leader", rf.me))
				break
			}

		}

		// Back off to minimise collision in case of failed election
		// Has no effect if this instance became leader since it will not call a new election
		backoff, _ := time.ParseDuration(fmt.Sprintf("%dms", rand.Intn(20)))
		time.Sleep(backoff)
	}
}

// Start will initiate a vote to commit a new log entry
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.RLock()
	defer rf.mu.RUnlock()

	index := rf.lastApplied
	term := rf.term
	isLeader := rf.isLeader

	// Your code here (2B).

	return index, term, isLeader
}

// Kill will prevent this instance from participating in the cluster
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// Make is called the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.votedFor = -1
	rf.term = 0
	rf.isLeader = false
	rf.lastApplied = 0
	rf.commitIndex = 0

	electionTimeoutBase, _ := time.ParseDuration(fmt.Sprintf("%dms", 150))
	electionTimeoutJitter, _ := time.ParseDuration(fmt.Sprintf("%dms", rand.Intn(50)))
	rf.electionTimeout = electionTimeoutBase + electionTimeoutJitter
	rf.lastHeardFromLeader = time.Now()

	println(fmt.Sprintf("Creating instance with ID %d, election timeout %d", me, rf.electionTimeout.Milliseconds()))

	// Background tasks
	go rf.maybeSendHeartbeat()
	go rf.maybeRequestVote()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
