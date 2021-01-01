package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"../labrpc"
)

// import "bytes"
// import "../labgob"

//
// as each Raft peer becomes aware that successive log entries are
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

// Raft is a Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.RWMutex        // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	electionTimeout     time.Duration // Timeout for leader heartbeat. Calls for election if timeout reached
	electionTimeoutBase time.Duration // Time between checking if a leader election is necessary
	lastHeardFromLeader time.Time     // Time since the last AppendEntry message has arrived from the leader

	term     int  // latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor int  // candidateId that received vote in current term (or null if none)
	isLeader bool // whether this peer thinks it's the leader of a cluster
}

// Write locked access methods
func (rf *Raft) resetElectionTimeout() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.lastHeardFromLeader = time.Now()
}

func (rf *Raft) updateTerm(newTerm int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.votedFor = -1
	rf.term = newTerm
}

func (rf *Raft) advanceTerm() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.votedFor = rf.me
	rf.term++
}

func (rf *Raft) grantVote(votee int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.votedFor = votee
}

func (rf *Raft) demoteToFollower() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.isLeader = false
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

// GetState returns relevant state information
func (rf *Raft) getState() (int, bool, int) {
	rf.mu.RLock()
	defer rf.mu.RUnlock()

	return rf.term, rf.isLeader, rf.votedFor
}

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

// RequestVoteArgs are passed to the RequestVote RPC method
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int
	CandidateID int
}

// RequestVoteReply is filled in by the RequestVote RPC method
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// RequestVote is called by the RPC framework to request this raft instance's vote in a leader election
func (rf *Raft) RequestVote(message *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	term, _ := rf.GetState()

	rf.resetElectionTimeout()
	// If we are in a new epoch some instance just became leader
	// Make sure this instance is demoted to a follower since a new leader is sending messages
	if message.Term > term {
		println(fmt.Sprintf("Instance %d saw new term %d, updated its old term %d", rf.me, message.Term, term))

		rf.updateTerm(message.Term)
		rf.demoteToFollower()
	}

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

	shouldGrantVote := termIsCurrent && noConflictingVotes

	return shouldGrantVote
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply, resultCh chan bool) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	term, _ := rf.GetState()

	if ok {
		if reply.Term > term {

			rf.mu.RLock()
			println(fmt.Sprintf("Instance %d saw new term %d, updated its old term %d", rf.me, reply.Term, rf.term))
			rf.mu.RUnlock()

			rf.updateTerm(reply.Term)
			rf.demoteToFollower()
		}

		if reply.VoteGranted {
			rf.mu.RLock()
			println(fmt.Sprintf("Instance %d granted vote by instance %d", rf.me, server))
			rf.mu.RUnlock()

		}

		resultCh <- reply.VoteGranted
	}
	return ok
}

// AppendEntryArgs is sent to peers by the leader to initate the commit of a new log entry
// or periodically as a heartbeat
type AppendEntryArgs struct {
	Term     int
	LeaderID int
}

// AppendEntryReply is filled in by peers to inform the leader that they have received a new log entry
type AppendEntryReply struct {
	Term    int
	Success bool
}

// AppendEntry is called by the RPC framework to add new log entries
func (rf *Raft) AppendEntry(message *AppendEntryArgs, reply *AppendEntryReply) {

	// Your code here (2A, 2B).
	rf.resetElectionTimeout()
	term, _ := rf.GetState()

	// if some older leader sends messages, disregard
	if message.Term < term {
		reply.Term = term
		reply.Success = false

		return
	}

	// If we are in a new epoch some instance just became leader
	// Make sure this instance is demoted to a follower since a new leader is sending messages
	if message.Term > term {
		println(fmt.Sprintf("Instance %d saw new term %d, updated its old term %d", rf.me, message.Term, term))

		rf.updateTerm(message.Term)
		rf.demoteToFollower()
	}

	reply.Term = term
	reply.Success = true
}

func (rf *Raft) sendAppendEntry(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntry", args, reply)
	term, _ := rf.GetState()

	// Demote to follower because instances exist with a higher term
	if ok && reply.Term > term {
		rf.updateTerm(reply.Term)
		rf.demoteToFollower()

		rf.mu.RLock()
		println(fmt.Sprintf("Instance %d was demoted as leader because term has advanced", rf.me))
		rf.mu.RUnlock()
	}
	return ok
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
		votesReceived := 1
		majority := (len(rf.peers) / 2) + 1
		rf.advanceTerm()
		rf.mu.RLock()
		me := rf.me
		term := rf.term
		rf.mu.RUnlock()
		println(fmt.Sprintf("Instance %d initiated leader vote for new term %d, haven't heard from current leader in %dms", me, term, elapsed.Milliseconds()))

		args := &RequestVoteArgs{}
		args.Term = rf.term
		args.CandidateID = rf.me

		tally := make(chan bool)

		// Request vote from all peers
		for i := range rf.peers {
			if i != rf.me {
				reply := &RequestVoteReply{}
				go rf.sendRequestVote(i, args, reply, tally)
			}
		}

		// Wait for votes until a majority has voted
		// If no peer responds after 1 second skip
		for j := 1; j < majority; j++ {
			select {
			case vote := <-tally:
				if vote {
					votesReceived++
				}
			case <-time.After(1 * time.Second):
				println(fmt.Sprintf("Instance %d vote tally timed out", me))

				continue
			}

		}

		rf.resetElectionTimeout()

		// maybe make this instance leader
		if votesReceived >= majority {
			println(fmt.Sprintf("Instance %d number of votes %d", me, votesReceived))

			rf.promoteToLeader()
			go rf.sendHeartbeat()
			println(fmt.Sprintf("Instance %d became leader", me))
		} else {
			println(fmt.Sprintf("Instance %d failed to become leader", me))
		}

		// Back off to minimise collision in case of failed election
		backoff, _ := time.ParseDuration(fmt.Sprintf("%dms", rand.Intn(20)))
		time.Sleep(backoff)
	}
}

// Start will initiate a vote to commit a new log entry
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

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

func (rf *Raft) logState() {
	for {
		if rf.killed() {
			continue
		}
		rf.mu.Lock()
		println(fmt.Sprintf("STATE Instance %d, isLeader %t, current term %d, last voted for %d", rf.me, rf.isLeader, rf.term, rf.votedFor))
		rf.mu.Unlock()
		time.Sleep(1 * time.Second)
	}
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
	rf.votedFor = -1
	rf.term = 0

	// Your initialization code here (2A, 2B, 2C).
	electionTimeoutBase, _ := time.ParseDuration(fmt.Sprintf("%dms", 150))
	electionTimeoutJitter, _ := time.ParseDuration(fmt.Sprintf("%dms", rand.Intn(50)))
	rf.electionTimeout = electionTimeoutBase + electionTimeoutJitter
	rf.electionTimeoutBase = electionTimeoutBase
	rf.lastHeardFromLeader = time.Now()

	println(fmt.Sprintf("Creating instance with ID %d, election timeout %d", me, rf.electionTimeout.Milliseconds()))

	// Background tasks
	// go rf.logState()
	go rf.maybeSendHeartbeat()
	go rf.maybeRequestVote()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
