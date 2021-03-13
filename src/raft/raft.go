package raft

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"../labrpc"
)

func min(a int, b int) int {
	if a <= b {
		return a
	} else {
		return b
	}
}

func max(a int, b int) int {
	if a >= b {
		return a
	} else {
		return b
	}
}

// import "bytes"
// import "../labgob"

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type LogEntry struct {
	Command interface{}
	Term    int
}

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	applyCh   chan ApplyMsg

	log []*LogEntry

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Raft metadata
	currentTerm         int
	currentState        int // 0 follower, 1 candidate, 2 leader
	votedFor            int
	electionTimeout     time.Duration
	lastHeardFromLeader time.Time

	// Log replication data
	commitIndex int // index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int // index of highest log entry applied to state machine (initialized to 0, increases monotonically)
	nextIndex   []int
	matchIndex  []int
}

func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term := rf.currentTerm
	isleader := rf.currentState == 2

	return term, isleader
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	LeaderCommit int

	Entries []*LogEntry
}

type AppendEntriesReply struct {
	Term    int  // currentTerm of follower
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	t := time.Now()
	rf.lastHeardFromLeader = t

	if args.Term >= rf.currentTerm {
		if rf.currentState > 0 {
			println(fmt.Sprintf("Server %d converted to follower", rf.me))
		}
		rf.currentTerm = args.Term
		rf.currentState = 0

		// TODO: Reply false if last log entry of follower differs from leader
		// TODO: Remove conflicting entries from follower log
		if len(args.Entries) > 0 {
			println(fmt.Sprintf("Server %d received new entries from leader", rf.me))
			rf.log = append(rf.log, args.Entries...)
			rf.applyCh <- ApplyMsg{true, args.Entries[0].Command, len(rf.log)}
		}

		if args.LeaderCommit > rf.commitIndex {
			newCommitIndex := min(args.LeaderCommit, len(rf.log))
			println(fmt.Sprintf("Server %d moved commit index forward to %d", rf.me, newCommitIndex))
			rf.commitIndex = newCommitIndex
		}

		reply.Term = rf.currentTerm
		reply.Success = true

		// Leader is in old term
	} else {
		reply.Term = rf.currentTerm
		reply.Success = false
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && args.Term >= rf.currentTerm {
		// Join great new leader
		rf.votedFor = args.CandidateId
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		println(fmt.Sprintf("Server %d responded to vote request from server %d", rf.me, args.CandidateId))
	} else {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		println(fmt.Sprintf("Server %d refused to vote for server %d", rf.me, args.CandidateId))
	}

}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply, resultCh chan bool) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	if ok {
		resultCh <- reply.VoteGranted
	}

	return ok
}

func (rf *Raft) maybeStartElection() {
	for {
		t := time.Now()

		rf.mu.Lock()
		isLeader := rf.currentState == 2
		timeSinceLastHeardFromLeader := t.Sub(rf.lastHeardFromLeader)
		leaderTimeout := timeSinceLastHeardFromLeader > rf.electionTimeout
		rf.mu.Unlock()

		if !isLeader && leaderTimeout {
			println(fmt.Sprintf("Server %d starting leader election", rf.me))

			rf.mu.Lock()
			rf.currentState = 1
			rf.currentTerm++
			rf.lastHeardFromLeader = t
			rf.votedFor = rf.me
			rf.mu.Unlock()

			votesReceived := 1
			args := &RequestVoteArgs{}
			args.Term = rf.currentTerm
			args.CandidateId = rf.me

			tally := make(chan bool)

			for peer := range rf.peers {
				if peer != rf.me {
					reply := &RequestVoteReply{}

					go rf.sendRequestVote(peer, args, reply, tally)
				}
			}

			for j := 1; j < len(rf.peers); j++ {
				select {
				case vote := <-tally:
					if vote {
						votesReceived++
					}
				case <-time.After(250 * time.Millisecond):
					continue
				}
			}

			// Become leader if got enough votes and server hasn't turned into follower during election
			rf.mu.Lock()
			if votesReceived >= (len(rf.peers)/2)+1 && rf.currentState > 0 {
				rf.currentState = 2
				currentCommitIndex := rf.commitIndex

				go rf.sendHeartbeat(rf.currentTerm, currentCommitIndex)
				println(fmt.Sprintf("Instance %d became leader", rf.me))
			}
			rf.mu.Unlock()

			// Failed to become leader
			if rf.currentState != 2 {
				println(fmt.Sprintf("Server %d failed to become leader", rf.me))
				rf.mu.Lock()
				rf.votedFor = -1
				rf.mu.Unlock()

				// Potentially failed because of collision, add random backoff
				backoff, _ := time.ParseDuration(fmt.Sprintf("%dms", rand.Intn(100)))
				time.Sleep(backoff)
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func (rf *Raft) maybeSendHeartbeat() {
	for {
		rf.mu.Lock()
		isLeader := rf.currentState == 2
		currentTerm := rf.currentTerm
		currentCommitIndex := rf.commitIndex
		rf.mu.Unlock()

		if isLeader {
			rf.sendHeartbeat(currentTerm, currentCommitIndex)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (rf *Raft) sendHeartbeat(forTerm int, withCommitIndex int) {
	args := &AppendEntriesArgs{}
	args.LeaderId = rf.me
	args.Term = forTerm
	args.LeaderCommit = withCommitIndex

	for peer := range rf.peers {
		if peer != rf.me {
			reply := &AppendEntriesReply{}
			go rf.sendAppendEntries(peer, args, reply)
		}
	}
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	println(fmt.Sprintf("Server %d received a command", rf.me))

	rf.mu.Lock()
	term := rf.currentTerm
	isLeader := rf.currentState == 2
	rf.mu.Unlock()

	if !isLeader {
		return rf.commitIndex, term, isLeader
	}

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	logEntry := &LogEntry{}
	logEntry.Command = command
	logEntry.Term = term

	args := &AppendEntriesArgs{}
	args.Entries = []*LogEntry{logEntry}
	args.LeaderCommit = rf.commitIndex
	args.LeaderId = rf.me

	rf.log = append(rf.log, logEntry)
	rf.applyCh <- ApplyMsg{true, command, len(rf.log)}

	if len(rf.log) > 0 {
		args.PrevLogIndex = len(rf.log)
		args.PrevLogTerm = rf.log[args.PrevLogIndex-1].Term
	} else {
		args.PrevLogIndex = -1
		args.PrevLogTerm = term
	}
	args.Term = term

	for peer := range rf.peers {
		if peer != rf.me {
			reply := &AppendEntriesReply{}

			rf.sendAppendEntries(peer, args, reply)
		}
	}

	rf.commitIndex++
	index := rf.commitIndex
	println(fmt.Sprintf("Server %d applied command at index %d", rf.me, rf.commitIndex))

	return index, term, isLeader
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	println(fmt.Sprintf("Server %d got killed", rf.me))
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh

	// Your initialization code here (2A, 2B, 2C).
	electionTimeoutBase, _ := time.ParseDuration(fmt.Sprintf("%dms", 150))
	electionTimeoutJitter, _ := time.ParseDuration(fmt.Sprintf("%dms", rand.Intn(100)))
	rf.electionTimeout = electionTimeoutBase + electionTimeoutJitter
	rf.lastHeardFromLeader = time.Now()
	rf.votedFor = -1

	go rf.maybeStartElection()
	go rf.maybeSendHeartbeat()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	println(fmt.Sprintf("Server %d started", rf.me))

	return rf
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
