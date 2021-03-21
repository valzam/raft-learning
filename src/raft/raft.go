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

type Log struct {
	mu      sync.Mutex
	entries []LogEntry
}

func (log *Log) getEntry(index int) (*LogEntry, bool) {
	log.mu.Lock()
	defer log.mu.Unlock()

	if index == 0 || len(log.entries) < index {
		return &LogEntry{}, false
	}

	return &log.entries[index-1], true
}

func (log *Log) getAllEntriesFrom(index int) []LogEntry {
	log.mu.Lock()
	defer log.mu.Unlock()

	if index < 1 {
		return []LogEntry{}
	}

	return log.entries[index-1:]
}

func (log *Log) putEntry(entry LogEntry) (int, bool) {
	log.mu.Lock()
	defer log.mu.Unlock()

	log.entries = append(log.entries, entry)
	return len(log.entries), true
}

func (log *Log) appendEntries(entries []LogEntry) (int, bool) {
	log.mu.Lock()
	defer log.mu.Unlock()

	log.entries = append(log.entries, entries...)
	return len(log.entries), true
}

func (log *Log) overwriteEntries(entries []LogEntry, from int) (int, bool) {
	log.mu.Lock()
	defer log.mu.Unlock()

	log.entries = append(log.entries[:from], entries...)
	return len(log.entries), true
}

func (log *Log) length() int {
	log.mu.Lock()
	defer log.mu.Unlock()

	return len(log.entries)
}

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	applyCh   chan ApplyMsg

	log *Log

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Election metadata
	currentTerm         int
	currentState        int // 0 follower, 1 candidate, 2 leader
	votedFor            int
	electionTimeout     time.Duration
	lastHeardFromLeader time.Time

	// Log replicatio metadata
	commitIndex int // index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int // index of highest log entry applied to state machine (initialized to 0, increases monotonically)
	nextIndex   [10]int
	matchIndex  [10]int
}

func (rf *Raft) isLeader() bool {
	return rf.currentState == 2
}

func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term := rf.currentTerm
	isleader := rf.isLeader()

	return term, isleader
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	LeaderCommit int

	Entries []LogEntry
}

type AppendEntriesReply struct {
	Term    int  // currentTerm of follower
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

type RetryAppendEntries struct {
	followerId        int
	needsRetry        bool
	needsPreviousLogs bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	t := time.Now()
	rf.lastHeardFromLeader = t

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Leader is in old term, ignore message
	if args.Term < rf.currentTerm {
		println(fmt.Sprintf("Server %d refused to apply entries", rf.me))

		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// This server thinks it's the leader, convert to follower
	if rf.currentState > 0 {
		println(fmt.Sprintf("Server %d converted to follower", rf.me))
	}
	rf.currentTerm = args.Term
	rf.currentState = 0

	// Maybe increment commit index
	if args.LeaderCommit > rf.commitIndex {
		newCommitIndex := min(args.LeaderCommit, rf.log.length())
		rf.commitIndex = newCommitIndex
		println(fmt.Sprintf("Server %d moved commit index forward to %d", rf.me, newCommitIndex))
	}

	// Short curcuit for empty messages / heartbeat messages
	if len(args.Entries) == 0 {
		reply.Term = rf.currentTerm
		reply.Success = true

		return
	}

	// Short curcuit for special case: Follower has empty log
	// This makes later code simpler
	if rf.log.length() == 0 {
		println(fmt.Sprintf("Server %d started with empty log", rf.me))

		rf.log.appendEntries(args.Entries)
		reply.Term = rf.currentTerm
		reply.Success = true
		rf.applyCh <- ApplyMsg{true, args.Entries[0].Command, rf.log.length()}

		return
	}

	// Leader is sending new logs and follower already has log entries

	println(fmt.Sprintf("Server %d received new entries from leader", rf.me))

	// Is the last known entry the same as on the leader? --> Request previous logs
	lastEntry, ok := rf.log.getEntry(args.PrevLogIndex)
	if ok && lastEntry.Term != args.PrevLogTerm {
		println(fmt.Sprintf("Server %d found conflicting history", rf.me))

		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// All good, apply logs
	reply.Term = rf.currentTerm
	reply.Success = true

	// Simple version: Overwrite any overlap, append rest
	rf.log.overwriteEntries(args.Entries, args.PrevLogIndex)

	rf.applyCh <- ApplyMsg{true, args.Entries[0].Command, rf.log.length()}

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	return ok
}

// Async function to replicate logs to followers
// Accept: followerId, args to be replicated, built up by leader
// Deals with
// - Making RPC call
// - Check if network error --> retry
// - Check if log inconsistency --> Decrement nextIndex and retry
// - If successful return true on resultCh

func (rf *Raft) replicateLogEntries(server int, args AppendEntriesArgs, resultCh chan bool) {
	println(fmt.Sprintf("Trying to send log to server %d", server))

	for {
		// // Follower already has up2date logs
		// if args.PrevLogIndex >= rf.nextIndex[server]               {
		// 	println(fmt.Sprintf("Server %d already has current log state", server))

		// 	break
		// }

		// Follower is behind, allow to catch up
		// if args.PrevLogIndex >= rf.nextIndex[server] {
		// 	args.PrevLogIndex = rf.nextIndex[server] - 1
		// }

		reply := &AppendEntriesReply{}
		// Send all entries from nextIndex to the end
		args.Entries = rf.log.getAllEntriesFrom(rf.nextIndex[server])

		ok := rf.sendAppendEntries(server, &args, reply)

		if ok {
			if !reply.Success {
				println(fmt.Sprintf("Server %d was reachable but found a conflict", server))

				rf.nextIndex[server]--
				args.PrevLogIndex--
				lastEntry, ok := rf.log.getEntry(rf.log.length())
				if ok {
					args.PrevLogTerm = lastEntry.Term
				} else {
					println(fmt.Sprintf("Server %d could not accept replication", server))
					break
				}
				// Follower was reachable and added the log entries
			} else if reply.Success {
				println(fmt.Sprintf("Log replicated to server %d", server))

				rf.nextIndex[server] = rf.log.length() + 1
				resultCh <- true
				break
			}
		} else {
			println(fmt.Sprintf("Server %d didn't return AppendEntries RPC", server))
		}
		// Small backoff then retry
		time.Sleep(25 * time.Millisecond)

	}
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
		isLeader := rf.isLeader()
		timeSinceLastHeardFromLeader := t.Sub(rf.lastHeardFromLeader)
		leaderTimeout := timeSinceLastHeardFromLeader > rf.electionTimeout
		rf.mu.Unlock()

		if !isLeader && leaderTimeout {
			println(fmt.Sprintf("Server %d starting leader election because leader hasn't contacted in %dms", rf.me, timeSinceLastHeardFromLeader.Milliseconds()))

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
				nextIndexToSend := rf.log.length() + 1
				for i := range rf.peers {
					rf.nextIndex[i] = nextIndexToSend
					rf.matchIndex[i] = 0
				}
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
		if rf.isLeader() {
			rf.mu.Lock()
			currentTerm := rf.currentTerm
			currentCommitIndex := rf.commitIndex
			rf.mu.Unlock()

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

	rf.mu.Lock()
	term := rf.currentTerm
	isLeader := rf.isLeader()
	currentCommitIndex := rf.commitIndex
	rf.mu.Unlock()

	if !isLeader {
		return currentCommitIndex, term, isLeader
	}

	// Your code here (2B).
	logEntry := LogEntry{command, term}
	println(fmt.Sprintf("Server %d sending log entry %+v", rf.me, logEntry))

	// Build payload for follower RPC
	args := AppendEntriesArgs{}
	args.LeaderCommit = currentCommitIndex
	args.LeaderId = rf.me
	args.Term = term
	args.PrevLogIndex = 0
	args.PrevLogTerm = term

	if rf.log.length() >= 1 {
		// Get the current head of the log
		args.PrevLogIndex = rf.log.length()
		lastEntry, ok := rf.log.getEntry(rf.log.length())
		if ok {
			args.PrevLogTerm = lastEntry.Term
		}
	}
	// Add to own log
	rf.log.appendEntries([]LogEntry{logEntry})

	rf.applyCh <- ApplyMsg{true, command, rf.log.length()}

	// Send entries to followers and gather responses
	resultCh := make(chan bool)
	for peer := range rf.peers {
		if peer != rf.me {
			go rf.replicateLogEntries(peer, args, resultCh)
		}
	}

	// Get all follower replies with retry flag
	successfulReplication := 1
	for j := 1; j < len(rf.peers); j++ {
		select {
		case response := <-resultCh:
			if response {
				successfulReplication++
			}
		case <-time.After(500 * time.Millisecond):
			continue
		}
	}

	if successfulReplication >= (len(rf.peers)/2)+1 {
		rf.mu.Lock()

		rf.commitIndex++
		rf.lastApplied++

		rf.mu.Unlock()
		println(fmt.Sprintf("Server %d applied command at index %d", rf.me, rf.commitIndex))
	}

	return rf.commitIndex, term, isLeader
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
	rf.log = &Log{}

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
