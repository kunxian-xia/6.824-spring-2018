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
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

// import "bytes"
// import "labgob"

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

const (
	Follower  = iota //0
	Candidate        //1
	Leader           //2
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	state       int
	currentTerm int //current term
	votedFor    int

	electionTimeout  int
	heartbeatTimeout int
	electionTimer    *time.Timer
	resetCh          chan int
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	logs     []ApplyMsg
	logTerms []int

	nextIndex  []int
	matchIndex []int

	commitIndex int
	lastApplied int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.state == Leader
	DPrintf("%d: %d %d\n", rf.me, rf.currentTerm, rf.state)
	rf.mu.Unlock()
	return term, isleader
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

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []ApplyMsg
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	DPrintf("[%d, %d] recieve heartbeat from [%d, %d]\n", rf.currentTerm, rf.me,
		args.Term, args.LeaderId)
	if args.Term >= rf.currentTerm { //recieves heartbeat from leader
		if len(rf.logTerms) < args.PrevLogIndex ||
			rf.logTerms[args.PrevLogIndex-1] != args.PrevLogTerm {
			//report log inconsistency
			reply.Success = false
		} else {
			//sync logs from leader
			for i := 0; i < len(args.Entries); i++ {
				if i+args.PrevLogIndex < len(rf.logs) {
					rf.logs[i+args.PrevLogIndex] = args.Entries[i]
				} else {
					rf.logs = append(rf.logs, args.Entries[i])
				}
			}
		}

		rf.mu.Lock()
		rf.state = Follower
		rf.currentTerm = args.Term
		rf.votedFor = args.LeaderId
		reply.Term = args.Term
		rf.mu.Unlock()
		rf.resetCh <- 1
	} else {
		reply.Term = rf.currentTerm
		reply.Success = false
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int
	CandidateId int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	DPrintf("[%d, %d] recieve vote request from [%d, %d]\n", rf.currentTerm,
		rf.me, args.Term, args.CandidateId)
	if args.Term > rf.currentTerm ||
		(args.Term == rf.currentTerm &&
			(rf.votedFor == -1 || rf.votedFor == args.CandidateId)) {
		DPrintf("[%d, %d] vote [%d, %d]\n", rf.currentTerm, rf.me, args.Term,
			args.CandidateId)
		rf.mu.Lock()
		rf.votedFor = args.CandidateId
		rf.currentTerm = args.Term
		rf.mu.Unlock()
		reply.Term = args.Term
		reply.VoteGranted = true
		go func() {
			rf.resetCh <- 1
		}()

	} else if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	}
	/*else if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		rf.mu.Lock()
		rf.votedFor = args.CandidateId
		rf.currentTerm = args.Term
		rf.mu.Unlock()
		reply.Term = args.Term
		reply.VoteGranted = true
		rf.resetCh <- 1
		//DPrintf("[%d, %d] vote for [%d, %d]\n", rf.currentTerm, rf.me, args.Term,
		//args.CandidateId)
	}
	*/
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
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
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
func (rf *Raft) lmode(term int) {
	heartbeat := func(server int) {
		var reply AppendEntriesReply
		rf.mu.Lock()
		if rf.state != Leader || rf.currentTerm != term {
			return
		}
		args := new(AppendEntriesArgs)
		args.Term = rf.currentTerm
		args.LeaderId = rf.me
		rf.mu.Unlock()

		DPrintf("[%d,%d] send heartbeat to %d\n", rf.currentTerm, rf.me,
			server)
		ok := rf.sendAppendEntries(server, args, &reply)
		if ok == true {
			if reply.Term > rf.currentTerm {
				DPrintf("[%d, %d] discover server with higher term (%d, %d)\n",
					rf.currentTerm, rf.me, server, reply.Term)
				DPrintf("[%d, %d] leader -> follower\n", rf.currentTerm, rf.me)
				rf.mu.Lock()
				rf.currentTerm = reply.Term
				rf.state = Follower
				rf.votedFor = -1
				rf.mu.Unlock()
				rf.resetCh <- 1
				return
			}
		} else {
			DPrintf("[%d, %d] send heartbeat to %d failed\n", rf.currentTerm, rf.me,
				server)
		}
	}

	for {
		if rf.state != Leader {
			return
		}
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				go heartbeat(i)
			}
		}
		time.Sleep(time.Duration(rf.heartbeatTimeout) * time.Millisecond)
	}
}

//
func (rf *Raft) cmode(term int) {
	//call each
	var mux sync.Mutex
	votes := 1
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(server int) {
			for {
				var args *RequestVoteArgs
				var reply RequestVoteReply

				if rf.state != Candidate || rf.currentTerm > term {
					return
				}
				//DPrintf("[%d, %d]: send vote request to %d\n", rf.currentTerm,
				//rf.me, server)
				//pack vote request args
				args = new(RequestVoteArgs)
				args.Term = term
				args.CandidateId = rf.me

				ok := rf.sendRequestVote(server, args, &reply)
				if ok {
					if reply.Term > rf.currentTerm {
						rf.mu.Lock()
						rf.currentTerm = reply.Term
						rf.state = Follower
						rf.votedFor = -1
						rf.resetCh <- 1
						rf.mu.Unlock()
					} else if reply.VoteGranted == true {
						mux.Lock()
						votes += 1
						mux.Unlock()
						DPrintf("[%d, %d] vote for [%d, %d]\n", reply.Term, server,
							rf.currentTerm, rf.me)
						if votes > len(rf.peers)/2 && rf.state != Leader {
							//switch to leader mode
							DPrintf("[%d, %d] candidate -> leader\n", rf.currentTerm, rf.me)
							rf.state = Leader
							go rf.lmode(rf.currentTerm)
						}
					}
					return
				}
				//try to reconnect the remote node after some fixed time
				time.Sleep(time.Duration(rf.heartbeatTimeout) * time.Millisecond)
			}
			//DPrintf("[%d,%d] exit from cmode\n", rf.currentTerm, rf.me)
		}(i)
	}
}
func (rf *Raft) fmode() {
	for {
		select {
		case <-rf.electionTimer.C:
			if rf.state != Leader {
				//start leader election
				DPrintf("[%d, %d]: follower -> candidate\n", rf.currentTerm, rf.me)
				rf.mu.Lock()
				rf.currentTerm += 1
				rf.state = Candidate
				rf.votedFor = rf.me
				rf.mu.Unlock()
				rf.electionTimer.Reset(time.Duration(rf.electionTimeout) * time.Millisecond)
				go rf.cmode(rf.currentTerm)
			}
		case <-rf.resetCh:
			//reset the election timer
			// if discover server with higher term
			// or leader
			if !rf.electionTimer.Stop() {
				<-rf.electionTimer.C
			}
			rf.electionTimer.Reset(time.Duration(rf.electionTimeout) * time.Millisecond)
		}
	}
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.state = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.heartbeatTimeout = 100 + rand.Intn(50)
	rf.electionTimeout = 300 + rand.Intn(300)
	rf.electionTimer = time.NewTimer(time.Duration(rf.electionTimeout) * time.Millisecond)
	rf.resetCh = make(chan int)
	//
	DPrintf("==== %d =====\n", me)
	DPrintf("heartbeat timeOut: %d\n", rf.heartbeatTimeout)
	DPrintf("election timeout: %d\n", rf.electionTimeout)
	go rf.fmode()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
