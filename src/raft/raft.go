package raft

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/http"
	"net/rpc"
	"sync"
	"time"

	"github.com/fatih/color"
)

const Debug = 1

var c *color.Color

func init() {
	c = color.New(color.FgBlue)
	//log.Init("./", log.Stdout)
}
func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		//log.Debugf(format, a...)
		log.Printf(format, a...)
	} else {
		fmt.Printf(format, a...)
	}
	return
}

type Peer struct {
	ip   string
	port string
}

func NewPeer(ip string, port string) *Peer {
	peer := new(Peer)
	peer.ip = ip
	peer.port = port
	return peer
}

type State int

const (
	FOLLOWER  = iota
	CANDIDATE = iota
	LEADER    = iota
)

type RaftServer struct {
	mux   sync.Mutex
	peers []*Peer
	me    int

	state       State
	votedFor    int
	leaderId    int
	currentTerm int

	resetTimer    chan int
	exitCh        chan int
	electionTimer *time.Timer

	electionTimeout  time.Duration
	heartbeatTimeout time.Duration
}
type Log struct {
}
type AppendEntryArgs struct {
	Term     int
	LeaderId int
	Entries  []Log
}

type AppendEntryReply struct {
	Term int
}

type RequestVoteArgs struct {
	Term        int // term in which this election happens
	CandidateId int // who is asking for vote
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

//handler for RequestVote RPC
func (rf *RaftServer) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	rf.mux.Lock()
	if args.Term > rf.currentTerm {
		DPrintf("(%d, %d, %d) <- (%d, %d)\n", rf.me, rf.votedFor, rf.currentTerm,
			args.CandidateId, args.Term)
		rf.state = FOLLOWER
		rf.currentTerm = args.Term
		rf.votedFor = args.CandidateId
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		rf.mux.Unlock()
		fmt.Printf("HandleVote:  %d vote for %d in term %d\n", rf.me,
			args.CandidateId, args.Term)
		rf.resetTimer <- 0
	} else {
		switch rf.state {
		case FOLLOWER:
			//receives a vote request
			if args.Term < rf.currentTerm {
				reply.VoteGranted = false
				reply.Term = rf.currentTerm
			} else {
				if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
					rf.votedFor = args.CandidateId
					reply.VoteGranted = true
					reply.Term = rf.currentTerm
					fmt.Printf("HandleVote:  %d vote for %d in term %d\n", rf.me,
						args.CandidateId, args.Term)
				} else {
					reply.VoteGranted = false
					reply.Term = rf.currentTerm
				}
			}
			break
		case CANDIDATE:
		case LEADER:
			//candidate's votedFor must be itself
			reply.Term = rf.currentTerm
			reply.VoteGranted = false
			//assert(server.votedFor == server.me)
			break
		}
		rf.mux.Unlock()
	}
	return nil
}

//handler for AppendEntries RPC
func (rf *RaftServer) AppendEntries(args *AppendEntryArgs, reply *AppendEntryReply) error {
	rf.mux.Lock()
	defer rf.mux.Unlock()
	//DPrintf("node %d receives heartbeat from %d\n", rf.me, args.LeaderId)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
	} else {
		rf.currentTerm = args.Term
		rf.leaderId = args.LeaderId
		rf.state = FOLLOWER
		reply.Term = rf.currentTerm
		rf.resetTimer <- 0
	}
	return nil
}

func NewRaftServer(peers []*Peer, me int) *RaftServer {
	rf := new(RaftServer)
	rf.peers = peers
	rf.me = me
	rf.state = FOLLOWER
	rf.votedFor = -1
	rf.currentTerm = 0
	rf.leaderId = -1
	rf.electionTimeout = time.Duration(300+rand.Intn(300)) * time.Millisecond
	rf.heartbeatTimeout = time.Duration(150) * time.Millisecond
	rf.exitCh = make(chan int, 1)
	rf.resetTimer = make(chan int, 10)

	//debug info
	DPrintf("====== node #%d ===========\n", rf.me)
	DPrintf("election timeout: %f\n", rf.electionTimeout.Seconds())
	DPrintf("hearbeat timeout: %f\n", rf.heartbeatTimeout.Seconds())
	return rf
}

func (rf *RaftServer) Init() {
	//register rpc listener
	serv := rpc.NewServer()
	serv.Register(rf)

	oldMux := http.DefaultServeMux
	mux := http.NewServeMux()
	http.DefaultServeMux = mux

	serv.HandleHTTP(rpc.DefaultRPCPath, rpc.DefaultDebugPath)
	http.DefaultServeMux = oldMux

	l, e := net.Listen("tcp", ":"+rf.peers[rf.me].port)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, mux)

	//start timer and begin as a follower
	rf.electionTimer = time.NewTimer(rf.electionTimeout)
	go rf.fMode()
}

func (rf *RaftServer) lMode() {
	hbReq := func(remote string) {
		var args *AppendEntryArgs
		rf.mux.Lock()
		if rf.state == LEADER {
			args = &AppendEntryArgs{Term: rf.currentTerm, LeaderId: rf.me, Entries: nil}
		} else {
			args = nil
		}
		rf.mux.Unlock()
		if args == nil {
			return
		}
		var reply AppendEntryReply
		cli, err := rpc.DialHTTP("tcp", remote)
		if err != nil {
			//
			fmt.Println(err)
			return
		}
		err = cli.Call("RaftServer.AppendEntries", args, &reply)
		if err != nil {
			//
			fmt.Println(err)
			return
		}
		rf.mux.Lock()
		defer rf.mux.Unlock()
		if reply.Term > rf.currentTerm {
			//switch to follower
			rf.currentTerm = reply.Term
			rf.state = FOLLOWER
			rf.resetTimer <- 0
		}
	}
	for {
		select {
		case <-rf.exitCh:
			return
		default:
			for i := 0; i < len(rf.peers); i++ {
				if i != rf.me {
					go hbReq(rf.peers[i].ip + ":" + rf.peers[i].port)
				}
			}
			time.Sleep(rf.heartbeatTimeout)
		}
	}

}
func (rf *RaftServer) cMode() {
	voteMux := sync.Mutex{}
	votes := 1
	voteReq := func(remote string) {
		var reply *RequestVoteReply
		client, err := rpc.DialHTTP("tcp", remote)
		if err != nil {
			//log.Println(err)
			return
		}
		rf.mux.Lock()
		args := &RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me}
		rf.mux.Unlock()
		reply = &RequestVoteReply{}
		err = client.Call("RaftServer.RequestVote", args, reply)
		if err != nil {
			DPrintf("#%d cannot connect %s\n", rf.me, remote)
			return
		}
		if rf.state == CANDIDATE {
			rf.mux.Lock()
			defer rf.mux.Unlock()
			if reply.Term > rf.currentTerm {
				//change to follower
				rf.currentTerm = reply.Term
				rf.state = FOLLOWER
				rf.resetTimer <- 0
				return
			}
			if reply.VoteGranted {
				voteMux.Lock()
				votes++
				voteMux.Unlock()
				if votes > len(rf.peers)/2 {
					//switch to leader
					fmt.Printf(">>>> node #%d is leader in term %d\n", rf.me, rf.currentTerm)
					//c.Printf("node #%d is leader in term %d\n", rf.me, rf.currentTerm)
					//c.Printf("node #%d is leader in term %d\n", rf.me, rf.currentTerm)
					//fmt.Printf("\033[%sm node #%d is leader in term %d \033[m\n", log.Blue, rf.me, rf.currentTerm)
					rf.state = LEADER
					rf.resetTimer <- 0
					go rf.lMode()
					return
				}
			}

		}
	}
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go voteReq(rf.peers[i].ip + ":" + rf.peers[i].port)
		}
	}
}
func (rf *RaftServer) fMode() {
	for {
		select {
		case <-rf.exitCh:
			DPrintf("node #%d will exit\n", rf.me)
			return
		case <-rf.resetTimer:
			if !rf.electionTimer.Stop() {
				<-rf.electionTimer.C
			}
			//DPrintf("node %d resets election timer", rf.me)
			rf.electionTimer.Reset(rf.electionTimeout)
		case <-rf.electionTimer.C:
			//ignore election timer for leader
			if rf.state != LEADER {

				DPrintf("node #%d election times out\n", rf.me)
				rf.mux.Lock()
				rf.currentTerm++
				rf.state = CANDIDATE
				rf.votedFor = rf.me
				rf.mux.Unlock()
				DPrintf("node %d switch from F to C in term %d\n", rf.me, rf.currentTerm)
				rf.electionTimer.Reset(rf.electionTimeout)
				go rf.cMode()

			}
		}
	}
}

func (rf *RaftServer) Kill() {
	close(rf.exitCh)
}

func (rf *RaftServer) GetState() State {
	return rf.state
}
