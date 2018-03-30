package consensus

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"sync"
	"time"
)

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

type TxType int

const ()

type Transaction struct {
	txtype TxType
}
type Log struct {
	tx   Transaction
	term int
}
type RaftServer struct {
	mux   sync.Mutex
	peers []*Peer
	me    int

	state       State
	votedFor    int
	leaderId    int
	currentTerm int

	exitCh  chan int 
}

type AppendEntryArgs struct {
	term     int
	leaderId int
	entries  []Log
}

type AppendEntryReply struct {
	term int
}

type RequestVoteArgs struct {
	term        int // term in which this election happens
	candidateId int // who is asking for vote
}

type RequestVoteReply struct {
	term        int
	voteGranted bool
}


//handler for RequestVote RPC
func (server *RaftServer) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	if args.term > server.currentTerm {
		server.state = FOLLOWER
		server.currentTerm = args.term
		server.votedFor = args.candidateId
		reply.term = server.currentTerm
		reply.voteGranted = true
	} else {
		switch server.state {
		case FOLLOWER:
			//receives a vote request
			if args.term < server.currentTerm {
				reply.voteGranted = false
			} else {
				if server.votedFor == 0 || server.votedFor == args.candidateId {
					server.votedFor = args.candidateId
					reply.voteGranted = true
				} else {
					reply.voteGranted = false
				}
				//server.currentTerm = args.term
			}
			reply.term = server.currentTerm
			break

		case CANDIDATE:
		case LEADER:
			//candidate's votedFor must be itself
			reply.term = server.currentTerm
			reply.voteGranted = false
			//assert(server.votedFor == server.me)
			if server.votedFor != server.me {
			} else {
			}
			break

		default:
			//panic()
			break
		}
	}
	return nil
}

//handler for AppendEntries RPC
func (server *RaftServer) AppendEntries(args *AppendEntryArgs, reply *AppendEntryReply) error {
	if args.term < server.currentTerm {
		reply.term = server.currentTerm
	} else {
		server.currentTerm = args.term
		server.leaderId = args.leaderId
		server.state = FOLLOWER
		reply.term = server.currentTerm
	}
	return nil
}

func NewRaftServer(peers []*Peer, me int) *RaftServer {
	server := new(RaftServer)
	server.peers = peers
	server.me = me
	server.state = FOLLOWER
	server.votedFor = -1
	server.currentTerm = 0
	server.leaderId = -1
	return server
}
func (server *RaftServer) Init() {
	n := len(server.peers)
	server.timeout = make(chan int)
	server.leader = make(chan int)
	server.newterm = make(chan int, n)

	rpc.Register(server)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", ":"+server.peers[server.me].port)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}
func (server *RaftServer) Run() {
	for {
		select {
		case <-server.timeout:
			server.mux.Lock()
			switch server.state {
			case FOLLOWER:
				//start an election
				server.state = CANDIDATE
				go server.candidate()
				break
			case CANDIDATE:
				//start a new term
				break

			case LEADER:
				//ignore
				break
			}
			server.mux.Unlock()
		case <-server.leader:

			go server.follower()
			//switch to follower
		case <-server.newterm:
			//switch to follower
		}
	}
}
