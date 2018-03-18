package consensus

import (
	"time"
	"log"
	"net"
	"net/http"
	"net/rpc"
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

type RaftServer struct {
	peers []*Peer
	me    int

	state       State
	votedFor    int
	leaderId    int
	currentTerm int

	status int
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

func (server *RaftServer) Timeout() {

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
	rpc.Register(server)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", ":"+server.peers[server.me].port)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

func (server *RaftServer) Run() {
	if server.status == 0 {
		//server fall asleep unless event happens
		/*
		 * each server has three goroutines:
		 *     main: will execute event handler
		 *     rpc: monitor network
		 *     timer: most time will be sleeping
		 *
		 */
		 for true {
			 switch server.state {
				 case 
			 }
		 }
	}
}

/*
type Raft struct {
}

func Make(peers []*Peer, me int) *Raft {

}

func (raft *Raft) Start(command interface{}) {

}

func (raft *Raft) GetState() (term int, isLeader bool) {

}
*/
